// writebench issues uniquely identified, single-vector Put RPCs on a fixed
// arrival schedule. Full client concurrency consumes a slot as unsent, never
// queues deferred work beyond the measurement window.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	pb "example.com/vector-storage/api/vectorstore/v1"
	sdk "example.com/vector-storage/sdk/go"
	"google.golang.org/grpc/status"
)

type second struct{ Started, Ack, Failed int }
type report struct {
	Target, Started, Ack, Unsent int
	BySecond                     []second
	Errors                       map[string]int
	ErrorExample                 map[string]string
	AckShard                     [16]int
	LastAckNS                    int64
	PeakInflight                 int
	LatencyHistogramUS           map[int]int
	ElapsedSeconds               float64
	Interrupted                  bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	target := flag.String("endpoint", "", "public gRPC gateway")
	peerEndpoint := flag.String("peer", "", "second gateway used for mixed or partition routing")
	byPartition := flag.Bool("by-partition", false, "when using two gateways, route by DataPartition parity")
	barrier := flag.String("barrier", "", "file containing common start time in Unix nanoseconds")
	out := flag.String("output", "", "output report")
	clientID := flag.Int("client", 0, "unique client number")
	rate := flag.Int("rate", 5000, "planned starts per second")
	seconds := flag.Int("seconds", 60, "measurement seconds")
	limit := flag.Int("inflight", 128, "maximum parallel RPCs")
	flag.Parse()
	if *rate <= 0 || *seconds <= 0 || *limit <= 0 || *out == "" || *barrier == "" {
		return errors.New("invalid load options")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c, err := sdk.Dial(ctx, *target, os.Getenv("API_TOKEN"), sdk.Options{Timeout: 3 * time.Second})
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err = c.Health(ctx, &pb.Empty{}); err != nil {
		return err
	}
	var peer *sdk.Client
	if *peerEndpoint != "" {
		peer, err = sdk.Dial(ctx, *peerEndpoint, os.Getenv("API_TOKEN"), sdk.Options{Timeout: 3 * time.Second})
		if err != nil {
			return err
		}
		defer peer.Close()
		if _, err = peer.Health(ctx, &pb.Empty{}); err != nil {
			return err
		}
	}
	if err = os.WriteFile(*out+".ready", []byte("ready"), 0600); err != nil {
		return err
	}
	startNS := int64(0)
	for startNS == 0 && ctx.Err() == nil {
		b, err := os.ReadFile(*barrier)
		if err == nil {
			startNS, err = strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	now := time.Now()
	start := now.Add(time.Duration(startNS - now.UnixNano()))
	end := start.Add(time.Duration(*seconds) * time.Second)
	if time.Until(start) < 0 {
		return errors.New("missed start barrier")
	}
	time.Sleep(time.Until(start))
	r := report{Target: *rate * *seconds, Errors: map[string]int{}, ErrorExample: map[string]string{}, BySecond: make([]second, *seconds+5), LatencyHistogramUS: map[int]int{}}
	var wg sync.WaitGroup
	var mu sync.Mutex
	sem := make(chan struct{}, *limit)
	bounds := []int{100, 250, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000, 2000000, 3000000, 10000000}
	var templates [512][64]float64
	for i := range templates {
		for j := range templates[i] {
			templates[i][j] = 0.1 * math.Sin(float64((i+1)*(j+1)))
			if j == 0 {
				templates[i][j] += float64((i % 8) * 20)
			}
		}
	}
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for next := 0; next < r.Target && ctx.Err() == nil; {
		tickTime := time.Now()
		if !tickTime.Before(end) {
			break
		}
		due := min(r.Target, int(tickTime.Sub(start).Nanoseconds()*int64(*rate)/1e9)+1)
		for next < due {
			id := next
			next++
			select {
			case sem <- struct{}{}:
			default:
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				key := fmt.Sprintf("w-%02d-%08d", *clientID, id)
				h := sha256.Sum256([]byte(key))
				hash := binary.BigEndian.Uint64(h[:8])
				shard := int(hash % uint64(len(r.AckShard)))
				writer := c
				if peer != nil {
					// The next hash bit mixes every one of the 16 shards over both gateways.
					gateway := int((hash / uint64(len(r.AckShard))) % 2)
					if *byPartition {
						gateway = shard % 2
					}
					if gateway != *clientID%2 {
						writer = peer
					}
				}
				v := templates[id%len(templates)]
				v[63] += float64(id+1)*0.0000001 + float64(*clientID)*0.000001
				s := time.Now()
				if !s.Before(end) || ctx.Err() != nil {
					return
				}
				mu.Lock()
				r.Started++
				r.BySecond[int(s.Sub(start)/time.Second)].Started++
				r.PeakInflight = max(r.PeakInflight, len(sem))
				mu.Unlock()
				res, err := writer.Insert(ctx, key, v[:], nil, key)
				at := time.Now()
				mu.Lock()
				defer mu.Unlock()
				bucket := &r.BySecond[min(len(r.BySecond)-1, int(at.Sub(start)/time.Second))]
				if err != nil || !res.GetDurable() || res.GetToken().GetId() != key || res.GetToken().GetRevision() != 1 {
					code := "invalid_ack"
					if err != nil {
						code = status.Code(err).String()
						if _, ok := r.ErrorExample[code]; !ok {
							r.ErrorExample[code] = err.Error()
						}
					}
					r.Errors[code]++
					bucket.Failed++
					return
				}
				bucket.Ack++
				r.Ack++
				r.LastAckNS = max(r.LastAckNS, at.UnixNano())
				r.AckShard[shard]++
				micros := int((at.Sub(s) + time.Microsecond - 1) / time.Microsecond)
				bound := micros
				if b := sort.SearchInts(bounds, micros); b < len(bounds) {
					bound = bounds[b]
				}
				r.LatencyHistogramUS[bound]++
			}()
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
		}
	}
	r.ElapsedSeconds = time.Since(start).Seconds()
	r.Interrupted = ctx.Err() != nil && time.Now().Before(end)
	wg.Wait()
	r.Unsent = r.Target - r.Started
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(*out, data, 0600)
}
