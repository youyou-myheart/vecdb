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
	"google.golang.org/grpc"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type second struct{ Started, Ack, Failed int }
type report struct {
	Target, Started, Ack, Unsent int
	BySecond                     []second
	Errors                       map[string]int
	ErrorExample                 map[string]string
	AckShard                     []int
	RPCDestinations              map[string]int
	DroppedInflight              int
	AckLatenciesUS               []int
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

// Without an explicit routing file, use the original 64-dimensional one-box
// distribution. With a file, spread clients across the collection's centers.
func loadCenters(path string) ([][]float64, float64, error) {
	if path == "" {
		centers := make([][]float64, 8)
		for i := range centers {
			centers[i] = make([]float64, 64)
			centers[i][0] = float64(i * 20)
		}
		return centers, .1, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	var centers [][]float64
	if err = json.Unmarshal(raw, &centers); err != nil {
		return nil, 0, err
	}
	if len(centers) == 0 || len(centers[0]) == 0 {
		return nil, 0, errors.New("empty routing centers")
	}
	dimension := len(centers[0])
	jitter := .1
	for i, center := range centers {
		if len(center) != dimension {
			return nil, 0, errors.New("inconsistent center dimensions")
		}
		for _, previous := range centers[:i] {
			distance := 0.0
			for k, x := range center {
				delta := x - previous[k]
				distance += delta * delta
			}
			if distance == 0 {
				return nil, 0, errors.New("duplicate routing center")
			}
			jitter = math.Min(jitter, math.Sqrt(distance)/(5*math.Sqrt(float64(dimension))))
		}
	}
	return centers, jitter, nil
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
	centersFile := flag.String("centers", "", "JSON array of collection centers; enables per-ACK latency and RPC peer capture")
	keyPrefix := flag.String("key-prefix", "", "optional unique key namespace")
	shardCount := flag.Int("shards", 16, "number of data partitions")
	flag.Parse()
	if *rate <= 0 || *seconds <= 0 || *limit <= 0 || *out == "" || *barrier == "" || *shardCount < 1 || *shardCount > 64 || *clientID < 0 {
		return errors.New("invalid load options")
	}
	centers, jitter, err := loadCenters(*centersFile)
	if err != nil {
		return err
	}
	prefix := "w-"
	if *keyPrefix != "" {
		prefix += *keyPrefix + "-"
	}
	detailed := *centersFile != ""
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
	r := report{Target: *rate * *seconds, Errors: map[string]int{}, ErrorExample: map[string]string{}, BySecond: make([]second, *seconds+5), LatencyHistogramUS: map[int]int{}, AckShard: make([]int, *shardCount)}
	if detailed {
		r.RPCDestinations = make(map[string]int)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	sem := make(chan struct{}, *limit)
	bounds := []int{100, 250, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000, 2000000, 3000000, 10000000}
	dimension := len(centers[0])
	centerOffset := 0
	if detailed {
		centerOffset = *clientID
	}
	templates := make([][]float64, 512)
	for i := range templates {
		center := centers[(i+centerOffset)%len(centers)]
		templates[i] = make([]float64, dimension)
		for j := range center {
			templates[i][j] = center[j] + jitter*math.Sin(float64((i+1)*(j+1)))
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
				r.DroppedInflight++
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				key := fmt.Sprintf("%s%02d-%08d", prefix, *clientID, id)
				h := sha256.Sum256([]byte(key))
				hash := binary.BigEndian.Uint64(h[:8])
				shard := int(hash % uint64(len(r.AckShard)))
				writer := c
				if peer != nil {
					// The next hash bit mixes each shard over both gateways.
					gateway := int((hash / uint64(len(r.AckShard))) % 2)
					if *byPartition {
						gateway = shard % 2
					}
					if gateway != *clientID%2 {
						writer = peer
					}
				}
				v := append([]float64(nil), templates[id%len(templates)]...)
				if detailed {
					v[dimension-1] += jitter * .1 * float64((id*123457+*clientID*15485863)%1000003) / 1000003
				} else {
					v[dimension-1] += float64(id+1)*0.0000001 + float64(*clientID)*0.000001
				}
				s := time.Now()
				if !s.Before(end) || ctx.Err() != nil {
					return
				}
				mu.Lock()
				r.Started++
				r.BySecond[int(s.Sub(start)/time.Second)].Started++
				r.PeakInflight = max(r.PeakInflight, len(sem))
				mu.Unlock()
				var remote grpcpeer.Peer
				var res *pb.MutationResponse
				var err error
				if detailed {
					zero := uint64(0)
					req := &pb.PutRequest{Id: key, Vector: v, RequestId: key, ExpectedRevision: &zero}
					res, err = writer.Put(ctx, req, grpc.Peer(&remote))
				} else {
					res, err = writer.Insert(ctx, key, v, nil, key)
				}
				at := time.Now()
				mu.Lock()
				defer mu.Unlock()
				if detailed {
					destination := "unknown"
					if remote.Addr != nil {
						destination = remote.Addr.String()
					}
					r.RPCDestinations[destination]++
				}
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
				if detailed {
					r.AckLatenciesUS = append(r.AckLatenciesUS, micros)
				}
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
