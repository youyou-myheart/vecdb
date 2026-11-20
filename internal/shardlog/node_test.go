package shardlog

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"example.com/vector-storage/internal/lease"
)

type memoryLog struct{ entries [][]byte }

func (m *memoryLog) ReadCommitted(ctx context.Context, w Window) ([][]byte, error) {
	if w.After > uint64(len(m.entries)) {
		return nil, ErrGap
	}
	end := min(uint64(len(m.entries)), w.After+uint64(w.Limit))
	return m.entries[w.After:end], ctx.Err()
}
func elect(term uint64) func(context.Context) (lease.Grant, time.Time, error) {
	return func(context.Context) (lease.Grant, time.Time, error) {
		return lease.Grant{Epoch: term, Expires: time.Now().Add(time.Minute).UnixMilli(), Remaining: time.Minute}, time.Now(), nil
	}
}
func TestGroupElectionAndIndependentReaders(t *testing.T) {
	ctx := context.Background()
	log := &memoryLog{}
	appendEntry := func(_ context.Context, p []byte) error {
		log.entries = append(log.entries, p)
		return nil
	}
	g := Open(nil, appendEntry, log)
	if err := g.Propose(ctx, 1, []byte("before")); !errors.Is(err, ErrFenced) {
		t.Fatal(err)
	}
	if err := g.Campaign(ctx, elect(1)); err != nil {
		t.Fatal(err)
	}
	if err := g.Propose(ctx, 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	g.StepDown()
	if err := g.Campaign(ctx, elect(2)); !errors.Is(err, ErrFenced) {
		t.Fatal("retired incarnation revived", err)
	}
	successor := Open(nil, appendEntry, log)
	if err := successor.Campaign(ctx, elect(2)); err != nil {
		t.Fatal(err)
	}
	if err := successor.Propose(ctx, 1, []byte("stale")); !errors.Is(err, ErrFenced) {
		t.Fatal(err)
	}
	if err := successor.Propose(ctx, 2, []byte("second")); err != nil {
		t.Fatal(err)
	}
	var follower Reader[[][]byte] = g
	for i, want := range []string{"first", "second"} {
		page, err := follower.ReadCommitted(ctx, Window{After: uint64(i), Limit: 1})
		if err != nil || len(page) != 1 || string(page[0]) != want {
			t.Fatal(page, err)
		}
	}
}
func TestStepDownDuringProposeOrCampaign(t *testing.T) {
	ctx := context.Background()
	var n Node
	if err := n.Campaign(ctx, elect(7)); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	g := Open(&n, func(context.Context, int) error { close(entered); <-release; return nil }, &memoryLog{})
	done := make(chan error, 1)
	go func() { done <- g.Propose(ctx, 7, 1) }()
	<-entered
	n.StepDown()
	close(release)
	if err := <-done; !errors.Is(err, ErrFenced) {
		t.Fatal("late storage success acknowledged", err)
	}
	var delayed Node
	err := delayed.Campaign(ctx, func(ctx context.Context) (lease.Grant, time.Time, error) { delayed.StepDown(); return elect(8)(ctx) })
	if !errors.Is(err, ErrFenced) {
		t.Fatal("late election confirmation revived node", err)
	}
}
func TestExpiryDuringProposePreventsAcknowledgement(t *testing.T) {
	ctx := context.Background()
	var writes int
	var g Group[int, [][]byte]
	g = Open(nil, func(context.Context, int) error {
		writes++
		// Model a delayed storage reply. No caller consults the lease clock.
		limit := time.After(3 * time.Second)
		for g.handle.ActiveTerm(1) {
			select {
			case <-limit:
				t.Fatal("background expiry did not withdraw the term")
			case <-time.After(time.Millisecond):
			}
		}
		return nil
	}, &memoryLog{})
	defer g.StepDown()
	if err := g.Campaign(ctx, func(context.Context) (lease.Grant, time.Time, error) {
		return lease.Grant{Epoch: 1, Expires: 1, Remaining: 200 * time.Millisecond}, time.Now(), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.Propose(ctx, 1, 1); !errors.Is(err, ErrFenced) || writes != 1 {
		t.Fatal("expired in-flight proposal acknowledged", err, writes)
	}
	if err := g.Propose(ctx, 1, 2); !errors.Is(err, ErrFenced) || writes != 1 {
		t.Fatal("expired term reached storage", err, writes)
	}
}

func TestTailContiguityAndOverflow(t *testing.T) {
	s := Stream("test/")
	ctx := context.Background()
	read := func(_ context.Context, keys [][]byte) ([][]byte, error) {
		if len(keys) != 3 || string(keys[2]) != string(s.Key(math.MaxUint64)) {
			t.Fatal(keys)
		}
		return [][]byte{{1}, nil, {3}}, nil
	}
	after := uint64(math.MaxUint64 - 3)
	page, err := s.Tail(ctx, read, after, math.MaxUint64, 4, false)
	if err != nil || len(page) != 1 {
		t.Fatal(page, err)
	}
	if _, err := s.Tail(ctx, read, after, math.MaxUint64, 4, true); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	if page, err := s.Tail(ctx, nil, math.MaxUint64, math.MaxUint64, 1, true); err != nil || len(page) != 0 {
		t.Fatal(page, err)
	}
}
