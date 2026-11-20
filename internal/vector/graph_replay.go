package vector

import (
	"context"
	"errors"
	"sync"

	"example.com/vector-storage/internal/shardlog"
	"github.com/klauspost/compress/s2"
)

const graphReplayEntries = 16
const graphReplayBytes int64 = 8 << 20
const graphReplayConcurrency = 4

// One snapshot and one batched metadata read per replay window. Validate the
// entire hash chain before downloading anything. An oversized object is read
// alone; subsequent objects cannot multiply the existing single-object limit.
func (j graphJournal) replayEntries(ctx context.Context, after GraphPosition, target GraphPosition) ([]GraphEntry, error) {
	tx, err := j.c.root.begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if after.Sequence >= target.Sequence {
		return nil, errors.New("invalid graph replay range")
	}
	values, err := j.log(nil, tx).ReadCommitted(ctx, shardlog.Window{After: after.Sequence, Through: target.Sequence, Limit: graphReplayEntries})
	if err != nil {
		return nil, err
	}
	entries := make([]GraphEntry, 0, len(values))
	var bytes int64
	previous := after
	for n, raw := range values {
		var entry GraphEntry
		body, err := graphEntryHeader(raw, &entry)
		if err != nil {
			return nil, err
		}
		if entry.Position.Sequence != after.Sequence+uint64(n)+1 || entry.Position.Hash != entryHash(entry) || entry.Previous != previous {
			return nil, errors.New("graph journal discontinuity or hash mismatch")
		}
		size := entry.Object.Size
		if entry.Payload != nil {
			size = int64(entry.Payload.Size)
		}
		if size < 0 || size > 512<<20 {
			return nil, errors.New("graph object size outside limit")
		}
		if len(entries) > 0 && size > graphReplayBytes-bytes {
			break
		}
		if entry.Position.Sequence == target.Sequence && entry.Position != target {
			return nil, errors.New("graph durable hash mismatch")
		}
		if body != nil {
			entry.PayloadData, err = s2.Decode(nil, body)
			if err != nil {
				return nil, err
			}
		}
		entries = append(entries, entry)
		bytes += size
		previous = entry.Position
	}
	return entries, nil
}

type loadedGraphEntry struct {
	delta *GraphDelta
	state *IndexSnapshot
	err   error
}

// Download/decode independent immutable objects concurrently, overlapping IO
// with ordered apply. Never apply two patches to the same HNSW concurrently.
// Cancel and join every loader before releasing Service.syncMu, including on
// corruption, checkpoint fallback, retirement or timeout.
func (g *graphReplica) replayBatch(ctx context.Context, s *Service, entries []GraphEntry) (err error) {
	defer g.track("graph_replay_batch")(&err)
	if g.input != nil {
		defer func() {
			g.input.cluster.observations.pipeline.observeBatch("graph_replay", string(g.journal.id), len(entries), err)
		}()
	}
	// The healthy steady-state path commonly has just one entry. Avoid a
	// goroutine/channel round trip when there is no independent IO to overlap.
	if len(entries) == 1 {
		entry := entries[0]
		delta, reset := g.delta, g.reset
		if entry.Position != g.prepared {
			delta, reset, err = g.loadEntry(ctx, entry)
			if err != nil {
				return err
			}
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if entry.Previous != g.position {
			return errors.New("graph replay apply order mismatch")
		}
		if err = g.apply(s, delta, reset, entry.Position); err != nil {
			return err
		}
		g.delta, g.reset, g.prepared = nil, nil, GraphPosition{}
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	results := make([]chan loadedGraphEntry, len(entries))
	for n := range results {
		results[n] = make(chan loadedGraphEntry, 1)
	}
	width := min(graphReplayConcurrency, len(entries))
	// Snapshot prepared fields: ordered apply clears them while loaders run.
	prepared, delta, reset := g.prepared, g.delta, g.reset
	for worker := 0; worker < width; worker++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for n := start; n < len(entries); n += width {
				if ctx.Err() != nil {
					return
				}
				var result loadedGraphEntry
				if entries[n].Position == prepared {
					result.delta, result.state = delta, reset
				} else {
					result.delta, result.state, result.err = g.loadEntry(ctx, entries[n])
				}
				results[n] <- result
			}
		}(worker)
	}
	for n, entry := range entries {
		var loaded loadedGraphEntry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case loaded = <-results[n]:
		}
		if loaded.err != nil {
			return loaded.err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if entry.Previous != g.position {
			return errors.New("graph replay apply order mismatch")
		}
		if err = g.apply(s, loaded.delta, loaded.state, entry.Position); err != nil {
			return err
		}
		g.delta, g.reset, g.prepared = nil, nil, GraphPosition{}
	}
	return nil
}
