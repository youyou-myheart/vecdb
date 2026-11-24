package vector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"example.com/vector-storage/internal/shardlog"
	tikverr "github.com/tikv/client-go/v2/error"
)

type graphRawClient interface {
	rawDataClient
	last(ctx context.Context, start, end []byte) ([]byte, []byte, error)
}

type graphRawRun struct {
	ID      string `json:"id"`
	After   uint64 `json:"after"`
	Through uint64 `json:"through,omitempty"`
	Closing bool   `json:"closing,omitempty"`
}

func (j graphJournal) rawStream(run string) shardlog.Stream {
	return shardlog.Stream(j.key("raw-entries/" + run + "/"))
}

// Stop readers from accepting the old run's tail before recovery samples it.
// A later write to the old run can never change the sealed sequence range.
func (j graphJournal) closeRawRun(ctx context.Context, token []byte) error {
	tx, err := j.c.root.begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var h GraphHead
	err = readJSON(ctx, tx, j.key("head"), &h)
	if tikverr.IsErrNotFound(err) || err == nil && (h.RawActive == nil || h.RawActive.Closing) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = j.c.fenceIndexGuard(ctx, tx, j.id, token); err != nil {
		return err
	}
	if err = j.c.ReplicaFence(j.id, j.owner)(ctx, tx); err != nil {
		return err
	}
	h.RawActive.Closing = true
	if err = putJSON(tx, j.key("head"), h); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (h *GraphHead) sealRawRun() {
	if h.RawActive == nil {
		return
	}
	run := *h.RawActive
	if h.Committed.Sequence > run.After {
		run.Closing = false
		run.Through = h.Committed.Sequence
		h.RawSealed = append(h.RawSealed, run)
	}
	h.RawActive = nil
}

func (j graphJournal) rawTail(ctx context.Context, h GraphHead, recovering bool) (GraphHead, error) {
	run := h.RawActive
	if run.Closing != recovering {
		return h, ErrConflict
	}
	if j.c.rawGraph == nil {
		return h, errors.New("raw graph storage unavailable")
	}
	prefix := j.rawStream(run.ID)
	end := append([]byte(strings.TrimSuffix(string(prefix), "/")), '0')
	key, value, err := j.c.rawGraph.last(ctx, []byte(prefix), end)
	if err != nil {
		return h, err
	}
	if !recovering {
		// RawKV has no MVCC snapshot. Verify the anchor AFTER sampling the
		// tail so a reader cannot observe a late write beyond a sealed run.
		tx, err := j.c.root.begin()
		if err != nil {
			return h, err
		}
		defer tx.Rollback()
		var latest GraphHead
		if err = readJSON(ctx, tx, j.key("head"), &latest); err != nil {
			return h, err
		}
		if latest.RawActive == nil || *latest.RawActive != *run {
			return h, ErrConflict
		}
	}
	if len(key) == 0 {
		if h.Committed.Sequence != run.After {
			return h, shardlog.ErrGap
		}
		return h, nil
	}
	var tail GraphEntry
	if err := decodeGraphEntry(value, &tail, false); err != nil {
		return h, err
	}
	if !bytes.Equal(key, prefix.Key(tail.Position.Sequence)) || tail.Position.Sequence <= run.After ||
		tail.Term != h.Term || tail.Previous.Sequence != tail.Position.Sequence-1 ||
		tail.Position.Hash != entryHash(tail) || tail.Position.Sequence < h.Committed.Sequence ||
		tail.Position.DataOffset < h.Committed.DataOffset || tail.LogBytes < h.LogBytes ||
		tail.LogBytes < h.CheckpointBytes || tail.CreatedMS <= 0 ||
		(tail.Position.Sequence == h.Committed.Sequence && tail.Position != h.Committed) {
		return h, errors.New("invalid raw graph tail")
	}
	h.Committed, h.Visible = tail.Position, tail.Position
	h.Sources, h.SourceEpochs, h.LogBytes = tail.Sources, tail.SourceEpochs, tail.LogBytes
	return h, nil
}

// Bootstrap and placement can use the persisted prefix while the publisher
// is recovering. Readers that need the latest tail must wait for recovery.
func (j graphJournal) bootstrapHeadIn(ctx context.Context, tx transactionAPI) (GraphHead, error) {
	h, err := j.headIn(ctx, tx)
	if errors.Is(err, ErrConflict) && h.RawActive != nil && h.RawActive.Closing {
		return h, nil
	}
	return h, err
}

func (j graphJournal) bootstrapHead(ctx context.Context) (GraphHead, error) {
	tx, err := j.c.root.begin()
	if err != nil {
		return GraphHead{}, err
	}
	defer tx.Rollback()
	return j.bootstrapHeadIn(ctx, tx)
}

func (j graphJournal) rawStreamAt(h GraphHead, seq uint64) (shardlog.Stream, bool) {
	if h.RawActive != nil && seq > h.RawActive.After && seq <= h.Committed.Sequence {
		return j.rawStream(h.RawActive.ID), true
	}
	i := sort.Search(len(h.RawSealed), func(n int) bool { return h.RawSealed[n].Through >= seq })
	if i < len(h.RawSealed) && seq > h.RawSealed[i].After {
		return j.rawStream(h.RawSealed[i].ID), true
	}
	return "", false
}

func (j graphJournal) readEntryIn(ctx context.Context, tx transactionAPI, h GraphHead, seq uint64, entry *GraphEntry, withBody bool) error {
	if stream, ok := j.rawStreamAt(h, seq); ok {
		raw, err := j.c.rawGraph.Get(ctx, stream.Key(seq))
		if err != nil {
			return err
		}
		if raw == nil {
			return fmt.Errorf("%w at %d", shardlog.ErrGap, seq)
		}
		return decodeGraphEntry(raw, entry, withBody)
	}
	return readGraphEntry(ctx, tx, j.stream().Key(seq), entry, withBody)
}

func (j graphJournal) rawEntryReader(tx transactionAPI, head GraphHead, after uint64) shardlog.ReadBatch {
	return func(ctx context.Context, keys [][]byte) ([][]byte, error) {
		rows := make([][]byte, len(keys))
		var rawKeys, txnKeys [][]byte
		var rawPositions, txnPositions []int
		for i, key := range keys {
			seq := after + uint64(i) + 1
			if stream, ok := j.rawStreamAt(head, seq); ok {
				rawKeys = append(rawKeys, stream.Key(seq))
				rawPositions = append(rawPositions, i)
			} else {
				txnKeys = append(txnKeys, key)
				txnPositions = append(txnPositions, i)
			}
		}
		for _, portion := range []struct {
			keys      [][]byte
			positions []int
			get       shardlog.ReadBatch
		}{
			{txnKeys, txnPositions, logReader(tx)},
			{rawKeys, rawPositions, j.c.rawGraph.BatchGet},
		} {
			if len(portion.keys) == 0 {
				continue
			}
			values, err := portion.get(ctx, portion.keys)
			if err != nil {
				return nil, err
			}
			if len(values) != len(portion.positions) {
				return nil, errors.New("invalid graph storage batch response")
			}
			for i, pos := range portion.positions {
				rows[pos] = values[i]
			}
		}
		return rows, nil
	}
}

func (j graphJournal) proposeRawSlot(ctx context.Context, w *indexWriter, token []byte, run graphRawRun, seq uint64, raw []byte) error {
	w.mu.Lock()
	generation := w.lease.Generation
	matches := bytes.Equal(token, w.lease.token()) && w.known && w.head.RawActive != nil &&
		*w.head.RawActive == run && w.head.Committed.Sequence+1 == seq
	w.mu.Unlock()
	if !matches || w.replicaLease == nil || !w.replicaLease.ActiveTerm(j.owner.Epoch) {
		return ErrFenced
	}
	key := j.rawStream(run.ID).Key(seq)
	err := w.leaderLease.WithLeader(ctx, generation, func(ctx context.Context) error {
		return j.c.rawGraph.BatchPut(ctx, [][]byte{key}, [][]byte{raw})
	})
	if err != nil {
		// Never reuse this run after an unconfirmed write. A recovery can
		// seal the slot if it arrived, or isolate it in the old run otherwise.
		w.invalidate()
		return fmt.Errorf("raw graph append not confirmed: %w", logStorageError(err))
	}
	return nil
}
