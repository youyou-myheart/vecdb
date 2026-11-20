package vector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	tikvutil "github.com/tikv/client-go/v2/util"
)

// A durable entry is replayable immediately. Local applied positions may lag
// this journal independently on each replica.
type GraphPosition struct {
	Sequence   uint64 `json:"sequence"`
	DataOffset uint64 `json:"data_offset"`
	Hash       string `json:"hash"`
}
type GraphHead struct {
	SourceEpochs           map[string]uint64 `json:"source_epochs,omitempty"`
	CheckpointSourceEpochs map[string]uint64 `json:"checkpoint_source_epochs,omitempty"`
	LogBytes               uint64            `json:"log_bytes,omitempty"`
	CheckpointBytes        uint64            `json:"checkpoint_bytes,omitempty"`
	CheckpointSources      map[string]uint64 `json:"checkpoint_sources,omitempty"`
	FloorSources           map[string]uint64 `json:"floor_sources,omitempty"`
	Sources                map[string]uint64 `json:"sources,omitempty"`
	Floor                  GraphPosition     `json:"floor"`
	Term                   uint64            `json:"term"`
	Leader                 Ownership         `json:"leader"`
	Committed              GraphPosition     `json:"committed"`
	// Visible is retained for API compatibility and equals Committed.
	// Per-replica applied/queryable positions are reported separately.
	Visible            GraphPosition `json:"visible"`
	Checkpoint         *Manifest     `json:"checkpoint,omitempty"`
	CheckpointPosition GraphPosition `json:"checkpoint_position"`
}
type GraphEntry struct {
	SourceEpochs map[string]uint64 `json:"source_epochs,omitempty"`
	Payload      *GraphPayload     `json:"payload,omitempty"`
	PayloadData  []byte            `json:"payload_data,omitempty"` // Inline delta bytes; entryHash authenticates them through Payload.SHA256.
	LogBytes     uint64            `json:"log_bytes,omitempty"`
	CreatedMS    int64             `json:"created_ms,omitempty"`
	// Cumulative source watermarks make the latest entry a self-contained ACK.
	Sources  map[string]uint64 `json:"sources,omitempty"`
	Term     uint64            `json:"term"`
	Previous GraphPosition     `json:"previous"`
	Position GraphPosition     `json:"position"`
	Kind     string            `json:"kind"`
	Object   Manifest          `json:"object"`
}
type graphAck struct {
	Owner    Ownership     `json:"owner"`
	Position GraphPosition `json:"position"`
}
type graphJournal struct {
	c     *Coordinator
	id    IndexPartitionID
	owner Ownership
}

func (j graphJournal) key(suffix string) []byte {
	return []byte(j.c.root.Prefix + "cluster/graph/" + string(j.id) + "/" + suffix)
}
func readJSON(ctx context.Context, tx transactionAPI, key []byte, out any) error {
	b, e := tx.Get(ctx, key)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, out)
}

// The stored head is a control/compaction anchor, changed only on leadership,
// checkpoint, or retention operations. The immutable tail supplies the current
// durable position and source ACKs; appending never rewrites the head.
func (j graphJournal) headIn(ctx context.Context, tx transactionAPI) (GraphHead, error) {
	var h GraphHead
	err := readJSON(ctx, tx, j.key("head"), &h)
	if err != nil && !tikverr.IsErrNotFound(err) {
		return h, err
	}
	if h.Visible != h.Committed || h.Floor.Sequence > h.Visible.Sequence || h.Floor.Sequence > h.CheckpointPosition.Sequence {
		return h, errors.New("invalid durable graph anchor (legacy publication protocol is not supported)")
	}
	key, raw, err := tx.Last(ctx, j.key("entries/"), j.key("entries0"))
	if err != nil && !tikverr.IsErrNotFound(err) {
		return h, err
	}
	if len(key) == 0 {
		if h.Committed != h.Floor {
			return h, errors.New("missing graph tail above retention floor")
		}
		return h, nil
	}
	var tail GraphEntry
	if err := decodeGraphEntry(raw, &tail, false); err != nil {
		return h, err
	}
	if string(key) != string(j.stream().Key(tail.Position.Sequence)) ||
		tail.Position.Sequence == 0 || tail.Previous.Sequence != tail.Position.Sequence-1 ||
		tail.Position.Hash != entryHash(tail) || tail.Term > h.Term ||
		tail.Position.Sequence < h.Committed.Sequence || tail.Position.DataOffset < h.Committed.DataOffset ||
		tail.LogBytes < h.LogBytes || tail.LogBytes < h.CheckpointBytes ||
		(j.c.referenceLogs && tail.CreatedMS <= 0) ||
		(tail.Position.Sequence == h.Committed.Sequence && tail.Position != h.Committed) {
		return h, errors.New("invalid durable graph tail")
	}
	h.Committed, h.Visible, h.Sources = tail.Position, tail.Position, tail.Sources
	h.LogBytes = tail.LogBytes
	h.SourceEpochs = tail.SourceEpochs
	return h, nil
}
func (j graphJournal) Head(ctx context.Context) (GraphHead, error) {
	tx, e := j.c.root.begin()
	if e != nil {
		return GraphHead{}, e
	}
	defer tx.Rollback()
	return j.headIn(ctx, tx)
}
func (j graphJournal) Entry(ctx context.Context, seq uint64) (GraphEntry, error) {
	tx, e := j.c.root.begin()
	if e != nil {
		return GraphEntry{}, e
	}
	defer tx.Rollback()
	h, e := j.headIn(ctx, tx)
	if e != nil {
		return GraphEntry{}, e
	}
	if seq <= h.Floor.Sequence {
		return GraphEntry{}, ErrLogCompacted
	}
	return j.entryIn(ctx, tx, seq)
}
func (j graphJournal) entryIn(ctx context.Context, tx transactionAPI, seq uint64) (GraphEntry, error) {
	var entry GraphEntry
	e := readGraphEntry(ctx, tx, j.stream().Key(seq), &entry, true)
	if e == nil && (entry.Position.Sequence != seq || entry.Position.Hash != entryHash(entry)) {
		e = errors.New("graph journal hash mismatch")
	}
	return entry, e
}
func entryHash(e GraphEntry) string {
	e.Position.Hash = ""
	e.PayloadData = nil
	b, _ := json.Marshal(e)
	return fmt.Sprintf("%x", sha256.Sum256(b))
}
func (j graphJournal) Claim(ctx context.Context) (GraphHead, error) {
	tx, e := j.c.root.begin()
	if e != nil {
		return GraphHead{}, e
	}
	defer tx.Rollback()
	if e = j.c.ReplicaFence(j.id, j.owner)(ctx, tx); e != nil {
		return GraphHead{}, e
	}
	h, e := j.headIn(ctx, tx)
	if e != nil {
		return h, e
	}
	if sameOwner(h.Leader, j.owner) {
		return h, nil
	}
	if !sameOwner(h.Leader, j.owner) {
		if h.Term == ^uint64(0) {
			return h, errors.New("graph term exhausted")
		}
		h.Term++
		h.Leader = j.owner
	}
	if e = putJSON(tx, j.key("head"), h); e != nil {
		return h, e
	}
	return h, tx.Commit(ctx)
}
func (j graphJournal) Commit(ctx context.Context, base GraphHead, offset uint64, kind string, object Manifest) (GraphEntry, error) {
	return j.commit(ctx, base, offset, kind, object, nil, nil)
}
func (j graphJournal) commit(ctx context.Context, base GraphHead, offset uint64, kind string, object Manifest, input *Delivery, cluster *ClusterService) (GraphEntry, error) {
	var inputs []Delivery
	if input != nil {
		inputs = []Delivery{*input}
	}
	return j.commitInputs(ctx, base, offset, kind, object, inputs, cluster)
}

func (j graphJournal) commitInputs(ctx context.Context, base GraphHead, offset uint64, kind string, object Manifest, inputs []Delivery, cluster *ClusterService) (GraphEntry, error) {
	return j.commitPayload(ctx, base, offset, kind, object, inputs, cluster, nil)
}

func (j graphJournal) commitPayload(ctx context.Context, base GraphHead, offset uint64, kind string, object Manifest, inputs []Delivery, cluster *ClusterService, payload []byte) (GraphEntry, error) {
	return j.commitWithAuthority(ctx, base, offset, kind, object, inputs, cluster, payload, nil, nil)
}
func (j graphJournal) commitWithAuthority(ctx context.Context, base GraphHead, offset uint64, kind string, object Manifest, inputs []Delivery, cluster *ClusterService, payload []byte, token []byte, writer *indexWriter) (GraphEntry, error) {
	var entry GraphEntry
	tx, e := j.c.root.begin()
	if e != nil {
		return entry, e
	}
	defer tx.Rollback()
	// Generic metadata writes prefer 1PC. The leased append below enforces
	// single-slot 1PC for deltas and a bounded transaction for object resets.
	if native, ok := tx.(interface{ SetEnable1PC(bool) }); ok {
		native.SetEnable1PC(true)
	}
	native, nativeSlot := j.nativeAppend(tx)
	nativeSlot = nativeSlot && writer != nil
	var h GraphHead
	leasedReset := writer != nil && kind == "reset"
	if writer == nil || leasedReset {
		if e = j.c.fencePublisher(ctx, tx, j.id, j.owner, leasedReset); e != nil {
			return entry, e
		}
		h, e = j.headIn(ctx, tx)
		if e != nil {
			return entry, e
		}
	} else {
		h = cloneGraphHead(base)
	}
	if writer != nil {
		if e = writer.authority(token); e != nil {
			return entry, e
		}
		if leasedReset {
			// The grant can roll over in storage during the object upload.
			// Its replacement may not yet be installed by the local writer.
			e = j.c.fenceIndexGuard(ctx, tx, j.id, token)
		} else if !nativeSlot {
			// A takeover cannot change the guard until the durable lease
			// expires. Touch it to serialize an already-started commit with takeover.
			e = tx.Set(j.c.indexGuardKey(j.id), token)
		}
		if e != nil {
			return entry, e
		}
	}
	for _, input := range inputs {
		if input.foldEpoch && nativeSlot {
			e = validateAdmitted(h, input)
		} else {
			e = checkSourceEpoch(h, input)
		}
		if e != nil {
			return entry, e
		}
	}
	if !sameOwner(h.Leader, j.owner) || h.Term != base.Term || h.Committed != base.Committed || h.Committed != h.Visible {
		return entry, ErrConflict
	}
	if h.Committed.Sequence == ^uint64(0) || offset < h.Committed.DataOffset || (kind != "delta" && kind != "reset") {
		return entry, errors.New("invalid graph commit")
	}
	if j.c.referenceLogs && ((kind == "delta") != (len(payload) > 0)) {
		return entry, errors.New("reference delta requires a TiKV payload; reset requires an object")
	}
	for _, input := range inputs {
		if writer == nil {
			e = cluster.validateDelivery(ctx, tx, j.id, input, h)
		} else {
			e = validateAdmitted(h, input)
		}
		if e != nil {
			return entry, e
		}
		if input.Through <= h.Sources[fmt.Sprint(input.Source)] {
			return entry, ErrConflict
		}
		if h.Sources == nil {
			h.Sources = map[string]uint64{}
		}
		key := fmt.Sprint(input.Source)
		h.Sources[key] = input.Through
		if input.foldEpoch && nativeSlot && input.Owner.Epoch > h.SourceEpochs[key] {
			if h.SourceEpochs == nil {
				h.SourceEpochs = map[string]uint64{}
			}
			h.SourceEpochs[key] = input.Owner.Epoch
		}
	}
	if e = j.adoptObject(ctx, tx, object); e != nil {
		return entry, e
	}
	entry = GraphEntry{LogBytes: h.LogBytes, Sources: h.Sources, SourceEpochs: maps.Clone(h.SourceEpochs), Term: h.Term, Previous: h.Committed, Position: GraphPosition{Sequence: h.Committed.Sequence + 1, DataOffset: offset}, Kind: kind, Object: object}
	if j.c.referenceLogs {
		entry.CreatedMS = txnMillis(tx)
		added := uint64(len(payload)) + uint64(max(int64(0), object.Size))
		if added > ^uint64(0)-h.LogBytes {
			return entry, errors.New("graph byte counter exhausted")
		}
		entry.LogBytes = h.LogBytes + added
	}
	if len(payload) > 0 {
		if len(payload) > maxGraphPayload {
			return entry, errors.New("invalid graph payload size")
		}
		entry.Payload = &GraphPayload{Size: len(payload), SHA256: fmt.Sprintf("%x", sha256.Sum256(payload))}
		entry.PayloadData = payload
	}
	entry.Position.Hash = entryHash(entry)
	key := j.stream().Key(entry.Position.Sequence)
	encodeStart := time.Now()
	encoded, err := encodeGraphEntry(entry)
	if cluster != nil {
		cluster.observations.pipeline.observe("graph_entry_encode", string(j.id), time.Since(encodeStart), err)
	}
	if err != nil {
		return entry, err
	}
	// A reset adopts an uploaded object and changes metadata. Its grant still
	// bounds that transaction, including on the compatibility storage path.
	var resetExpiry int64
	if leasedReset {
		resetExpiry, err = writer.commitExpiry(token)
		if err != nil {
			return entry, err
		}
		if txnMillis(tx) >= resetExpiry {
			return entry, ErrFenced
		}
	}
	var commit func(context.Context) error
	if nativeSlot {
		if leasedReset {
			if err = native.insert(key, encoded); err != nil {
				return entry, err
			}
			commit = func(ctx context.Context) error { return native.commitBeforeLeaseExpiry(ctx, resetExpiry) }
		} else {
			commit = func(ctx context.Context) error {
				return j.proposeSlot(ctx, writer, token, native, entry.Position.Sequence, encoded)
			}
		}
	} else {
		if _, e = tx.Get(ctx, key); e == nil {
			return entry, ErrConflict
		} else if !tikverr.IsErrNotFound(e) {
			return entry, e
		}
		// Competing appenders choose the same immutable key. Its write conflict
		// check is atomic with fencing, source epochs and object adoption.
		if e = tx.Set(key, encoded); e != nil {
			return entry, e
		}
		if writer != nil {
			if e = writer.authority(token); e != nil {
				return entry, e
			}
		}
		commit = tx.Commit
	}
	var details *tikvutil.CommitDetails
	if nativeSlot && cluster != nil {
		ctx = context.WithValue(ctx, tikvutil.CommitDetailCtxKey, &details)
	}
	started := time.Now()
	err = commit(ctx)
	if cluster != nil {
		p := &cluster.observations.pipeline
		p.observe("graph_tikv_commit", string(j.id), time.Since(started), err)
		if details != nil {
			p.observe("graph_tikv_prewrite", string(j.id), details.PrewriteTime, err)
			p.observe("graph_tikv_latest_ts", string(j.id), details.GetLatestTsTime, err)
			if !leasedReset && err == nil {
				p.observe("graph_tikv_one_phase", string(j.id), details.PrewriteTime, nil)
			}
			p.observe("graph_tikv_commit_ts", string(j.id), details.GetCommitTsTime, err)
			p.observe("graph_tikv_commit_primary", string(j.id), details.CommitTime, err)
			details.Mu.Lock()
			backoff := time.Duration(details.Mu.CommitBackoffTime)
			details.Mu.Unlock()
			p.observe("graph_tikv_backoff", string(j.id), backoff, err)
		}
		if err == nil && len(payload) > 0 {
			p.graphEntryBytes.WithLabelValues(string(j.id), "delta").Add(float64(len(payload)))
			p.graphEntryBytes.WithLabelValues(string(j.id), "stored").Add(float64(len(encoded)))
		}
	}
	var exists *tikverr.ErrKeyExist
	if nativeSlot && errors.As(err, &exists) {
		err = ErrConflict
	}
	return entry, err
}

// Coverage describes the captured graph, not the possibly newer durable tail.
func (j graphJournal) saveCheckpoint(ctx context.Context, pos GraphPosition, m Manifest, dataManifestKey []byte, coverage *GraphHead) error {
	tx, e := j.c.root.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = j.c.ReplicaFence(j.id, j.owner)(ctx, tx); e != nil {
		return e
	}
	h, e := j.headIn(ctx, tx)
	if e != nil {
		return e
	}
	if coverage == nil {
		if h.Visible != pos {
			return ErrConflict
		}
		h.CheckpointSourceEpochs = maps.Clone(h.SourceEpochs)
	} else {
		if pos.Sequence > h.Visible.Sequence || pos.Sequence < h.CheckpointPosition.Sequence || pos.Sequence < h.Floor.Sequence {
			return ErrConflict
		}
		if pos != coverage.Committed || !sameOwner(h.Leader, j.owner) {
			return ErrConflict
		}
		if pos.Sequence > h.Floor.Sequence {
			var entry GraphEntry
			if e = readGraphEntry(ctx, tx, j.stream().Key(pos.Sequence), &entry, false); e != nil {
				return e
			}
			if entry.Position != pos || entryHash(entry) != pos.Hash || entry.LogBytes != coverage.LogBytes || !maps.Equal(entry.Sources, coverage.Sources) || !maps.Equal(entry.SourceEpochs, coverage.SourceEpochs) {
				return errors.New("checkpoint coverage mismatch")
			}
		} else if pos != h.Floor {
			return ErrConflict
		}
		h.CheckpointBytes = coverage.LogBytes
		h.CheckpointSources = maps.Clone(coverage.Sources)
		h.CheckpointSourceEpochs = maps.Clone(coverage.SourceEpochs)
	}
	if e = j.adoptObject(ctx, tx, m); e != nil {
		return e
	}
	h.Checkpoint = &m
	h.CheckpointPosition = pos
	if e = putJSON(tx, j.key("head"), h); e != nil {
		return e
	}
	// Identity routing uses this manifest to reset the data stream and prune
	// its source log. Routed data streams publish their own checkpoints.
	if dataManifestKey != nil {
		if e = putJSON(tx, dataManifestKey, m); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}

func (j graphJournal) Progress(ctx context.Context, owner Ownership) (GraphPosition, error) {
	tx, e := j.c.root.begin()
	if e != nil {
		return GraphPosition{}, e
	}
	defer tx.Rollback()
	var ack graphAck
	e = readJSON(ctx, tx, j.key("progress/"+string(owner.Partition)), &ack)
	if e != nil {
		return GraphPosition{}, e
	}
	if !sameOwner(ack.Owner, owner) {
		return GraphPosition{}, ErrFenced
	}
	return ack.Position, nil
}
