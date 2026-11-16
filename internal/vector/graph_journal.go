package vector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	tikverr "github.com/tikv/client-go/v2/error"
)

// A single pending entry bounds speculative memory and makes publication and
// promotion independent of the speed of incoming data writes.
type GraphPosition struct {
	Sequence   uint64 `json:"sequence"`
	DataOffset uint64 `json:"data_offset"`
	Hash       string `json:"hash"`
}
type GraphHead struct {
	Sources            map[string]uint64 `json:"sources,omitempty"`
	Floor              GraphPosition     `json:"floor"`
	Term               uint64            `json:"term"`
	Leader             Ownership         `json:"leader"`
	Committed          GraphPosition     `json:"committed"`
	Visible            GraphPosition     `json:"visible"`
	Checkpoint         *Manifest         `json:"checkpoint,omitempty"`
	CheckpointPosition GraphPosition     `json:"checkpoint_position"`
}
type GraphEntry struct {
	Term     uint64        `json:"term"`
	Previous GraphPosition `json:"previous"`
	Position GraphPosition `json:"position"`
	Kind     string        `json:"kind"`
	Object   Manifest      `json:"object"`
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
func (j graphJournal) headIn(ctx context.Context, tx transactionAPI) (GraphHead, error) {
	var h GraphHead
	e := readJSON(ctx, tx, j.key("head"), &h)
	if tikverr.IsErrNotFound(e) {
		e = nil
	}
	if e == nil && (h.Visible.Sequence > h.Committed.Sequence || h.Committed.Sequence-h.Visible.Sequence > 1 || h.Visible.DataOffset > h.Committed.DataOffset || h.Floor.Sequence > h.Visible.Sequence || h.Floor.Sequence > h.CheckpointPosition.Sequence) {
		e = errors.New("invalid graph publication head")
	}
	return h, e
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
	var entry GraphEntry
	e = readJSON(ctx, tx, j.key(fmt.Sprintf("entries/%020d", seq)), &entry)
	if e == nil && (entry.Position.Sequence != seq || entry.Position.Hash != entryHash(entry)) {
		e = errors.New("graph journal hash mismatch")
	}
	return entry, e
}
func entryHash(e GraphEntry) string {
	e.Position.Hash = ""
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
	var entry GraphEntry
	tx, e := j.c.root.begin()
	if e != nil {
		return entry, e
	}
	defer tx.Rollback()
	if e = j.c.ReplicaFence(j.id, j.owner)(ctx, tx); e != nil {
		return entry, e
	}
	h, e := j.headIn(ctx, tx)
	if e != nil {
		return entry, e
	}
	if !sameOwner(h.Leader, j.owner) || h.Term != base.Term || h.Committed != base.Committed || h.Committed != h.Visible {
		return entry, ErrConflict
	}
	if h.Committed.Sequence == ^uint64(0) || offset < h.Committed.DataOffset || (kind != "delta" && kind != "reset") {
		return entry, errors.New("invalid graph commit")
	}
	if input != nil {
		if e = cluster.validateDelivery(ctx, tx, j.id, *input, h.Sources[fmt.Sprint(input.Source)]); e != nil {
			return entry, e
		}
		if input.Through <= h.Sources[fmt.Sprint(input.Source)] {
			return entry, ErrConflict
		}
		if h.Sources == nil {
			h.Sources = map[string]uint64{}
		}
		h.Sources[fmt.Sprint(input.Source)] = input.Through
	}
	if e = j.adoptObject(ctx, tx, object); e != nil {
		return entry, e
	}
	entry = GraphEntry{Term: h.Term, Previous: h.Committed, Position: GraphPosition{Sequence: h.Committed.Sequence + 1, DataOffset: offset}, Kind: kind, Object: object}
	entry.Position.Hash = entryHash(entry)
	h.Committed = entry.Position
	if e = putJSON(tx, j.key(fmt.Sprintf("entries/%020d", entry.Position.Sequence)), entry); e != nil {
		return entry, e
	}
	if e = putJSON(tx, j.key("head"), h); e != nil {
		return entry, e
	}
	return entry, tx.Commit(ctx)
}
func (j graphJournal) Ack(ctx context.Context, pos GraphPosition) error {
	tx, e := j.c.root.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = j.c.Fence(j.owner)(ctx, tx); e != nil {
		return e
	}
	p, e := j.c.planIn(ctx, tx, j.id)
	if e != nil {
		return e
	}
	if !containsReplica(p.Serving, j.owner.Partition) {
		return ErrFenced
	}
	h, e := j.headIn(ctx, tx)
	if e != nil {
		return e
	}
	if h.Committed != pos {
		return ErrConflict
	}
	if e = putJSON(tx, j.key("head"), h); e != nil {
		return e
	}
	if e = putJSON(tx, j.key("acks/"+string(j.owner.Partition)), graphAck{j.owner, pos}); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

// Publication waits only for the graph leader's validated application. Followers
// replay asynchronously; an offline follower never blocks indexing progress.
func (j graphJournal) Publish(ctx context.Context) (bool, error) {
	tx, e := j.c.root.begin()
	if e != nil {
		return false, e
	}
	defer tx.Rollback()
	if e = j.c.ReplicaFence(j.id, j.owner)(ctx, tx); e != nil {
		return false, e
	}
	h, e := j.headIn(ctx, tx)
	if e != nil {
		return false, e
	}
	if !sameOwner(h.Leader, j.owner) {
		return false, ErrFenced
	}
	if h.Committed == h.Visible {
		return false, nil
	}
	var ack graphAck
	if e = readJSON(ctx, tx, j.key("acks/"+string(j.owner.Partition)), &ack); e != nil {
		return false, e
	}
	if !sameOwner(ack.Owner, j.owner) || ack.Position != h.Committed {
		return false, ErrConflict
	}
	h.Visible = h.Committed
	if e = putJSON(tx, j.key("head"), h); e != nil {
		return false, e
	}
	e = tx.Commit(ctx)
	return e == nil, e
}
func (j graphJournal) SaveCheckpoint(ctx context.Context, pos GraphPosition, m Manifest, repo *Repository) error {
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
	if h.Visible != pos {
		return ErrConflict
	}
	if e = j.adoptObject(ctx, tx, m); e != nil {
		return e
	}
	h.Checkpoint = &m
	h.CheckpointPosition = pos
	if e = putJSON(tx, j.key("head"), h); e != nil {
		return e
	}
	prefix := repo.Prefix
	if repo.IndexPrefix != "" {
		prefix = repo.IndexPrefix
	}
	if e = putJSON(tx, []byte(prefix+"manifest"), m); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

func (j graphJournal) Report(ctx context.Context, pos GraphPosition) error {
	tx, e := j.c.root.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	owner, e := j.c.readOwner(ctx, tx, j.owner.Partition)
	if e != nil {
		return e
	}
	if !sameOwner(owner, j.owner) || !owner.Ready || owner.Expires <= txnMillis(tx) {
		return ErrFenced
	}
	if e = putJSON(tx, j.key("progress/"+string(j.owner.Partition)), graphAck{j.owner, pos}); e != nil {
		return e
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
