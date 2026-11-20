// Node is the local end of a partition log. A storage-backed election supplies
// terms. Each successor fences the previous writer during recovery.
package shardlog

import (
	"context"
	"time"

	"example.com/vector-storage/internal/lease"
)

// Node's zero value is a follower. One control loop campaigns or renews. A
// retired Node cannot be revived by a delayed election result. StepDown before
// releasing the durable grant; a new incarnation gets a fresh Node.
type Node struct {
	handle lease.Handle
}

// Campaign installs authority only after elect has durably acquired/renewed a
// term and prepared its writer. started is the start of the actual grant RPC.
func (n *Node) Campaign(ctx context.Context, elect func(context.Context) (lease.Grant, time.Time, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	grant, started, err := elect(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !n.handle.Confirm(started, grant) {
		return ErrFenced
	}
	return nil
}

// WithLeader admits an already-pinned MVCC snapshot. Checking the monotonic
// clock here also covers a process resuming before its expiry callback runs.
// At completion only the currently published term can acknowledge the batch.
func (n *Node) WithLeader(ctx context.Context, term uint64, appendEntry func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	grant, ok := n.Snapshot()
	if !ok || grant.Epoch != term {
		return ErrFenced
	}
	if err := appendEntry(ctx); err != nil {
		return err
	}
	if !n.handle.ActiveTerm(term) {
		return ErrFenced
	}
	return nil
}

func (n *Node) Snapshot() (lease.Snapshot, bool) { return n.handle.Snapshot() }
func (n *Node) InLeaderLease(epoch *uint64) bool { return n.handle.InLeaderLease(epoch) }
func (n *Node) StepDown()                        { n.handle.Retire() }

// Window describes a committed range in a pinned storage view. A reader can
// resume from After independently of every other follower or dispatcher.
type Window struct {
	After, Through uint64
	Limit          int
	ByteLimit      int
}

// Reader is the read-only side of a partition. It requires no election or
// write lease. Implementations choose a pinned data/GC snapshot for each read.
type Reader[T any] interface {
	ReadCommitted(context.Context, Window) (T, error)
}

// Group binds a partition's local election to its immutable WAL. A read view
// may be pinned to the same MVCC snapshot as the consumer's recovery cursor.
// All views of an incarnation share its Node; followers need only Reader.
type Group[P, T any] struct {
	*Node
	Reader[T]
	appendEntry func(context.Context, P) error
}

func Open[P, T any](node *Node, appendEntry func(context.Context, P) error, view Reader[T]) Group[P, T] {
	if node == nil {
		node = &Node{}
	}
	return Group[P, T]{Node: node, Reader: view, appendEntry: appendEntry}
}

// Propose reads only the locally published term. The writer must fence late
// writes on takeover (a pinned MVCC slot or an epoch-isolated key space).
func (g Group[P, T]) Propose(ctx context.Context, term uint64, proposal P) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.appendEntry == nil || !g.handle.ActiveTerm(term) {
		return ErrFenced
	}
	if err := g.appendEntry(ctx, proposal); err != nil {
		return err
	}
	if !g.handle.ActiveTerm(term) {
		return ErrFenced // persistence may have succeeded
	}
	return nil
}
