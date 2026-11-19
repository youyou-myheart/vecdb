package lease

import (
	"context"
	"time"
)

// Election drives a storage-backed campaign from a control loop. Its adapter
// owns the atomic KV writes, including any placement or commit fences that
// need to share the lease transaction.
type Election[T any] struct {
	Acquire func(context.Context) (owner T, ready bool, err error)
	Renew   func(context.Context, T) (T, error)
	Release func(context.Context, T, error)
}

// Step acquires a missing term or renews the held term. prepare restores
// component state before the term can be published as ready.
func (e Election[T]) Step(ctx context.Context, held *T, prepare func(T) error) (T, time.Time, error) {
	var owner T
	if held == nil {
		started := time.Now()
		acquired, ready, err := e.Acquire(ctx)
		if err != nil {
			return owner, time.Time{}, err
		}
		if prepare != nil {
			if err = prepare(acquired); err != nil {
				e.Release(ctx, acquired, err)
				return owner, time.Time{}, err
			}
		}
		if ready {
			return acquired, started, nil
		}
		owner = acquired
	} else {
		owner = *held
	}
	return Refresh(ctx, func(ctx context.Context) (T, error) { return e.Renew(ctx, owner) })
}

// Refresh pairs a confirmed durable result with the start of its KV
// operation. Applying its result cannot extend authority across IO latency.
func Refresh[T any](ctx context.Context, operation func(context.Context) (T, error)) (T, time.Time, error) {
	started := time.Now()
	value, err := operation(ctx)
	return value, started, err
}
