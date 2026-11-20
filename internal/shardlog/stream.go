// Package shardlog couples partition elections and immutable logs. Each
// consumer follows its own contiguous prefix of committed entries.
package shardlog

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrSplitRequired = errors.New("mutations require separate one-phase requests")
	ErrFenced        = errors.New("log writer lease expired")
	ErrOccupied      = errors.New("log slot already occupied")
	ErrGap           = errors.New("missing committed log slot")
)

// Stream is the on-disk prefix of one partition (or one sealed writer epoch).
type Stream string

func (s Stream) Key(lsn uint64) []byte { return []byte(fmt.Sprintf("%s%020d", s, lsn)) }

// ReadBatch must return values in request order, with nil for absent slots.
// A pinned snapshot can be supplied when the consumer also checks a GC floor.
type ReadBatch func(context.Context, [][]byte) ([][]byte, error)

// Tail reads up to limit consecutive slots in (after, through]. While following
// a live writer, stop at the first hole even if a later slot is already present.
// A sealed range requires every requested slot to exist.
func (s Stream) Tail(ctx context.Context, read ReadBatch, after, through uint64, limit int, sealed bool) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(s) == 0 || limit < 1 || limit > 4096 || through < after {
		return nil, errors.New("invalid log tail range")
	}
	count := min(uint64(limit), through-after)
	if count == 0 {
		return nil, nil
	}
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = s.Key(after + uint64(i) + 1)
	}
	values, err := read(ctx, keys)
	if err != nil {
		return nil, err
	}
	if len(values) != len(keys) {
		return nil, errors.New("invalid storage batch response")
	}
	for i, value := range values {
		if value == nil {
			if sealed {
				return nil, fmt.Errorf("%w at %d", ErrGap, after+uint64(i)+1)
			}
			return values[:i], nil
		}
	}
	return values, nil
}
