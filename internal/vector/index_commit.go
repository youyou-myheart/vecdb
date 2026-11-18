package vector

import (
	"context"
	"math/rand"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
)

// Retry only definite TiKV write conflicts. Each attempt rechecks the graph base and fences
// in a fresh transaction, reusing the immutable uploaded object. Unknown
// outcomes and semantic conflicts return to normal journal recovery instead.
func retryIndexCommit(ctx context.Context, commit func() error, conflict func()) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := commit()
		writeConflict := tikverr.IsErrWriteConflict(err)
		if !writeConflict || attempt >= 15 {
			return err
		}
		if writeConflict {
			conflict()
		}
		delay := time.Duration(min(attempt+1, 10)+rand.Intn(4)) * time.Millisecond
		if !waitIndexRetry(ctx, delay) {
			return ctx.Err()
		}
	}
}
