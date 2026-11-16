package vector

import (
	"context"
	"errors"
	"math/rand"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
)

// Retry definite TiKV write conflicts and source-lock NOWAIT failures that
// precede all graph prewrites. Each attempt rechecks the graph base and fences
// in a fresh transaction, reusing the immutable uploaded object. Unknown
// outcomes and semantic conflicts return to normal journal recovery instead.
func retryIndexCommit(ctx context.Context, commit func() error, conflict func()) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := commit()
		writeConflict := tikverr.IsErrWriteConflict(err)
		if (!writeConflict && !errors.Is(err, errSourceLockBusy)) || attempt >= 15 {
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
