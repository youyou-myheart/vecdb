package vector

import (
	"context"
	"errors"
	"fmt"
	"testing"

	tikverr "github.com/tikv/client-go/v2/error"
)

func indexWriteConflict() error {
	return tikverr.NewErrWriteConflictWithArgs(1, 2, 3, []byte("index-fence"), 0)
}

func TestIndexCommitRetriesOnlyDefiniteConflicts(t *testing.T) {
	calls, retries := 0, 0
	err := retryIndexCommit(context.Background(), func() error {
		calls++
		if calls < 3 {
			return fmt.Errorf("commit: %w", indexWriteConflict())
		}
		return nil
	}, func() { retries++ })
	if err != nil || calls != 3 || retries != 2 {
		t.Fatal(err, calls, retries)
	}
	for _, expected := range []error{injected, ErrConflict, ErrFenced, context.DeadlineExceeded} {
		calls = 0
		err = retryIndexCommit(context.Background(), func() error { calls++; return expected }, func() { t.Fatal("retried unknown/semantic outcome") })
		if !errors.Is(err, expected) || calls != 1 {
			t.Fatal(err, calls)
		}
	}
}

func TestIndexCommitRetryIsBoundedAndCancelable(t *testing.T) {
	calls := 0
	err := retryIndexCommit(context.Background(), func() error { calls++; return indexWriteConflict() }, func() {})
	if !tikverr.IsErrWriteConflict(err) || calls != 16 {
		t.Fatal(err, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls = 0
	err = retryIndexCommit(ctx, func() error { calls++; return indexWriteConflict() }, cancel)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal(err, calls)
	}
}

type indexCommitHookTx struct {
	transactionAPI
	before func() error
}

func (tx indexCommitHookTx) Commit(ctx context.Context) error {
	if err := tx.before(); err != nil {
		return err
	}
	return tx.transactionAPI.Commit(ctx)
}

func TestIndexCommitConflictReusesObjectAndRechecksFence(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(fmt.Sprint(revoke), func(t *testing.T) {
			f, _, source := routedFixture(t)
			f.write(t, "v", false)
			g := f.services[0].graphReplica
			head, err := g.journal.Claim(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = g.enqueue(f.ctx, deliveryFor(t, f, source, 0)); err != nil {
				t.Fatal(err)
			}
			head, err = g.journal.Head(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			original := f.r.beginTxn
			calls := 0
			var hooked func() (transactionAPI, error)
			hooked = func() (transactionAPI, error) {
				tx, err := original()
				if err != nil {
					return nil, err
				}
				return indexCommitHookTx{tx, func() error {
					calls++
					if calls == 1 {
						if revoke {
							f.r.beginTxn = original
							_, err := f.c.Update(f.ctx, g.journal.owner, false, true)
							f.r.beginTxn = hooked
							if err != nil {
								t.Fatal(err)
							}
						}
						return indexWriteConflict()
					}
					return nil
				}}, nil
			}
			f.r.beginTxn = hooked
			err = g.commitRouted(f.ctx, f.services[0], head)
			f.r.beginTxn = original
			if revoke && (!errors.Is(err, ErrFenced) || calls != 1) {
				t.Fatal(err, calls)
			}
			if !revoke && (err != nil || calls != 2) {
				t.Fatal(err, calls)
			}
			objects := f.objects.Client.(*partitionBlobs)
			if len(objects.values) != 1 {
				t.Fatal("conflict re-uploaded graph object", len(objects.values))
			}
			h, err := g.journal.Head(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(1)
			if revoke {
				want = 0
			}
			if h.Committed.Sequence != 1+want || h.Sources["0"] != want {
				t.Fatal("partial/duplicate commit", h)
			}
		})
	}
}
