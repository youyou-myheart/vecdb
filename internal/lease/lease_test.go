package lease

import (
	"testing"
	"time"
)

func TestInLeaderLeaseTracksConfirmedEpoch(t *testing.T) {
	var h Handle
	var before, after uint64
	if h.InLeaderLease(&before) {
		t.Fatal("unconfirmed lease is live")
	}
	now := time.Now()
	if !h.Confirm(now, Grant{Epoch: 3, Expires: 100, Remaining: time.Minute}) ||
		!h.InLeaderLease(&before) || before != 3 {
		t.Fatal("confirmed epoch is missing", before)
	}
	if h.Confirm(now.Add(-time.Hour), Grant{Epoch: 4, Expires: 200, Remaining: time.Minute}) {
		t.Fatal("expired reply granted authority")
	}
	if !h.Confirm(now, Grant{Epoch: 4, Expires: 200, Remaining: time.Minute}) ||
		!h.InLeaderLease(&after) || after != 4 || before == after {
		t.Fatal("epoch change was not observable", before, after)
	}
	if h.Confirm(now, Grant{Epoch: 3, Expires: 300, Remaining: time.Minute}) {
		t.Fatal("old epoch superseded the current one")
	}
	h.Retire()
	if h.Confirm(now, Grant{Epoch: 5, Expires: 300, Remaining: time.Minute}) || h.InLeaderLease(nil) {
		t.Fatal("renewal resurrected a retired handle")
	}
}

func TestExpiredSnapshotFailsWithoutAnotherTick(t *testing.T) {
	var h Handle
	h.current.Store(&Snapshot{Epoch: 1, Deadline: time.Now().Add(-time.Second)})
	epoch := uint64(7)
	if h.InLeaderLease(&epoch) || epoch != 7 {
		t.Fatal("expired lease admitted an operation", epoch)
	}
}

func TestControlTimerWithdrawsTermAndPreservesGrantOrder(t *testing.T) {
	var h Handle
	defer h.Retire()
	const term = 3
	// The safety margin leaves 100 ms for the first renewal.
	if !h.Confirm(time.Now(), Grant{Epoch: term, Expires: 100, Remaining: 150 * time.Millisecond}) {
		t.Fatal("failed to publish first term")
	}
	firstDeadline := h.current.Load().Deadline
	if !h.Confirm(time.Now(), Grant{Epoch: term, Expires: 200, Remaining: time.Minute}) {
		t.Fatal("failed to renew")
	}
	// Let the original deadline pass without reading the lease.
	time.Sleep(time.Until(firstDeadline) + 5*time.Millisecond)
	if !h.ActiveTerm(term) {
		t.Fatal("expired grant withdrew its renewal")
	}
	if !h.Confirm(time.Now(), Grant{Epoch: term + 1, Expires: 300, Remaining: 70 * time.Millisecond}) {
		t.Fatal("failed to install successor")
	}
	limit := time.After(3 * time.Second)
	for h.ActiveTerm(term + 1) {
		select {
		case <-limit:
			t.Fatal("idle grant was not withdrawn by its timer")
		case <-time.After(time.Millisecond):
		}
	}
	if h.Confirm(time.Now(), Grant{Epoch: term, Expires: 400, Remaining: time.Minute}) {
		t.Fatal("delayed lower term reactivated after timeout")
	}
	if !h.Confirm(time.Now(), Grant{Epoch: term + 2, Expires: 500, Remaining: time.Minute}) || !h.ActiveTerm(term+2) {
		t.Fatal("confirmed new term unavailable")
	}
}
