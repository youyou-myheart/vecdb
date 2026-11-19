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
