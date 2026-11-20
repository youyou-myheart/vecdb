// Package lease manages locally confirmed lease grants. A control loop must
// confirm a grant only after its durable transaction has returned success.
package lease

import (
	"sync"
	"sync/atomic"
	"time"
)

// Grant describes one confirmed term. Expires uses the store's clock, while
// Remaining is measured from the start of the grant transaction.
type Grant struct {
	Epoch     uint64
	Expires   int64
	Remaining time.Duration
}

type Snapshot struct {
	Epoch    uint64
	Expires  int64
	Deadline time.Time
}

// Handle has a usable zero value. It must not be copied after first use.
type Handle struct {
	mu      sync.Mutex
	retired bool
	last    *Snapshot // highest confirmed grant, retained after expiry
	timer   *time.Timer
	current atomic.Pointer[Snapshot]
}

// Confirm publishes a confirmed grant for its epoch. The clock is captured
// before beginning KV IO, so a slow response cannot extend local authority.
func (h *Handle) Confirm(started time.Time, grant Grant) bool {
	margin := max(50*time.Millisecond, grant.Remaining/10)
	deadline := started.Add(max(time.Duration(0), grant.Remaining-margin))
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.retired || grant.Epoch == 0 || !time.Now().Before(deadline) {
		return false
	}
	if prev := h.last; prev != nil {
		if grant.Epoch < prev.Epoch || grant.Epoch == prev.Epoch && grant.Expires < prev.Expires {
			return false
		}
	}
	if h.timer != nil {
		h.timer.Stop()
	}
	next := &Snapshot{Epoch: grant.Epoch, Expires: grant.Expires, Deadline: deadline}
	h.last = next
	h.current.Store(next)
	// A callback already running during renewal can withdraw only its own grant.
	h.timer = time.AfterFunc(time.Until(deadline), func() { h.current.CompareAndSwap(next, nil) })
	return true
}

// Retire permanently withdraws this handle. A delayed renewal cannot revive it.
func (h *Handle) Retire() {
	h.mu.Lock()
	h.retired = true
	if h.timer != nil {
		h.timer.Stop()
	}
	h.current.Store(nil)
	h.mu.Unlock()
}

// ActiveTerm reads the state published by the control loop and expiry timer.
func (h *Handle) ActiveTerm(term uint64) bool {
	s := h.current.Load()
	return s != nil && s.Epoch == term
}

// Snapshot returns the current grant only while its local monotonic deadline
// is live. Reading it never contacts the store.
func (h *Handle) Snapshot() (Snapshot, bool) {
	s := h.current.Load()
	if s == nil || !time.Now().Before(s.Deadline) {
		return Snapshot{}, false
	}
	return *s, true
}

// InLeaderLease checks local authority and, on success, writes the current
// epoch to epoch if non-nil. To validate work spanning two calls, require
// both calls to succeed and compare the returned epochs.
func (h *Handle) InLeaderLease(epoch *uint64) bool {
	snapshot, ok := h.Snapshot()
	if ok && epoch != nil {
		*epoch = snapshot.Epoch
	}
	return ok
}
