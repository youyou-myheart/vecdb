package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	tikverr "github.com/tikv/client-go/v2/error"
	"time"
)

func (r *Repository) ensureScheduler(ctx context.Context, mode string, cooldown time.Duration) error {
	tx, e := r.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	key := []byte(r.Prefix + "cluster/scheduler")
	wanted := struct {
		Mode     string `json:"mode"`
		Cooldown int64  `json:"cooldown_ms"`
	}{mode, cooldown.Milliseconds()}
	b, e := tx.Get(ctx, key)
	if e == nil {
		var old struct {
			Mode     string `json:"mode"`
			Cooldown int64  `json:"cooldown_ms"`
		}
		if e = json.Unmarshal(b, &old); e != nil {
			return e
		}
		if old != wanted {
			return errors.New("scheduler configuration mismatch")
		}
		return nil
	}
	if !tikverr.IsErrNotFound(e) {
		return e
	}
	if e = putJSON(tx, key, wanted); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

func (r *Repository) ensureLoadMode(ctx context.Context) error {
	tx, e := r.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	key := []byte(r.Prefix + "cluster-mode")
	b, e := tx.Get(ctx, key)
	if e == nil {
		if string(b) != "v6-load-tasks-2" {
			return errors.New("load scheduling requires a fresh namespace; cannot mix protocols")
		}
		return nil
	}
	if !tikverr.IsErrNotFound(e) {
		return e
	}
	if e = tx.Set(key, []byte("v6-load-tasks-2")); e != nil {
		return e
	}
	if e = tx.Set([]byte(r.Prefix+"schema"), []byte(fmt.Sprintf("v9:dimension=%d:metric=l2:load=1:tasks=1", r.Dimension))); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

func (r *Repository) ensureClusterMode(ctx context.Context, count int) error {
	if count == 1 {
		return r.ensureMode(ctx, true)
	}
	tx, e := r.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	key := []byte(r.Prefix + "cluster-mode")
	b, e := tx.Get(ctx, key)
	if e == nil {
		if string(b) != "v5-replica-tasks-2" {
			return errors.New("replica mode requires a fresh namespace; legacy mode cannot be mixed")
		}
		return nil
	}
	if !tikverr.IsErrNotFound(e) {
		return e
	}
	if e = tx.Set(key, []byte("v5-replica-tasks-2")); e != nil {
		return e
	}
	if e = tx.Set([]byte(r.Prefix+"schema"), []byte(fmt.Sprintf("v8:dimension=%d:metric=l2:replicas=2:tasks=1", r.Dimension))); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

// A distinct namespace protocol prevents independent rebuilders from corrupting
// graph-replicated shards during a rolling start with incompatible binaries.
func (r *Repository) ensureGraphMode(ctx context.Context, scheduler, domain string) error {
	return r.ensureGraphProtocol(ctx, scheduler, domain, false)
}
func (r *Repository) ensureGraphProtocol(ctx context.Context, scheduler, domain string, retention bool) error {
	tx, e := r.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	key := []byte(r.Prefix + "cluster-mode")
	want := "v8-graph-async-" + scheduler + "-" + domain + "-2"
	if retention {
		want = "v9-graph-retention-" + scheduler + "-" + domain + "-2"
	}
	b, e := tx.Get(ctx, key)
	if e == nil {
		if string(b) != want {
			return errors.New("graph replication requires a fresh namespace; cannot mix index protocols")
		}
		return nil
	}
	if !tikverr.IsErrNotFound(e) {
		return e
	}
	if e = tx.Set(key, []byte(want)); e != nil {
		return e
	}
	// Existing replica/task storage schema remains compatible; cluster-mode is the fence.
	if e = tx.Set([]byte(r.Prefix+"schema"), []byte(fmt.Sprintf("v8:dimension=%d:metric=l2:replicas=2:tasks=1", r.Dimension))); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
