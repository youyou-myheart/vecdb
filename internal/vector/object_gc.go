package vector

import (
	"context"
	"encoding/json"
	"errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"strings"
)

type trackedObject struct {
	Manifest    Manifest `json:"manifest"`
	State       string   `json:"state"`
	Expires     int64    `json:"expires_ms"`
	NextAttempt int64    `json:"next_attempt_ms"`
}

func (j graphJournal) objectKey(m Manifest) []byte { return j.key("objects/" + m.Generation) }

// Intent precedes upload. Expiry permits collection, but adoption and retirement
// still conflict on this record, so a GC tombstone cannot become live again.
func (j graphJournal) stageObject(ctx context.Context, r *Repository, m Manifest) error {
	if !strings.HasPrefix(m.Key, r.Namespace()+"snapshots/") || m.Generation == "" {
		return errors.New("object outside index namespace")
	}
	tx, e := r.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = j.c.ReplicaFence(j.id, j.owner)(ctx, tx); e != nil {
		return e
	}
	_, e = tx.Get(ctx, j.objectKey(m))
	if e == nil {
		return ErrConflict
	}
	if !tikverr.IsErrNotFound(e) {
		return e
	}
	if e = putJSON(tx, j.objectKey(m), trackedObject{Manifest: m, State: "staged", Expires: txnMillis(tx) + 3600000}); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (j graphJournal) adoptObject(ctx context.Context, tx transactionAPI, m Manifest) error {
	var o trackedObject
	e := readJSON(ctx, tx, j.objectKey(m), &o)
	// Older binaries produced unregistered objects. Keep these objects forever;
	// the collector never scans or guesses about unrelated S3 keys.
	if tikverr.IsErrNotFound(e) {
		return nil
	}
	if e != nil {
		return e
	}
	if o.Manifest.Key != m.Key || o.Manifest.SHA256 != m.SHA256 || o.Manifest.Size != m.Size {
		return errors.New("object intent mismatch")
	}
	if o.State != "staged" || o.Expires <= txnMillis(tx) {
		return ErrFenced
	}
	o.State = "live"
	o.Manifest = m
	return putJSON(tx, j.objectKey(m), o)
}
func (j graphJournal) retireObject(ctx context.Context, tx transactionAPI, r *Repository, m Manifest) error {
	var o trackedObject
	e := readJSON(ctx, tx, j.objectKey(m), &o)
	if tikverr.IsErrNotFound(e) {
		return nil
	}
	if e != nil {
		return e
	}
	if o.State != "live" || o.Manifest.Key != m.Key || !strings.HasPrefix(m.Key, r.Namespace()+"snapshots/") {
		return errors.New("invalid object retirement")
	}
	o.State = "garbage"
	o.NextAttempt = 0
	return putJSON(tx, j.objectKey(m), o)
}

// Permanent tombstones prevent re-adoption and allow repeated deletion of late
// uploads. A bounded rotating scan avoids starving new intents behind live ones.
func (j graphJournal) CollectObjects(ctx context.Context, r *Repository, o graphBlobs, limit int) error {
	if limit < 1 || limit > 256 {
		return ErrInvalidSubscription
	}
	tx, e := r.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = j.c.ReplicaFence(j.id, j.owner)(ctx, tx); e != nil {
		return e
	}
	prefix := string(j.key("objects/"))
	cursorKey := j.key("object-sweep-cursor")
	cursor, e := tx.Get(ctx, cursorKey)
	if e != nil && !tikverr.IsErrNotFound(e) {
		return e
	}
	start := []byte(prefix)
	if len(cursor) > 0 {
		if !strings.HasPrefix(string(cursor), prefix) {
			return errors.New("invalid object sweep cursor")
		}
		start = append(append([]byte{}, cursor...), 0)
	}
	it, e := tx.Iter(start, j.key("objects0"))
	if e != nil {
		return e
	}
	defer it.Close()
	var remove []string
	var last []byte
	count := 0
	for it.Valid() && count < limit {
		key := append([]byte{}, it.Key()...)
		var object trackedObject
		if e = json.Unmarshal(it.Value(), &object); e != nil {
			return e
		}
		if !strings.HasPrefix(object.Manifest.Key, r.Namespace()+"snapshots/") {
			return errors.New("invalid tracked object prefix")
		}
		now := txnMillis(tx)
		if object.State == "staged" && object.Expires <= now {
			object.State = "garbage"
		}
		if object.State == "garbage" && object.NextAttempt <= now {
			object.NextAttempt = now + 60000
			if e = putJSON(tx, key, object); e != nil {
				return e
			}
			remove = append(remove, object.Manifest.Key)
		}
		last = key
		count++
		if e = it.Next(); e != nil {
			return e
		}
	}
	if !it.Valid() {
		last = nil
	}
	if len(last) == 0 {
		e = tx.Delete(cursorKey)
	} else {
		e = tx.Set(cursorKey, last)
	}
	if e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	// Only persisted garbage markers can authorize deletion. Failure leaves the
	// tombstone eligible for another sweep after the retry interval.
	for _, key := range remove {
		if e = o.removeObject(ctx, key); e != nil {
			return e
		}
	}
	return nil
}
