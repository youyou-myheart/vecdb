package vector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

var ErrConflict = errors.New("revision or idempotency conflict")
var ErrNotFound = errors.New("not found")

type Record struct {
	ID       string            `json:"id"`
	Revision uint64            `json:"revision"`
	Vector   []float64         `json:"vector,omitempty"`
	Payload  map[string]string `json:"payload,omitempty"`
	Deleted  bool              `json:"deleted"`
}
type Mutation struct {
	RequestID        string            `json:"request_id"`
	Vector           []float64         `json:"vector,omitempty"`
	Payload          map[string]string `json:"payload,omitempty"`
	ExpectedRevision *uint64           `json:"expected_revision,omitempty"`
}
type receipt struct {
	Digest string `json:"digest"`
	Record Record `json:"record"`
}
type Repository struct {
	Retain        func(context.Context, transactionAPI, uint64) (uint64, error)
	Fence         func(context.Context, transactionAPI) error
	ReceiptPrefix string
	IndexPrefix   string
	Client        *txnkv.Client
	Prefix        string
	Dimension     int
	beginTxn      func() (transactionAPI, error)
	scanLimit     int
}

func NewRepository(pd []string, namespace string, dimension int) (*Repository, error) {
	if dimension < 1 || dimension > 4096 || namespace == "" {
		return nil, errors.New("invalid collection configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := txnkv.NewClient(pd)
	if err != nil {
		return nil, err
	}
	r := &Repository{Client: c, Prefix: "vector-v0/" + hex.EncodeToString([]byte(namespace)) + "/", Dimension: dimension}
	if err = r.ensureSchema(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return r, nil
}
func (r *Repository) Namespace() string {
	if r.IndexPrefix != "" {
		return r.IndexPrefix
	}
	return r.Prefix
}
func (r *Repository) VectorDimension() int { return r.Dimension }
func (r *Repository) begin() (transactionAPI, error) {
	if r.beginTxn != nil {
		return r.beginTxn()
	}
	tx, err := r.Client.Begin()
	if err != nil {
		return nil, err
	}
	return tikvTransaction{tx}, nil
}
func (r *Repository) ensureSchema(ctx context.Context) error {
	tx, err := r.begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	key := []byte(r.Prefix + "schema")
	b, err := tx.Get(ctx, key)
	schema := []byte(fmt.Sprintf("v2:dimension=%d:metric=l2:log=1", r.Dimension))
	if tikverr.IsErrNotFound(err) {
		if err = tx.Set(key, schema); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	// Upgrade requires all old writers to be stopped before this binary opens the namespace.
	if string(b) == fmt.Sprintf("v1:dimension=%d:metric=l2", r.Dimension) {
		if err = tx.Set(key, schema); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if string(b) == fmt.Sprintf("v9:dimension=%d:metric=l2:load=1:tasks=1", r.Dimension) || string(b) == fmt.Sprintf("v8:dimension=%d:metric=l2:replicas=2:tasks=1", r.Dimension) {
		return nil
	}
	if string(b) == fmt.Sprintf("v7:dimension=%d:metric=l2:load=1", r.Dimension) {
		return nil
	}
	if string(b) == fmt.Sprintf("v6:dimension=%d:metric=l2:replicas=2", r.Dimension) {
		return nil
	}
	if string(b) == fmt.Sprintf("v5:dimension=%d:metric=l2:cluster-master=1", r.Dimension) {
		return nil
	}
	if string(b) == fmt.Sprintf("v4:dimension=%d:metric=l2:cluster=1", r.Dimension) {
		return nil
	}
	if string(b) == fmt.Sprintf("v3:dimension=%d:metric=l2:partition-layout=1", r.Dimension) {
		return nil
	}
	if string(b) != string(schema) {
		return errors.New("collection schema mismatch")
	}
	return nil
}

func (r *Repository) key(kind, id string) []byte {
	prefix := r.Prefix
	if kind == "request" && r.ReceiptPrefix != "" {
		prefix = r.ReceiptPrefix
	}
	return []byte(prefix + kind + "/" + hex.EncodeToString([]byte(id)))
}
func readRecord(ctx context.Context, tx transactionAPI, key []byte) (Record, error) {
	b, err := tx.Get(ctx, key)
	if tikverr.IsErrNotFound(err) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, err
	}
	var rec Record
	err = json.Unmarshal(b, &rec)
	return rec, err
}
func (r *Repository) Get(ctx context.Context, id string) (Record, error) {
	tx, e := r.begin()
	if e != nil {
		return Record{}, e
	}
	defer tx.Rollback()
	return readRecord(ctx, tx, r.key("record", id))
}
func (r *Repository) Mutate(ctx context.Context, id string, m Mutation, deleted bool) (Record, error) {
	for attempt := 0; ; attempt++ {
		rec, err := r.mutateOnce(ctx, id, m, deleted)
		// Only a definite write conflict is retried. Unknown commits retain the request ID contract.
		if !tikverr.IsErrWriteConflict(errors.Unwrap(err)) || attempt >= 15 {
			return rec, err
		}
		timer := time.NewTimer(time.Duration(min(attempt+1, 10)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Record{}, ctx.Err()
		case <-timer.C:
		}
	}
}
func (r *Repository) mutateOnce(ctx context.Context, id string, m Mutation, deleted bool) (Record, error) {
	if id == "" || len(id) > 256 || m.RequestID == "" || len(m.RequestID) > 256 {
		return Record{}, errors.New("id and request_id required, at most 256 bytes")
	}
	if !deleted {
		if err := ValidateVector(m.Vector, r.Dimension); err != nil {
			return Record{}, err
		}
	}
	body, marshalErr := json.Marshal(struct {
		ID      string
		M       Mutation
		Deleted bool
	}{id, m, deleted})
	if marshalErr != nil {
		return Record{}, marshalErr
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(body))
	tx, e := r.begin()
	if e != nil {
		return Record{}, e
	}
	defer tx.Rollback()
	reqKey := r.key("request", m.RequestID)
	b, e := tx.Get(ctx, reqKey)
	if e == nil {
		var old receipt
		if e = json.Unmarshal(b, &old); e != nil {
			return Record{}, e
		}
		if old.Digest != digest {
			return Record{}, ErrConflict
		}
		return old.Record, nil
	}
	if !tikverr.IsErrNotFound(e) {
		return Record{}, e
	}
	old, e := readRecord(ctx, tx, r.key("record", id))
	if e != nil && !errors.Is(e, ErrNotFound) {
		return Record{}, e
	}
	if m.ExpectedRevision != nil && *m.ExpectedRevision != old.Revision {
		return Record{}, ErrConflict
	}
	if old.Revision == ^uint64(0) {
		return Record{}, errors.New("record revision exhausted")
	}
	rec := Record{ID: id, Revision: old.Revision + 1, Vector: m.Vector, Payload: m.Payload, Deleted: deleted}
	if deleted {
		rec.Vector = nil
		rec.Payload = nil
	}
	encoded, _ := json.Marshal(rec)
	ack, _ := json.Marshal(receipt{Digest: digest, Record: rec})
	head, e := r.readCounter(ctx, tx, "log-head")
	if e != nil {
		return Record{}, e
	}
	if head == ^uint64(0) {
		return Record{}, errors.New("log sequence exhausted")
	}
	sequence := head + 1
	event, _ := json.Marshal(LogEvent{sequence, rec})
	counter, _ := json.Marshal(sequence)
	for k, v := range map[string][]byte{string(r.key("record", id)): encoded, string(r.logKey(sequence)): event, r.Prefix + "log-head": counter, string(reqKey): ack} {
		if e = tx.Set([]byte(k), v); e != nil {
			return Record{}, e
		}
	}
	// Commit errors are not blindly retried: caller retries with the same request ID.
	if e = tx.Commit(ctx); e != nil {
		return Record{}, fmt.Errorf("commit not confirmed; retry same request_id: %w", e)
	}
	return rec, nil
}
func (r *Repository) Scan(ctx context.Context, kind string) ([]Record, error) {
	tx, e := r.begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	return r.scanTxn(ctx, tx, kind)
}
func (r *Repository) scanTxn(ctx context.Context, tx transactionAPI, kind string) ([]Record, error) {
	var e error
	prefix := r.Prefix + kind + "/"
	it, e := tx.Iter([]byte(prefix), []byte(r.Prefix+kind+"0"))
	if e != nil {
		return nil, e
	}
	defer it.Close()
	records := []Record{}
	for it.Valid() {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		limit := r.scanLimit
		if limit == 0 {
			limit = 100000
		}
		if len(records) >= limit {
			return nil, errors.New("prototype scan limit reached; no partial view published")
		}
		var rec Record
		if e = json.Unmarshal(it.Value(), &rec); e != nil {
			return nil, e
		}
		records = append(records, rec)
		if e = it.Next(); e != nil {
			return nil, e
		}
	}
	return records, nil
}

type Manifest struct {
	Partitions map[string]Manifest `json:"partitions,omitempty"`
	LogVersion int                 `json:"log_version,omitempty"`
	Offset     uint64              `json:"offset,omitempty"`
	Format     string              `json:"format,omitempty"`
	Generation string              `json:"generation"`
	Key        string              `json:"key"`
	SHA256     string              `json:"sha256"`
	Size       int64               `json:"size"`
	Dimension  int                 `json:"dimension"`
}

func (r *Repository) Manifest(ctx context.Context) (*Manifest, error) {
	tx, e := r.begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	b, e := tx.Get(ctx, []byte(r.Namespace()+"manifest"))
	if tikverr.IsErrNotFound(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var m Manifest
	e = json.Unmarshal(b, &m)
	return &m, e
}
func (r *Repository) Publish(ctx context.Context, expected string, m Manifest) error {
	tx, e := r.begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if r.Fence != nil {
		if e = r.Fence(ctx, tx); e != nil {
			return e
		}
	}
	key := []byte(r.Namespace() + "manifest")
	b, e := tx.Get(ctx, key)
	actual := ""
	if e == nil {
		var old Manifest
		if e = json.Unmarshal(b, &old); e != nil {
			return e
		}
		actual = old.Generation
		if old.LogVersion == LogVersion && (m.LogVersion != LogVersion || m.Offset < old.Offset) {
			return ErrConflict
		}
	} else if !tikverr.IsErrNotFound(e) {
		return e
	}
	if actual != expected {
		return ErrConflict
	}
	if m.LogVersion != 0 {
		head, err := r.readCounter(ctx, tx, "log-head")
		if err != nil {
			return err
		}
		floor, err := r.readCounter(ctx, tx, "log-floor")
		if err != nil {
			return err
		}
		if m.LogVersion != LogVersion || m.Offset > head || m.Offset < floor || m.Format != graphFormat {
			return errors.New("invalid checkpoint cursor")
		}
	}
	b, e = json.Marshal(m)
	if e != nil {
		return e
	}
	if e = tx.Set(key, b); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

type iteratorAPI interface {
	Valid() bool
	Value() []byte
	Key() []byte
	Next() error
	Close()
}
type transactionAPI interface {
	StartTS() uint64
	Get(context.Context, []byte) ([]byte, error)
	Set([]byte, []byte) error
	Delete([]byte) error
	Commit(context.Context) error
	Rollback() error
	Iter([]byte, []byte) (iteratorAPI, error)
}
type tikvTransaction struct{ *transaction.KVTxn }

func (t tikvTransaction) Iter(start, end []byte) (iteratorAPI, error) {
	return t.KVTxn.Iter(start, end)
}
