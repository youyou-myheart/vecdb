package vector

import (
	"context"

	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"example.com/vector-storage/internal/shardlog"
	"fmt"
	"sync"

	"github.com/tikv/client-go/v2/rawkv"
)

type rawDataClient interface {
	Get(context.Context, []byte) ([]byte, error)
	BatchPut(context.Context, [][]byte, [][]byte) error
	CompareAndSwap(context.Context, []byte, []byte, []byte) ([]byte, bool, error)
	BatchGet(context.Context, [][]byte) ([][]byte, error)
	Close() error
}

type tikvRawDataClient struct{ *rawkv.Client }

func (c tikvRawDataClient) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.Client.Get(ctx, key)
}

func (c tikvRawDataClient) BatchPut(ctx context.Context, keys, values [][]byte) error {
	return c.Client.BatchPut(ctx, keys, values)
}

func (c tikvRawDataClient) CompareAndSwap(ctx context.Context, key, old, next []byte) ([]byte, bool, error) {
	return c.Client.CompareAndSwap(ctx, key, old, next)
}

func (c tikvRawDataClient) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	return c.Client.BatchGet(ctx, keys)
}

type rawDataStore struct {
	client    rawDataClient
	namespace string
	local     sync.Map // logical partition prefix -> current local mutationGroup
	shards    sync.Map // logical partition prefix -> *rawReadCache
}

func newRawDataStore(client rawDataClient, namespace string) *rawDataStore {
	return &rawDataStore{client: client, namespace: namespace}
}

func openRawDataStore(ctx context.Context, pd []string, namespace string) (*rawDataStore, error) {
	if len(pd) == 0 {
		return nil, errors.New("raw data backend requires PD endpoints")
	}
	client, err := rawkv.NewClientWithOpts(ctx, pd)
	if err != nil {
		return nil, err
	}
	return newRawDataStore(tikvRawDataClient{client.SetAtomicForCAS(true)}, namespace), nil
}

func (s *rawDataStore) Close() error { return s.client.Close() }

func (s *rawDataStore) shard(prefix string) rawShardStore {
	return rawShardStore{client: s.client, prefix: "vector-raw-v1/" + hex.EncodeToString([]byte(prefix)) + "/"}
}

type rawShardStore struct {
	client rawDataClient
	prefix string
}

// Fence is the most recently claimed owner. Until Active catches up, readers
// must retry: the new owner is sealing the previous epoch's accepted prefix.
type rawShardHead struct {
	Fence     uint64 `json:"fence"`
	Session   string `json:"session"`
	Active    uint64 `json:"active"`
	Seal      string `json:"seal,omitempty"`
	Base      uint64 `json:"base"`
	Published uint64 `json:"published"`
}

func (h rawShardHead) sameEpoch(other rawShardHead) bool {
	h.Published, other.Published = 0, 0
	return h == other
}

type rawSealedEpoch struct {
	Previous string `json:"previous,omitempty"`
	Epoch    uint64 `json:"epoch"`
	Batches  uint64 `json:"batches"`
	End      uint64 `json:"end"`
}

func (s rawShardStore) headKey() []byte { return []byte(s.prefix + "head") }

func (s rawShardStore) epochPrefix(epoch uint64) string {
	return fmt.Sprintf("%swal/%020d/", s.prefix, epoch)
}

func (s rawShardStore) batchKey(epoch, number uint64) []byte {
	return shardlog.Stream(s.epochPrefix(epoch)).Key(number)
}

func (s rawShardStore) sealKey(digest string) []byte { return []byte(s.prefix + "seals/" + digest) }

func (s rawShardStore) head(ctx context.Context) (rawShardHead, []byte, error) {
	value, err := s.client.Get(ctx, s.headKey())
	if err != nil || value == nil {
		return rawShardHead{}, value, err
	}
	var h rawShardHead
	if err = json.Unmarshal(value, &h); err != nil {
		return h, nil, err
	}
	if h.Fence == 0 || h.Session == "" || h.Active > h.Fence || h.Published < h.Base {
		return h, nil, errors.New("invalid raw data partition head")
	}
	return h, value, nil
}

func (s rawShardStore) swapHead(ctx context.Context, old []byte, h rawShardHead) (bool, error) {
	next, err := json.Marshal(h)
	if err != nil {
		return false, err
	}
	_, swapped, err := s.client.CompareAndSwap(ctx, s.headKey(), old, next)
	return swapped, err
}

func (s rawShardStore) put(ctx context.Context, key, val []byte) error {
	return s.client.BatchPut(ctx, [][]byte{key}, [][]byte{val})
}

// WAL keys are consecutive and immutable. BatchGet returns values in key
// order, with nil for an absent key. A later slot is unusable until the gap
// closes, even if the two keys were read from different TiKV Regions.
func (s rawShardStore) readEpoch(ctx context.Context, h *rawShardHistory, epoch, after, through uint64) (uint64, error) {
	const pageSize = 4
	for after < through && ctx.Err() == nil {
		values, err := shardlog.Stream(s.epochPrefix(epoch)).Tail(ctx, s.client.BatchGet, after, through, pageSize, false)
		if err != nil {
			return after, err
		}
		if len(values) == 0 {
			break
		}
		page := make([]mutationLogBatch, 0, len(values))
		for i, value := range values {
			var batch mutationLogBatch
			if err = json.Unmarshal(value, &batch); err != nil {
				return after, err
			}
			if batch.Number != after+uint64(i)+1 {
				return after, errors.New("raw batch number mismatch")
			}
			page = append(page, batch)
		}
		if err = h.extend(page); err != nil {
			return after, err
		}
		after += uint64(len(page))
		// On a partially filled page, probe again starting at the hole. A
		// cross-Region BatchGet could have observed that hole before it saw
		// an earlier slot. An absent first slot now certifies the whole prefix.
	}
	return after, ctx.Err()
}

func (s rawShardStore) loadSealed(ctx context.Context, h rawShardHead) (*rawShardHistory, error) {
	history := newRawShardHistory()
	var chain []rawSealedEpoch
	for digest, epoch, end := h.Seal, h.Active, h.Base; digest != ""; {
		val, err := s.client.Get(ctx, s.sealKey(digest))
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(val)
		if hex.EncodeToString(sum[:]) != digest {
			return nil, errors.New("raw data seal missing or corrupt")
		}
		var seal rawSealedEpoch
		if err = json.Unmarshal(val, &seal); err != nil {
			return nil, err
		}
		if seal.Epoch == 0 || seal.Epoch >= epoch || seal.Batches == 0 || seal.End == 0 || seal.End > end {
			return nil, errors.New("invalid raw data seal")
		}
		chain = append(chain, seal)
		digest, epoch, end = seal.Previous, seal.Epoch, seal.End-1
	}
	for i := len(chain) - 1; i >= 0; i-- {
		seal := chain[i]
		count, err := s.readEpoch(ctx, history, seal.Epoch, 0, seal.Batches)
		if err != nil {
			return nil, err
		}
		if count != seal.Batches || history.end() != seal.End {
			return nil, errors.New("sealed raw epoch has missing batches")
		}
	}
	if history.end() != h.Base {
		return nil, errors.New("raw data seal does not match persisted base")
	}
	return history, nil
}

type rawReadCache struct {
	mu      sync.Mutex
	head    rawShardHead
	history *rawShardHistory
	batches uint64
}

func (b *mutationGroup) readOwnedRecord(id string) (Record, error) {
	var before, after uint64
	if b.lease == nil || !b.lease.InLeaderLease(&before) || before != b.lease.owner.Epoch {
		return Record{}, ErrFenced
	}
	var record Record
	var found bool
	switch {
	case b.lease.wal != nil:
		record, found = b.lease.wal.record(id)
	case b.rawWriter != nil && before == b.rawWriter.owner.Epoch:
		history := b.rawWriter.history
		snap := rawShardSnapshot{history: history, end: history.end()}
		record, found = snap.record(id)
		record = cloneRecord(record)
	default:
		return Record{}, ErrFenced
	}
	if !b.lease.InLeaderLease(&after) || after != before {
		return Record{}, ErrFenced
	}
	if !found {
		return Record{}, ErrNotFound
	}
	return record, nil
}

func (b *mutationGroup) confirmedSnapshot() (*rawShardSnapshot, bool) {
	var before, after uint64
	if b.rawWriter == nil || !b.lease.InLeaderLease(&before) || before != b.rawWriter.owner.Epoch {
		return nil, false
	}
	history := b.rawWriter.history
	view := &rawShardSnapshot{history: history, end: history.end()}
	return view, b.lease.InLeaderLease(&after) && before == after
}

func (s *rawDataStore) localSnapshot(prefix string) (*rawShardSnapshot, bool) {
	if group, ok := s.local.Load(prefix); ok {
		return group.(*mutationGroup).confirmedSnapshot()
	}
	return nil, false
}

func (s *rawDataStore) snapshot(ctx context.Context, prefix string) (*rawShardSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if snap, ok := s.localSnapshot(prefix); ok {
		return snap, nil
	}
	shard := s.shard(prefix)
	entry, _ := s.shards.LoadOrStore(prefix, &rawReadCache{})
	cache := entry.(*rawReadCache)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	// Probe the cached epoch before reading the fence. If a successor has
	// sealed the epoch in the meantime, its seal determines the valid prefix.
	retriedPublished := false
	for changes := 0; changes < 3; {
		var tailErr error
		if cache.history != nil {
			cache.batches, tailErr = shard.readEpoch(ctx, cache.history, cache.head.Active, cache.batches, ^uint64(0))
			// A freshly discovered epoch with no WAL entries was empty at the
			// discovery read: its immutable first slot is still absent now.
			if changes > 0 && tailErr == nil && cache.batches == 0 && cache.head.Published == cache.head.Base {
				return &rawShardSnapshot{history: cache.history, end: cache.head.Base}, nil
			}
		}
		h, val, err := shard.head(ctx)
		if err != nil {
			return nil, err
		}
		if val == nil {
			cache.history = nil
			return &rawShardSnapshot{history: newRawShardHistory()}, nil
		}
		if h.Fence != h.Active {
			cache.history = nil
			return nil, fmt.Errorf("%w: raw data partition is recovering", ErrPartitionUnavailable)
		}
		if cache.history != nil && cache.head.sameEpoch(h) {
			if tailErr != nil {
				return nil, tailErr
			}
			end := cache.history.end()
			if end < cache.head.Published {
				return nil, errors.New("raw data partition is missing a published batch")
			}
			cache.head = h
			// Older writers can advance Published between the tail probe and
			// the fence read. Verify that new lower bound once. Concurrent
			// later advances become the lower bound for the next reader.
			if end < h.Published && !retriedPublished {
				retriedPublished = true
				continue
			}
			return &rawShardSnapshot{history: cache.history, end: end}, nil
		}
		history, err := shard.loadSealed(ctx, h)
		if err != nil {
			return nil, err
		}
		cache.head, cache.history, cache.batches = h, history, 0
		changes++
	}
	return nil, fmt.Errorf("%w: raw data partition epoch changed", ErrPartitionUnavailable)
}

func (s *rawDataStore) recover(ctx context.Context, prefix string, owner Ownership) (*rawShardWriter, error) {
	if owner.Epoch == 0 || owner.Session == "" {
		return nil, ErrFenced
	}
	shard := s.shard(prefix)
	for ctx.Err() == nil {
		h, val, err := shard.head(ctx)
		if err != nil {
			return nil, err
		}
		if h.Fence > owner.Epoch || h.Fence == owner.Epoch && h.Session != owner.Session {
			return nil, ErrFenced
		}
		if h.Fence < owner.Epoch {
			h.Fence, h.Session = owner.Epoch, owner.Session
			swapped, err := shard.swapHead(ctx, val, h)
			if err != nil {
				return nil, err
			}
			if !swapped {
				continue
			}
			// Reload the exact persisted fence before sealing its previous WAL.
			continue
		}
		history, err := shard.loadSealed(ctx, h)
		if err != nil {
			return nil, err
		}
		var count uint64
		if h.Active != 0 {
			count, err = shard.readEpoch(ctx, history, h.Active, 0, ^uint64(0))
			if err != nil {
				return nil, err
			}
		}
		end := history.end()
		if end < h.Published {
			return nil, errors.New("raw recovery would lose published batches")
		}
		if h.Active != owner.Epoch {
			if count != 0 {
				seal := rawSealedEpoch{Previous: h.Seal, Epoch: h.Active, Batches: count, End: end}
				encoded, err := json.Marshal(seal)
				if err != nil {
					return nil, err
				}
				sum := sha256.Sum256(encoded)
				h.Seal = hex.EncodeToString(sum[:])
				if err = shard.put(ctx, shard.sealKey(h.Seal), encoded); err != nil {
					return nil, err
				}
			}
			h.Active, h.Base, h.Published = owner.Epoch, end, end
			swapped, err := shard.swapHead(ctx, val, h)
			if err != nil {
				return nil, err
			}
			if !swapped {
				continue
			}
			count = 0
		}
		return newRawShardWriter(shard, owner, history, count), nil
	}
	return nil, ctx.Err()
}
