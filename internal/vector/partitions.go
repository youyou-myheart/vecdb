package vector

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/google/uuid"
	tikverr "github.com/tikv/client-go/v2/error"
)

type DataPartitionID int
type IndexPartitionID string
type PartitionBinding struct {
	Data  DataPartitionID  `json:"data_partition"`
	Index IndexPartitionID `json:"index_partition"`
}
type PartitionLayout struct {
	Routing        *ClusterRouting    `json:"routing,omitempty"`
	Version        int                `json:"version"`
	Epoch          uint64             `json:"epoch"`
	Hash           string             `json:"hash"`
	DataPartitions int                `json:"data_partitions"`
	Bindings       []PartitionBinding `json:"bindings"`
}

// These are separate decisions even while the first index-placement policy is 1:1.
type PartitionRouter interface {
	DataForID(string) DataPartitionID
	IndexForRecord(DataPartitionID, Record) (IndexPartitionID, error)
	QueryIndexes(Search) []IndexPartitionID
}
type IdentityPartitionRouter struct{ layout PartitionLayout }

func newLayout(n int) (PartitionLayout, error) {
	if n < 1 || n > 64 {
		return PartitionLayout{}, errors.New("PARTITIONS must be 1..64")
	}
	l := PartitionLayout{Version: 1, Epoch: 1, Hash: "sha256-be64-v1", DataPartitions: n}
	for j := 0; j < n; j++ {
		l.Bindings = append(l.Bindings, PartitionBinding{DataPartitionID(j), IndexPartitionID(fmt.Sprintf("index-%04d", j))})
	}
	return l, nil
}
func (r IdentityPartitionRouter) DataForID(id string) DataPartitionID {
	h := sha256.Sum256([]byte(id))
	return DataPartitionID(binary.BigEndian.Uint64(h[:8]) % uint64(r.layout.DataPartitions))
}
func (r IdentityPartitionRouter) IndexForRecord(data DataPartitionID, _ Record) (IndexPartitionID, error) {
	for _, b := range r.layout.Bindings {
		if b.Data == data {
			return b.Index, nil
		}
	}
	return "", errors.New("data partition has no index binding")
}
func (r IdentityPartitionRouter) QueryIndexes(_ Search) []IndexPartitionID {
	out := make([]IndexPartitionID, len(r.layout.Bindings))
	for j, b := range r.layout.Bindings {
		out[j] = b.Index
	}
	return out
}
func (r *Repository) ensureLayout(ctx context.Context, n int) (PartitionLayout, error) {
	wanted, e := newLayout(n)
	if e != nil {
		return wanted, e
	}
	tx, e := r.begin()
	if e != nil {
		return wanted, e
	}
	defer tx.Rollback()
	key := []byte(r.Prefix + "partition-layout")
	b, e := tx.Get(ctx, key)
	if e == nil {
		var stored PartitionLayout
		if e = json.Unmarshal(b, &stored); e != nil {
			return wanted, e
		}
		if !reflect.DeepEqual(stored, wanted) {
			return wanted, errors.New("partition layout mismatch; repartitioning requires explicit migration")
		}
		return stored, nil
	}
	if !tikverr.IsErrNotFound(e) {
		return wanted, e
	}
	if n > 1 {
		it, err := tx.Iter([]byte(r.Prefix+"record/"), []byte(r.Prefix+"record0"))
		if err != nil {
			return wanted, err
		}
		hasRecords := it.Valid()
		it.Close()
		head, err := r.logHead(ctx, tx)
		if err != nil {
			return wanted, err
		}
		_, err = tx.Get(ctx, []byte(r.Prefix+"manifest"))
		hasManifest := err == nil
		if err != nil && !tikverr.IsErrNotFound(err) {
			return wanted, err
		}
		if hasRecords || head != 0 || hasManifest {
			return wanted, errors.New("existing unpartitioned namespace must use PARTITIONS=1")
		}
	}
	if n > 1 {
		if e = tx.Set([]byte(r.Prefix+"schema"), []byte(fmt.Sprintf("v3:dimension=%d:metric=l2:partition-layout=1", r.Dimension)+dataLogSchemaSuffix)); e != nil {
			return wanted, e
		}
	}
	b, _ = json.Marshal(wanted)
	if e = tx.Set(key, b); e != nil {
		return wanted, e
	}
	return wanted, tx.Commit(ctx)
}

type PartitionIndex interface {
	Sync(context.Context) error
	Search(Search) ([]Hit, error)
	Checkpoint(context.Context) (Manifest, error)
	Rebuild(context.Context) error
	MaybeRebuild(context.Context) error
	Prune(context.Context) (uint64, error)
}

var ErrPartitionUnavailable = errors.New("index partition unavailable")

type PartitionedService struct {
	router    PartitionRouter
	layout    PartitionLayout
	data      map[DataPartitionID]*Repository
	indexes   map[IndexPartitionID]PartitionIndex
	dimension int
}

func OpenPartitionedService(ctx context.Context, root *Repository, objects ObjectAPI, count int) (*PartitionedService, error) {
	return OpenPartitionedServiceWithIndexPolicy(ctx, root, objects, count, IndexPolicy{})
}

func OpenPartitionedServiceWithIndexPolicy(ctx context.Context, root *Repository, objects ObjectAPI, count int, policy IndexPolicy) (*PartitionedService, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	policy = policy.clone()
	if err := root.ensureMode(ctx, false); err != nil {
		return nil, err
	}
	layout, err := root.ensureLayout(ctx, count)
	if err != nil {
		return nil, err
	}
	p := &PartitionedService{layout: layout, router: IdentityPartitionRouter{layout}, data: map[DataPartitionID]*Repository{}, indexes: map[IndexPartitionID]PartitionIndex{}, dimension: root.Dimension}
	for _, b := range layout.Bindings {
		data := *root
		data.collectionPrefix = root.Prefix
		if count > 1 {
			data.Prefix = fmt.Sprintf("%sdata/%04d/", root.Prefix, b.Data)
			if err = data.ensureSchema(ctx); err != nil {
				return nil, err
			}
		}
		// Index ownership is resolved via the persisted binding, never by hashing again.
		indexID, err := p.router.IndexForRecord(b.Data, Record{})
		if err != nil {
			return nil, err
		}
		indexRepo := data
		if count > 1 {
			indexRepo.IndexPrefix = fmt.Sprintf("%sindex/%s/", root.Prefix, indexID)
		}
		service, err := openIndexService(ctx, &indexRepo, objects, policy, nil)
		if err != nil {
			return nil, err
		}
		p.data[b.Data] = &data
		p.indexes[indexID] = service
	}
	return p, nil
}
func (p *PartitionedService) Layout() PartitionLayout {
	l := p.layout
	l.Bindings = append([]PartitionBinding(nil), l.Bindings...)
	if l.Routing != nil {
		r := *l.Routing
		r.Centers = make([][]float64, len(l.Routing.Centers))
		for i, v := range l.Routing.Centers {
			r.Centers[i] = append([]float64(nil), v...)
		}
		l.Routing = &r
	}
	return l
}
func (p *PartitionedService) Get(ctx context.Context, id string) (Record, error) {
	return p.data[p.router.DataForID(id)].Get(ctx, id)
}
func (p *PartitionedService) Mutate(ctx context.Context, id string, m Mutation, deleted bool) (Record, error) {
	return p.data[p.router.DataForID(id)].Mutate(ctx, id, m, deleted)
}
func (p *PartitionedService) each(ctx context.Context, fn func(PartitionIndex) error) error {
	ids := p.router.QueryIndexes(Search{})
	errs := make([]error, len(ids))
	var wg sync.WaitGroup
	for j, id := range ids {
		wg.Add(1)
		go func(j int, id IndexPartitionID) {
			defer wg.Done()
			if e := ctx.Err(); e != nil {
				errs[j] = e
				return
			}
			index, ok := p.indexes[id]
			if !ok {
				errs[j] = ErrPartitionUnavailable
				return
			}
			errs[j] = fn(index)
		}(j, id)
	}
	wg.Wait()
	for j, e := range errs {
		if e != nil {
			return fmt.Errorf("%w: %s: %w", ErrPartitionUnavailable, ids[j], e)
		}
	}
	return nil
}
func (p *PartitionedService) Sync(ctx context.Context) error {
	return p.each(ctx, func(i PartitionIndex) error { return i.Sync(ctx) })
}
func (p *PartitionedService) Rebuild(ctx context.Context) error {
	return p.each(ctx, func(i PartitionIndex) error { return i.Rebuild(ctx) })
}
func (p *PartitionedService) MaybeRebuild(ctx context.Context) error {
	return p.each(ctx, func(i PartitionIndex) error { return i.MaybeRebuild(ctx) })
}
func (p *PartitionedService) Search(q Search) ([]Hit, error) {
	return p.SearchContext(context.Background(), q)
}
func (p *PartitionedService) SearchContext(ctx context.Context, q Search) ([]Hit, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPartitionUnavailable, err)
	}
	if e := validateQuery(q, p.dimension); e != nil {
		return nil, e
	}
	if q.EFSearch < 0 || q.EFSearch > 4096 {
		return nil, errors.New("ef_search must be 0..4096")
	}
	ids, err := p.queryIndexes(q)
	if err != nil {
		return nil, err
	}
	results := make([][]Hit, len(ids))
	errs := make([]error, len(ids))
	var wg sync.WaitGroup
	var visibilityIndex IndexPartitionID
	if q.MinVisible != nil && q.visibilityAll {
		// The deleted ID has no single vector destination.
	} else if q.MinVisible != nil && q.visibilityIndex != "" {
		visibilityIndex = q.visibilityIndex
	} else if q.MinVisible != nil {
		var e error
		record := Record{ID: q.MinVisible.ID}
		if p.layout.Routing != nil {
			record, e = p.Get(ctx, q.MinVisible.ID)
			if e != nil {
				return nil, e
			}
		}
		visibilityIndex, e = p.router.IndexForRecord(p.router.DataForID(q.MinVisible.ID), record)
		if e != nil {
			return nil, e
		}
	}
	for j, id := range ids {
		wg.Add(1)
		go func(j int, id IndexPartitionID) {
			defer wg.Done()
			index, ok := p.indexes[id]
			if !ok {
				errs[j] = ErrPartitionUnavailable
				return
			}
			local := q
			if id != visibilityIndex && !q.visibilityAll {
				local.MinVisible = nil
			}
			if searcher, ok := index.(interface {
				SearchContext(context.Context, Search) ([]Hit, error)
			}); ok {
				results[j], errs[j] = searcher.SearchContext(ctx, local)
			} else {
				results[j], errs[j] = index.Search(local)
			}
		}(j, id)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w", ErrPartitionUnavailable, ctx.Err())
	case <-done:
	}
	all := make([]Hit, 0, len(ids)*q.K)
	for j, e := range errs {
		if e != nil {
			if errors.Is(e, ErrNotVisible) {
				return nil, ErrNotVisible
			}
			return nil, fmt.Errorf("%w: %s: %w", ErrPartitionUnavailable, ids[j], e)
		}
		all = append(all, results[j]...)
	}
	all = deduplicateHits(all)
	sort.Slice(all, func(a, b int) bool {
		if all[a].Distance == all[b].Distance {
			return all[a].ID < all[b].ID
		}
		return all[a].Distance < all[b].Distance
	})
	if len(all) > q.K {
		all = all[:q.K]
	}
	return all, nil
}

// Each child snapshot commits independently. A failed aggregate does not undo
// already published children, and is never reported as an atomic global snapshot.
func (p *PartitionedService) Checkpoint(ctx context.Context) (Manifest, error) {
	out := Manifest{Format: "partition-set-v1", Generation: uuid.NewString(), Dimension: p.dimension, Partitions: map[string]Manifest{}}
	for _, id := range p.router.QueryIndexes(Search{}) {
		m, e := p.indexes[id].Checkpoint(ctx)
		if e != nil {
			return Manifest{}, fmt.Errorf("%w: %s: %w", ErrPartitionUnavailable, id, e)
		}
		out.Partitions[string(id)] = m
	}
	if len(out.Partitions) == 1 {
		for _, m := range out.Partitions {
			return m, nil
		}
	}
	return out, nil
}
func (p *PartitionedService) PruneState(ctx context.Context) (any, error) {
	floors := map[string]uint64{}
	for _, id := range p.router.QueryIndexes(Search{}) {
		floor, e := p.indexes[id].Prune(ctx)
		if e != nil {
			return nil, e
		}
		floors[string(id)] = floor
	}
	if len(floors) == 1 {
		for _, floor := range floors {
			return map[string]uint64{"retained_after": floor}, nil
		}
	}
	return map[string]any{"partitions": floors}, nil
}

func (p *PartitionedService) VectorDimension() int { return p.dimension }

var ErrInvalidSearchPartitions = errors.New("invalid search partitions")

func (p *PartitionedService) queryIndexes(q Search) ([]IndexPartitionID, error) {
	ids := p.router.QueryIndexes(q)
	if len(q.IndexPartitions) == 0 {
		return ids, nil
	}
	if len(q.IndexPartitions) > len(ids) || q.MinVisible != nil {
		return nil, ErrInvalidSearchPartitions
	}
	allowed := make(map[IndexPartitionID]bool, len(ids))
	for _, id := range ids {
		allowed[id] = true
	}
	seen := make(map[IndexPartitionID]bool)
	for _, id := range q.IndexPartitions {
		if !allowed[id] || seen[id] {
			return nil, ErrInvalidSearchPartitions
		}
		seen[id] = true
	}
	return q.IndexPartitions, nil
}

// A move can be visible on two lagging shards. Prefer the newest returned
// revision, then distance; this does not make an eventual view authoritative.
func deduplicateHits(hits []Hit) []Hit {
	best := make(map[string]Hit, len(hits))
	for _, h := range hits {
		old, ok := best[h.ID]
		if !ok || h.Revision > old.Revision || h.Revision == old.Revision && h.Distance < old.Distance {
			best[h.ID] = h
		}
	}
	out := make([]Hit, 0, len(best))
	for _, h := range best {
		out = append(out, h)
	}
	return out
}
