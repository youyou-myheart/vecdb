package vector

import (
	"errors"
	"example.com/vector-storage/internal/ann"
	"maps"
	"slices"
	"sort"
)

var ErrGraphCorrupt = errors.New("graph replica requires recovery")

// GraphDelta is a reversible, versioned materialized mutation, not an insertion
// command. Followers never call addPoint or evaluate distances while replaying.
type GraphDelta struct {
	references []ReferenceChange
	Links      []ann.NodeLinks        `json:"links,omitempty"`
	Format     string                 `json:"format"`
	Dimension  int                    `json:"dimension"`
	Config     IndexConfig            `json:"config"`
	BeforeNext uint64                 `json:"before_next"`
	AfterNext  uint64                 `json:"after_next"`
	Records    []RecordChange         `json:"records"`
	Nodes      map[uint64]nodeVersion `json:"nodes"`
	Native     []byte                 `json:"native"`
}
type RecordChange struct {
	Before Record `json:"before"`
	After  Record `json:"after"`
}

const deltaFormat = "hnsw-materialized-v1"

// GenerateDelta leaves the visible index unchanged. Holding the index lock
// through rollback prevents reads from observing the speculative construction.
func (i *HNSWIndex) GenerateDelta(batch []Record) (GraphDelta, error) {
	if err := validateBatch(batch, i.dimension); err != nil {
		return GraphDelta{}, err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.poison != nil {
		return GraphDelta{}, i.poison
	}
	if i.rebuildDelta != nil {
		return GraphDelta{}, ErrConflict
	}
	d := GraphDelta{Format: deltaFormat, Dimension: i.dimension, Config: i.config, BeforeNext: i.next, Nodes: map[uint64]nodeVersion{}}
	before := map[string]Record{}
	for _, r := range batch {
		if _, ok := before[r.ID]; !ok {
			before[r.ID] = cloneRecord(i.records[r.ID])
		}
	}
	epoch := i.epoch
	if err := i.graph.BeginPatch(); err != nil {
		return d, err
	}
	if err := i.applyLocked(batch); err != nil {
		i.poison = errors.Join(ErrGraphCorrupt, err)
		return d, i.poison
	}
	native, err := i.graph.FinishPatch()
	if err != nil {
		i.poison = errors.Join(ErrGraphCorrupt, err)
		return d, i.poison
	}
	d.Native = native
	d.AfterNext = i.next
	for id, old := range before {
		if i.records[id].Revision > old.Revision {
			d.Records = append(d.Records, RecordChange{old, cloneRecord(i.records[id])})
		}
	}
	sort.Slice(d.Records, func(a, b int) bool { return d.Records[a].After.ID < d.Records[b].After.ID })
	for n := d.BeforeNext; n < d.AfterNext; {
		n++
		d.Nodes[n] = i.nodes[n]
		delete(i.nodes, n)
	}
	i.changeRecordsLocked(d.Records, true)
	i.next = d.BeforeNext
	i.epoch = epoch
	d.Links, err = ann.PatchLinks(native)
	return d, err
}
func (i *HNSWIndex) changeRecordsLocked(changes []RecordChange, reverse bool) {
	for _, change := range changes {
		from, to := change.Before, change.After
		if reverse {
			from, to = to, from
		}
		i.postings.remove(from)
		if from.Revision != 0 && !from.Deleted {
			i.live--
		}
		if to.Revision == 0 {
			delete(i.records, from.ID)
		} else {
			i.records[to.ID] = cloneRecord(to)
			i.postings.add(to)
			if !to.Deleted {
				i.live++
			}
		}
	}
}
func (i *HNSWIndex) ApplyDelta(d GraphDelta, reverse bool) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.applyDeltaLocked(d, reverse)
}
func (i *HNSWIndex) PrepareDelta(d GraphDelta) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if e := i.applyDeltaLocked(d, false); e != nil {
		return e
	}
	return i.applyDeltaLocked(d, true)
}
func (i *HNSWIndex) applyDeltaLocked(d GraphDelta, reverse bool) error {
	if d.references != nil {
		if reverse {
			return errors.New("referenced delta cannot be reversed before expansion")
		}
		var err error
		d, err = i.expandReferenceDelta(d)
		if err != nil {
			return err
		}
	}
	if i.poison != nil {
		return i.poison
	}
	if i.rebuildDelta != nil {
		return ErrConflict
	}
	if d.Format != deltaFormat || d.Dimension != i.dimension || d.Config != i.config || d.AfterNext < d.BeforeNext || d.AfterNext-d.BeforeNext != uint64(len(d.Nodes)) {
		return errors.New("graph delta header mismatch")
	}
	expected := d.BeforeNext
	if reverse {
		expected = d.AfterNext
	}
	if i.next != expected {
		return errors.New("graph delta base mismatch")
	}
	revisions := make(map[string]uint64, len(d.Records))
	liveNodes := map[string]int{}
	for n, meta := range d.Nodes {
		if n <= d.BeforeNext || n > d.AfterNext {
			return errors.New("invalid delta node")
		}
		liveNodes[meta.ID]++
	}
	for _, ch := range d.Records {
		r := ch.After
		if revisions[r.ID] != 0 || r.Revision <= ch.Before.Revision || (ch.Before.Revision != 0 && ch.Before.ID != r.ID) {
			return errors.New("invalid delta revision")
		}
		revisions[r.ID] = r.Revision
		if err := validateBatch([]Record{r}, i.dimension); err != nil {
			return err
		}
		old := ch.Before
		if reverse {
			old = ch.After
		}
		if !sameRecord(i.records[r.ID], old) {
			return errors.New("graph delta record base mismatch")
		}
		want := 1
		if r.Deleted {
			want = 0
		}
		if liveNodes[r.ID] != want {
			return errors.New("graph delta node/record mismatch")
		}
	}
	for n, meta := range d.Nodes {
		revision, ok := revisions[meta.ID]
		if !ok {
			return errors.New("graph delta orphan node")
		}
		if revision != meta.Revision || (reverse && i.nodes[n] != meta) {
			return errors.New("graph delta node version mismatch")
		}
	}
	if err := i.graph.ApplyPatch(d.Native, reverse); err != nil {
		i.poison = errors.Join(ErrGraphCorrupt, err)
		return i.poison
	}
	i.changeRecordsLocked(d.Records, reverse)
	if reverse {
		for n := range d.Nodes {
			delete(i.nodes, n)
		}
		i.next = d.BeforeNext
	} else {
		for n, m := range d.Nodes {
			i.nodes[n] = m
		}
		i.next = d.AfterNext
	}
	i.epoch++
	return nil
}

func sameRecord(a, b Record) bool {
	return a.ID == b.ID && a.Revision == b.Revision && a.Deleted == b.Deleted && slices.Equal(a.Vector, b.Vector) && maps.Equal(a.Payload, b.Payload)
}
