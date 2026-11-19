package vector

import (
	"container/heap"
	"errors"
	"math"
	"sort"
	"sync"
)

var ErrNotVisible = errors.New("visibility token not reached")

func ValidateVector(v []float64, dimension int) error {
	if len(v) != dimension {
		return errors.New("vector dimension mismatch")
	}
	for _, x := range v {
		if math.IsNaN(x) || math.IsInf(x, 0) || math.Abs(x) > 1e15 {
			return errors.New("vector values must be finite and within +/-1e15")
		}
	}
	return nil
}

type Token struct {
	ID       string `json:"id"`
	Revision uint64 `json:"revision"`
}
type Search struct {
	visibilityIndex IndexPartitionID
	visibilityAll   bool
	IndexPartitions []IndexPartitionID `json:"index_partitions,omitempty"`
	Exact           bool               `json:"exact,omitempty"`
	EFSearch        int                `json:"ef_search,omitempty"`
	Vector          []float64          `json:"vector"`
	K               int                `json:"k"`
	Filter          map[string]string  `json:"filter"`
	MinVisible      *Token             `json:"min_visible,omitempty"`
}
type Hit struct {
	ID       string            `json:"id"`
	Revision uint64            `json:"revision"`
	Distance float64           `json:"distance"`
	Payload  map[string]string `json:"payload"`
}

// ExactIndex is a correctness baseline, not ANN. A single lock publishes complete batches.
type ExactIndex struct {
	mu        sync.RWMutex
	records   map[string]Record
	dimension int
}

func NewIndex(d int) *ExactIndex { return &ExactIndex{records: map[string]Record{}, dimension: d} }
func (i *ExactIndex) Apply(batch []Record) error {
	if err := validateBatch(batch, i.dimension); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, r := range batch {
		if r.Revision > i.records[r.ID].Revision {
			i.records[r.ID] = cloneRecord(r)
		}
	}
	return nil
}
func (i *ExactIndex) Snapshot() []Record {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make([]Record, 0, len(i.records))
	for _, r := range i.records {
		out = append(out, cloneRecord(r))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out
}
func (i *ExactIndex) Search(q Search) ([]Hit, error) {
	if e := ValidateVector(q.Vector, i.dimension); e != nil {
		return nil, e
	}
	if q.K < 1 || q.K > 1000 {
		return nil, errors.New("k must be between 1 and 1000")
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	if q.MinVisible != nil {
		if q.MinVisible.ID == "" || q.MinVisible.Revision == 0 {
			return nil, errors.New("invalid visibility token")
		}
		if i.records[q.MinVisible.ID].Revision < q.MinVisible.Revision {
			return nil, ErrNotVisible
		}
	}
	best := make(topK, 0, q.K)
	for _, r := range i.records {
		if r.Deleted || !matches(r, q.Filter) {
			continue
		}
		best.offer(candidate{r.ID, r.Revision, l2(q.Vector, r.Vector)}, q.K)
	}
	out := materialize(best, i.records)

	return out, nil
}

// Callers must not be able to mutate an active query view via shared maps/slices.
func cloneRecord(r Record) Record {
	r.Vector = append([]float64(nil), r.Vector...)
	if r.Payload != nil {
		p := make(map[string]string, len(r.Payload))
		for k, v := range r.Payload {
			p[k] = v
		}
		r.Payload = p
	}
	return r
}

type candidate struct {
	id       string
	revision uint64
	distance float64
}

func better(a, b candidate) bool {
	if a.distance == b.distance {
		return a.id < b.id
	}
	return a.distance < b.distance
}

type topK []candidate

func (h topK) Len() int           { return len(h) }
func (h topK) Less(i, j int) bool { return better(h[j], h[i]) }
func (h topK) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *topK) Push(x any)        { *h = append(*h, x.(candidate)) }
func (h *topK) Pop() any          { a := *h; x := a[len(a)-1]; *h = a[:len(a)-1]; return x }
func (h *topK) offer(c candidate, k int) {
	if len(*h) < k {
		heap.Push(h, c)
	} else if better(c, (*h)[0]) {
		(*h)[0] = c
		heap.Fix(h, 0)
	}
}
func l2(a, b []float64) float64 {
	var d float64
	for j, x := range a {
		v := x - b[j]
		d += v * v
	}
	return d
}
func matches(r Record, f map[string]string) bool {
	for k, v := range f {
		actual, ok := r.Payload[k]
		if !ok || actual != v {
			return false
		}
	}
	return true
}
func clonePayload(p map[string]string) map[string]string {
	if p == nil {
		return nil
	}
	out := make(map[string]string, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}
func materialize(h topK, records map[string]Record) []Hit {
	sort.Slice(h, func(a, b int) bool { return better(h[a], h[b]) })
	out := make([]Hit, len(h))
	for j, c := range h {
		out[j] = Hit{c.id, c.revision, c.distance, clonePayload(records[c.id].Payload)}
	}
	return out
}
func validateBatch(batch []Record, d int) error {
	for _, r := range batch {
		if r.ID == "" || r.Revision == 0 {
			return errors.New("invalid record identity/version")
		}
		if !r.Deleted {
			if err := ValidateVector(r.Vector, d); err != nil {
				return err
			}
		}
	}
	return nil
}
func validateQuery(q Search, d int) error {
	if err := ValidateVector(q.Vector, d); err != nil {
		return err
	}
	if q.K < 1 || q.K > 1000 {
		return errors.New("k must be between 1 and 1000")
	}
	if q.MinVisible != nil && (q.MinVisible.ID == "" || q.MinVisible.Revision == 0) {
		return errors.New("invalid visibility token")
	}
	return nil
}
