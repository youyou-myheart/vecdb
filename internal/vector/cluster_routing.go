package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	tikverr "github.com/tikv/client-go/v2/error"
	"math"
	"reflect"
)

// ClusterRouting is immutable within a namespace. Worker placement/epochs are
// deliberately absent: failover must not change a vector's logical destination.
type ClusterRouting struct {
	Version uint64      `json:"version"`
	Centers [][]float64 `json:"centers"`
}

func (r *ClusterRouting) validate(d int) error {
	if r.Version != 1 || len(r.Centers) < 1 || len(r.Centers) > 64 {
		return errors.New("routing requires version=1 and 1..64 centers")
	}
	for _, v := range r.Centers {
		if e := ValidateVector(v, d); e != nil {
			return e
		}
	}
	return nil
}
func (r *ClusterRouting) target(v []float64) IndexPartitionID {
	best, distance := 0, math.Inf(1)
	for n, c := range r.Centers {
		score := l2(v, c)
		if score < distance {
			best, distance = n, score
		}
	}
	return IndexPartitionID(fmt.Sprintf("index-%04d", best))
}
func sourceID(d DataPartitionID) IndexPartitionID {
	return IndexPartitionID(fmt.Sprintf("data-%04d", d))
}
func sourceLayout(n int) PartitionLayout {
	l := PartitionLayout{}
	for d := 0; d < n; d++ {
		l.Bindings = append(l.Bindings, PartitionBinding{DataPartitionID(d), sourceID(DataPartitionID(d))})
	}
	return l
}

type clusterRouter struct {
	IdentityPartitionRouter
	route *ClusterRouting
}

func (r clusterRouter) IndexForRecord(_ DataPartitionID, v Record) (IndexPartitionID, error) {
	if len(v.Vector) == 0 {
		return "", errors.New("cluster routing requires the event vector")
	}
	return r.route.target(v.Vector), nil
}
func (r *Repository) ensureRoutedLayout(ctx context.Context, n int, route *ClusterRouting, scheduler, domain string, retention bool) (PartitionLayout, error) {
	return r.ensureRoutedLayoutWithBackend(ctx, n, route, scheduler, domain, retention, false)
}

func (r *Repository) ensureRoutedLayoutWithBackend(ctx context.Context, n int, route *ClusterRouting, scheduler, domain string, retention, rawBackend bool) (PartitionLayout, error) {
	raw, _ := json.Marshal(route)
	var frozen ClusterRouting
	if e := json.Unmarshal(raw, &frozen); e != nil {
		return PartitionLayout{}, e
	}
	route = &frozen
	l := PartitionLayout{Version: 2, Epoch: route.Version, Hash: "sha256-be64-v1", DataPartitions: n, Routing: route}
	if n < 1 || n > 64 {
		return l, errors.New("data partitions must be 1..64")
	}
	if e := route.validate(r.Dimension); e != nil {
		return l, e
	}
	for i := range route.Centers {
		l.Bindings = append(l.Bindings, PartitionBinding{Data: -1, Index: IndexPartitionID(fmt.Sprintf("index-%04d", i))})
	}
	tx, e := r.begin()
	if e != nil {
		return l, e
	}
	defer tx.Rollback()
	key := []byte(r.Prefix + "partition-layout")
	raw, e = tx.Get(ctx, key)
	hasLayout := e == nil
	if hasLayout {
		var old PartitionLayout
		if e = json.Unmarshal(raw, &old); e != nil {
			return l, e
		}
		if !reflect.DeepEqual(old, l) {
			return l, errors.New("immutable clustered layout mismatch; use a fresh namespace")
		}
	} else if !tikverr.IsErrNotFound(e) {
		return l, e
	} else {
		head, err := r.logHead(ctx, tx)
		if err != nil {
			return l, err
		}
		if head != 0 {
			return l, errors.New("cluster routing requires a fresh namespace")
		}
		if e = putJSON(tx, key, l); e != nil {
			return l, e
		}
	}
	previous := fmt.Sprintf("v14-cluster-routing-lease-%s-%s-retention=%t", scheduler, domain, retention)
	mode := previous + "-owner-append-v1"
	if rawBackend {
		mode += "-data-raw-v1"
	} else {
		mode += "-data-wal-v1"
	}
	key = []byte(r.Prefix + "cluster-mode")
	raw, e = tx.Get(ctx, key)
	if e == nil && string(raw) != mode {
		if rawBackend {
			return l, errors.New("DATA_BACKEND=raw requires a fresh namespace")
		}
		if string(raw) != previous && string(raw) != previous+"-owner-append-v1" {
			return l, errors.New("incompatible cluster routing protocol")
		}
		nodes, err := (&Coordinator{root: r}).nodesIn(ctx, tx)
		if err != nil {
			return l, err
		}
		if len(nodes) > 0 {
			return l, errors.New("stop all old cluster processes and wait for node leases to expire before upgrading data writers")
		}
	}
	if e != nil && !tikverr.IsErrNotFound(e) {
		return l, e
	}
	if e != nil && rawBackend && hasLayout {
		return l, errors.New("DATA_BACKEND=raw requires a fresh namespace")
	}
	if e = tx.Set(key, []byte(mode)); e != nil {
		return l, e
	}
	return l, tx.Commit(ctx)
}
func (c *ClusterService) indexRepository(id IndexPartitionID) *Repository {
	r := *c.root
	r.Prefix = c.root.Prefix + "index/" + string(id) + "/"
	r.IndexPrefix = r.Prefix
	return &r
}
