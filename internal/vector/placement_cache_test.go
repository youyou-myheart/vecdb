package vector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

type placementBatchTx struct {
	transactionAPI
	observe func([][]byte) error
}

func (tx placementBatchTx) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	if err := tx.observe(keys); err != nil {
		return nil, err
	}
	return batchGet(ctx, tx.transactionAPI, keys)
}

func TestPlacementLoopsShareOneSnapshot(t *testing.T) {
	for _, replicas := range []int{1, 2} {
		for _, dataFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d-replicas_data-first=%t", replicas, dataFirst), func(t *testing.T) {
				f, c, _ := routedFixture(t)
				c.replicas = replicas
				c.coordinator.ttl = 6000
				base := f.r.beginTxn
				var batches [][]string
				f.r.beginTxn = func() (transactionAPI, error) {
					tx, err := base()
					return placementBatchTx{tx, func(keys [][]byte) error {
						var names []string
						for _, k := range keys {
							names = append(names, string(k))
						}
						batches = append(batches, names)
						return nil
					}}, err
				}
				read := func(data bool) {
					t.Helper()
					if _, err := c.placementValues(f.ctx, data); err != nil {
						t.Fatal(err)
					}
				}
				read(dataFirst)
				read(!dataFirst)
				indexKey := string(c.coordinator.assignmentKey(c.layout.Bindings[0].Index))
				if replicas > 1 {
					indexKey = string(c.coordinator.planKey(c.layout.Bindings[0].Index))
				}
				dataKey := string(c.coordinator.assignmentKey(sourceID(0)))
				if len(batches) != 1 || !slices.Contains(batches[0], dataKey) || !slices.Contains(batches[0], indexKey) {
					t.Fatal("expected one joint placement read", batches)
				}
				read(true)
				if len(batches) != 2 || len(batches[1]) != c.layout.DataPartitions || slices.Contains(batches[1], indexKey) {
					t.Fatal("fast data tick polled indexes", batches)
				}
				// An index tick prefetches data, but a late data tick must read
				// the new assignment even when nobody consumed that snapshot.
				read(false)
				c.placement.started = time.Now().Add(-2 * dispatchControlInterval)
				read(true)
				if len(batches) != 4 || slices.Contains(batches[3], indexKey) {
					t.Fatal("accepted stale prefetched data", batches)
				}
			})
		}
	}
}

func TestPlacementBatchFailureDoesNotGrantOrStarveOtherFamily(t *testing.T) {
	f, c, _ := routedFixture(t)
	c.replicas = 2
	key := string(c.coordinator.planKey(c.layout.Bindings[0].Index))
	base := f.r.beginTxn
	f.r.beginTxn = func() (transactionAPI, error) {
		tx, err := base()
		return placementBatchTx{tx, func(keys [][]byte) error {
			for _, k := range keys {
				if string(k) == key {
					return injected
				}
			}
			return nil
		}}, err
	}
	values, err := c.placementValues(f.ctx, true)
	if err != nil || values[string(c.coordinator.assignmentKey(sourceID(0)))] == nil {
		t.Fatal("unavailable index hid data assignment", values, err)
	}
	if _, err = c.indexPlacement(f.ctx); !errors.Is(err, injected) {
		t.Fatal("failed joint snapshot served to index", err)
	}
}

func TestIndexLeaseRenewalReadsOwnerOnce(t *testing.T) {
	f, _, _ := leasedFixture(t)
	g := f.services[0].graphReplica
	current, err := f.c.Resolve(f.ctx, g.journal.owner.Partition)
	if err != nil {
		t.Fatal(err)
	}
	reads := map[string]int{}
	base := f.r.beginTxn
	f.r.beginTxn = func() (transactionAPI, error) { tx, err := base(); return &countedScheduleTx{tx, reads}, err }
	grant, err := f.c.grantIndexLease(f.ctx, g.journal.id, g.journal.owner)
	if err != nil || grant.Expires > current.Expires {
		t.Fatal(grant, err)
	}
	if n := reads[string(f.c.ownerKey(g.journal.owner.Partition))]; n != 1 {
		t.Fatalf("owner Get count %d", n)
	}
}
