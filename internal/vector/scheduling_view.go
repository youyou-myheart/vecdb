package vector

import "context"

// schedulingView belongs to exactly one scheduling transaction. Its plan and
// owner keys are read-only during placement calculation; assignment, fencing
// and cooldown writes still go through the original transaction. Never reuse
// this view across ticks, transactions, or publication/pruning operations.
type schedulingView struct {
	coordinator *Coordinator
	tx          transactionAPI
	plans       map[IndexPartitionID]planRead
	owners      map[IndexPartitionID]ownerRead
}
type planRead struct {
	value ReplicaPlan
	err   error
}
type ownerRead struct {
	value Ownership
	err   error
}

func newSchedulingView(c *Coordinator, tx transactionAPI) *schedulingView {
	return &schedulingView{coordinator: c, tx: tx, plans: map[IndexPartitionID]planRead{}, owners: map[IndexPartitionID]ownerRead{}}
}
func (v *schedulingView) plan(ctx context.Context, id IndexPartitionID) (ReplicaPlan, error) {
	if result, ok := v.plans[id]; ok {
		return result.value, result.err
	}
	value, err := v.coordinator.planIn(ctx, v.tx, id)
	v.plans[id] = planRead{value, err}
	return value, err
}
func (v *schedulingView) owner(ctx context.Context, id IndexPartitionID) (Ownership, error) {
	if result, ok := v.owners[id]; ok {
		return result.value, result.err
	}
	value, err := v.coordinator.readOwner(ctx, v.tx, id)
	v.owners[id] = ownerRead{value, err}
	return value, err
}
