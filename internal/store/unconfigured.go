package store

import (
	"context"

	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
)

var ErrBackendNotConfigured = errors.New("storage backend is not in this instance's config file")

type UnconfiguredPartition struct {
	info types.PartitionInfo
}

var _ Partition = &UnconfiguredPartition{}

func NewUnconfiguredPartition(info types.PartitionInfo) *UnconfiguredPartition {
	return &UnconfiguredPartition{info: info}
}

func (p *UnconfiguredPartition) err() error {
	return errors.Errorf("partition %d of queue '%s' is on '%s': %w",
		p.info.PartitionNum, p.info.Queue.Name, p.info.StorageName, ErrBackendNotConfigured)
}

func (p *UnconfiguredPartition) Produce(context.Context, types.ProduceBatch, clock.Time) error {
	return p.err()
}

func (p *UnconfiguredPartition) Lease(context.Context, types.LeaseBatch, LeaseOptions) error {
	return p.err()
}

func (p *UnconfiguredPartition) Complete(context.Context, types.CompleteBatch) error {
	return p.err()
}

func (p *UnconfiguredPartition) Retry(context.Context, types.RetryBatch) error {
	return p.err()
}

func (p *UnconfiguredPartition) List(context.Context, *[]*types.Item, types.ListOptions) error {
	return p.err()
}

func (p *UnconfiguredPartition) ListScheduled(context.Context, *[]*types.Item, types.ListOptions) error {
	return p.err()
}

func (p *UnconfiguredPartition) Add(context.Context, []*types.Item, clock.Time) error {
	return p.err()
}

func (p *UnconfiguredPartition) Delete(context.Context, []types.ItemID) error {
	return p.err()
}

func (p *UnconfiguredPartition) Clear(context.Context, types.ClearRequest) error {
	return p.err()
}

func (p *UnconfiguredPartition) Stats(context.Context, *types.PartitionStats, clock.Time) error {
	return p.err()
}

func (p *UnconfiguredPartition) ScanForActions(context.Context, *types.LifeCycleScan, clock.Time) error {
	return p.err()
}

func (p *UnconfiguredPartition) ScanForScheduled(context.Context, *types.LifeCycleScan, clock.Time) error {
	return p.err()
}

func (p *UnconfiguredPartition) TakeAction(context.Context, types.LifeCycleBatch, *types.PartitionState) error {
	return p.err()
}

func (p *UnconfiguredPartition) Info() types.PartitionInfo {
	return p.info
}

func (p *UnconfiguredPartition) UpdateQueueInfo(info types.QueueInfo) {
	p.info.Queue = info
}

func (p *UnconfiguredPartition) Close(context.Context) error {
	return nil
}
