package store

import (
	"context"
	"log/slog"

	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/placement"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
)

const (
	LevelDebugAll = slog.LevelDebug
	LevelDebug    = slog.LevelDebug + 1

	LifeCyclePageSize = 1_000
)

var (
	ErrQueueNotExist = reply.NewRequestFailed("queue does not exist")
)

func systemIfEmpty(namespace string) string {
	if namespace == "" {
		return auth.SystemNamespace
	}
	return namespace
}

func ForEachQueue(ctx context.Context, queues Queues, visit func(types.QueueInfo) bool) error {
	const pageSize = 1_000
	var pivot []byte
	for ctx.Err() == nil {
		var page []types.QueueInfo
		if err := queues.List(ctx, &page, types.ListOptions{Pivot: pivot, Limit: pageSize}); err != nil {
			return errors.Errorf("Queues.List(): %w", err)
		}
		for _, queue := range page {
			if !visit(queue) {
				return nil
			}
		}
		if len(page) < pageSize {
			return nil
		}
		pivot = []byte(page[len(page)-1].Name)
	}
	return ctx.Err()
}

type LeaseOptions struct {
	LeaseDeadline clock.Time
}

type Queues interface {
	Get(ctx context.Context, name string, queue *types.QueueInfo) error

	Add(ctx context.Context, info types.QueueInfo) error

	Update(ctx context.Context, info types.QueueInfo) error

	List(ctx context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error

	Delete(ctx context.Context, queueName string) error

	Close(ctx context.Context) error
}

type Partition interface {
	Produce(ctx context.Context, batch types.ProduceBatch, now clock.Time) error

	Lease(ctx context.Context, batch types.LeaseBatch, opts LeaseOptions) error

	Complete(ctx context.Context, batch types.CompleteBatch) error

	Retry(ctx context.Context, batch types.RetryBatch) error

	List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error

	ListScheduled(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error

	Add(ctx context.Context, items []*types.Item, now clock.Time) error

	Delete(ctx context.Context, ids []types.ItemID) error

	Clear(ctx context.Context, req types.ClearRequest) error

	Stats(ctx context.Context, stats *types.PartitionStats, now clock.Time) error

	ScanForActions(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error

	ScanForScheduled(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error

	TakeAction(ctx context.Context, batch types.LifeCycleBatch, stats *types.PartitionState) error

	Info() types.PartitionInfo

	UpdateQueueInfo(info types.QueueInfo)

	Close(ctx context.Context) error
}

func Find(needle string, haystack []PartitionStorage) PartitionStorage {
	for _, i := range haystack {
		if i.Name == needle {
			return i
		}
	}
	return PartitionStorage{}
}

type Config struct {
	Queues           Queues
	Namespaces       Namespaces
	Users            Users
	APIKeys          APIKeys
	Roles            Roles
	RoleBindings     RoleBindings
	PartitionStorage []PartitionStorage
}

func (c Config) Backends() []placement.Backend {
	backends := make([]placement.Backend, 0, len(c.PartitionStorage))
	for _, storage := range c.PartitionStorage {
		backends = append(backends, placement.Backend{Name: storage.Name, Affinity: storage.Affinity})
	}
	return backends
}

type PartitionStorage struct {
	PartitionStore PartitionStore
	Affinity       float64
	Name           string
}

type PartitionStore interface {
	Get(types.PartitionInfo) Partition
}
