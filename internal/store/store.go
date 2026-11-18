package store

import (
	"cmp"
	"context"
	"iter"
	"log/slog"
	"math"
	"slices"
	"time"

	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
)

const (
	LevelDebugAll = slog.LevelDebug
	LevelDebug    = slog.LevelDebug + 1
)

var (
	ErrQueueAlreadyExists = reply.NewInvalidOption("queue already exists")
	ErrQueueNotExist      = reply.NewRequestFailed("queue does not exist")
	theFuture             = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
)

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

	ScanForActions(ctx context.Context, now clock.Time) iter.Seq2[types.Action, error]

	ScanForScheduled(ctx context.Context, now clock.Time) iter.Seq2[types.Action, error]

	TakeAction(ctx context.Context, batch types.LifeCycleBatch, stats *types.PartitionState) error

	LifeCycleInfo(ctx context.Context, info *types.LifeCycleInfo) error

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

func CanPlacePartitions(backends []PartitionStorage) bool {
	for _, b := range backends {
		if b.Affinity > 0 {
			return true
		}
	}
	return false
}

func PlacePartitions(backends []PartitionStorage, info types.QueueInfo) ([]types.PartitionInfo, error) {
	if info.RequestedPartitions > maxQueuePartitions {
		return nil, reply.NewInvalidOption("partitions is invalid; cannot be greater than %d", maxQueuePartitions)
	}
	var placed []types.PartitionInfo
	info.PartitionInfo = nil
	for idx, count := range assignPartitions(backends, info.RequestedPartitions) {
		for i := 0; i < min(count, maxQueuePartitions); i++ {
			placed = append(placed, types.PartitionInfo{
				StorageName:  backends[idx].Name,
				PartitionNum: len(placed),
				Queue:        info,
			})
		}
	}
	return placed, nil
}

func assignPartitions(backends []PartitionStorage, totalPartitions int) []int {
	assignments := make([]int, len(backends))
	var totalAffinity float64
	var preferred []int
	for i, b := range backends {
		if b.Affinity > 0 {
			totalAffinity += b.Affinity
			preferred = append(preferred, i)
		}
	}
	if len(preferred) == 0 {
		return assignments
	}

	var assigned int
	for _, i := range preferred {
		assignments[i] = int(math.Floor((backends[i].Affinity / totalAffinity) * float64(totalPartitions)))
		assigned += assignments[i]
	}

	slices.SortStableFunc(preferred, func(a, b int) int {
		return cmp.Compare(backends[b].Affinity, backends[a].Affinity)
	})
	for i := 0; assigned < totalPartitions; i++ {
		assignments[preferred[i%len(preferred)]]++
		assigned++
	}
	return assignments
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

type PartitionStorage struct {
	PartitionStore PartitionStore
	Affinity       float64
	Name           string
}

type PartitionStore interface {
	Get(types.PartitionInfo) Partition
}
