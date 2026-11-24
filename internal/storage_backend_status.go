package internal

import (
	"context"
	"maps"
	"slices"

	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
)

type StorageBackendStatus struct {
	Backend    types.Backend
	Configured bool
	SafeToStop bool
	Partitions []BackendPartition
	Rebalance  RebalancePlan
}

type BackendPartition struct {
	Queue     string
	Partition int
	State     string
	Readable  bool
	Counts    types.PartitionState
}

type RebalancePlan struct {
	Create []QueuePartitions
	Drain  []QueuePartitions
}

type QueuePartitions struct {
	Queue      string
	Partitions []int
}

func (qm *QueuesManager) ListStorageBackends(ctx context.Context) ([]StorageBackendStatus, error) {
	if qm.inShutdown.Load() {
		return nil, ErrServiceShutdown
	}

	saved, err := qm.backends.List(ctx)
	if err != nil {
		return nil, err
	}

	var queues []types.QueueInfo
	if err := store.ForEachQueue(ctx, qm.conf.StorageConfig.Queues, func(queue types.QueueInfo) bool {
		queues = append(queues, queue)
		return true
	}); err != nil {
		return nil, err
	}

	statuses := make([]StorageBackendStatus, 0, len(saved))
	index := make(map[string]int, len(saved))
	for _, backend := range saved {
		index[backend.Name] = len(statuses)
		statuses = append(statuses, StorageBackendStatus{
			Backend:    backend,
			Configured: qm.backends.Configured(backend.Name),
			Rebalance:  RebalancePlan{Create: []QueuePartitions{}, Drain: []QueuePartitions{}},
		})
	}

	rebalancing := rebalanceBackends(statuses)
	for _, queue := range queues {
		counts := qm.partitionCounts(ctx, queue.Name)
		for _, p := range queue.PartitionInfo {
			i, ok := index[p.StorageName]
			if !ok {
				continue
			}
			partition := BackendPartition{Queue: queue.Name, Partition: p.PartitionNum, State: p.State()}
			state, loaded := counts[p.PartitionNum]
			switch {
			case !loaded:
				partition.Counts.Failures = 1
			default:
				partition.Counts = state
				partition.Readable = state.Failures == 0
			}
			statuses[i].Partitions = append(statuses[i].Partitions, partition)
		}
		if rebalancing != nil {
			addPending(statuses, index, queue, rebalancing)
		}
	}

	for i := range statuses {
		statuses[i].SafeToStop = statuses[i].Backend.Affinity == 0 && len(statuses[i].Partitions) == 0
	}
	return statuses, nil
}

func (qm *QueuesManager) partitionCounts(ctx context.Context, name string) map[int]types.PartitionState {
	q, err := qm.Get(ctx, name)
	if err != nil {
		qm.log.Warn("cannot load queue for its partition counts", "queue", name, "error", err)
		return nil
	}
	counts := make(map[int]types.PartitionState)
	for _, l := range q.GetAll() {
		var found []types.PartitionCount
		if err := l.PartitionCounts(ctx, &found); err != nil {
			qm.log.Warn("cannot read partition counts", "queue", name, "error", err)
			continue
		}
		for _, c := range found {
			counts[c.Partition] = c.State
		}
	}
	return counts
}

func rebalanceBackends(statuses []StorageBackendStatus) []types.Backend {
	backends := make([]types.Backend, 0, len(statuses))
	configured := make(map[string]bool, len(statuses))
	for _, status := range statuses {
		backends = append(backends, status.Backend)
		configured[status.Backend.Name] = status.Configured
	}
	if refuseRebalance(backends, func(name string) bool { return configured[name] }) != nil {
		return nil
	}
	return backends
}

func addPending(statuses []StorageBackendStatus, index map[string]int, queue types.QueueInfo,
	backends []types.Backend) {
	pending := pendingRebalance(queue, backends)

	creates := make(map[string][]int)
	drains := make(map[string][]int)
	for _, p := range pending.Create {
		creates[p.StorageName] = append(creates[p.StorageName], p.PartitionNum)
	}
	for _, p := range pending.Drain {
		drains[p.StorageName] = append(drains[p.StorageName], p.PartitionNum)
	}

	for _, name := range slices.Sorted(maps.Keys(creates)) {
		i, ok := index[name]
		if !ok {
			continue
		}
		plan := &statuses[i].Rebalance
		plan.Create = append(plan.Create, QueuePartitions{Queue: queue.Name, Partitions: creates[name]})
	}
	for _, name := range slices.Sorted(maps.Keys(drains)) {
		i, ok := index[name]
		if !ok {
			continue
		}
		plan := &statuses[i].Rebalance
		plan.Drain = append(plan.Drain, QueuePartitions{Queue: queue.Name, Partitions: drains[name]})
	}
}
