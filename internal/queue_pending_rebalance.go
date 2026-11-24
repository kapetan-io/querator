package internal

import (
	"context"
	"slices"

	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/placement"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
)

type QueuePendingRebalance struct {
	Create  []types.PartitionInfo
	Drain   []types.PartitionInfo
	Refused string
}

func (qm *QueuesManager) PendingRebalance(ctx context.Context, name string) (QueuePendingRebalance, error) {
	if qm.inShutdown.Load() {
		return QueuePendingRebalance{}, ErrServiceShutdown
	}

	var saved types.QueueInfo
	if err := qm.conf.StorageConfig.Queues.Get(ctx, name, &saved); err != nil {
		if errors.Is(err, store.ErrQueueNotExist) {
			return QueuePendingRebalance{}, reply.NewInvalidOption("queue does not exist; no such queue named '%s'", name)
		}
		return QueuePendingRebalance{}, errors.With("queue", name).Errorf("Queues.Get(): %w", err)
	}

	pending, err := qm.PendingRebalances(ctx, []types.QueueInfo{saved})
	if err != nil {
		return QueuePendingRebalance{}, err
	}
	return pending[0], nil
}

func (qm *QueuesManager) PendingRebalances(ctx context.Context, queues []types.QueueInfo) ([]QueuePendingRebalance, error) {
	if qm.inShutdown.Load() {
		return nil, ErrServiceShutdown
	}

	pending := make([]QueuePendingRebalance, len(queues))
	if len(queues) == 0 {
		return pending, nil
	}

	backends, err := qm.backends.List(ctx)
	if err != nil {
		return nil, err
	}
	if err := refuseRebalance(backends, qm.backends.Configured); err != nil {
		for i := range pending {
			pending[i].Refused = err.Error()
		}
		return pending, nil
	}

	for i, queue := range queues {
		pending[i] = pendingRebalance(queue, backends)
	}
	return pending, nil
}

func pendingRebalance(queue types.QueueInfo, backends []types.Backend) QueuePendingRebalance {
	rebalanced, err := placement.Rebalance(queue, backends)
	if err != nil {
		return QueuePendingRebalance{Refused: "placement.Rebalance(): " + err.Error()}
	}
	if !rebalanced.Changed {
		return QueuePendingRebalance{}
	}

	before := make(map[int]types.PartitionInfo, len(queue.PartitionInfo))
	for _, p := range queue.PartitionInfo {
		before[p.PartitionNum] = p
	}

	var result QueuePendingRebalance
	for _, p := range rebalanced.Queue.PartitionInfo {
		old, existed := before[p.PartitionNum]
		switch {
		case !existed:
			result.Create = append(result.Create, p)
		case p.Draining && !old.Draining:
			result.Drain = append(result.Drain, p)
		}
	}
	byNumber := func(a, b types.PartitionInfo) int { return a.PartitionNum - b.PartitionNum }
	slices.SortFunc(result.Create, byNumber)
	slices.SortFunc(result.Drain, byNumber)
	return result
}
