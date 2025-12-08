package internal

import (
	"context"
	"github.com/dustin/go-humanize"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
)

// runLifecycle processes lifecycle actions for all partitions that are due
func (l *Logical) runLifecycle(state *QueueState) {
	now := l.conf.Clock.Now().UTC()

	for _, partition := range state.Partitions {
		if partition.Lifecycle.NextLifecycleRun.After(now) {
			continue
		}

		if partition.State.Failures != 0 {
			if !l.recoverPartition(partition, now) {
				continue
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), l.conf.ReadTimeout)
		actions, err := l.scanPartitionLifecycle(ctx, partition)
		cancel()
		if err != nil {
			partition.Lifecycle.Failures++
			retryIn := lifecycleBackOff.Next(partition.Lifecycle.Failures)
			partition.Lifecycle.NextLifecycleRun = now.Add(retryIn)
			partition.State.Failures = partition.Lifecycle.Failures
			l.log.Warn("lifecycle scan failed; retrying...",
				"partition", partition.Info.PartitionNum,
				"retry", retryIn,
				"error", err)
			continue
		}
		partition.Lifecycle.Failures = 0

		// Batch DLQ actions for manager processing
		var dlqActions []types.Action

		for _, a := range actions {
			switch a.Action {
			case types.ActionLeaseExpired:
				// Check if max attempts exceeded - route to DLQ or delete
				if partition.Info.Queue.MaxAttempts != 0 && a.Item.Attempts >= partition.Info.Queue.MaxAttempts {
					if partition.Info.Queue.DeadQueue != "" {
						// Route to DLQ via manager
						a.Action = types.ActionItemMaxAttempts
						dlqActions = append(dlqActions, a)
					} else {
						// No DLQ - delete the item
						a.Action = types.ActionDeleteItem
						partition.LifeCycleRequests.Add(&types.LifeCycleRequest{
							PartitionNum: partition.Info.PartitionNum,
							Actions:      []types.Action{a},
						})
					}
				} else {
					// Normal lease expiry - re-queue item
					partition.LifeCycleRequests.Add(&types.LifeCycleRequest{
						PartitionNum: partition.Info.PartitionNum,
						Actions:      []types.Action{a},
					})
				}
			case types.ActionItemExpired, types.ActionItemMaxAttempts:
				if partition.Info.Queue.DeadQueue == "" {
					a.Action = types.ActionDeleteItem
					partition.LifeCycleRequests.Add(&types.LifeCycleRequest{
						PartitionNum: partition.Info.PartitionNum,
						Actions:      []types.Action{a},
					})
				} else {
					// Batch DLQ actions for single manager call
					dlqActions = append(dlqActions, a)
				}
			case types.ActionDeleteItem:
				partition.LifeCycleRequests.Add(&types.LifeCycleRequest{
					PartitionNum: partition.Info.PartitionNum,
					Actions:      []types.Action{a},
				})
			case types.ActionQueueScheduledItem:
				partition.LifeCycleRequests.Add(&types.LifeCycleRequest{
					PartitionNum: partition.Info.PartitionNum,
					Actions:      []types.Action{a},
				})
			}
		}

		// Process batched DLQ actions if any
		if len(dlqActions) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
			req := &types.LifeCycleRequest{
				RequestTimeout:   l.conf.WriteTimeout,
				PartitionInfo:    partition.Info,
				PartitionNum:     partition.Info.PartitionNum,
				Actions:          dlqActions,
				PartitionStorage: partition.Store,
			}
			err := l.conf.Manager.LifeCycle(ctx, req)
			cancel()
			if err != nil {
				l.log.Warn("manager lifecycle failed",
					"partition", partition.Info.PartitionNum,
					"num_actions", len(dlqActions),
					"error", err)
			}
		}

		// Execute the lifecycle actions against storage
		if len(partition.LifeCycleRequests.Requests) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
			if err := partition.Store.TakeAction(ctx, partition.LifeCycleRequests, &partition.State); err != nil {
				l.log.Error("while calling store.Partition.TakeAction()",
					"partition", partition.Info.PartitionNum,
					"queueName", l.conf.Name,
					"error", err)
			}
			cancel()
			partition.LifeCycleRequests.Reset()
		}

		ctx, cancel = context.WithTimeout(context.Background(), l.conf.ReadTimeout)
		var info types.LifeCycleInfo
		err = partition.Store.LifeCycleInfo(ctx, &info)
		cancel()
		if err != nil {
			partition.Lifecycle.Failures++
			retryIn := lifecycleBackOff.Next(partition.Lifecycle.Failures)
			partition.Lifecycle.NextLifecycleRun = now.Add(retryIn)
			partition.State.Failures = partition.Lifecycle.Failures
			l.log.Warn("lifecycle info failed; retrying...",
				"partition", partition.Info.PartitionNum,
				"retry", retryIn,
				"error", err)
			continue
		}

		if info.NextLeaseExpiry.After(now) {
			partition.Lifecycle.NextLifecycleRun = info.NextLeaseExpiry
		} else {
			partition.Lifecycle.NextLifecycleRun = now.Add(humanize.LongTime)
		}
	}
}

// runScheduled processes scheduled items for all partitions that are due
func (l *Logical) runScheduled(state *QueueState) {
	now := l.conf.Clock.Now().UTC()

	for _, partition := range state.Partitions {
		if partition.Lifecycle.NextScheduledRun.After(now) {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), l.conf.ReadTimeout)
		actions, err := l.scanPartitionScheduled(ctx, partition)
		cancel()
		if err != nil {
			partition.Lifecycle.Failures++
			retryIn := lifecycleBackOff.Next(partition.Lifecycle.Failures)
			partition.Lifecycle.NextScheduledRun = now.Add(retryIn)
			l.log.Warn("scheduled scan failed; retrying...",
				"partition", partition.Info.PartitionNum,
				"retry", retryIn,
				"error", err)
			continue
		}
		partition.Lifecycle.Failures = 0

		for _, a := range actions {
			partition.LifeCycleRequests.Add(&types.LifeCycleRequest{
				PartitionNum: partition.Info.PartitionNum,
				Actions:      []types.Action{a},
			})
		}

		// Execute the scheduled actions against storage
		if len(partition.LifeCycleRequests.Requests) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
			if err := partition.Store.TakeAction(ctx, partition.LifeCycleRequests, &partition.State); err != nil {
				l.log.Error("while calling store.Partition.TakeAction()",
					"partition", partition.Info.PartitionNum,
					"queueName", l.conf.Name,
					"error", err)
			}
			cancel()
			partition.LifeCycleRequests.Reset()
		}

		partition.Lifecycle.NextScheduledRun = now.Add(humanize.LongTime)
	}
}

// minNextLifecycleRun finds the minimum NextLifecycleRun across all partitions
func (l *Logical) minNextLifecycleRun(state *QueueState) clock.Duration {
	now := l.conf.Clock.Now().UTC()
	var minTime clock.Time

	for _, partition := range state.Partitions {
		if minTime.IsZero() || partition.Lifecycle.NextLifecycleRun.Before(minTime) {
			minTime = partition.Lifecycle.NextLifecycleRun
		}
	}

	if minTime.IsZero() {
		return humanize.LongTime
	}

	if minTime.Before(now) {
		return 0
	}

	return minTime.Sub(now)
}

// minNextScheduledRun finds the minimum NextScheduledRun across all partitions
func (l *Logical) minNextScheduledRun(state *QueueState) clock.Duration {
	now := l.conf.Clock.Now().UTC()
	var minTime clock.Time

	for _, partition := range state.Partitions {
		if minTime.IsZero() || partition.Lifecycle.NextScheduledRun.Before(minTime) {
			minTime = partition.Lifecycle.NextScheduledRun
		}
	}

	if minTime.IsZero() {
		return humanize.LongTime
	}

	if minTime.Before(now) {
		return 0
	}

	return minTime.Sub(now)
}

// recoverPartition attempts to recover a failed partition
func (l *Logical) recoverPartition(partition *Partition, now clock.Time) bool {
	ctx, cancel := context.WithTimeout(context.Background(), l.conf.ReadTimeout)
	var stats types.PartitionStats
	defer cancel()

	if err := partition.Store.Stats(ctx, &stats, now); err != nil {
		partition.Lifecycle.Failures++
		retryIn := lifecycleBackOff.Next(partition.Lifecycle.Failures)
		partition.Lifecycle.NextLifecycleRun = now.Add(retryIn)
		partition.State.Failures = partition.Lifecycle.Failures
		l.log.Warn("recover partition attempt failed; retrying...",
			"partition", partition.Info.PartitionNum,
			"retry", retryIn,
			"error", err)
		return false
	}

	if stats.Total-stats.NumLeased < 0 {
		l.log.Error("assertion failed; total count of items in the partition "+
			"cannot be more than the total leased count",
			"partition", partition.Info.PartitionNum,
			"total", stats.Total,
			"leased", stats.NumLeased)
		return false
	}

	partition.Lifecycle.Failures = 0
	partition.State.Failures = 0
	partition.State.UnLeased = stats.Total - stats.NumLeased
	partition.State.NumLeased = stats.NumLeased
	l.log.Info("partition recovered",
		"partition", partition.Info.PartitionNum,
		"unleased", partition.State.UnLeased,
		"leased", partition.State.NumLeased)
	return true
}

// scanPartitionLifecycle scans a partition for lifecycle actions
func (l *Logical) scanPartitionLifecycle(ctx context.Context, partition *Partition) ([]types.Action, error) {
	now := l.conf.Clock.Now().UTC()
	actions := make([]types.Action, 0, 1_000)

	for a, err := range partition.Store.ScanForActions(ctx, now) {
		if err != nil {
			return actions, err
		}
		actions = append(actions, a)
	}

	return actions, nil
}

// scanPartitionScheduled scans a partition for scheduled items
func (l *Logical) scanPartitionScheduled(ctx context.Context, partition *Partition) ([]types.Action, error) {
	now := l.conf.Clock.Now().UTC()
	actions := make([]types.Action, 0, 1_000)

	for a, err := range partition.Store.ScanForScheduled(ctx, now) {
		if err != nil {
			return actions, err
		}
		actions = append(actions, a)
	}

	return actions, nil
}
