package internal

import (
	"context"
	"fmt"

	"github.com/dustin/go-humanize"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tiger/assert"
)

func (l *Logical) runLifecycle(state *QueueState) {
	now := l.conf.Clock.Now().UTC()
	partition := duePartition(state.Partitions, now, func(p *Partition) clock.Time {
		return p.Lifecycle.NextLifecycleRun
	})
	if partition == nil {
		return
	}

	if partition.State.Failures != 0 {
		if !l.recoverPartition(partition, now) {
			return
		}
	}

	holdMoves := l.settleUnansweredMove(partition)

	pass := &partition.Lifecycle.ActionPass
	pivot, last := pass.Scan.Pivot, pass.Scan.Last
	ctx, cancel := context.WithTimeout(context.Background(), l.conf.ReadTimeout)
	pass.Scan.ResetPage()
	err := partition.Store.ScanForActions(ctx, &pass.Scan, now)
	cancel()
	if err != nil {
		partition.Lifecycle.Failures++
		retryIn := lifecycleBackOff.Next(min(partition.Lifecycle.Failures, lifecycleBackOffAttemptLimit))
		partition.Lifecycle.NextLifecycleRun = now.Add(retryIn)
		partition.State.Failures = partition.Lifecycle.Failures
		l.log.Warn("lifecycle scan failed; retrying...",
			"partition", partition.Info.PartitionNum,
			"retry", retryIn,
			"error", err)
		return
	}
	partition.Lifecycle.Failures = 0

	var dlqActions []types.Action
	for _, a := range pass.Scan.Actions {
		switch a.Action {
		case types.ActionLeaseExpired:
			if partition.Info.Queue.MaxAttempts != 0 && a.Item.Attempts >= partition.Info.Queue.MaxAttempts {
				if partition.Info.Queue.DeadQueue != "" {
					a.Action = types.ActionItemMaxAttempts
					dlqActions = append(dlqActions, a)
				} else {
					a.Action = types.ActionDeleteItem
					partition.LifeCycleRequests.Add(&types.LifeCycleRequest{
						PartitionNum: partition.Info.PartitionNum,
						Actions:      []types.Action{a},
					})
				}
			} else {
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
		default:
			assert.Unreachable(fmt.Sprintf("undefined action kind '0x%X'", int(a.Action)))
		}
	}

	if holdMoves && len(dlqActions) > 0 {
		pass.Unfinished = true
	}

	if len(dlqActions) > 0 && !pass.Unfinished {
		ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
		req := &types.LifeCycleRequest{
			RequestTimeout:   l.conf.WriteTimeout,
			PartitionInfo:    partition.Info,
			PartitionNum:     partition.Info.PartitionNum,
			Actions:          dlqActions,
			PartitionStorage: partition.Store,
			PartitionState:   &partition.State,
		}
		err := l.conf.Manager.LifeCycle(ctx, req)
		cancel()
		if errors.Is(err, ErrMoveUnanswered) {
			partition.Lifecycle.UnansweredMove = req
			l.wg.Go(func() { l.awaitMoveAnswer(req.HandOff) })
			pass.Unfinished = true
		} else if err != nil {
			if errors.Is(err, ErrMovedItemsKept) {
				partition.Lifecycle.UnansweredMove = req
			}
			pass.Unfinished = true
			l.log.Warn("manager lifecycle failed",
				"partition", partition.Info.PartitionNum,
				"num_actions", len(dlqActions),
				"error", err)
		}
	}

	if len(partition.LifeCycleRequests.Requests) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
		err := partition.Store.TakeAction(ctx, partition.LifeCycleRequests, &partition.State)
		cancel()
		for _, req := range partition.LifeCycleRequests.Requests {
			for _, a := range req.Actions {
				if a.Action == types.ActionLeaseExpired {
					pass.StartNextPassBy(a.Item.ExpireDeadline)
				}
			}
		}
		if err != nil {
			partition.LifeCycleRequests.Reset()
			pass.Scan.Pivot, pass.Scan.Last = pivot, last
			retryIn := l.backOffUnfinished(partition)
			partition.Lifecycle.NextLifecycleRun = l.conf.Clock.Now().UTC().Add(retryIn)
			l.log.Error("while calling store.Partition.TakeAction(); retrying...",
				"partition", partition.Info.PartitionNum,
				"queueName", l.conf.Name,
				"retry", retryIn,
				"error", err)
			return
		}
		partition.LifeCycleRequests.Reset()
	}

	pass.StartNextPassBy(pass.Scan.EarliestLeaseDeadline)
	pass.StartNextPassBy(pass.Scan.EarliestExpireDeadline)
	if pass.Scan.MidPass() {
		partition.Lifecycle.NextLifecycleRun = now
		return
	}

	next := pass.NextPassAt
	if pass.Moved {
		next = now
	}
	if pass.Unfinished {
		next = l.conf.Clock.Now().UTC().Add(l.backOffUnfinished(partition))
	} else {
		partition.Lifecycle.UnfinishedRuns = 0
	}
	if next.IsZero() {
		next = now.Add(humanize.LongTime)
	}
	partition.Lifecycle.NextLifecycleRun = next
	pass.End()
}

func (l *Logical) awaitMoveAnswer(req *types.ProduceRequest) {
	select {
	case <-req.ReadyCh:
	case <-l.untilShutdown.Done():
		return
	}
	select {
	case l.moveAnswered <- struct{}{}:
	default:
	}
}

func (l *Logical) wakeForAnsweredMoves(state *QueueState) {
	now := l.conf.Clock.Now().UTC()
	for _, p := range state.Partitions {
		if p.Lifecycle.UnansweredMove != nil && now.Before(p.Lifecycle.NextLifecycleRun) {
			p.Lifecycle.NextLifecycleRun = now
		}
	}
	if state.Paused {
		return
	}
	l.runLifecycle(state)
	l.assignWaitingRequests(state)
	state.LifecycleTimer.Reset(l.minNextLifecycleRun(state))
}

func (l *Logical) settleUnansweredMove(partition *Partition) (holdMoves bool) {
	req := partition.Lifecycle.UnansweredMove
	if req == nil {
		return false
	}
	select {
	case <-req.HandOff.ReadyCh:
	default:
		return true
	}
	partition.Lifecycle.UnansweredMove = nil

	ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
	defer cancel()
	if err := l.conf.Manager.SettleAnsweredMove(ctx, req); err != nil {
		if errors.Is(err, ErrMovedItemsKept) {
			partition.Lifecycle.UnansweredMove = req
		}
		l.log.Warn("manager lifecycle failed",
			"partition", partition.Info.PartitionNum,
			"num_actions", len(req.Actions),
			"error", err)
		return true
	}
	return false
}

func (l *Logical) backOffUnfinished(partition *Partition) clock.Duration {
	if lifecycleBackOff.Next(partition.Lifecycle.UnfinishedRuns) < lifecycleBackOff.Max {
		partition.Lifecycle.UnfinishedRuns++
	}
	retryIn := lifecycleBackOff.Next(partition.Lifecycle.UnfinishedRuns)
	l.log.Warn("lifecycle left due work undone; retrying...",
		"partition", partition.Info.PartitionNum,
		"retry", retryIn)
	return retryIn
}

func (l *Logical) runScheduled(state *QueueState) {
	now := l.conf.Clock.Now().UTC()
	partition := duePartition(state.Partitions, now, func(p *Partition) clock.Time {
		return p.Lifecycle.NextScheduledRun
	})
	if partition == nil {
		return
	}

	if err := l.enqueueScheduled(partition, now); err != nil {
		if lifecycleBackOff.Next(partition.Lifecycle.ScheduledFailures) < lifecycleBackOff.Max {
			partition.Lifecycle.ScheduledFailures++
		}
		retryIn := lifecycleBackOff.Next(partition.Lifecycle.ScheduledFailures)
		partition.Lifecycle.NextScheduledRun = now.Add(retryIn)
		l.log.Warn("scheduled run failed; retrying...",
			"partition", partition.Info.PartitionNum,
			"retry", retryIn,
			"error", err)
		return
	}
	partition.Lifecycle.ScheduledFailures = 0

	pass := &partition.Lifecycle.ScheduledPass
	pass.StartNextPassBy(pass.Scan.EarliestEnqueueAt)
	if pass.Scan.MidPass() {
		partition.Lifecycle.NextScheduledRun = now
		return
	}

	partition.Lifecycle.NextScheduledRun = pass.NextPassAt
	if pass.NextPassAt.IsZero() {
		partition.Lifecycle.NextScheduledRun = now.Add(humanize.LongTime)
	}
	pass.End()
}

func (l *Logical) enqueueScheduled(partition *Partition, now clock.Time) error {
	pass := &partition.Lifecycle.ScheduledPass
	pivot, last := pass.Scan.Pivot, pass.Scan.Last
	ctx, cancel := context.WithTimeout(context.Background(), l.conf.ReadTimeout)
	pass.Scan.ResetPage()
	err := partition.Store.ScanForScheduled(ctx, &pass.Scan, now)
	cancel()
	if err != nil {
		return fmt.Errorf("scan for scheduled items: %w", err)
	}

	if len(pass.Scan.Actions) == 0 {
		return nil
	}

	batch := types.LifeCycleBatch{Requests: []*types.LifeCycleRequest{{
		PartitionNum: partition.Info.PartitionNum,
		Actions:      pass.Scan.Actions,
	}}}
	ctx, cancel = context.WithTimeout(context.Background(), l.conf.WriteTimeout)
	err = partition.Store.TakeAction(ctx, batch, &partition.State)
	cancel()
	for _, a := range pass.Scan.Actions {
		partition.Lifecycle.RunLifecycleBy(a.Item.ExpireDeadline)
	}
	if err != nil {
		pass.Scan.Pivot, pass.Scan.Last = pivot, last
		return fmt.Errorf("enqueue scheduled items: %w", err)
	}
	return nil
}

func (l *Logical) timedRunDue(state *QueueState) bool {
	now := l.conf.Clock.Now().UTC()
	for _, p := range state.Partitions {
		if p.Lifecycle.RunDue(now) {
			return true
		}
	}
	return false
}

func duePartition(partitions []*Partition, now clock.Time, nextRun func(*Partition) clock.Time) *Partition {
	var due *Partition
	for _, p := range partitions {
		if nextRun(p).After(now) {
			continue
		}
		if due == nil || nextRun(p).Before(nextRun(due)) {
			due = p
		}
	}
	return due
}

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

func (l *Logical) recoverPartition(partition *Partition, now clock.Time) bool {
	ctx, cancel := context.WithTimeout(context.Background(), l.conf.ReadTimeout)
	var stats types.PartitionStats
	defer cancel()

	if err := partition.Store.Stats(ctx, &stats, now); err != nil {
		partition.Lifecycle.Failures++
		retryIn := lifecycleBackOff.Next(min(partition.Lifecycle.Failures, lifecycleBackOffAttemptLimit))
		partition.Lifecycle.NextLifecycleRun = now.Add(retryIn)
		partition.State.Failures = partition.Lifecycle.Failures
		l.log.Warn("recover partition attempt failed; retrying...",
			"partition", partition.Info.PartitionNum,
			"retry", retryIn,
			"error", err)
		return false
	}

	if stats.Total-stats.NumLeased < 0 {
		partition.Lifecycle.Failures++
		partition.Lifecycle.NextLifecycleRun = now.Add(lifecycleBackOff.Next(min(partition.Lifecycle.Failures, lifecycleBackOffAttemptLimit)))
		l.log.Error("assertion failed; total count of items in the partition "+
			"cannot be more than the total leased count",
			"partition", partition.Info.PartitionNum,
			"total", stats.Total,
			"leased", stats.NumLeased)
		return false
	}

	partition.State.Failures = 0
	partition.State.UnLeased = stats.Total - stats.NumLeased
	partition.State.NumLeased = stats.NumLeased
	l.log.Info("partition recovered",
		"partition", partition.Info.PartitionNum,
		"unleased", partition.State.UnLeased,
		"leased", partition.State.NumLeased)
	return true
}
