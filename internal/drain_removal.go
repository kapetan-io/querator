package internal

import (
	"context"
	"slices"

	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
)

const removalRecountInterval = 10 * clock.Minute

func (p *Partition) CountedEmpty() bool {
	return p.State.UnLeased == 0 && p.State.NumLeased == 0 && p.State.Scheduled == 0
}

func (p *Partition) RemovalAllowed() bool {
	return p.Info.Draining && p.State.Failures == 0 && p.Lifecycle.UnansweredMove == nil
}

func (l *Logical) scheduleRemovalCheck(p *Partition, now clock.Time) bool {
	if !p.RemovalAllowed() || !p.CountedEmpty() || p.Lifecycle.RemovalFailures != 0 ||
		!now.Before(p.Lifecycle.NextLifecycleRun) {
		return false
	}
	p.Lifecycle.NextLifecycleRun = now
	return true
}

func (l *Logical) removeIfEmpty(state *QueueState, p *Partition, now clock.Time) {
	if !p.RemovalAllowed() {
		return
	}
	if !p.CountedEmpty() && p.Lifecycle.RemovalFailures == 0 && now.Before(p.Lifecycle.NextRemovalCount) {
		return
	}
	p.Lifecycle.NextRemovalCount = now.Add(removalRecountInterval)

	ctx, cancel := context.WithTimeout(context.Background(), l.conf.ReadTimeout)
	var stats types.PartitionStats
	err := p.Store.Stats(ctx, &stats, now)
	cancel()
	if err != nil {
		l.retryRemovalLater(p, now, "cannot read the draining partition's counts", err)
		return
	}
	if stats.Total != 0 || stats.Scheduled != 0 {
		p.State.UnLeased = stats.Total - stats.NumLeased
		p.State.NumLeased = stats.NumLeased
		p.State.Scheduled = stats.Scheduled
		p.Lifecycle.RemovalFailures = 0
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), l.conf.WriteTimeout)
	err = p.Store.DeleteStorage(ctx)
	cancel()
	if err != nil {
		l.retryRemovalLater(p, now, "cannot delete the draining partition's storage", err)
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), l.conf.WriteTimeout)
	stored, err := l.conf.Manager.RemovePartition(ctx, l.conf.Name, p.Info.PartitionNum)
	cancel()
	if err != nil {
		l.retryRemovalLater(p, now, "cannot remove the emptied draining partition from the queue record", err)
		return
	}

	l.log.Info("removed empty draining partition",
		"partition", p.Info.PartitionNum, "storage_name", p.Info.StorageName)
	state.Partitions = slices.DeleteFunc(state.Partitions, func(held *Partition) bool { return held == p })
	l.conf.StoragePartitions = slices.DeleteFunc(l.conf.StoragePartitions, func(held store.Partition) bool {
		return held == p.Store
	})
	ctx, cancel = context.WithTimeout(context.Background(), l.conf.WriteTimeout)
	if err := p.Store.Close(ctx); err != nil {
		l.log.Warn("while closing a removed partition", "partition", p.Info.PartitionNum, "error", err)
	}
	cancel()

	l.adoptRecord(state, stored)
	if l.conf.Queue != nil {
		l.conf.Queue.AdoptRevision(stored)
	}
}

func (l *Logical) retryRemovalLater(p *Partition, now clock.Time, msg string, err error) {
	if lifecycleBackOff.Next(p.Lifecycle.RemovalFailures) < lifecycleBackOff.Max {
		p.Lifecycle.RemovalFailures++
	}
	retryIn := lifecycleBackOff.Next(p.Lifecycle.RemovalFailures)
	if retry := now.Add(retryIn); retry.Before(p.Lifecycle.NextLifecycleRun) {
		p.Lifecycle.NextLifecycleRun = retry
	}
	l.log.Warn(msg+"; retrying...", "partition", p.Info.PartitionNum, "retry", retryIn, "error", err)
}
