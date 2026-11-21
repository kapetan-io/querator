package internal

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
)

const immediateRetryWindow = 100 * time.Millisecond

func (l *Logical) assignToPartitions(state *QueueState) {
	for _, req := range state.LifeCycles.Requests {
		p := state.GetPartition(req.PartitionNum)
		if p == nil {
			l.log.Warn("LifeCycleRequest received for invalid partition; skipping request",
				"partition", req.PartitionNum)
			continue
		}
		p.LifeCycleRequests.Add(req)
	}

	l.assignProduceRequests(state)

	l.assignLeaseRequests(state)
	l.assignCompleteRequests(state)
	l.assignRetryRequests(state)
}

func (l *Logical) notifyScheduled(p *Partition) bool {
	var earliestScheduled time.Time
	for _, r := range p.ProduceRequests.Requests {
		for _, item := range r.Items {
			if !item.EnqueueAt.IsZero() {
				if earliestScheduled.IsZero() || item.EnqueueAt.Before(earliestScheduled) {
					earliestScheduled = item.EnqueueAt
				}
			}
		}
	}
	if earliestScheduled.IsZero() {
		return false
	}

	now := l.conf.Clock.Now().UTC()
	return earliestScheduled.After(now) && p.Lifecycle.RunScheduledBy(earliestScheduled)
}

func (l *Logical) notifyScheduledRetry(p *Partition) bool {
	var earliestScheduled time.Time
	for _, r := range p.RetryRequests.Requests {
		for _, item := range r.Items {
			if !item.RetryAt.IsZero() {
				if earliestScheduled.IsZero() || item.RetryAt.Before(earliestScheduled) {
					earliestScheduled = item.RetryAt
				}
			}
		}
	}
	if earliestScheduled.IsZero() {
		return false
	}

	now := l.conf.Clock.Now().UTC()
	return earliestScheduled.After(now) && p.Lifecycle.RunScheduledBy(earliestScheduled)
}

func (l *Logical) notifyExpired(p *Partition) bool {
	var earliestExpire time.Time
	for _, r := range p.ProduceRequests.Requests {
		for _, item := range r.Items {
			if earliestExpire.IsZero() || item.ExpireDeadline.Before(earliestExpire) {
				earliestExpire = item.ExpireDeadline
			}
		}
	}
	if earliestExpire.IsZero() {
		return false
	}

	now := l.conf.Clock.Now().UTC()
	return earliestExpire.After(now) && p.Lifecycle.RunLifecycleBy(earliestExpire)
}

func (l *Logical) assignProduceRequests(state *QueueState) {
	sortPartitionsByLoad(state.Partitions)
	for _, req := range state.Producers.Requests {
		for _, item := range req.Items {
			item.ExpireDeadline = l.conf.Clock.Now().UTC().Add(l.conf.ExpireTimeout)
		}

		l.log.LogAttrs(req.Context, LevelDebugAll, "Produce",
			slog.Int("partition", state.Partitions[0].Info.PartitionNum),
			slog.Int("items", len(req.Items)))

		p := state.Partitions[0]
		if p.State.Failures != 0 || p.Info.ReadOnly {
			l.log.Warn("no healthy partitions or partitions are read-only, produce request not assigned",
				"num_items", len(req.Items))
			return
		}

		state.Partitions[0].Produce(req)
		sortPartitionsByLoad(state.Partitions)
	}
}

func (l *Logical) assignLeaseRequests(state *QueueState) {
	for _, req := range state.Leases.Requests {
		if req == nil {
			continue
		}

		assigned := lastHealthyPartition(state.Partitions)
		if assigned == nil {
			l.log.Warn("no healthy partitions, lease request not assigned",
				"client_id", req.ClientID,
				"num_requested", req.NumRequested)
			continue
		}

		l.log.LogAttrs(req.Context, LevelDebugAll, "lease partition assignment",
			slog.Int("partition", assigned.Info.PartitionNum),
			slog.Int("num_requested", req.NumRequested),
			slog.String("client_id", req.ClientID))

		assigned.Lease(req)

		sortPartitionsByLoad(state.Partitions)
	}
}

func (l *Logical) assignCompleteRequests(state *QueueState) {
	for _, req := range state.Completes.Requests {
		if req == nil {
			continue
		}

		p := state.GetPartition(req.Partition)
		if p == nil {
			req.Err = reply.NewRetryRequest("partition not found;"+
				" partition '%d' may have moved, retry again", req.Partition)
			continue
		}
		if p.State.Failures != 0 {
			req.Err = reply.NewRetryRequest("partition not available;"+
				" partition '%d' is failing, retry again", req.Partition)
			continue
		}
		p.CompleteRequests.Add(req)
	}
}

func (l *Logical) assignRetryRequests(state *QueueState) {
	for _, req := range state.Retries.Requests {
		if req == nil {
			continue
		}
		p := state.GetPartition(req.Partition)
		if p == nil {
			req.Err = reply.NewRetryRequest("partition not found;"+
				" partition '%d' may have moved, retry again", req.Partition)
			continue
		}
		if p.State.Failures != 0 {
			req.Err = reply.NewRetryRequest("partition not available;"+
				" partition '%d' is failing, retry again", req.Partition)
			continue
		}
		p.RetryRequests.Add(req)
	}
}

func lastHealthyPartition(partitions []*Partition) *Partition {
	for _, p := range slices.Backward(partitions) {
		if p.State.Failures == 0 {
			return p
		}
	}
	return nil
}

func sortPartitionsByLoad(partitions []*Partition) {
	slices.SortFunc(partitions, func(a, b *Partition) int {
		if a.State.Failures < b.State.Failures {
			return -1
		}
		if a.State.Failures > b.State.Failures {
			return +1
		}
		if !a.Info.ReadOnly && b.Info.ReadOnly {
			return -1
		}
		if a.Info.ReadOnly && !b.Info.ReadOnly {
			return +1
		}
		if a.State.UnLeased < b.State.UnLeased {
			return -1
		}
		if a.State.UnLeased > b.State.UnLeased {
			return +1
		}
		return 0
	})
}

func (l *Logical) finalizeRequests(state *QueueState) {
	for _, req := range state.Producers.Requests {
		if !req.Assigned {
			continue
		}
		close(req.ReadyCh)
	}

	for _, req := range state.Completes.Requests {
		if req != nil {
			close(req.ReadyCh)
		}
	}

	for _, req := range state.Retries.Requests {
		if req != nil {
			close(req.ReadyCh)
		}
	}

	for i, req := range state.Leases.Requests {
		if req == nil {
			continue
		}
		if len(req.Items) != 0 || req.Err != nil {
			state.Leases.MarkNil(i)
			close(req.ReadyCh)
		}
	}
}

func (l *Logical) storeProduce(ctx context.Context, p *Partition, batch types.ProduceBatch) bool {
	if len(batch.Requests) == 0 {
		return false
	}
	if err := p.Store.Produce(ctx, batch, l.conf.Clock.Now().UTC()); err != nil {
		l.log.Error("while calling store.Partition.Produce()",
			"partition", p.Info.PartitionNum,
			"queueName", l.conf.Name,
			"error", err)

		var unstored, stored int
		for _, req := range batch.Requests {
			unstored += len(req.Items) - req.Stored
			stored += req.Stored
			req.Err = ErrInternalRetry
		}
		p.State.UnLeased -= unstored
		return stored != 0
	}
	return true
}

func (l *Logical) applyToPartitions(state *QueueState) {

	scheduledUpdated := false
	lifecycleUpdated := false
	for _, p := range state.Partitions {
		ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)

		if len(p.ProduceRequests.Requests) != 0 {
			clients, handOffs := p.ProduceRequests, p.ProduceRequests.HandOffs()
			if len(handOffs.Requests) != 0 {
				clients = p.ProduceRequests.ClientProduces()
			}
			stored := l.storeProduce(ctx, p, clients)
			if l.storeProduce(ctx, p, handOffs) {
				stored = true
			}
			if stored {
				p.State.MostRecentDeadline = l.conf.Clock.Now().UTC().Add(l.conf.ExpireTimeout)
				if l.notifyScheduled(p) {
					scheduledUpdated = true
				}
				if l.notifyExpired(p) {
					lifecycleUpdated = true
				}
			}
		}

		if len(p.LeaseRequests.Requests) != 0 {
			leaseDeadline := l.conf.Clock.Now().UTC().Add(l.conf.LeaseTimeout)
			if err := p.Store.Lease(ctx, p.LeaseRequests, store.LeaseOptions{
				LeaseDeadline: leaseDeadline,
			}); err != nil {
				l.log.Error("while calling store.Partition.Lease()",
					"partition", p.Info.PartitionNum,
					"error", err)
			}
			p.SettleLeases()
			p.State.MostRecentDeadline = leaseDeadline

			if p.Lifecycle.RunLifecycleBy(leaseDeadline) {
				lifecycleUpdated = true
			}

			if l.log.Enabled(ctx, LevelDebugAll) {
				for _, req := range p.LeaseRequests.Requests {
					l.log.LogAttrs(req.Context, LevelDebugAll, "Leased",
						slog.Int("partition", req.Partition),
						slog.Int("num_requested", req.NumRequested),
						slog.String("client_id", req.ClientID),
						slog.Int("items_leased", len(req.Items)))
				}
			}
		}

		err := p.Store.Complete(ctx, p.CompleteRequests)
		p.State.NumLeased = max(0, p.State.NumLeased-p.CompleteRequests.Completed())
		if err != nil {
			l.log.Error("while calling store.Partition.Complete()",
				"partition", p.Info.PartitionNum,
				"queueName", l.conf.Name,
				"error", err)

			for _, req := range p.CompleteRequests.Requests {
				req.Err = ErrInternalRetry
			}
		}

		err = p.Store.Retry(ctx, p.RetryRequests)
		p.State.NumLeased = max(0, p.State.NumLeased-p.RetryRequests.Retried())
		p.State.UnLeased += p.RetryRequests.UnLeased()
		immediate := l.conf.Clock.Now().UTC().Add(immediateRetryWindow)
		for _, req := range p.RetryRequests.Requests {
			for _, item := range req.Items {
				if !item.Dead && item.RetryAt.Before(immediate) {
					p.Lifecycle.ItemMoved(item.ID)
				}
			}
		}
		if err != nil {
			l.log.Error("while calling store.Partition.Retry()",
				"partition", p.Info.PartitionNum,
				"queueName", l.conf.Name,
				"error", err)

			for _, req := range p.RetryRequests.Requests {
				req.Err = ErrInternalRetry
			}
		} else if l.notifyScheduledRetry(p) {
			scheduledUpdated = true
		}

		if err := p.Store.TakeAction(ctx, p.LifeCycleRequests, &p.State); err != nil {
			l.log.Error("while calling store.Partition.TakeAction()",
				"partition", p.Info.PartitionNum,
				"queueName", l.conf.Name,
				"error", err)

		}
		cancel()
	}

	if scheduledUpdated {
		state.ScheduledTimer.Reset(l.minNextScheduledRun(state))
	}

	if lifecycleUpdated {
		state.LifecycleTimer.Reset(l.minNextLifecycleRun(state))
	}
}
