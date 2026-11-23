package internal

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/kapetan-io/querator/internal/placement"
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
	load := state.load
	for _, req := range state.Producers.Requests {
		for _, item := range req.Items {
			item.ExpireDeadline = l.conf.Clock.Now().UTC().Add(l.conf.ExpireTimeout)
		}

		info, ok := placement.ProduceInto(load.queue, load.unleased, load.failing)
		if !ok {
			l.log.Warn("no healthy partitions, produce request not assigned",
				"num_items", len(req.Items))
			return
		}

		l.log.LogAttrs(req.Context, LevelDebugAll, "Produce",
			slog.Int("partition", info.PartitionNum),
			slog.Int("items", len(req.Items)))

		load.partitions[info.PartitionNum].Produce(req)
	}
}

func (l *Logical) assignLeaseRequests(state *QueueState) {
	load := state.load
	for _, req := range state.Leases.Requests {
		if req == nil {
			continue
		}
		info, ok := placement.LeaseFrom(load.queue, load.unleased, load.failing)
		if !ok {
			l.log.Warn("no healthy partitions, lease request not assigned",
				"client_id", req.ClientID,
				"num_requested", req.NumRequested)
			continue
		}
		assigned := load.partitions[info.PartitionNum]

		l.log.LogAttrs(req.Context, LevelDebugAll, "lease partition assignment",
			slog.Int("partition", assigned.Info.PartitionNum),
			slog.Int("num_requested", req.NumRequested),
			slog.String("client_id", req.ClientID))

		assigned.Lease(req)
	}
}

type queueLoad struct {
	queue      types.QueueInfo
	partitions map[int]*Partition
}

func (q queueLoad) unleased(info types.PartitionInfo) int {
	return q.partitions[info.PartitionNum].State.UnLeased
}

func (q queueLoad) failing(info types.PartitionInfo) bool {
	return q.partitions[info.PartitionNum].State.Failures != 0
}

func (l *Logical) loadOf(state *QueueState) queueLoad {
	load := queueLoad{
		queue:      l.conf.QueueInfo,
		partitions: make(map[int]*Partition, len(state.Partitions)),
	}
	load.queue.PartitionInfo = make([]types.PartitionInfo, 0, len(state.Partitions))
	for _, p := range state.Partitions {
		load.partitions[p.Info.PartitionNum] = p
		load.queue.PartitionInfo = append(load.queue.PartitionInfo, p.Info)
	}
	return load
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
	for _, req := range batch.Requests {
		for _, item := range req.Items {
			if !item.EnqueueAt.IsZero() {
				p.State.Scheduled++
			}
		}
	}
	return true
}

type heldHandOff struct {
	request *types.ProduceRequest
	items   []*types.Item
	held    map[string]struct{}
}

func (h heldHandOff) restore(p *Partition) {
	sent := h.request.Items
	h.request.Items = h.items
	p.State.UnLeased -= len(h.held)
	if h.request.Err == nil {
		return
	}

	var prefix, stored int
	for _, item := range h.items {
		if _, ok := h.held[string(item.SourceID)]; ok {
			prefix++
			continue
		}
		if stored == min(h.request.Stored, len(sent)) {
			break
		}
		stored++
		prefix++
	}
	h.request.Stored = prefix
}

func (l *Logical) storeHandOffs(ctx context.Context, state *QueueState, p *Partition,
	handOffs types.ProduceBatch) bool {
	var draining []*Partition
	for _, other := range state.Partitions {
		if other != p && other.Info.Draining {
			draining = append(draining, other)
		}
	}
	if len(draining) == 0 || len(handOffs.Requests) == 0 {
		return l.storeProduce(ctx, p, handOffs)
	}

	var sending types.ProduceBatch
	var restoring []heldHandOff
	for _, req := range handOffs.Requests {
		sourceIDs := make([]types.ItemID, 0, len(req.Items))
		for _, item := range req.Items {
			sourceIDs = append(sourceIDs, item.SourceID)
		}

		var found []types.ItemID
		var err error
		for _, other := range draining {
			if err = other.Store.FindSourceIDs(ctx, sourceIDs, &found); err != nil {
				l.log.Error("while calling store.Partition.FindSourceIDs()",
					"partition", other.Info.PartitionNum,
					"queueName", l.conf.Name,
					"error", err)
				break
			}
		}
		if err != nil {
			req.Err = ErrInternalRetry
			p.State.UnLeased -= len(req.Items)
			continue
		}
		if len(found) == 0 {
			sending.Add(req)
			continue
		}

		held := make(map[string]struct{}, len(found))
		for _, source := range found {
			held[string(source)] = struct{}{}
		}
		restoring = append(restoring, heldHandOff{request: req, items: req.Items, held: held})
		req.Items = slices.DeleteFunc(slices.Clone(req.Items), func(item *types.Item) bool {
			_, ok := held[string(item.SourceID)]
			return ok
		})
		if len(req.Items) != 0 {
			sending.Add(req)
		}
	}

	stored := l.storeProduce(ctx, p, sending)
	for _, h := range restoring {
		h.restore(p)
	}
	return stored
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
			if l.storeHandOffs(ctx, state, p, handOffs) {
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
		p.State.Scheduled += p.RetryRequests.Scheduled()
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
