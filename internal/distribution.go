package internal

import (
	"context"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/transport"
	"log/slog"
	"slices"
	"time"
)

// assignToPartitions assigns requests to appropriate partitions
func (l *Logical) assignToPartitions(state *QueueState) {
	// ==== LifeCycles ====
	for _, req := range state.LifeCycles.Requests {
		p := state.GetPartition(req.PartitionNum)
		if p == nil {
			l.log.Warn("LifeCycleRequest received for invalid partition; skipping request",
				"partition", req.PartitionNum)
			continue
		}
		p.LifeCycleRequests.Add(req)
	}

	// ==== Producers ====
	l.assignProduceRequests(state)

	// TODO: We should inspect the produce requests, and attempt to assign produced
	//  items with waiting lease requests if our queue is caught up.
	//  (Check for cancel or expire lease requests first)

	// ==== Lease ====
	l.assignLeaseRequests(state)
	// ==== Complete ====
	l.assignCompleteRequests(state)
	// ==== Retry ====
	l.assignRetryRequests(state)
}

// notifyScheduled iterates through the produced items. If any are scheduled
// items, then update the partition lifecycle state for the provided partition.
// Returns true if NextScheduledRun was updated.
func (l *Logical) notifyScheduled(p *Partition) bool {
	// Find the earliest scheduled item in the requests
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
	if earliestScheduled.After(now) && earliestScheduled.Before(p.Lifecycle.NextScheduledRun) {
		p.Lifecycle.NextScheduledRun = earliestScheduled
		return true
	}
	return false
}

// notifyScheduledRetry iterates through the retry items. If any have scheduled
// RetryAt times, update the partition lifecycle state for the provided partition.
// Returns true if NextScheduledRun was updated.
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
	if earliestScheduled.After(now) && earliestScheduled.Before(p.Lifecycle.NextScheduledRun) {
		p.Lifecycle.NextScheduledRun = earliestScheduled
		return true
	}
	return false
}

func (l *Logical) assignProduceRequests(state *QueueState) {
	for _, req := range state.Producers.Requests {
		// Assign a ExpireTimeout to each item
		for _, item := range req.Items {
			item.ExpireDeadline = l.conf.Clock.Now().UTC().Add(l.conf.ExpireTimeout)
		}

		l.log.LogAttrs(req.Context, LevelDebugAll, "Produce",
			slog.Int("partition", state.Partitions[0].Info.PartitionNum),
			slog.Int("items", len(req.Items)))

		// TODO: This should check the Status of the Partition, not just the failures
		//  See docs/adr/0023-partition-maintenance.md
		if state.Partitions[0].State.Failures != 0 {
			// If the chosen sorted partition has failed, then all partitions have failed, as
			// partition sorting ensures failed partitions are sorted last.
			l.log.Warn("no healthy partitions, produce request not assigned",
				"num_items", len(req.Items))
			return
		}

		// Preform Opportunistic Partition Distribution
		// See docs/adr/0019-partition-items-distribution.md for details
		state.Partitions[0].Produce(req)
		sortPartitionsByLoad(state.Partitions)
	}
}

func (l *Logical) assignLeaseRequests(state *QueueState) {
	// Assign lease requests to StoragePartitions
	for _, req := range state.Leases.Requests {
		if req == nil {
			continue
		}
		// Perform a reverse search through state.Partitions finding the first partition where b.State.Failures == 0
		var assigned *Partition
		for i := len(state.Partitions) - 1; i >= 0; i-- {
			// TODO: This should check for partition availability see docs/adr/0023-partition-maintenance.md
			if state.Partitions[i].State.Failures == 0 {
				assigned = state.Partitions[i]
				break
			}
		}

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

		// Assign the request to the chosen partition
		assigned.Lease(req)

		// Perform Opportunistic Partition Distribution
		// See docs/adr/0019-partition-items-distribution.md for details
		sortPartitionsByLoad(state.Partitions)
	}
}

func (l *Logical) assignCompleteRequests(state *QueueState) {
nextRequest:
	for _, req := range state.Completes.Requests {
		if req == nil {
			continue
		}
		var found bool
		for i, p := range state.Partitions {
			// Assign the complete request to the partition if found
			if req.Partition == p.Info.PartitionNum {
				if p.State.Failures != 0 {
					req.Err = transport.NewRetryRequest("partition not available;"+
						" partition '%d' is failing, retry again", req.Partition)
					continue nextRequest
				}
				state.Partitions[i].CompleteRequests.Add(req)
				found = true
			}
		}
		if !found {
			req.Err = transport.NewRetryRequest("partition not found;"+
				" partition '%d' may have moved, retry again", req.Partition)
		}
	}
}

func (l *Logical) assignRetryRequests(state *QueueState) {
nextRequest:
	for _, req := range state.Retries.Requests {
		if req == nil {
			continue
		}
		var found bool
		for i, p := range state.Partitions {
			// Assign the retry request to the partition if found
			if req.Partition == p.Info.PartitionNum {
				if p.State.Failures != 0 {
					req.Err = transport.NewRetryRequest("partition not available;"+
						" partition '%d' is failing, retry again", req.Partition)
					continue nextRequest
				}
				state.Partitions[i].RetryRequests.Add(req)
				found = true
			}
		}
		if !found {
			req.Err = transport.NewRetryRequest("partition not found;"+
				" partition '%d' may have moved, retry again", req.Partition)
		}
	}
}

// sortPartitionsByLoad sorts partitions by failures (ascending) then UnLeased (ascending)
// This implements the Opportunistic Distribution algorithm - see docs/adr/0019-partition-items-distribution.md
func sortPartitionsByLoad(partitions []*Partition) {
	slices.SortFunc(partitions, func(a, b *Partition) int {
		// Ensure failed Partitions are sorted last
		if a.State.Failures < b.State.Failures {
			return -1
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

// finalizeRequests closes ready channels for completed requests
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

// applyToPartitions is intended as the sync point where we optimise interaction with the
// data store. Right now (2025-04-03) this does the simple dumb thing
func (l *Logical) applyToPartitions(state *QueueState) {
	// TODO: applyToPartitions needs to indicate if a partition failed, which one, and if
	//  we should re-distribute	requests to other partitions.

	// TODO: Future: Querator should handle Partition failure by redistributing the batches
	//  to partitions which have not failed. (note: make sure we reduce the
	//  Partitions[].State.UnLeased when re-assigning)

	// TODO: Identify a degradation vs a failure and set Partition[].Failures as appropriate,
	//  and adjust the item count in state.Partitions for failed partitions

	// TODO: Interaction with each partition should probably be async so we can send multiple
	//  writes simultaneously. If one of the partitions is slow, it won't hold up the other
	//  partitions?

	scheduledUpdated := false
	for _, p := range state.Partitions {
		ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)

		// ==== Produce ====
		if len(p.ProduceRequests.Requests) != 0 {
			if err := p.Store.Produce(ctx, p.ProduceRequests, l.conf.Clock.Now().UTC()); err != nil {
				l.log.Error("while calling store.Partition.Produce()",
					"partition", p.Info.PartitionNum, "error", err)
				// TODO: Handle degradation
			}
			// Only if Produce was successful
			p.State.MostRecentDeadline = l.conf.Clock.Now().UTC().Add(l.conf.ExpireTimeout)
			if l.notifyScheduled(p) {
				scheduledUpdated = true
			}
		}

		// ==== Lease ====
		if len(p.LeaseRequests.Requests) != 0 {
			leaseDeadline := l.conf.Clock.Now().UTC().Add(l.conf.LeaseTimeout)
			if err := p.Store.Lease(ctx, p.LeaseRequests, store.LeaseOptions{
				LeaseDeadline: leaseDeadline,
			}); err != nil {
				l.log.Error("while calling store.Partition.Lease()",
					"partition", p.Info.PartitionNum,
					"error", err)
				// TODO: Handle degradation
			}
			// Only if Lease was successful
			p.State.MostRecentDeadline = leaseDeadline

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

		// ==== Complete ====
		if err := p.Store.Complete(ctx, p.CompleteRequests); err != nil {
			l.log.Error("while calling store.Partition.Complete()",
				"partition", p.Info.PartitionNum,
				"queueName", l.conf.Name,
				"error", err)

			// Completes are a bit different, in that we can't assign them to different
			// partitions, a complete request is FOR a specific partition, if there was
			// an error talking to that partition we need to tell the client.
			for _, req := range p.CompleteRequests.Requests {
				// TODO: We likely want to inform the client they should retry
				//  instead of return an internal error
				req.Err = ErrInternalRetry
			}
			// TODO: Handle degradation
		}

		// ==== Retry ====
		if err := p.Store.Retry(ctx, p.RetryRequests); err != nil {
			l.log.Error("while calling store.Partition.Retry()",
				"partition", p.Info.PartitionNum,
				"queueName", l.conf.Name,
				"error", err)

			// Retries are similar to completes, they are FOR a specific partition
			for _, req := range p.RetryRequests.Requests {
				req.Err = ErrInternalRetry
			}
			// TODO: Handle degradation
		} else if l.notifyScheduledRetry(p) {
			// Notify scheduler if any items were scheduled for retry
			scheduledUpdated = true
		}

		// ==== LifeCycle ====
		if err := p.Store.TakeAction(ctx, p.LifeCycleRequests, &p.State); err != nil {
			l.log.Error("while calling store.Partition.TakeAction()",
				"partition", p.Info.PartitionNum,
				"queueName", l.conf.Name,
				"error", err)

		}
		cancel()
	}

	// Reset scheduled timer if any partition's NextScheduledRun was updated
	if scheduledUpdated {
		state.ScheduledTimer.Reset(l.minNextScheduledRun(state))
	}
}
