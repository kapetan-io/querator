package internal

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tiger/assert"
	"log/slog"
)

// handleColdRequests routes cold requests to specific handlers
func (l *Logical) handleColdRequests(state *QueueState, req *Request) {
	switch req.Method {
	case MethodStorageItemsList, MethodStorageItemsImport, MethodStorageItemsDelete:
		l.handleStorageRequests(req)
	case MethodQueueStats:
		l.handleStats(state, req)
	case MethodQueuePause:
		l.handlePause(state, req)
	case MethodQueueClear:
		l.handleClear(state, req)
	case MethodUpdateInfo:
		info := req.Request.(types.QueueInfo)
		l.conf.QueueInfo = info
		for _, d := range state.Partitions {
			d.Store.UpdateQueueInfo(info)
			d.Info.Queue = info
		}
		close(req.ReadyCh)
	case MethodUpdatePartitions:
		p := req.Request.([]store.Partition)
		l.conf.StoragePartitions = p
		close(req.ReadyCh)
	case MethodReload:
		l.handleReload(state, req)
	default:
		assert.Unreachable(fmt.Sprintf("undefined request method '%d'", req.Method))
	}
}

func (l *Logical) handleStorageRequests(req *Request) {
	sr := req.Request.(StorageRequest)

	switch req.Method {
	case MethodStorageItemsList:
		switch sr.ListKind {
		case types.ListItems:
			if err := l.conf.StoragePartitions[sr.Partition].List(req.Context, sr.Items, sr.Options); err != nil {
				req.Err = err
			}
		case types.ListScheduled:
			if err := l.conf.StoragePartitions[sr.Partition].ListScheduled(req.Context, sr.Items, sr.Options); err != nil {
				req.Err = err
			}
		default:
			assert.Unreachable(fmt.Sprintf("undefined list kind '%d'", sr.ListKind))
		}
	case MethodStorageItemsImport:
		if err := l.conf.StoragePartitions[sr.Partition].Add(req.Context, *sr.Items, l.conf.Clock.Now().UTC()); err != nil {
			req.Err = err
		}
	case MethodStorageItemsDelete:
		if err := l.conf.StoragePartitions[sr.Partition].Delete(req.Context, sr.IDs); err != nil {
			req.Err = err
		}
	default:
		assert.Unreachable(fmt.Sprintf("undefined request method '%d'", req.Method))
	}
	close(req.ReadyCh)
}

func (l *Logical) handleStats(state *QueueState, r *Request) {
	qs := r.Request.(*types.LogicalStats)
	qs.LeaseWaiting = len(state.Leases.Requests)
	qs.ProduceWaiting = len(state.Producers.Requests)
	qs.CompleteWaiting = len(state.Completes.Requests)
	qs.InFlight = int(l.inFlight.Load())

	now := l.conf.Clock.Now().UTC()
	for _, p := range l.conf.StoragePartitions {
		var ps types.PartitionStats
		if err := p.Stats(r.Context, &ps, now); err != nil {
			r.Err = err
		}
		ps.Partition = p.Info().PartitionNum
		// Look up the in-memory partition by number. state.Partitions is
		// continuously reordered by sortPartitionsByLoad (ADR-0019), so it
		// cannot be indexed by partition number.
		s := state.GetPartition(p.Info().PartitionNum)
		// TODO Override the on disk stats with in-memory stats. We should
		//  warn if these stats do not match, or avoid querying the disk, or
		//  make fetching on disk stats a separate call.
		if s != nil {
			ps.Failures = s.State.Failures
			ps.NumLeased = s.State.NumLeased
			ps.UnLeased = s.State.UnLeased
			ps.NextLifecycleRun = s.Lifecycle.NextLifecycleRun.Sub(now)
		}
		qs.Partitions = append(qs.Partitions, ps)
	}

	close(r.ReadyCh)
}

// handlePause sets or cancels state.Paused. While paused, requestLoop holds produce, lease,
// complete and retry requests in state; cancelling the pause applies them.
func (l *Logical) handlePause(state *QueueState, r *Request) {
	pr := r.Request.(*types.PauseRequest)
	close(r.ReadyCh)

	if pr.Pause == state.Paused {
		return
	}
	state.Paused = pr.Pause

	if state.Paused {
		l.log.Debug("paused", "logical", l.instanceID)
		return
	}
	l.log.Debug("un-paused", "logical", l.instanceID)
	l.handleHotRequests(state, nil)
}

func (l *Logical) handleClear(_ *QueueState, req *Request) {
	// NOTE: When clearing a queue, ensure we flush any cached items. As of this current
	// version (V0), there is no cached data to sync, but this will likely change in the future.
	cr := req.Request.(*types.ClearRequest)

	if cr.Queue || cr.Scheduled {
		for _, p := range l.conf.StoragePartitions {
			if err := p.Clear(req.Context, *cr); err != nil {
				req.Err = err
				return
			}
		}
	}
	close(req.ReadyCh)
}

func (l *Logical) handleReload(state *QueueState, req *Request) {
	for _, partition := range state.Partitions {
		ctx, cancel := context.WithTimeout(req.Context, l.conf.ReadTimeout)
		var stats types.PartitionStats
		err := partition.Store.Stats(ctx, &stats, l.conf.Clock.Now().UTC())
		cancel()

		if err != nil {
			l.log.Warn("partition unavailable during reload",
				"partition", partition.Info.PartitionNum,
				"error", err)
			partition.State.Failures = 1
		} else {
			partition.State.UnLeased = stats.Total - stats.NumLeased
			partition.State.NumLeased = stats.NumLeased
			partition.State.Failures = 0
		}
	}

	close(req.ReadyCh)
}

func (l *Logical) handleShutdown(state *QueueState, req *types.ShutdownRequest) {
	// Cancel any open leases
	for _, r := range state.Leases.Requests {
		if r != nil {
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		}
	}

	// While paused, produce, complete and retry requests accumulate in state
	// unprocessed; cancel them or the in-flight drain below never finishes.
	for _, r := range state.Producers.Requests {
		if r != nil {
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		}
	}
	for _, r := range state.Completes.Requests {
		if r != nil {
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		}
	}
	for _, r := range state.Retries.Requests {
		if r != nil {
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		}
	}

	if err := l.rejectInFlight(req.Context); err != nil {
		l.log.Warn("shutdown context ended before in-flight requests drained", "error", err)
		req.Err = err
	}

	state.LifecycleTimer.Stop()
	state.ScheduledTimer.Stop()

	// TODO: Should close the partitions in `state.Partitions` not in conf.StoragePartitions
	//  as the list of partitions could have changed since init.
	for _, p := range l.conf.StoragePartitions {
		l.log.LogAttrs(req.Context, LevelDebugAll, "close partition",
			slog.Int("partition", p.Info().PartitionNum))
		if err := p.Close(req.Context); err != nil {
			l.log.Error("during partition close", "error", err)
			req.Err = err
		}
	}
	close(req.ReadyCh)
}

// rejectInFlight answers every request still in requestCh with ErrQueueShutdown until no
// caller is in flight, or until ctx ends.
func (l *Logical) rejectInFlight(ctx context.Context) error {
	for {
		if l.inFlight.Load() == 0 {
			return nil
		}
		select {
		case r := <-l.requestCh:
			rejectWithShutdown(r)
		case <-ctx.Done():
			return ctx.Err()
		default:
			// A caller in flight may not have sent its request yet, so check again.
		}
	}
}

func rejectWithShutdown(r *Request) {
	switch r.Method {
	case MethodProduce, MethodProduceInternal:
		o := r.Request.(*types.ProduceRequest)
		o.Err = ErrQueueShutdown
		close(o.ReadyCh)
	case MethodLease:
		o := r.Request.(*types.LeaseRequest)
		o.Err = ErrQueueShutdown
		close(o.ReadyCh)
	case MethodComplete:
		o := r.Request.(*types.CompleteRequest)
		o.Err = ErrQueueShutdown
		close(o.ReadyCh)
	case MethodRetry:
		o := r.Request.(*types.RetryRequest)
		o.Err = ErrQueueShutdown
		close(o.ReadyCh)
	case MethodStorageItemsList, MethodStorageItemsImport, MethodStorageItemsDelete,
		MethodQueueStats, MethodQueuePause, MethodQueueClear, MethodUpdateInfo,
		MethodUpdatePartitions, MethodReload:
		r.Err = ErrQueueShutdown
		close(r.ReadyCh)
	default:
		assert.Unreachable(fmt.Sprintf("undefined request method '%d'", r.Method))
	}
}
