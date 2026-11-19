package internal

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tiger/assert"
	"log/slog"
)

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
		s := state.GetPartition(p.Info().PartitionNum)
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
	cr := req.Request.(*types.ClearRequest)

	if cr.Queue || cr.Scheduled {
		for _, p := range l.conf.StoragePartitions {
			err := p.Clear(req.Context, *cr)
			if err == nil {
				continue
			}
			if req.Err == nil {
				req.Err = err
				continue
			}
			l.log.Warn("partition clear failed",
				"partition", p.Info().PartitionNum,
				"error", err)
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
	for _, r := range state.Leases.Requests {
		if r != nil {
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		}
	}

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
