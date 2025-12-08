package internal

import (
	"fmt"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
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
		// TODO: Update queue info for store.Partitions[i].Info and state.Partitions.Info()
		for _, d := range state.Partitions {
			d.Store.UpdateQueueInfo(info)
		}
		close(req.ReadyCh)
	case MethodUpdatePartitions:
		p := req.Request.([]store.Partition)
		l.conf.StoragePartitions = p
		close(req.ReadyCh)
	case MethodReloadPartitions:
		// TODO(reload) Reload the partitions
		// TODO: Move all the inline code into handleXXX methods
	default:
		panic(fmt.Sprintf("undefined request method '%d'", req.Method))
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
		panic(fmt.Sprintf("undefined request method '%d'", req.Method))
	}
	close(req.ReadyCh)
}

func (l *Logical) handleStats(state *QueueState, r *Request) {
	qs := r.Request.(*types.LogicalStats)
	qs.LeaseWaiting = len(state.Leases.Requests)
	qs.ProduceWaiting = len(state.Producers.Requests)
	qs.CompleteWaiting = len(state.Completes.Requests)
	qs.InFlight = int(l.inFlight.Load())

	for _, p := range l.conf.StoragePartitions {
		var ps types.PartitionStats
		if err := p.Stats(r.Context, &ps, l.conf.Clock.Now().UTC()); err != nil {
			r.Err = err
		}
		s := state.Partitions[p.Info().PartitionNum]
		// TODO Override the on disk stats with in-memory stats. We should
		//  warn if these stats do not match, or avoid querying the disk, or
		//  make fetching on disk stats a separate call.
		ps.Failures = s.State.Failures
		ps.NumLeased = s.State.NumLeased
		qs.Partitions = append(qs.Partitions, ps)
	}

	close(r.ReadyCh)
}

// handlePause places Logical into a special loop where operations none of the // produce,
// lease, complete, retry operations will be processed until we leave the loop.
func (l *Logical) handlePause(state *QueueState, r *Request) {
	pr := r.Request.(*types.PauseRequest)

	if !pr.Pause {
		close(r.ReadyCh)
		return
	}
	close(r.ReadyCh)

	l.log.Debug("paused", "logical", l.instanceID)
	defer l.log.Debug("un-paused", "logical", l.instanceID)

	for req := range l.requestCh {
		if l.isHotRequest(req) {
			l.consumeHotCh(state, req)
		} else {
			switch req.Method {
			case MethodQueuePause:
				pr := req.Request.(*types.PauseRequest)
				if pr.Pause {
					// Already paused, ignore this request
					close(req.ReadyCh)
					continue
				}
				// Cancel the pause
				close(req.ReadyCh)
				// Process any hot requests waiting
				l.handleHotRequests(state, nil)
				return
			default:
				l.handleColdRequests(state, req)
			}
		}
	}
}

func (l *Logical) handleClear(_ *QueueState, req *Request) {
	// NOTE: When clearing a queue, ensure we flush any cached items. As of this current
	// version (V0), there is no cached data to sync, but this will likely change in the future.
	cr := req.Request.(*types.ClearRequest)

	if cr.Queue {
		// Ask the store to clean up any items in the data store which are not currently out for lease
		if err := l.conf.StoragePartitions[0].Clear(req.Context, cr.Destructive); err != nil {
			req.Err = err
		}
	}
	// TODO(thrawn01): Support clearing retry and scheduled queues
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

	// Consume all requests currently in flight
	for l.inFlight.Load() != 0 {
		select {
		case r := <-l.requestCh:
			switch r.Method {
			case MethodProduce:
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
			default:
				panic(fmt.Sprintf("undefined request method '%d'", r.Method))
			}
		default:
			// If there is a request still in flight, it is possible
			// it hasn't been added to the channel yet, so we continue trying
			// until there are no more waiting requests.
		}
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
