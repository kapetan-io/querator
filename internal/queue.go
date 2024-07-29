package internal

import (
	"context"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrQueueShutdown     = transport.NewRequestFailed("queue is shutting down")
	ErrDuplicateClientID = transport.NewInvalidOption("duplicate client id")
	ErrRequestTimeout    = transport.NewRetryRequest("request timeout, try again")
	ErrInternalRetry     = transport.NewRetryRequest("internal error, try your request again")
)

const (
	queueChSize            = 20_000
	maxRequestTimeout      = 15 * time.Minute
	minRequestTimeout      = 10 * time.Millisecond
	MethodStorageQueueList = iota
	MethodStorageQueueAdd
	MethodStorageQueueDelete
	MethodQueueStats
	MethodQueuePause
	MethodQueueClear
	MethodUpdateInfo
)

type QueueOptions struct {
	types.QueueInfo
	// If defined, is the logger used by the queue
	Logger duh.StandardLogger
	// QueueStore is the store interface used to persist items for this specific queue
	QueueStore store.Queue

	// TODO: Make these configurable at the Service level
	// WriteTimeout (Optional) The time it should take for a single batched write to complete
	WriteTimeout time.Duration
	// ReadTimeout (Optional) The time it should take for a single batched read to complete
	ReadTimeout time.Duration
	// MaxReserveBatchSize is the maximum number of items a client can request in a single reserve request
	MaxReserveBatchSize int
	// MaxProduceBatchSize is the maximum number of items a client can produce in a single produce request
	MaxProduceBatchSize int
	// MaxCompleteSize is the maximum number of ids a client can complete in a single complete request
	MaxCompleteSize int // TODO: implement this limit and limit storage deletion also
}

// Queue manages job is to evenly distribute and consume items from a single queue. Ensuring consumers and
// producers are handled fairly and efficiently. Since a Queue is the synchronization point for R/W
// there can ONLY BE ONE instance of a Queue running anywhere in the cluster at any given time. All consume
// and produce requests MUST go through this queue singleton.
type Queue struct {
	reserveQueueCh  chan *types.ReserveRequest
	produceQueueCh  chan *types.ProduceRequest
	completeQueueCh chan *types.CompleteRequest

	shutdownCh     chan *types.ShutdownRequest
	queueRequestCh chan *QueueRequest
	wg             sync.WaitGroup
	opts           QueueOptions
	inShutdown     atomic.Bool
}

func NewQueue(opts QueueOptions) (*Queue, error) {
	set.Default(&opts.Logger, slog.Default())
	set.Default(&opts.MaxReserveBatchSize, 1_000)
	set.Default(&opts.MaxProduceBatchSize, 1_000)
	set.Default(&opts.MaxCompleteSize, 1_000)

	if opts.QueueStore == nil {
		return nil, transport.NewInvalidOption("QueueOptions.QueuesStore cannot be nil")
	}

	q := &Queue{
		// Queue requests are any request that doesn't require special batch processing
		queueRequestCh: make(chan *QueueRequest),
		// Shutdowns require special handling in the sync loop
		shutdownCh: make(chan *types.ShutdownRequest),
		opts:       opts,
	}

	// These are request queues that queue requests from clients until the sync loop has
	// time to process them. When they get processed, every request in the queue is handled
	// in a batch.
	q.reserveQueueCh = make(chan *types.ReserveRequest, queueChSize)
	q.produceQueueCh = make(chan *types.ProduceRequest, queueChSize)
	q.completeQueueCh = make(chan *types.CompleteRequest, queueChSize)

	q.wg.Add(1)
	go q.synchronizationLoop()
	return q, nil
}

// Produce is called by clients who wish to produce an item to the queue. This
// call will block until the item has been written to the queue or until the request
// is cancelled via the passed context or RequestTimeout is reached.
func (q *Queue) Produce(ctx context.Context, req *types.ProduceRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if len(req.Items) == 0 {
		return transport.NewInvalidOption("items cannot be empty; at least one item is required")
	}

	if len(req.Items) > q.opts.MaxProduceBatchSize {
		return transport.NewInvalidOption("too many items in request; max_produce_batch_size is"+
			" %d but received %d", q.opts.MaxProduceBatchSize, len(req.Items))
	}

	if req.RequestTimeout == time.Duration(0) {
		return transport.NewInvalidOption("request timeout is required; '5m' is recommended, 15m is the maximum")
	}

	if req.RequestTimeout >= maxRequestTimeout {
		return transport.NewInvalidOption("request timeout is invalid; maximum timeout is '15m' but"+
			" '%s' was requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout <= minRequestTimeout {
		return transport.NewInvalidOption("request timeout is invalid; minimum timeout is '10ms' but"+
			" '%s' was requested", req.RequestTimeout.String())
	}

	req.RequestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	req.ReadyCh = make(chan struct{})
	req.Context = ctx

	// TODO: Handle channel full, return a "service over loaded retry again" error
	q.produceQueueCh <- req

	// Wait until the request has been processed
	<-req.ReadyCh
	return req.Err
}

// Reserve is called by clients wanting to reserve a new item from the queue. This call
// will block until the request is cancelled via the passed context or the RequestTimeout
// is reached.
//
// # Context Cancellation
// IT IS NOT recommend to use context.WithTimeout() or context.WithDeadline() with Reserve() since
// Reserve() will block until the duration provided via ReserveRequest.RequestTimeout has been reached.
// Callers SHOULD cancel the context if the client has gone away, in this case Queue will abort the reservation
// request. If the context is cancelled after reservation has been written to the data store
// then those reservations will remain reserved until they can be offered to another client after the
// ReserveDeadline has been reached. See doc/adr/0009-client-timeouts.md
//
// # Unique Requests
// ClientID must NOT be empty and each request must be unique, Non-unique requests will be rejected with
// ErrDuplicateClientID. See doc/adr/0007-encourage-simple-clients.md for an explanation.
func (q *Queue) Reserve(ctx context.Context, req *types.ReserveRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if strings.TrimSpace(req.ClientID) == "" {
		return transport.NewInvalidOption("invalid client_id; cannot be empty")
	}

	if req.NumRequested <= 0 {
		return transport.NewInvalidOption("invalid batch_size; must be greater than zero")
	}

	if req.NumRequested > q.opts.MaxReserveBatchSize {
		return transport.NewInvalidOption("invalid batch_size; exceeds maximum limit max_reserve_batch_size is %d, "+
			"but %d was requested", q.opts.MaxProduceBatchSize, req.NumRequested)
	}

	if req.RequestTimeout == time.Duration(0) {
		return transport.NewInvalidOption("request timeout is required; '5m' is recommended, 15m is the maximum")
	}

	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidOption("invalid request timeout; maximum timeout is '15m' but '%s' "+
			"requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout <= minRequestTimeout {
		return transport.NewInvalidOption("request timeout is invalid; minimum timeout is '10ms' but"+
			" '%s' was requested", req.RequestTimeout.String())
	}

	req.RequestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	req.ReadyCh = make(chan struct{})
	req.Context = ctx

	// TODO: Handle channel full, return a "service over loaded retry again" error
	q.reserveQueueCh <- req

	// Wait until the request has been processed
	<-req.ReadyCh
	return req.Err
}

// Complete is called by clients who wish to mark an item as complete. The call will block
// until the item has been marked as complete or until the request is cancelled via the passed
// context or RequestTimeout is reached.
func (q *Queue) Complete(ctx context.Context, req *types.CompleteRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidOption("request timeout is invalid; maximum timeout is '15m' but '%s' "+
			"requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout == time.Duration(0) {
		return transport.NewInvalidOption("request timeout is required; '5m' is recommended, 15m is the maximum")
	}

	req.RequestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	req.ReadyCh = make(chan struct{})
	req.Context = ctx

	// TODO: Handle channel full, return a "service over loaded retry again" error
	q.completeQueueCh <- req

	// Wait until the request has been processed
	<-req.ReadyCh
	return req.Err
}

// QueueStats retrieves stats about the queue and items in storage
func (q *Queue) QueueStats(ctx context.Context, stats *types.QueueStats) error {
	r := QueueRequest{
		Method:  MethodQueueStats,
		Request: stats,
	}
	return q.queueRequest(ctx, &r)
}

// Pause pauses processing of produce, reserve, complete and defer operations until the pause duration
// is reached or the queue is un-paused by calling Pause() with types.PauseRequest.Pause = false
func (q *Queue) Pause(ctx context.Context, req *types.PauseRequest) error {
	if req.Pause {
		// TODO: Add tests for these cases
		if req.PauseDuration.Nanoseconds() == 0 {
			return transport.NewInvalidOption("pause_duration is invalid; cannot be empty")
		}

		if req.PauseDuration <= 100*time.Millisecond {
			return transport.NewInvalidOption("pause_duration is invalid; cannot be less than 100ms")
		}
	}

	r := QueueRequest{
		Method:  MethodQueuePause,
		Request: req,
	}
	return q.queueRequest(ctx, &r)
}

// Clear clears enqueued items from the queue
func (q *Queue) Clear(ctx context.Context, req *types.ClearRequest) error {
	if !req.Queue && !req.Defer && !req.Scheduled {
		return transport.NewInvalidOption("invalid clear request; one of 'queue', 'defer'," +
			" 'scheduled' must be true")
	}

	r := QueueRequest{
		Method:  MethodQueueClear,
		Request: req,
	}
	return q.queueRequest(ctx, &r)
}

func (q *Queue) UpdateInfo(ctx context.Context, info types.QueueInfo) error {
	r := QueueRequest{
		Method:  MethodUpdateInfo,
		Request: info,
	}
	return q.queueRequest(ctx, &r)
}

// -------------------------------------------------
// Methods to manage queue storage
// -------------------------------------------------

func (q *Queue) StorageQueueList(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
	req := StorageRequest{
		Items:   items,
		Options: opts,
	}

	// TODO: Test for invalid pivot

	r := QueueRequest{
		Method:  MethodStorageQueueList,
		Request: req,
	}
	return q.queueRequest(ctx, &r)
}

func (q *Queue) StorageQueueAdd(ctx context.Context, items *[]*types.Item) error {
	// TODO: Test for empty list
	r := QueueRequest{
		Method: MethodStorageQueueAdd,
		Request: StorageRequest{
			Items: items,
		},
	}
	return q.queueRequest(ctx, &r)
}

func (q *Queue) StorageQueueDelete(ctx context.Context, ids []types.ItemID) error {
	if len(ids) == 0 {
		return transport.NewInvalidOption("ids is invalid; cannot be empty")
	}

	r := QueueRequest{
		Method: MethodStorageQueueDelete,
		Request: StorageRequest{
			IDs: ids,
		},
	}
	return q.queueRequest(ctx, &r)
}

// -------------------------------------------------
// Main Loop and Handlers
// See doc/adr/0003-rw-sync-point.md for an explanation of this design
// -------------------------------------------------

// TODO: Break up this loop into smaller handlers to reduce the size of this method.
func (q *Queue) synchronizationLoop() {
	defer q.wg.Done()

	state := QueueState{
		Reservations: types.ReserveBatch{
			Requests: make([]*types.ReserveRequest, 0, 5_000),
			Total:    0,
		},
		Producers: types.Batch[types.ProduceRequest]{
			Requests: make([]*types.ProduceRequest, 0, 5_000),
		},
		Completes: types.Batch[types.CompleteRequest]{
			Requests: make([]*types.CompleteRequest, 0, 5_000),
		},
	}

	for {
		select {

		// -----------------------------------------------
		case req := <-q.produceQueueCh:
			// Consume all items in the channel, so we can process the entire batch

			// TODO: Consider making this a separate method to consume all items and handle request timeout
			//  this will avoid the need for a label and this code might become complex if we over flow with
			//  requests.
			state.Producers.Add(req)
		CONTINUE1:
			for {
				select {
				case req := <-q.produceQueueCh:
					// TODO(thrawn01): Ensure we don't go beyond our max number of state.Producers, if we do,
					//  then we must inform the client that we are overloaded, and they must try again. We
					//  cannot hold on to a request and let it sit in the channel, else we break the
					//  request_timeout contract we have with the client.

					// TODO(thrawn01): Paused queues must also process this channel, and place produce requests
					//  into state.Producers, and handle requests if this channel is full.
					state.Producers.Add(req)
				default:
					break CONTINUE1
				}
			}

			// FUTURE: Inspect the Waiting Reserve Requests, and attempt to assign produced items with
			//  waiting reserve requests if our queue is caught up.
			//  (Check for cancel or expire reserve requests first)

			// FUTURE: Buffer the produced items at the top of the queue into memory, so we don't need
			//  to query them from the database when we reserve items later. Doing so avoids the ListReservable()
			//  step, in addition, we can back fill reservable items into memory when synchronizationLoop() isn't
			//  actively producing or consuming.

			// Assign a DeadTimeout and Calculate an appropriate write timeout such that we respect RequestDeadline
			// if we are experiencing a saturation event
			writeTimeout := maxRequestTimeout
			for _, req := range state.Producers.Requests {
				// Cancel any produce requests that have timed out
				if time.Now().UTC().After(req.RequestDeadline) {
					req.Err = ErrRequestTimeout
					state.Producers.Remove(req)
					close(req.ReadyCh)
					continue
				}
				// Assign a DeadTimeout to each item
				for _, item := range req.Items {
					item.DeadDeadline = time.Now().UTC().Add(q.opts.DeadTimeout)
				}
				// The writeTimeout should be equal to the request with the least amount of request timeout left.
				timeLeft := time.Now().UTC().Sub(req.RequestDeadline)
				if timeLeft < writeTimeout {
					writeTimeout = timeLeft
				}
			}

			// If we allow a calculated write timeout to be a few milliseconds, then the store.Add()
			// is almost guaranteed to fail, so we ensure the write timeout is something reasonable.
			if writeTimeout < q.opts.WriteTimeout {
				// WriteTimeout comes from the storage implementation as the user who configured the
				// storage option should know a reasonable timeout value for the configuration chosen.
				writeTimeout = q.opts.WriteTimeout
			}

			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			if err := q.opts.QueueStore.Produce(ctx, state.Producers); err != nil {
				q.opts.Logger.Error("while calling QueueStore.Produce()", "error", err,
					"category", "queue", "queueName", q.opts.Name)
				cancel()
				// Let clients that are timed out, know we are done with them.
				for _, req := range state.Producers.Requests {
					if time.Now().UTC().After(req.RequestDeadline) {
						req.Err = ErrRequestTimeout
						close(req.ReadyCh)
						continue
					}
				}
				continue
			}
			cancel()

			// Tell the waiting clients the items have been produced
			for _, req := range state.Producers.Requests {
				close(req.ReadyCh)
			}
			state.Producers.Reset()

		// -----------------------------------------------
		case req := <-q.reserveQueueCh:
			// Consume all items in the channel, so we can process the entire batch

			// TODO(thrawn01): Ensure we don't go beyond our max number of state.Reservations, return
			//  an error to the client

			addIfUnique(&state.Reservations, req)
		CONTINUE2:
			for {
				select {
				case req := <-q.reserveQueueCh:
					addIfUnique(&state.Reservations, req)
				default:
					break CONTINUE2
				}
			}

			// Remove any clients that have timed out and find the next request to timeout.
			writeTimeout := q.nextTimeout(&state.Reservations)

			// If we allow a calculated write timeout to be a few milliseconds, then the store.Add()
			// is almost guaranteed to fail, so we ensure the write timeout is something reasonable.
			if writeTimeout < q.opts.WriteTimeout {
				// WriteTimeout comes from the storage implementation as the user who configured the
				// storage option should know a reasonable timeout value for the configuration chosen.
				writeTimeout = q.opts.WriteTimeout
			}

			// Send the batch that each request wants to the store. If there are items that can be reserved the
			// store will assign items to each batch request.
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			if err := q.opts.QueueStore.Reserve(ctx, state.Reservations, store.ReserveOptions{
				ReserveDeadline: time.Now().UTC().Add(q.opts.ReserveTimeout),
			}); err != nil {
				q.opts.Logger.Error("while calling QueueStore.Reserve()", "error", err,
					"category", "queue", "queueName", q.opts.Name)
				cancel()
				continue
			}
			cancel()

			// Inform clients they have reservations ready or if there was an error
			for i, req := range state.Reservations.Requests {
				if req == nil {
					continue
				}
				if len(req.Items) != 0 || req.Err != nil {
					state.Reservations.MarkNil(i)
					close(req.ReadyCh)
				}
			}
			q.stateCleanUp(&state)

		// -----------------------------------------------
		case req := <-q.completeQueueCh:
			// TODO(thrawn01): Ensure we don't go beyond our max number of state.Completes, return
			//  an error to the client

			state.Completes.Add(req)
		CONTINUE3:
			for {
				select {
				case req := <-q.completeQueueCh:
					state.Completes.Add(req)
				default:
					break CONTINUE3
				}
			}

			writeTimeout := maxRequestTimeout
			for _, req := range state.Completes.Requests {
				// Cancel any produce requests that have timed out
				if time.Now().UTC().After(req.RequestDeadline) {
					req.Err = ErrRequestTimeout
					state.Completes.Remove(req)
					close(req.ReadyCh)
					continue
				}
				// The writeTimeout should be equal to the request with the least amount of request timeout left.
				timeLeft := time.Now().UTC().Sub(req.RequestDeadline)
				if timeLeft < writeTimeout {
					writeTimeout = timeLeft
				}
			}

			// If we allow a calculated write timeout to be a few milliseconds, then the store.Add()
			// is almost guaranteed to fail, so we ensure the write timeout is something reasonable.
			if writeTimeout < q.opts.WriteTimeout {
				// WriteTimeout comes from the storage implementation as the user who configured the
				// storage option should know a reasonable timeout value for the configuration chosen.
				writeTimeout = q.opts.WriteTimeout
			}

			var err error
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			if err = q.opts.QueueStore.Complete(ctx, state.Completes); err != nil {
				q.opts.Logger.Error("while calling QueueStore.Complete()", "error", err,
					"category", "queue", "queueName", q.opts.Name)
			}
			cancel()

			// Tell the waiting clients that items have been marked as complete
			for _, req := range state.Completes.Requests {
				if err != nil {
					req.Err = ErrInternalRetry
				}
				close(req.ReadyCh)
			}
			state.Completes.Reset()

		case req := <-q.queueRequestCh:
			q.handleQueueRequests(&state, req)
			// If we shut down during a pause, exit immediately
			if q.inShutdown.Load() {
				return
			}
			q.stateCleanUp(&state)

		case req := <-q.shutdownCh:
			q.handleShutdown(&state, req)
			return

		case <-state.NextMaintenanceCh:
			q.stateCleanUp(&state)

			// TODO: Preform queue maintenance, cleaning up reserved items that have not been completed and
			//  moving items into the dead letter queue.
		}
	}
}

// stateCleanUp is responsible for cleaning the QueueState by removing clients that have timed out,
// and finding the next reserve request that will time out and wetting the wakeup timer.
func (q *Queue) stateCleanUp(state *QueueState) {
	next := q.nextTimeout(&state.Reservations)
	if next.Nanoseconds() != 0 {
		q.opts.Logger.Debug("next maintenance window",
			"duration", next.String(), "queue", q.opts.Name)
		state.NextMaintenanceCh = time.After(next)
	}
	state.Reservations.FilterNils()
}

func (q *Queue) Shutdown(ctx context.Context) error {
	if q.inShutdown.Swap(true) {
		return nil
	}

	req := &types.ShutdownRequest{
		ReadyCh: make(chan struct{}),
		Context: ctx,
	}

	// Wait until q.synchronizationLoop() shutdown is complete or until
	// our context is cancelled.
	select {
	case q.shutdownCh <- req:
		q.wg.Wait()
		return req.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *Queue) queueRequest(ctx context.Context, r *QueueRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}

	r.ReadyCh = make(chan struct{})
	r.Context = ctx

	select {
	case q.queueRequestCh <- r:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-r.ReadyCh:
		return r.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// -------------------------------------------------
// Handlers for the main and pause loops
// -------------------------------------------------

func (q *Queue) handleQueueRequests(state *QueueState, req *QueueRequest) {
	switch req.Method {
	case MethodStorageQueueList, MethodStorageQueueAdd, MethodStorageQueueDelete:
		q.handleStorageRequests(req)
	case MethodQueueStats:
		q.handleStats(state, req)
	case MethodQueuePause:
		q.handlePause(state, req)
	case MethodQueueClear:
		q.handleClear(state, req)
	case MethodUpdateInfo:
		info := req.Request.(types.QueueInfo)
		q.opts.QueueInfo = info
		close(req.ReadyCh)
	default:
		panic(fmt.Sprintf("unknown queue request method '%d'", req.Method))
	}
}

func (q *Queue) handleClear(_ *QueueState, req *QueueRequest) {
	// NOTE: When clearing a queue, ensure we flush any cached items. As of this current
	// version (V0), there is no cached data to sync, but this will likely change in the future.
	cr := req.Request.(*types.ClearRequest)

	if cr.Queue {
		// Ask the store to clean up any items in the data store which are not currently out for reservation
		if err := q.opts.QueueStore.Clear(req.Context, cr.Destructive); err != nil {
			req.Err = err
		}
	}
	// TODO(thrawn01): Support clearing defer and scheduled
	close(req.ReadyCh)
}

func (q *Queue) handleStorageRequests(req *QueueRequest) {
	sr := req.Request.(StorageRequest)
	switch req.Method {
	case MethodStorageQueueList:
		if err := q.opts.QueueStore.List(req.Context, sr.Items, sr.Options); err != nil {
			req.Err = err
		}
	case MethodStorageQueueAdd:
		if err := q.opts.QueueStore.Add(req.Context, *sr.Items); err != nil {
			req.Err = err
		}
	case MethodStorageQueueDelete:
		if err := q.opts.QueueStore.Delete(req.Context, sr.IDs); err != nil {
			req.Err = err
		}
	default:
		panic(fmt.Sprintf("unknown storage request method '%d'", req.Method))
	}
	close(req.ReadyCh)
}

func (q *Queue) handleStats(state *QueueState, r *QueueRequest) {
	qs := r.Request.(*types.QueueStats)
	if err := q.opts.QueueStore.Stats(r.Context, qs); err != nil {
		r.Err = err
	}
	qs.ProduceWaiting = len(q.produceQueueCh)
	qs.ReserveWaiting = len(q.reserveQueueCh)
	qs.CompleteWaiting = len(q.completeQueueCh)
	qs.ReserveBlocked = len(state.Reservations.Requests)
	close(r.ReadyCh)
}

// handlePause places Queue into a special loop where operations are limited and none of the
// produce, reserve, complete, defer operations will be processed until we leave the loop.
func (q *Queue) handlePause(state *QueueState, r *QueueRequest) {
	pr := r.Request.(*types.PauseRequest)

	// NOTE: Since we are not currently paused, and yet we get an un-pause request likely the user
	// wants us to sync any cached state and reload from the data store if necessary.
	// As of this current version (V0), there is no cached data to sync, but this will likely
	// change in the future.
	if !pr.Pause {
		close(r.ReadyCh)
		return
	}
	timeoutCh := time.NewTimer(pr.PauseDuration)
	close(r.ReadyCh)

	q.opts.Logger.Warn("queue paused", "queue", q.opts.Name)
	defer q.opts.Logger.Warn("queue un-paused", "queue", q.opts.Name)
	for {
		select {
		// TODO: Need to handle request timeouts
		case req := <-q.shutdownCh:
			q.handleShutdown(state, req)
			return
		case req := <-q.queueRequestCh:
			switch req.Method {
			case MethodQueuePause:
				pr := req.Request.(*types.PauseRequest)
				// Update the pause timeout if we receive another request to pause
				if pr.Pause {
					timeoutCh = time.NewTimer(pr.PauseDuration)
					close(req.ReadyCh)
					continue
				}
				// Cancel the pause
				close(req.ReadyCh)
				return
			default:
				q.handleQueueRequests(state, req)
			}
		case <-timeoutCh.C:
			// Cancel the pause
			return
		}
	}
}

func (q *Queue) handleShutdown(state *QueueState, req *types.ShutdownRequest) {
	q.stateCleanUp(state)
	for _, r := range state.Producers.Requests {
		r.Err = ErrQueueShutdown
		close(r.ReadyCh)
	}
	for _, r := range state.Reservations.Requests {
		r.Err = ErrQueueShutdown
		close(r.ReadyCh)
	}
	if err := q.opts.QueueStore.Close(req.Context); err != nil {
		req.Err = err
	}
	close(req.ReadyCh)
}

// -------------------------------------------------
// Support Functions & Methods
// -------------------------------------------------

// TODO: I don't think we should pass by ref. We should use escape analysis to decide
func (q *Queue) nextTimeout(r *types.ReserveBatch) time.Duration {
	var soon *types.ReserveRequest

	for i, req := range r.Requests {
		if req == nil {
			continue
		}

		// If request has already expired
		if time.Now().UTC().After(req.RequestDeadline) {
			// Inform our waiting client
			req.Err = ErrRequestTimeout
			close(req.ReadyCh)
			r.MarkNil(i)
			continue
		}

		// If client has gone away
		if req.Context.Err() != nil {
			r.Total -= req.NumRequested
			req.Err = req.Context.Err()
			close(req.ReadyCh)
			r.MarkNil(i)
			continue
		}

		// If there is no soon
		if soon == nil {
			soon = req
			continue
		}

		// Will this happen sooner?
		if req.RequestDeadline.Before(soon.RequestDeadline) {
			soon = req
		}
	}

	// No sooner than now =)
	if soon == nil {
		return time.Duration(0)
	}

	// How soon is it? =)
	return soon.RequestDeadline.Sub(time.Now().UTC())
}

// addIfUnique adds a ReserveRequest to the batch. Returns false if the ReserveRequest.ClientID is a duplicate
// and the request was not added to the batch
func addIfUnique(r *types.ReserveBatch, req *types.ReserveRequest) bool {
	for _, existing := range r.Requests {
		if existing.ClientID == req.ClientID {
			req.Err = ErrDuplicateClientID
			close(req.ReadyCh)
			return false
		}
	}
	r.Total += req.NumRequested
	r.Requests = append(r.Requests, req)
	return false
}
