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

const (
	maxRequestTimeout      = 15 * time.Minute
	minRequestTimeout      = 10 * time.Millisecond
	MethodStorageQueueList = iota
	MethodStorageQueueAdd
	MethodStorageQueueDelete
	MethodQueueStats
	MethodQueuePause
	MethodQueueClear
	MethodUpdateInfo

	DefaultMaxReserveBatchSize  = 1_000
	DefaultMaxProduceBatchSize  = 1_000
	DefaultMaxCompleteBatchSize = 1_000
	DefaultMaxRequestsPerQueue  = 3_000

	MsgRequestTimeout    = "request timeout; no items are in the queue, try again"
	MsgDuplicateClientID = "duplicate client id; a client cannot make multiple reserve requests to the same queue"
	MsgQueueInShutdown   = "queue is shutting down"
	MsgQueueOverLoaded   = "queue is overloaded; try again later"
)

var (
	ErrQueueShutdown  = transport.NewRequestFailed(MsgQueueInShutdown)
	ErrRequestTimeout = transport.NewRetryRequest(MsgRequestTimeout)
	ErrInternalRetry  = transport.NewRetryRequest("internal error, try your request again")
)

type QueueConfig struct {
	types.QueueInfo
	// If defined, is the logger used by the queue
	Logger duh.StandardLogger
	// QueueStore is the store interface used to persist items for this specific queue
	QueueStore store.Queue

	// TODO: Make these configurable at the Service level
	// WriteTimeout The time it should take for a single batched write to complete
	WriteTimeout time.Duration
	// ReadTimeout The time it should take for a single batched read to complete
	ReadTimeout time.Duration
	// MaxReserveBatchSize is the maximum number of items a client can request in a single reserve request
	MaxReserveBatchSize int
	// MaxProduceBatchSize is the maximum number of items a client can produce in a single produce request
	MaxProduceBatchSize int
	// MaxCompleteBatchSize is the maximum number of ids a client can mark complete in a single complete request
	MaxCompleteBatchSize int
	// MaxRequestsPerQueue is the maximum number of client requests a queue can handle before it returns an
	// queue overloaded message
	MaxRequestsPerQueue int
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
	conf           QueueConfig
	inFlight       atomic.Int32
	inShutdown     atomic.Bool
}

func NewQueue(conf QueueConfig) (*Queue, error) {
	set.Default(&conf.Logger, slog.Default())
	set.Default(&conf.MaxReserveBatchSize, DefaultMaxReserveBatchSize)
	set.Default(&conf.MaxProduceBatchSize, DefaultMaxProduceBatchSize)
	set.Default(&conf.MaxCompleteBatchSize, DefaultMaxCompleteBatchSize)
	set.Default(&conf.MaxRequestsPerQueue, DefaultMaxRequestsPerQueue)

	// TODO: Change this to a multiple of 4 once we implement /queue.defer
	if conf.MaxRequestsPerQueue%3 != 0 {
		return nil, transport.NewRetryRequest("MaxRequestsPerQueue must be a multiple of 3")
	}

	if conf.QueueStore == nil {
		return nil, transport.NewInvalidOption("QueueConfig.QueuesStore cannot be nil")
	}

	q := &Queue{
		// Queue requests are any request that doesn't require special batch processing
		queueRequestCh: make(chan *QueueRequest),
		// Shutdowns require special handling in the sync loop
		shutdownCh: make(chan *types.ShutdownRequest),
		conf:       conf,
	}

	// These are request queues that queue requests from clients until the sync loop has
	// time to process them. When they get processed, every request in the queue is handled
	// in a batch.
	fmt.Printf("NewQueue() - MaxRequestsPerQueue: %d\n", conf.MaxRequestsPerQueue/3)
	q.reserveQueueCh = make(chan *types.ReserveRequest, conf.MaxRequestsPerQueue/3)
	q.produceQueueCh = make(chan *types.ProduceRequest, conf.MaxRequestsPerQueue/3)
	q.completeQueueCh = make(chan *types.CompleteRequest, conf.MaxRequestsPerQueue/3)

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
	q.inFlight.Add(1)
	defer q.inFlight.Add(-1)

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

	if len(req.Items) == 0 {
		return transport.NewInvalidOption("items cannot be empty; at least one item is required")
	}

	if len(req.Items) > q.conf.MaxProduceBatchSize {
		return transport.NewInvalidOption("items is invalid; max_produce_batch_size is"+
			" %d but received %d", q.conf.MaxProduceBatchSize, len(req.Items))
	}

	req.RequestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	req.ReadyCh = make(chan struct{})
	req.Context = ctx

	fmt.Printf("Produce Len: %d \n", len(q.produceQueueCh))
	select {
	case q.produceQueueCh <- req:
	default:
		return transport.NewRetryRequest(MsgQueueOverLoaded)
	}

	// Wait until the request has been processed
	<-req.ReadyCh
	return req.Err
}

// Reserve is called by clients wanting to reserve a new item from the queue. This call
// will block until the request is cancelled via the passed context or the RequestTimeout
// is reached.
//
// # Context Cancellation
// IT IS NOT recommend to cancel wit context.WithTimeout() or context.WithDeadline() on Reserve() since
// Reserve() will block until the duration provided via ReserveRequest.RequestTimeout has been reached.
// Callers SHOULD cancel the context if the client has gone away, in this case Queue will abort the reservation
// request. If the context is cancelled after reservation has been written to the data store
// then those reservations will remain reserved until they can be offered to another client after the
// ReserveDeadline has been reached. See doc/adr/0009-client-timeouts.md
//
// # Unique Requests
// ClientID must NOT be empty and each request must be unique, Non-unique requests will be rejected with
// MsgDuplicateClientID. See doc/adr/0007-encourage-simple-clients.md for an explanation.
func (q *Queue) Reserve(ctx context.Context, req *types.ReserveRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}
	q.inFlight.Add(1)
	defer q.inFlight.Add(-1)

	if strings.TrimSpace(req.ClientID) == "" {
		return transport.NewInvalidOption("invalid client id; cannot be empty")
	}

	if req.NumRequested <= 0 {
		return transport.NewInvalidOption("invalid batch size; must be greater than zero")
	}

	if req.NumRequested > q.conf.MaxReserveBatchSize {
		return transport.NewInvalidOption("invalid batch size; max_reserve_batch_size is %d, "+
			"but %d was requested", q.conf.MaxProduceBatchSize, req.NumRequested)
	}

	if req.RequestTimeout == time.Duration(0) {
		return transport.NewInvalidOption("request timeout is required; '5m' is recommended, 15m is the maximum")
	}

	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidOption("request timeout is invalid; maximum timeout is '15m' but '%s' "+
			"requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout <= minRequestTimeout {
		return transport.NewInvalidOption("request timeout is invalid; minimum timeout is '10ms' but"+
			" '%s' was requested", req.RequestTimeout.String())
	}

	req.RequestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	req.ReadyCh = make(chan struct{})
	req.Context = ctx

	select {
	case q.reserveQueueCh <- req:
	default:
		return transport.NewRetryRequest(MsgQueueOverLoaded)
	}

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
	q.inFlight.Add(1)
	defer q.inFlight.Add(-1)

	if len(req.Ids) == 0 {
		return transport.NewInvalidOption("ids is invalid; list of ids cannot be empty")
	}

	if len(req.Ids) > q.conf.MaxCompleteBatchSize {
		return transport.NewInvalidOption("ids is invalid; max_complete_batch_size is"+
			" %d but received %d", q.conf.MaxCompleteBatchSize, len(req.Ids))
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

	select {
	case q.completeQueueCh <- req:
	default:
		return transport.NewRetryRequest(MsgQueueOverLoaded)
	}

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
		fmt.Printf("sync.loop\n")
		select {
		case req := <-q.produceQueueCh:
			q.handleProduceRequests(&state, req)

		case req := <-q.reserveQueueCh:
			q.handleReserveRequests(&state, req)

		case req := <-q.completeQueueCh:
			q.handleCompleteRequests(&state, req)

		case req := <-q.queueRequestCh:
			q.handleQueueRequests(&state, req)
			// If we shut down during a pause, exit immediately
			if q.inShutdown.Load() {
				fmt.Printf("sync.InShutdown\n")
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

func (q *Queue) handleProduceRequests(state *QueueState, req *types.ProduceRequest) {
	// Consume all requests in the channel, so we can process them in a batch
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
			item.DeadDeadline = time.Now().UTC().Add(q.conf.DeadTimeout)
		}
		// The writeTimeout should be equal to the request with the least amount of request timeout left.
		timeLeft := time.Now().UTC().Sub(req.RequestDeadline)
		if timeLeft < writeTimeout {
			writeTimeout = timeLeft
		}
	}

	// If we allow a calculated write timeout to be a few milliseconds, then the store.Add()
	// is almost guaranteed to fail, so we ensure the write timeout is something reasonable.
	if writeTimeout < q.conf.WriteTimeout {
		// WriteTimeout comes from the storage implementation as the user who configured the
		// storage option should know a reasonable timeout value for the configuration chosen.
		writeTimeout = q.conf.WriteTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	if err := q.conf.QueueStore.Produce(ctx, state.Producers); err != nil {
		q.conf.Logger.Error("while calling QueueStore.Produce()", "error", err,
			"category", "queue", "queueName", q.conf.Name)
		cancel()
		// Let clients that are timed out, know we are done with them.
		for _, req := range state.Producers.Requests {
			if time.Now().UTC().After(req.RequestDeadline) {
				req.Err = ErrRequestTimeout
				close(req.ReadyCh)
				continue
			}
		}
		// We get here if there was an internal error with the data store
		// TODO: If no new produce requests come in, this may never try again. We need the maintenance
		//  handler to try again at some reasonable time in the future.
		return
	}
	cancel()

	// Tell the waiting clients the items have been produced
	for _, req := range state.Producers.Requests {
		close(req.ReadyCh)
	}
	state.Producers.Reset()

	// If there are reservations waiting, then process reservations allowing them pick up the
	// items just placed into the queue.
	if state.Reservations.Total != 0 {
		q.handleReserveRequests(state, nil)
	}
}

func (q *Queue) handleReserveRequests(state *QueueState, req *types.ReserveRequest) {
	// TODO(thrawn01): Ensure we don't go beyond our max number of state.Reservations, return
	//  an error to the client

	// Consume all requests in the channel, so we can process them in a batch
	addIfUnique(&state.Reservations, req)
EMPTY:
	for {
		select {
		case req := <-q.reserveQueueCh:
			addIfUnique(&state.Reservations, req)
		default:
			break EMPTY
		}
	}

	// Remove any clients that have timed out and find the next request to timeout, which will
	// become our write timeout.
	writeTimeout := q.nextTimeout(&state.Reservations)

	// If we allow a calculated write timeout to be a few milliseconds, then the store.Add()
	// is almost guaranteed to fail, so we ensure the write timeout is something reasonable.
	if writeTimeout < q.conf.WriteTimeout {
		// WriteTimeout comes from the storage implementation as the user who configured the
		// storage option should know a reasonable timeout value for the configuration chosen.
		writeTimeout = q.conf.WriteTimeout
	}

	// Send the batch that each request wants to the store. If there are items that can be reserved the
	// store will assign items to each batch request.
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	if err := q.conf.QueueStore.Reserve(ctx, state.Reservations, store.ReserveOptions{
		ReserveDeadline: time.Now().UTC().Add(q.conf.ReserveTimeout),
	}); err != nil {
		q.conf.Logger.Error("while calling QueueStore.Reserve()", "error", err,
			"category", "queue", "queueName", q.conf.Name)
		cancel()
		// We get here if there was an internal error with the data store
		// TODO: If no new reserve requests come in, this may never try again. We need the maintenance
		//  handler to try again at some reasonable time in the future.
		return
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
	q.stateCleanUp(state)
}

func (q *Queue) handleCompleteRequests(state *QueueState, req *types.CompleteRequest) {
	// TODO(thrawn01): Ensure we don't go beyond our max number of state.Completes, return
	//  an error to the client

	// Consume all requests in the channel, so we can process them in a batch
	state.Completes.Add(req)
EMPTY:
	for {
		select {
		case req := <-q.completeQueueCh:
			state.Completes.Add(req)
		default:
			break EMPTY
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
	if writeTimeout < q.conf.WriteTimeout {
		// WriteTimeout comes from the storage implementation as the user who configured the
		// storage option should know a reasonable timeout value for the configuration chosen.
		writeTimeout = q.conf.WriteTimeout
	}

	var err error
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	if err = q.conf.QueueStore.Complete(ctx, state.Completes); err != nil {
		q.conf.Logger.Error("while calling QueueStore.Complete()", "error", err,
			"category", "queue", "queueName", q.conf.Name)
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
}

// stateCleanUp is responsible for cleaning the QueueState by removing clients that have timed out,
// and finding the next reserve request that will time out and wetting the wakeup timer.
func (q *Queue) stateCleanUp(state *QueueState) {
	fmt.Printf("stateCleanUp\n")
	next := q.nextTimeout(&state.Reservations)
	if next.Nanoseconds() != 0 {
		q.conf.Logger.Debug("next maintenance window",
			"duration", next.String(), "queue", q.conf.Name)
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
		q.conf.QueueInfo = info
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
		if err := q.conf.QueueStore.Clear(req.Context, cr.Destructive); err != nil {
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
		if err := q.conf.QueueStore.List(req.Context, sr.Items, sr.Options); err != nil {
			req.Err = err
		}
	case MethodStorageQueueAdd:
		if err := q.conf.QueueStore.Add(req.Context, *sr.Items); err != nil {
			req.Err = err
		}
	case MethodStorageQueueDelete:
		if err := q.conf.QueueStore.Delete(req.Context, sr.IDs); err != nil {
			req.Err = err
		}
	default:
		panic(fmt.Sprintf("unknown storage request method '%d'", req.Method))
	}
	close(req.ReadyCh)
}

func (q *Queue) handleStats(state *QueueState, r *QueueRequest) {
	qs := r.Request.(*types.QueueStats)
	if err := q.conf.QueueStore.Stats(r.Context, qs); err != nil {
		r.Err = err
	}
	qs.ProduceWaiting = len(q.produceQueueCh)
	qs.ReserveWaiting = len(q.reserveQueueCh)
	qs.CompleteWaiting = len(q.completeQueueCh)
	qs.ReserveBlocked = len(state.Reservations.Requests)
	qs.InFlight = int(q.inFlight.Load())
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

	q.conf.Logger.Warn("queue paused", "queue", q.conf.Name)
	defer q.conf.Logger.Warn("queue un-paused", "queue", q.conf.Name)
	for {
		fmt.Printf("handlePause.Loop\n")
		select {
		// TODO: Need to handle request timeouts by reading every item in the channels
		// and evaluating each. When we un-pause we push all those items back into the
		// channel in the same order.
		case req := <-q.shutdownCh:
			fmt.Printf("handlePause.Shutdown\n")
			q.handleShutdown(state, req)
			return
		case req := <-q.queueRequestCh:
			switch req.Method {
			case MethodQueuePause:
				pr := req.Request.(*types.PauseRequest)
				// Update the pause timeout if we receive another request to pause
				if pr.Pause {
					fmt.Printf("handlePause.Pause\n")
					timeoutCh = time.NewTimer(pr.PauseDuration)
					close(req.ReadyCh)
					continue
				}
				fmt.Printf("handlePause.UnPause\n")
				// Cancel the pause
				close(req.ReadyCh)
				// TODO: Resuming a pause should reload the data store interfaces
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
	fmt.Printf("handleShutdown\n")
	q.stateCleanUp(state)

	// Cancel any open reservations
	for _, r := range state.Reservations.Requests {
		r.Err = ErrQueueShutdown
		close(r.ReadyCh)
	}

	// Consume all requests currently in flight
	for q.inFlight.Load() != 0 {
		fmt.Printf("handleShutdown.QueueInFlight: %d\n", q.inFlight.Load())
		select {
		case r := <-q.produceQueueCh:
			fmt.Printf("handleShutdown.Produce\n")
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		case r := <-q.reserveQueueCh:
			fmt.Printf("handleShutdown.Reserve\n")
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		case r := <-q.completeQueueCh:
			fmt.Printf("handleShutdown.Reserve\n")
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		case <-time.After(100 * time.Millisecond):
			// all time for the closed requests handlers to exit
		case <-req.Context.Done():
			req.Err = req.Context.Err()
			return
		}
	}

	fmt.Printf("handleShutdown.Close() Store\n")
	if err := q.conf.QueueStore.Close(req.Context); err != nil {
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
func addIfUnique(r *types.ReserveBatch, req *types.ReserveRequest) {
	if req == nil {
		return
	}
	for _, existing := range r.Requests {
		if existing.ClientID == req.ClientID {
			req.Err = transport.NewInvalidOption(MsgDuplicateClientID)
			close(req.ReadyCh)
			return
		}
	}
	r.Total += req.NumRequested
	r.Requests = append(r.Requests, req)
}
