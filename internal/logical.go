package internal

import (
	"context"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	maxRequestTimeout      = 15 * clock.Minute
	minRequestTimeout      = 10 * clock.Millisecond
	MethodStorageQueueList = iota
	MethodStorageQueueAdd
	MethodStorageQueueDelete
	MethodQueueStats
	MethodQueuePause
	MethodQueueClear
	MethodUpdateInfo
	MethodUpdatePartitions

	DefaultMaxReserveBatchSize  = 1_000
	DefaultMaxProduceBatchSize  = 1_000
	DefaultMaxCompleteBatchSize = 1_000
	DefaultMaxRequestsPerQueue  = 500

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

type LogicalConfig struct {
	types.QueueInfo

	// If defined, is the logger used by the queue
	Logger duh.StandardLogger
	// TODO: Make these configurable at the Service level
	// WriteTimeout The time it should take for a single batched write to complete
	WriteTimeout clock.Duration
	// ReadTimeout The time it should take for a single batched read to complete
	ReadTimeout clock.Duration
	// MaxReserveBatchSize is the maximum number of items a client can request in a single reserve request
	MaxReserveBatchSize int
	// MaxProduceBatchSize is the maximum number of items a client can produce in a single produce request
	MaxProduceBatchSize int
	// MaxCompleteBatchSize is the maximum number of ids a client can mark complete in a single complete request
	MaxCompleteBatchSize int
	// MaxRequestsPerQueue is the maximum number of client requests a queue can handle before it returns an
	// queue overloaded message
	MaxRequestsPerQueue int
	// Clock is the clock provider used to calculate the current time
	Clock *clock.Provider
	// The initial partitions provided to the LogicalQueue at initialization.
	Partitions []store.Partition
}

// TODO: Modify the Logical to Handle many partitions
// TODO: Implement a round robin strategy which assigns reserves to each partition until the reservation is full or
//  partitions are exhausted.
// TODO: The number of partitions this Logical is assigned can change at any time.
// TODO: Create a new Partition which holds a set of Logical instances

// Logical produces and consumes items from the many partitions ensuring consumers and producers are handled fairly
// and efficiently. Since a Logical is the synchronization point for R/W there can ONLY BE ONE instance of a
// Logical running anywhere in the cluster at any given time. All consume and produce requests for the partitions
// assigned to this Logical instance MUST go through this singleton.
type Logical struct {
	reserveQueueCh  chan *types.ReserveRequest
	produceQueueCh  chan *types.ProduceRequest
	completeQueueCh chan *types.CompleteRequest

	shutdownCh     chan *types.ShutdownRequest
	queueRequestCh chan *QueueRequest
	wg             sync.WaitGroup
	conf           LogicalConfig
	inFlight       atomic.Int32
	inShutdown     atomic.Bool
}

func SpawnLogicalQueue(conf LogicalConfig) (*Logical, error) {
	set.Default(&conf.Logger, slog.Default())
	set.Default(&conf.MaxReserveBatchSize, DefaultMaxReserveBatchSize)
	set.Default(&conf.MaxProduceBatchSize, DefaultMaxProduceBatchSize)
	set.Default(&conf.MaxCompleteBatchSize, DefaultMaxCompleteBatchSize)
	set.Default(&conf.MaxRequestsPerQueue, DefaultMaxRequestsPerQueue)
	set.Default(&conf.Clock, clock.NewProvider())

	l := &Logical{
		// Logical requests are any request that doesn't require special batch processing
		queueRequestCh: make(chan *QueueRequest),
		// Shutdowns require special handling in the sync loop
		shutdownCh: make(chan *types.ShutdownRequest),
		conf:       conf,
	}

	// These are request queues that queue requests from clients until the sync loop has
	// time to process them. When they get processed, every request in the queue is handled
	// in a batch.
	l.reserveQueueCh = make(chan *types.ReserveRequest, conf.MaxRequestsPerQueue)
	l.produceQueueCh = make(chan *types.ProduceRequest, conf.MaxRequestsPerQueue)
	l.completeQueueCh = make(chan *types.CompleteRequest, conf.MaxRequestsPerQueue)

	l.wg.Add(1)
	go l.synchronizationLoop()
	return l, nil
}

// Produce is called by clients who wish to produce an item to the queue. This
// call will block until the item has been written to a partition or until the request
// is cancelled via the passed context or RequestTimeout is reached.
func (l *Logical) Produce(ctx context.Context, req *types.ProduceRequest) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}
	l.inFlight.Add(1)
	defer l.inFlight.Add(-1)

	if req.RequestTimeout == clock.Duration(0) {
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

	if len(req.Items) > l.conf.MaxProduceBatchSize {
		return transport.NewInvalidOption("items is invalid; max_produce_batch_size is"+
			" %d but received %d", l.conf.MaxProduceBatchSize, len(req.Items))
	}

	req.RequestDeadline = l.conf.Clock.Now().UTC().Add(req.RequestTimeout)
	req.ReadyCh = make(chan struct{})
	req.Context = ctx

	fmt.Printf("Produce Len: %d \n", len(l.produceQueueCh))
	select {
	case l.produceQueueCh <- req:
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
// Callers SHOULD cancel the context if the client has gone away, in this case Logical will abort the reservation
// request. If the context is cancelled after reservation has been written to the data store
// then those reservations will remain reserved until they can be offered to another client after the
// ReserveDeadline has been reached. See doc/adr/0009-client-timeouts.md
//
// # Unique Requests
// ClientID must NOT be empty and each request must be unique, Non-unique requests will be rejected with
// MsgDuplicateClientID. See doc/adr/0007-encourage-simple-clients.md for an explanation.
func (l *Logical) Reserve(ctx context.Context, req *types.ReserveRequest) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}
	l.inFlight.Add(1)
	defer l.inFlight.Add(-1)

	if strings.TrimSpace(req.ClientID) == "" {
		return transport.NewInvalidOption("invalid client id; cannot be empty")
	}

	if req.NumRequested <= 0 {
		return transport.NewInvalidOption("invalid batch size; must be greater than zero")
	}

	if req.NumRequested > l.conf.MaxReserveBatchSize {
		return transport.NewInvalidOption("invalid batch size; max_reserve_batch_size is %d, "+
			"but %d was requested", l.conf.MaxProduceBatchSize, req.NumRequested)
	}

	if req.RequestTimeout == clock.Duration(0) {
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

	req.RequestDeadline = l.conf.Clock.Now().UTC().Add(req.RequestTimeout)
	req.ReadyCh = make(chan struct{})
	req.Context = ctx

	select {
	case l.reserveQueueCh <- req:
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
func (l *Logical) Complete(ctx context.Context, req *types.CompleteRequest) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}
	l.inFlight.Add(1)
	defer l.inFlight.Add(-1)

	if len(req.Ids) == 0 {
		return transport.NewInvalidOption("ids is invalid; list of ids cannot be empty")
	}

	if len(req.Ids) > l.conf.MaxCompleteBatchSize {
		return transport.NewInvalidOption("ids is invalid; max_complete_batch_size is"+
			" %d but received %d", l.conf.MaxCompleteBatchSize, len(req.Ids))
	}

	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidOption("request timeout is invalid; maximum timeout is '15m' but '%s' "+
			"requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout == clock.Duration(0) {
		return transport.NewInvalidOption("request timeout is required; '5m' is recommended, 15m is the maximum")
	}

	req.RequestDeadline = l.conf.Clock.Now().UTC().Add(req.RequestTimeout)
	req.ReadyCh = make(chan struct{})
	req.Context = ctx

	select {
	case l.completeQueueCh <- req:
	default:
		return transport.NewRetryRequest(MsgQueueOverLoaded)
	}

	// Wait until the request has been processed
	<-req.ReadyCh
	return req.Err
}

// QueueStats retrieves stats about the queue and items in storage
func (l *Logical) QueueStats(ctx context.Context, stats *types.QueueStats) error {
	r := QueueRequest{
		Method:  MethodQueueStats,
		Request: stats,
	}
	return l.queueRequest(ctx, &r)
}

// Pause pauses processing of produce, reserve, complete and defer operations until the pause is cancelled.
// It is only used for testing and not exposed to the user via API, as such it is not considered apart of
// the public API.
func (l *Logical) Pause(ctx context.Context, req *types.PauseRequest) error {
	r := QueueRequest{
		Method:  MethodQueuePause,
		Request: req,
	}
	return l.queueRequest(ctx, &r)
}

// Clear clears enqueued items from the queue
func (l *Logical) Clear(ctx context.Context, req *types.ClearRequest) error {
	if !req.Queue && !req.Defer && !req.Scheduled {
		return transport.NewInvalidOption("invalid clear request; one of 'queue', 'defer'," +
			" 'scheduled' must be true")
	}

	r := QueueRequest{
		Method:  MethodQueueClear,
		Request: req,
	}
	return l.queueRequest(ctx, &r)
}

// UpdateInfo is called whenever QueueInfo changes. It is called during initialization of Logical struct and
// is intended to be called whenever queue configuration changes.
func (l *Logical) UpdateInfo(ctx context.Context, info types.QueueInfo) error {
	r := QueueRequest{
		Method:  MethodUpdateInfo,
		Request: info,
	}
	return l.queueRequest(ctx, &r)
}

// UpdatePartitions is called whenever the list of partitions this Logical Queue is responsible for changes.
// It is called during initialization of Logical struct and is intended to be called whenever the QueueManager
// rebalances partitions
func (l *Logical) UpdatePartitions(ctx context.Context, p []store.Partition) error {
	r := QueueRequest{
		Method:  MethodUpdatePartitions,
		Request: p,
	}
	return l.queueRequest(ctx, &r)
}

// -------------------------------------------------
// Methods to manage queue storage
// -------------------------------------------------

func (l *Logical) StorageQueueList(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
	req := StorageRequest{
		Items:   items,
		Options: opts,
	}

	// TODO: Test for invalid pivot

	r := QueueRequest{
		Method:  MethodStorageQueueList,
		Request: req,
	}
	return l.queueRequest(ctx, &r)
}

func (l *Logical) StorageQueueAdd(ctx context.Context, items *[]*types.Item) error {
	// TODO: Test for empty list
	r := QueueRequest{
		Method: MethodStorageQueueAdd,
		Request: StorageRequest{
			Items: items,
		},
	}
	return l.queueRequest(ctx, &r)
}

func (l *Logical) StorageQueueDelete(ctx context.Context, ids []types.ItemID) error {
	if len(ids) == 0 {
		return transport.NewInvalidOption("ids is invalid; cannot be empty")
	}

	r := QueueRequest{
		Method: MethodStorageQueueDelete,
		Request: StorageRequest{
			IDs: ids,
		},
	}
	return l.queueRequest(ctx, &r)
}

// -------------------------------------------------
// Main Loop and Handlers
// See doc/adr/0003-rw-sync-point.md for an explanation of this design
// -------------------------------------------------

// TODO: Break up this loop into smaller handlers to reduce the size of this method.
func (l *Logical) synchronizationLoop() {
	defer l.wg.Done()

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
		case req := <-l.produceQueueCh:
			l.handleProduceRequests(&state, req)

		case req := <-l.reserveQueueCh:
			l.handleReserveRequests(&state, req)

		case req := <-l.completeQueueCh:
			l.handleCompleteRequests(&state, req)

		case req := <-l.queueRequestCh:
			l.handleQueueRequests(&state, req)
			// If we shut down during a pause, exit immediately
			if l.inShutdown.Load() {
				fmt.Printf("sync.InShutdown\n")
				return
			}
			l.stateCleanUp(&state)

		case req := <-l.shutdownCh:
			l.handleShutdown(&state, req)
			return

		case <-state.NextMaintenanceCh:
			l.stateCleanUp(&state)

			// TODO: Preform queue maintenance, cleaning up reserved items that have not been completed and
			//  moving items into the dead letter queue.
		}
	}
}

func (l *Logical) handleProduceRequests(state *QueueState, req *types.ProduceRequest) {
	// Consume all requests in the channel, so we can process them in a batch
	state.Producers.Add(req)
EMPTY:
	for {
		select {
		case req := <-l.produceQueueCh:
			// TODO(thrawn01): Ensure we don't go beyond our max number of state.Producers, if we do,
			//  then we must inform the client that we are overloaded, and they must try again. We
			//  cannot hold on to a request and let it sit in the channel, else we break the
			//  request_timeout contract we have with the client.

			// TODO(thrawn01): Paused queues must also process this channel, and place produce requests
			//  into state.Producers, and handle requests if this channel is full.
			state.Producers.Add(req)
		default:
			break EMPTY
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
		if l.conf.Clock.Now().UTC().After(req.RequestDeadline) {
			req.Err = ErrRequestTimeout
			state.Producers.Remove(req)
			close(req.ReadyCh)
			continue
		}
		// Assign a DeadTimeout to each item
		for _, item := range req.Items {
			item.DeadDeadline = l.conf.Clock.Now().UTC().Add(l.conf.DeadTimeout)
		}
		// The writeTimeout should be equal to the request with the least amount of request timeout left.
		timeLeft := l.conf.Clock.Now().UTC().Sub(req.RequestDeadline)
		if timeLeft < writeTimeout {
			writeTimeout = timeLeft
		}
	}

	// If we allow a calculated write timeout to be a few milliseconds, then the store.Add()
	// is almost guaranteed to fail, so we ensure the write timeout is something reasonable.
	if writeTimeout < l.conf.WriteTimeout {
		// WriteTimeout comes from the storage implementation as the user who configured the
		// storage option should know a reasonable timeout value for the configuration chosen.
		writeTimeout = l.conf.WriteTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	if err := l.conf.Partitions[0].Produce(ctx, state.Producers); err != nil {
		l.conf.Logger.Error("while calling Partition.Produce()", "error", err,
			"category", "queue", "queueName", l.conf.Name)
		cancel()
		// Let clients that are timed out, know we are done with them.
		for _, req := range state.Producers.Requests {
			if l.conf.Clock.Now().UTC().After(req.RequestDeadline) {
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
		l.handleReserveRequests(state, nil)
	}
}

func (l *Logical) handleReserveRequests(state *QueueState, req *types.ReserveRequest) {
	// TODO(thrawn01): Ensure we don't go beyond our max number of state.Reservations, return
	//  an error to the client

	// Consume all requests in the channel, so we can process them in a batch
	addIfUnique(&state.Reservations, req)
EMPTY:
	for {
		select {
		case req := <-l.reserveQueueCh:
			addIfUnique(&state.Reservations, req)
		default:
			break EMPTY
		}
	}

	// Remove any clients that have timed out and find the next request to timeout, which will
	// become our write timeout.
	writeTimeout := l.nextTimeout(&state.Reservations)

	// If we allow a calculated write timeout to be a few milliseconds, then the store.Add()
	// is almost guaranteed to fail, so we ensure the write timeout is something reasonable.
	if writeTimeout < l.conf.WriteTimeout {
		// WriteTimeout comes from the storage implementation as the user who configured the
		// storage option should know a reasonable timeout value for the configuration chosen.
		writeTimeout = l.conf.WriteTimeout
	}

	// Send the batch that each request wants to the store. If there are items that can be reserved the
	// store will assign items to each batch request.
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	if err := l.conf.Partitions[0].Reserve(ctx, state.Reservations, store.ReserveOptions{
		ReserveDeadline: l.conf.Clock.Now().UTC().Add(l.conf.ReserveTimeout),
	}); err != nil {
		l.conf.Logger.Error("while calling Partition.Reserve()", "error", err,
			"category", "queue", "queueName", l.conf.Name)
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
	l.stateCleanUp(state)
}

func (l *Logical) handleCompleteRequests(state *QueueState, req *types.CompleteRequest) {
	// TODO(thrawn01): Ensure we don't go beyond our max number of state.Completes, return
	//  an error to the client

	// Consume all requests in the channel, so we can process them in a batch
	state.Completes.Add(req)
EMPTY:
	for {
		select {
		case req := <-l.completeQueueCh:
			state.Completes.Add(req)
		default:
			break EMPTY
		}
	}

	writeTimeout := maxRequestTimeout
	for _, req := range state.Completes.Requests {
		// Cancel any produce requests that have timed out
		if l.conf.Clock.Now().UTC().After(req.RequestDeadline) {
			req.Err = ErrRequestTimeout
			state.Completes.Remove(req)
			close(req.ReadyCh)
			continue
		}
		// The writeTimeout should be equal to the request with the least amount of request timeout left.
		timeLeft := l.conf.Clock.Now().UTC().Sub(req.RequestDeadline)
		if timeLeft < writeTimeout {
			writeTimeout = timeLeft
		}
	}

	// If we allow a calculated write timeout to be a few milliseconds, then the store.Add()
	// is almost guaranteed to fail, so we ensure the write timeout is something reasonable.
	if writeTimeout < l.conf.WriteTimeout {
		// WriteTimeout comes from the storage implementation as the user who configured the
		// storage option should know a reasonable timeout value for the configuration chosen.
		writeTimeout = l.conf.WriteTimeout
	}

	var err error
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	if err = l.conf.Partitions[0].Complete(ctx, state.Completes); err != nil {
		l.conf.Logger.Error("while calling Partition.Complete()", "error", err,
			"category", "queue", "queueName", l.conf.Name)
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
func (l *Logical) stateCleanUp(state *QueueState) {
	fmt.Printf("stateCleanUp\n")
	next := l.nextTimeout(&state.Reservations)
	if next.Nanoseconds() != 0 {
		l.conf.Logger.Debug("next maintenance window",
			"duration", next.String(), "queue", l.conf.Name)
		state.NextMaintenanceCh = l.conf.Clock.After(next)
	}
	state.Reservations.FilterNils()
}

func (l *Logical) Shutdown(ctx context.Context) error {
	if l.inShutdown.Swap(true) {
		return nil
	}

	req := &types.ShutdownRequest{
		ReadyCh: make(chan struct{}),
		Context: ctx,
	}

	// Wait until l.synchronizationLoop() shutdown is complete or until
	// our context is cancelled.
	select {
	case l.shutdownCh <- req:
		l.wg.Wait()
		return req.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Logical) queueRequest(ctx context.Context, r *QueueRequest) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	r.ReadyCh = make(chan struct{})
	r.Context = ctx

	select {
	case l.queueRequestCh <- r:
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

func (l *Logical) handleQueueRequests(state *QueueState, req *QueueRequest) {
	switch req.Method {
	case MethodStorageQueueList, MethodStorageQueueAdd, MethodStorageQueueDelete:
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
		close(req.ReadyCh)
	case MethodUpdatePartitions:
		p := req.Request.([]store.Partition)
		l.conf.Partitions = p
		close(req.ReadyCh)
	default:
		panic(fmt.Sprintf("unknown queue request method '%d'", req.Method))
	}
}

func (l *Logical) handleClear(_ *QueueState, req *QueueRequest) {
	// NOTE: When clearing a queue, ensure we flush any cached items. As of this current
	// version (V0), there is no cached data to sync, but this will likely change in the future.
	cr := req.Request.(*types.ClearRequest)

	if cr.Queue {
		// Ask the store to clean up any items in the data store which are not currently out for reservation
		if err := l.conf.Partitions[0].Clear(req.Context, cr.Destructive); err != nil {
			req.Err = err
		}
	}
	// TODO(thrawn01): Support clearing defer and scheduled
	close(req.ReadyCh)
}

func (l *Logical) handleStorageRequests(req *QueueRequest) {
	sr := req.Request.(StorageRequest)
	// TODO: This endpoint should list all items for all partitions this logical is handling
	switch req.Method {
	case MethodStorageQueueList:
		if err := l.conf.Partitions[0].List(req.Context, sr.Items, sr.Options); err != nil {
			req.Err = err
		}
	case MethodStorageQueueAdd:
		if err := l.conf.Partitions[0].Add(req.Context, *sr.Items); err != nil {
			req.Err = err
		}
	case MethodStorageQueueDelete:
		if err := l.conf.Partitions[0].Delete(req.Context, sr.IDs); err != nil {
			req.Err = err
		}
	default:
		panic(fmt.Sprintf("unknown storage request method '%d'", req.Method))
	}
	close(req.ReadyCh)
}

func (l *Logical) handleStats(state *QueueState, r *QueueRequest) {
	qs := r.Request.(*types.QueueStats)
	// TODO: return all Partition stats
	if err := l.conf.Partitions[0].Stats(r.Context, qs); err != nil {
		r.Err = err
	}
	qs.ProduceWaiting = len(l.produceQueueCh)
	qs.ReserveWaiting = len(l.reserveQueueCh)
	qs.CompleteWaiting = len(l.completeQueueCh)
	qs.ReserveBlocked = len(state.Reservations.Requests)
	qs.InFlight = int(l.inFlight.Load())
	close(r.ReadyCh)
}

// handlePause places Logical into a special loop where operations none of the // produce,
// reserve, complete, defer operations will be processed until we leave the loop.
func (l *Logical) handlePause(state *QueueState, r *QueueRequest) {
	pr := r.Request.(*types.PauseRequest)

	if !pr.Pause {
		close(r.ReadyCh)
		return
	}
	close(r.ReadyCh)

	l.conf.Logger.Warn("queue paused", "queue", l.conf.Name)
	defer l.conf.Logger.Warn("queue un-paused", "queue", l.conf.Name)

	for req := range l.queueRequestCh {
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
			return
		default:
			l.handleQueueRequests(state, req)
		}
	}
}

func (l *Logical) handleShutdown(state *QueueState, req *types.ShutdownRequest) {
	fmt.Printf("handleShutdown\n")
	l.stateCleanUp(state)

	// Cancel any open reservations
	for _, r := range state.Reservations.Requests {
		r.Err = ErrQueueShutdown
		close(r.ReadyCh)
	}

	// Consume all requests currently in flight
	for l.inFlight.Load() != 0 {
		fmt.Printf("handleShutdown.QueueInFlight: %d\n", l.inFlight.Load())
		select {
		case r := <-l.produceQueueCh:
			fmt.Printf("handleShutdown.Produce\n")
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		case r := <-l.reserveQueueCh:
			fmt.Printf("handleShutdown.Reserve\n")
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		case r := <-l.completeQueueCh:
			fmt.Printf("handleShutdown.Reserve\n")
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		case <-l.conf.Clock.After(100 * clock.Millisecond):
			// all time for the closed requests handlers to exit
		case <-req.Context.Done():
			req.Err = req.Context.Err()
			return
		}
	}

	fmt.Printf("handleShutdown.Close() Store\n")

	// TODO: Shutdown all partitions
	if err := l.conf.Partitions[0].Close(req.Context); err != nil {
		req.Err = err
	}
	close(req.ReadyCh)
}

// -------------------------------------------------
// Support Functions & Methods
// -------------------------------------------------

// TODO: I don't think we should pass by ref. We should use escape analysis to decide
func (l *Logical) nextTimeout(r *types.ReserveBatch) clock.Duration {
	var soon *types.ReserveRequest

	for i, req := range r.Requests {
		if req == nil {
			continue
		}

		// If request has already expired
		if l.conf.Clock.Now().UTC().After(req.RequestDeadline) {
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
		return clock.Duration(0)
	}

	// How soon is it? =)
	return soon.RequestDeadline.Sub(l.conf.Clock.Now().UTC())
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
