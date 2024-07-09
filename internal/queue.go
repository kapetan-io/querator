package internal

import (
	"context"
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
	ErrDuplicateClientID = transport.NewInvalidOption("duplicate client id")
	ErrRequestTimeout    = transport.NewRetryRequest("request timeout, try again")
	ErrQueueShutdown     = transport.NewRequestFailed("queue is shutting down")
)

const (
	queueChSize       = 20_000
	maxRequestTimeout = 15 * time.Minute
	minRequestTimeout = 10 * time.Millisecond
)

type QueueOptions struct {
	// The name of the queue
	Name string
	// If defined, is the logger used by the queue
	Logger duh.StandardLogger
	// ReserveTimeout is how long the reservation is valid for.
	// TODO: We must ensure the DeadTimeout is not less than the ReserveTimeout
	ReserveTimeout time.Duration
	// DeadQueue is the name of the dead letter queue for this queue.
	DeadQueue string
	// DeadTimeout is the time an item can wait in the queue regardless of attempts before
	// it is moved to the dead letter queue. This value is used if no DeadTimeout is provided
	// by the queued item.
	DeadTimeout time.Duration
	// QueueStore is the store interface used to persist items for this specific queue
	QueueStore store.Queue

	// TODO: Make these configurable
	MaxReserveBatchSize int
	MaxProduceBatchSize int
}

// Queue manages job is to evenly distribute and consume items from a single queue. Ensuring consumers and
// producers are handled fairly and efficiently. Since a Queue is the synchronization point for R/W
// there can ONLY BE ONE instance of a Queue running anywhere in the cluster at any given time. All consume
// and produce requests MUST go through this queue singleton.
type Queue struct {
	// TODO: I would love to see a lock free ring queue here, based on an array, such that we don't need
	//  to throw away the channel after we process all the items in the channel. An array might allow the L3
	//  cache to pre load that data and avoid expensive calls to DRAM.
	reserveQueueCh  atomic.Pointer[chan *types.ReserveRequest]
	produceQueueCh  atomic.Pointer[chan *types.ProduceRequest]
	completeQueueCh atomic.Pointer[chan *types.CompleteRequest]

	storageQueueCh chan *types.StorageRequest
	shutdownCh     chan *types.ShutdownRequest
	wg             sync.WaitGroup
	opts           QueueOptions
	inShutdown     atomic.Bool
}

func NewQueue(opts QueueOptions) (*Queue, error) {
	set.Default(&opts.Logger, slog.Default())
	set.Default(&opts.MaxReserveBatchSize, 1_000)
	set.Default(&opts.MaxProduceBatchSize, 1_000)

	if opts.QueueStore == nil {
		return nil, transport.NewInvalidOption("QueueOptions.QueueStore cannot be nil")
	}

	q := &Queue{
		shutdownCh:     make(chan *types.ShutdownRequest),
		storageQueueCh: make(chan *types.StorageRequest),
		opts:           opts,
	}

	// Next new wait queue
	rch := make(chan *types.ReserveRequest, queueChSize)
	q.reserveQueueCh.Store(&rch)
	pch := make(chan *types.ProduceRequest, queueChSize)
	q.produceQueueCh.Store(&pch)
	cch := make(chan *types.CompleteRequest, queueChSize)
	q.completeQueueCh.Store(&cch)

	q.wg.Add(1)
	go q.synchronizationLoop()
	return q, nil
}

func (q *Queue) Storage(ctx context.Context, req *types.StorageRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}

	req.Context = ctx
	req.ReadyCh = make(chan struct{})

	select {
	case q.storageQueueCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-req.ReadyCh:
		return req.Err
	case <-ctx.Done():
		return ctx.Err()
	}
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
		return transport.NewInvalidOption("request_timeout is required; '5m' is recommended, 15m is the maximum")
	}

	if req.RequestTimeout >= maxRequestTimeout {
		return transport.NewInvalidOption("request_timeout is invalid; maximum timeout is '15m' but"+
			" '%s' was requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout <= minRequestTimeout {
		return transport.NewInvalidOption("request_timeout is invalid; minimum timeout is '10ms' but"+
			" '%s' was requested", req.RequestTimeout.String())
	}

	req.Context = ctx
	req.ReadyCh = make(chan struct{})
	req.RequestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	*q.produceQueueCh.Load() <- req

	// Wait until the request has been processed
	<-req.ReadyCh
	return req.Err
}

// Reserve is called by clients wanting to reserve a new item from the queue. This call
// will block until the request is cancelled via the passed context or the RequestTimeout
// is reached.
//
// ### Context
// IT IS NOT recommend to use context.WithTimeout() or context.WithDeadline() with Reserve() since
// Reserve() will block until the duration provided via ReserveRequest.RequestTimeout has been reached.
// You may cancel the context if the client has gone away. In this case Queue will abort the reservation
// request. If the context is cancelled after reservation has been written to the data store
// then those reservations will remain reserved until they can be offered to another client after the
// ReserveDeadline has been reached.
//
// ### Unique Requests (TODO: Update this comment, it's out of date)
// ClientID must NOT be empty and each request must have a unique id, or it will
// be rejected with ErrDuplicateClientID. When recording a reservation in the data store the ClientID
// the reservation is for is also written. This can assist in diagnosing which clients are failing
// to process items properly. It is so used to avoid poor performing or bad behavior from clients
// who may attempt to issue many requests simultaneously in an attempt to increase throughput.
// Increased throughput can instead be achieved by raising the batch size.
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

	if int(req.NumRequested) > q.opts.MaxReserveBatchSize {
		return transport.NewInvalidOption("invalid batch_size; exceeds maximum limit max_reserve_batch_size is %d, "+
			"but %d was requested", q.opts.MaxProduceBatchSize, req.NumRequested)
	}

	if req.RequestTimeout == time.Duration(0) {
		return transport.NewInvalidOption("request_timeout is required; '5m' is recommended, 15m is the maximum")
	}

	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidOption("invalid request_timeout; maximum timeout is '15m' but '%s' "+
			"requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout <= minRequestTimeout {
		return transport.NewInvalidOption("request_timeout is invalid; minimum timeout is '10ms' but"+
			" '%s' was requested", req.RequestTimeout.String())
	}

	req.Context = ctx
	req.ReadyCh = make(chan struct{})
	req.RequestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	*q.reserveQueueCh.Load() <- req

	// Wait until the request has been processed
	<-req.ReadyCh
	return req.Err
}

func (q *Queue) Complete(ctx context.Context, req *types.CompleteRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidOption("request_timeout is invalid; maximum timeout is '15m' but '%s' "+
			"requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout == time.Duration(0) {
		return transport.NewInvalidOption("request_timeout is required; '5m' is recommended, 15m is the maximum")
	}

	req.Context = ctx
	req.ReadyCh = make(chan struct{})
	req.RequestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	*q.completeQueueCh.Load() <- req

	// Wait until the request has been processed
	<-req.ReadyCh
	return req.Err
}

// synchronizationLoop See doc/adr/0003-rw-sync-point.md for an explanation of this design

// TODO: Refactor the queue.go code to use Batch Request objects and replace wwr, wcr structs with Request objects
func (q *Queue) synchronizationLoop() {
	defer q.wg.Done()

	// TODO: Refactor this into a smaller loop with handler functions which pass around a state object with
	//  State.AddToWPR() and State.AddToWRR() etc....

	// TODO: Maybe introduce a types.ReserveBatch which ensures only unique clients are in the batch.
	// TODO: Make an ADR on why we don't allow the same client to make multiple reserve requests

	//reserves := types.Batch[types.ReserveRequest]{
	//	Requests: make(map[string]*types.ReserveRequest, 0, 5_000),
	//}

	reserves := types.ReserveBatch{
		Requests: make(map[string]types.ReserveRequest, 5_000),
		Total:    0,
	}

	wpr := waitingProduceRequests{
		Requests: make([]*types.ProduceRequest, 0, 5_000),
	}
	wcr := waitingCompleteRequests{
		Requests: make([]*types.CompleteRequest, 0, 5_000),
	}
	var timerCh <-chan time.Time

	addToWPR := func(req *types.ProduceRequest) {
		wpr.Requests = append(wpr.Requests, req)
	}

	addToWCR := func(req *types.CompleteRequest) {
		wcr.Requests = append(wcr.Requests, req)
	}

	addToWRR := func(req *types.ReserveRequest) {
		if _, ok := reserves.Requests[req.ClientID]; ok {
			req.err = ErrDuplicateClientID
			close(req.readyCh)
			return
		}
		reserves.Requests[req.ClientID] = req
		reserves.Total += int(req.NumRequested)
	}

	for {
		reserveQueueCh := q.reserveQueueCh.Load()
		produceQueueCh := q.produceQueueCh.Load()
		completeQueueCh := q.completeQueueCh.Load()
		select {

		// -----------------------------------------------
		case req := <-*produceQueueCh:
			// Swap the wait channel, so we can process the wait queue. Creating a
			// new channel allows any incoming waiting produce requests (wpr) that occur
			// while this routine is running to be queued into the new wait queue without
			// interfering with anything we are doing here.
			ch := make(chan *types.ProduceRequest, queueChSize)
			q.produceQueueCh.Store(&ch)
			close(*produceQueueCh)

			addToWPR(req)
			for req := range *produceQueueCh {
				addToWPR(req)
			}

			// FUTURE: Inspect the Waiting Reserve Requests, and attempt to assign produced items with
			//  waiting reserve requests if our queue is caught up.
			//  (Check for cancel or expire reserve requests first)

			// FUTURE: Buffer the produced items at the top of the queue into memory, so we don't need
			//  to query them from the database when we reserve items later. Doing so avoids the ListReservable()
			//  step, in addition, we can back fill reservable items into memory when synchronizationLoop() isn't
			//  actively producing or consuming.

			// `items` list is a complete list of all the items each producer provided us with.
			items := make([]*store.Item, 0, wpr.TotalItems)
			writeTimeout := 15 * time.Minute
			for _, req := range wpr.Requests {
				// Cancel any produce requests that have timed out
				if time.Now().UTC().After(req.requestDeadline) {
					req.err = ErrRequestTimeout
					close(req.readyCh)
					continue
				}
				for _, item := range req.Items {
					item.DeadDeadline = time.Now().UTC().Add(q.opts.DeadTimeout)
					items = append(items, item)
				}
				// The writeTimeout should be equal to the request with the least amount of request timeout left.
				timeLeft := time.Now().UTC().Sub(req.requestDeadline)
				if timeLeft < writeTimeout {
					writeTimeout = timeLeft
				}
			}

			// If we allow a calculated write timeout to be a few milliseconds, then the store.Write()
			// is almost guaranteed to fail, so we ensure the write timeout is something reasonable.
			if writeTimeout < q.opts.QueueStore.Options().WriteTimeout {
				// WriteTimeout comes from the storage implementation as the user who configured the
				// storage option should know a reasonable timeout value for the configuration chosen.
				writeTimeout = q.opts.QueueStore.Options().WriteTimeout
			}

			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			if err := q.opts.QueueStore.Write(ctx, items); err != nil {
				q.opts.Logger.Warn("while calling QueueStore.Write()", "error", err)
				cancel()
				// Let clients that are timed out, know we are done with them.
				for _, req := range wpr.Requests {
					if time.Now().UTC().After(req.requestDeadline) {
						req.err = ErrRequestTimeout
						close(req.readyCh)
						continue
					}
				}
				continue
			}
			cancel()

			// Tell the waiting clients the items have been produced
			for i, req := range wpr.Requests {
				close(req.readyCh)
				// Allow the GC to collect these requests
				wpr.Requests[i] = nil
			}

			// Reset the slice without re-allocating
			wpr.Requests = wpr.Requests[:0]
			wpr.TotalItems = 0

		// -----------------------------------------------
		case req := <-*reserveQueueCh:
			// Swap the wait channel, so we can process the wait queue. Creating a
			// new channel allows any incoming reserves that occur while this routine
			// is running to be queued into the new wait queue without interfering with
			// anything we are doing here.
			ch := make(chan *types.ReserveRequest, queueChSize)
			q.reserveQueueCh.Store(&ch)
			close(*reserveQueueCh)

			if !reserves.AddIfUnique(req) {
				req.Err = ErrDuplicateClientID
				close(req.ReadyCh)
			}

			for req := range *reserveQueueCh {
				if !reserves.AddIfUnique(req) {
					req.Err = ErrDuplicateClientID
					close(req.ReadyCh)
				}
			}

			// NOTE: V0 doing the simplest thing that works now, we can get fancy & efficient later.

			// Remove any clients that have timed out
			_ = findNextTimeout(&reserves)

			// Each request has a batch of items it requests, place all those batch requests in an array
			batch.Requests = make([]*store.ItemBatch, 0, len(reserves.Requests))
			for _, req := range reserves.Requests {
				batch.Requests = append(batch.Requests, &req.Batch)
				batch.TotalRequested += int(req.NumRequested)
			}

			// Send the batch that each request wants to the store. If there are items that can be reserved the
			// store will assign items to each batch request.
			ctx, cancel := context.WithTimeout(context.Background(), q.opts.QueueStore.Options().ReadTimeout)
			if err := q.opts.QueueStore.Reserve(ctx, batch, store.ReserveOptions{
				ReserveDeadline: time.Now().UTC().Add(q.opts.ReserveTimeout),
			}); err != nil {
				q.opts.Logger.Warn("while calling QueueStore.Reserve()", "error", err)
				cancel()
				continue
			}
			cancel()

			// Inform clients they have reservations ready or if there was an error
			for _, req := range reserves.Requests {
				if len(req.Batch.Items) != 0 || req.Batch.Err != nil {
					req.err = req.Batch.Err
					close(req.readyCh)
					reserves.Total -= len(req.Batch.Items)
					delete(reserves.Requests, req.ClientID)
				}
			}

			// Set a timer for remaining open reserve requests
			// Or when next reservation will expire
			next := findNextTimeout(&reserves)
			if next.Nanoseconds() != 0 {
				timerCh = time.After(next)
			}

		// -----------------------------------------------
		case req := <-*completeQueueCh:
			ch := make(chan *types.CompleteRequest, queueChSize)
			q.completeQueueCh.Store(&ch)
			close(*completeQueueCh)

			addToWCR(req)
			for req := range *completeQueueCh {
				addToWCR(req)
			}

			ids := make([]string, 0, wcr.TotalIDs)
			for _, req := range wcr.Requests {
				ids = append(ids, req.Ids...)
			}

			// TODO: We need to ensure all the id's are in reservation before we call complete
			if err := q.opts.QueueStore.Complete(req.Context, ids); err != nil {
				req.err = err
			}

			// Tell the waiting clients the items have been marked as complete
			for i, req := range wcr.Requests {
				close(req.readyCh)
				// Allow the GC to collect these requests
				wcr.Requests[i] = nil
			}

			wcr.Requests = wcr.Requests[:0]
			wcr.TotalIDs = 0

		// -----------------------------------------------
		case req := <-q.storageQueueCh:
			if req.ID != "" {
				if err := q.opts.QueueStore.Read(req.Context, &req.Items, req.ID, 1); err != nil {
					req.err = err
				}
			} else {
				if err := q.opts.QueueStore.Read(req.Context, &req.Items, req.Pivot, req.Limit); err != nil {
					req.err = err
				}
			}
			close(req.readyCh)

		// -----------------------------------------------
		case req := <-q.shutdownCh:
			for _, r := range wpr.Requests {
				r.err = ErrQueueShutdown
				close(r.readyCh)
			}
			for _, r := range reserves.Requests {
				r.err = ErrQueueShutdown
				close(r.readyCh)
			}
			if err := q.opts.QueueStore.Close(req.Context); err != nil {
				req.Err = err
			}
			close(req.readyCh)
			return

		// -----------------------------------------------
		case <-timerCh:
			// Find the next request that will time out, and notify any clients of expired reserves
			next := findNextTimeout(&reserves)
			if next.Nanoseconds() != 0 {
				timerCh = time.After(next)
			}

			// TODO: Preform queue maintenance, cleaning up reserved items that have not been completed and
			//  moving items into the dead letter queue.
		}
	}
}

func (q *Queue) Shutdown(ctx context.Context) error {
	if q.inShutdown.Swap(true) {
		return nil
	}

	req := &types.ShutdownRequest{
		readyCh: make(chan struct{}),
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

func findNextTimeout(r *waitingReserveRequests) time.Duration {
	var soon *types.ReserveRequest

	for _, req := range r.Requests {
		// If request has already expired
		if time.Now().UTC().After(req.requestDeadline) {
			// Inform our waiting client
			req.err = ErrRequestTimeout
			close(req.readyCh)
			r.Total -= int(req.NumRequested)
			delete(r.Requests, req.ClientID)
			continue
		}

		// If client has gone away
		if req.Context.Err() != nil {
			close(req.readyCh)
			r.Total -= int(req.NumRequested)
			req.err = req.Context.Err()
			delete(r.Requests, req.ClientID)
			continue
		}

		// If there is no soon
		if soon == nil {
			soon = req
			continue
		}

		// Will this happen sooner?
		if req.requestDeadline.Before(soon.requestDeadline) {
			soon = req
		}
	}

	// No sooner than now =)
	if soon == nil {
		return time.Duration(0)
	}

	// How soon is it? =)
	return time.Now().UTC().Sub(soon.requestDeadline)
}

type waitingReserveRequests struct {
	Requests map[string]*types.ReserveRequest
	// TODO: I think this is redundant now that we are using batches
	Total int
}

type waitingProduceRequests struct {
	Requests []*types.ProduceRequest
}

type waitingCompleteRequests struct {
	Requests []*types.CompleteRequest
}
