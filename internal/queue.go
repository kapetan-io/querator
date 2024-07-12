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
	ErrQueueShutdown     = transport.NewRequestFailed("queue is shutting down")
	ErrDuplicateClientID = transport.NewInvalidOption("duplicate client id")
	ErrRequestTimeout    = transport.NewRetryRequest("request timeout, try again")
	ErrInternalRetry     = transport.NewRetryRequest("internal error, try your request again")
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

	// WriteTimeout (Optional) The time it should take for a single batched write to complete
	WriteTimeout time.Duration
	// ReadTimeout (Optional) The time it should take for a single batched read to complete
	ReadTimeout time.Duration
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

	reserves := types.ReserveBatch{
		Requests: make([]*types.ReserveRequest, 0, 5_000),
		Total:    0,
	}

	producers := types.Batch[types.ProduceRequest]{
		Requests: make([]*types.ProduceRequest, 0, 5_000),
	}

	completes := types.Batch[types.CompleteRequest]{
		Requests: make([]*types.CompleteRequest, 0, 5_000),
	}

	var timerCh <-chan time.Time

	for {
		reserveQueueCh := q.reserveQueueCh.Load()
		produceQueueCh := q.produceQueueCh.Load()
		completeQueueCh := q.completeQueueCh.Load()
		select {

		// -----------------------------------------------
		case req := <-*produceQueueCh:
			// Swap the wait channel, so we can process the wait queue. Creating a
			// new channel allows any incoming waiting produce requests (producers) that occur
			// while this routine is running to be queued into the new wait queue without
			// interfering with anything we are doing here.
			ch := make(chan *types.ProduceRequest, queueChSize)
			q.produceQueueCh.Store(&ch)
			close(*produceQueueCh)

			producers.Add(req)
			for req := range *produceQueueCh {
				producers.Add(req)
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
			for _, req := range producers.Requests {
				// Cancel any produce requests that have timed out
				if time.Now().UTC().After(req.RequestDeadline) {
					req.Err = ErrRequestTimeout
					producers.Remove(req)
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
			if err := q.opts.QueueStore.Produce(ctx, producers); err != nil {
				q.opts.Logger.Error("while calling QueueStore.Add()", "error", err,
					"category", "queue", "queueName", q.opts.Name)
				cancel()
				// Let clients that are timed out, know we are done with them.
				for _, req := range producers.Requests {
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
			for _, req := range producers.Requests {
				close(req.ReadyCh)
			}
			producers.Reset()

		// -----------------------------------------------
		case req := <-*reserveQueueCh:
			// Swap the wait channel, so we can process the wait queue. Creating a
			// new channel allows any incoming reserves that occur while this routine
			// is running to be queued into the new wait queue without interfering with
			// anything we are doing here.
			ch := make(chan *types.ReserveRequest, queueChSize)
			q.reserveQueueCh.Store(&ch)
			close(*reserveQueueCh)

			addIfUnique(&reserves, req)
			for req := range *reserveQueueCh {
				addIfUnique(&reserves, req)
			}

			// Remove any clients that have timed out and find the next request to timeout.
			writeTimeout := nextTimeout(&reserves)

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
			if err := q.opts.QueueStore.Reserve(ctx, reserves, store.ReserveOptions{
				ReserveDeadline: time.Now().UTC().Add(q.opts.ReserveTimeout),
			}); err != nil {
				q.opts.Logger.Error("while calling QueueStore.Reserve()", "error", err,
					"category", "queue", "queueName", q.opts.Name)
				cancel()
				continue
			}
			cancel()

			// Inform clients they have reservations ready or if there was an error
			for _, req := range reserves.Requests {
				if len(req.Items) != 0 || req.Err != nil {
					close(req.ReadyCh)
					reserves.Remove(req)
				}
			}

			// Set a timer for remaining open reserve requests
			// Or when next reservation will expire
			next := nextTimeout(&reserves)
			if next.Nanoseconds() != 0 {
				timerCh = time.After(next)
			}

		// -----------------------------------------------
		case req := <-*completeQueueCh:
			ch := make(chan *types.CompleteRequest, queueChSize)
			q.completeQueueCh.Store(&ch)
			close(*completeQueueCh)

			completes.Add(req)
			for req := range *completeQueueCh {
				completes.Add(req)
			}

			// TODO: Abstract the writeTimeout calc into a function call
			writeTimeout := maxRequestTimeout
			for _, req := range producers.Requests {
				// Cancel any produce requests that have timed out
				if time.Now().UTC().After(req.RequestDeadline) {
					req.Err = ErrRequestTimeout
					producers.Remove(req)
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
			if err = q.opts.QueueStore.Complete(ctx, completes); err != nil {
				q.opts.Logger.Error("while calling QueueStore.Complete()", "error", err,
					"category", "queue", "queueName", q.opts.Name)
			}
			cancel()

			// Tell the waiting clients that items have been marked as complete
			for _, req := range completes.Requests {
				if err != nil {
					req.Err = ErrInternalRetry
				}
				close(req.ReadyCh)
			}
			completes.Reset()

		// -----------------------------------------------
		case req := <-q.storageQueueCh:
			if req.ID != "" {
				if err := q.opts.QueueStore.List(req.Context, &req.Items,
					types.ListOptions{Pivot: req.ID, Limit: 1}); err != nil {
					req.Err = err
				}
			} else {
				if err := q.opts.QueueStore.List(req.Context, &req.Items,
					types.ListOptions{Pivot: req.Pivot, Limit: req.Limit}); err != nil {
					req.Err = err
				}
			}
			close(req.ReadyCh)

		// -----------------------------------------------
		case req := <-q.shutdownCh:
			for _, r := range producers.Requests {
				r.Err = ErrQueueShutdown
				close(r.ReadyCh)
			}
			for _, r := range reserves.Requests {
				r.Err = ErrQueueShutdown
				close(r.ReadyCh)
			}
			if err := q.opts.QueueStore.Close(req.Context); err != nil {
				req.Err = err
			}
			close(req.ReadyCh)
			return

		// -----------------------------------------------
		case <-timerCh:
			// Find the next reserve request that will time out, and notify any clients of expired reserves
			next := nextTimeout(&reserves)
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

// TODO: I don't think we should pass by ref, but use escape analysis to decide
func nextTimeout(r *types.ReserveBatch) time.Duration {
	var soon *types.ReserveRequest

	for _, req := range r.Requests {
		// If request has already expired
		if time.Now().UTC().After(req.RequestDeadline) {
			// Inform our waiting client
			req.Err = ErrRequestTimeout
			close(req.ReadyCh)
			r.Total -= req.NumRequested
			r.Remove(req)
			continue
		}

		// If client has gone away
		if req.Context.Err() != nil {
			close(req.ReadyCh)
			r.Total -= req.NumRequested
			req.Err = req.Context.Err()
			r.Remove(req)
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
	return time.Now().UTC().Sub(soon.RequestDeadline)
}
