package internal

import (
	"context"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal/maps"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrDuplicateClientID = transport.NewInvalidRequest("duplicate client id")
	ErrRequestTimeout    = transport.NewRetryRequest("request timeout, try again")
	ErrQueueShutdown     = transport.NewRequestFailed("queue is shutting down")
)

const (
	queueChSize       = 20_000
	maxRequestTimeout = 15 * time.Minute
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
}

// Queue manages job is to evenly distribute and consume items from a single queue. Ensuring consumers and
// producers are handled fairly and efficiently. Since a Queue is the synchronization point for R/W
// there can ONLY BE ONE instance of a Queue running anywhere in the cluster at any given time. All consume
// and produce requests MUST go through this queue singleton.
type Queue struct {
	reserveQueueCh  atomic.Pointer[chan *ReserveRequest]
	produceQueueCh  atomic.Pointer[chan *ProduceRequest]
	completeQueueCh atomic.Pointer[chan *CompleteRequest]
	storageQueueCh  chan *StorageRequest
	shutdownCh      chan *ShutdownRequest
	wg              sync.WaitGroup
	opts            QueueOptions
	inShutdown      atomic.Bool
}

func NewQueue(opts QueueOptions) (*Queue, error) {
	set.Default(&opts.Logger, slog.Default())
	if opts.QueueStore == nil {
		return nil, errors.New("QueueOptions.QueueStore cannot be nil")
	}

	q := &Queue{
		shutdownCh:     make(chan *ShutdownRequest),
		storageQueueCh: make(chan *StorageRequest),
		opts:           opts,
	}

	// Assign new wait queue
	rch := make(chan *ReserveRequest, queueChSize)
	q.reserveQueueCh.Store(&rch)
	pch := make(chan *ProduceRequest, queueChSize)
	q.produceQueueCh.Store(&pch)
	cch := make(chan *CompleteRequest, queueChSize)
	q.completeQueueCh.Store(&cch)

	q.wg.Add(1)
	go q.processQueues()
	return q, nil
}

func (q *Queue) Storage(ctx context.Context, req *StorageRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}

	req.Context = ctx
	req.readyCh = make(chan struct{})

	select {
	case q.storageQueueCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-req.readyCh:
		return req.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Produce is called by clients who wish to produce an item to the queue. This
// call will block until the item has been written to the queue or until the request
// is cancelled via the passed context or RequestTimeout is reached.
func (q *Queue) Produce(ctx context.Context, req *ProduceRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidRequest("request_timeout is invalid; maximum timeout is '15m' but"+
			" '%s' was requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout == time.Duration(0) {
		req.RequestTimeout = maxRequestTimeout
	}

	req.Context = ctx
	req.readyCh = make(chan struct{})
	req.requestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	*q.produceQueueCh.Load() <- req

	// Wait until the request has been processed
	<-req.readyCh
	return req.err
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
// ### Unique Requests
// ClientID must NOT be empty and each request must have a unique id, or it will
// be rejected with ErrDuplicateClientID. When recording a reservation in the data store the ClientID
// the reservation is for is also written. This can assist in diagnosing which clients are failing
// to process items properly. It is so used to avoid poor performing or bad behavior from clients
// who may attempt to issue many requests simultaneously in an attempt to increase throughput.
// Increased throughput can instead be achieved by raising the batch size.
func (q *Queue) Reserve(ctx context.Context, req *ReserveRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if strings.TrimSpace(req.ClientID) == "" {
		return transport.NewInvalidRequest("client_id cannot be empty")
	}

	if req.BatchSize <= 0 {
		return transport.NewInvalidRequest("batch_size must be greater than zero")
	}

	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidRequest("request_timeout is invalid; maximum timeout is '15m' but '%s' "+
			"requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout == time.Duration(0) {
		req.RequestTimeout = maxRequestTimeout
	}

	req.Context = ctx
	req.readyCh = make(chan struct{})
	req.requestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	*q.reserveQueueCh.Load() <- req

	// Wait until the request has been processed
	<-req.readyCh
	return req.err
}

func (q *Queue) Complete(ctx context.Context, req *CompleteRequest) error {
	if q.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidRequest("request_timeout is invalid; maximum timeout is '15m' but '%s' "+
			"requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout == time.Duration(0) {
		req.RequestTimeout = maxRequestTimeout
	}

	req.Context = ctx
	req.readyCh = make(chan struct{})
	req.requestDeadline = time.Now().UTC().Add(req.RequestTimeout)
	*q.completeQueueCh.Load() <- req

	// Wait until the request has been processed
	<-req.readyCh
	return req.err
}

// processQueues processes the queues of both producing and reserving clients. It is the synchronization
// point where reads and writes to the queue are handled. Because reads and writes to the queue are
// handled by a single go routine (single threaded). We can preform optimizations where items produced to
// the queue can be marked as reserved before they are written to the data store.
//
// Single threaded design also allows batching reads and writes in the same transaction. Most databases
// benefit from batching R/W paths as it reduces IOPs, transaction overheard, network overhead, results
// in fewer locks, and logging. In addition, by preforming the R/W synchronization here in code, we avoid
// pushing that synchronization on to the datastore, which reduces datastore costs.
// TODO(thrawn01) reference my blog post(s) about this.
//
// ### Queue all the things
// The design goal here is that when this go routine is woken up by the go scheduler it can do as much work as
// possible without giving the scheduler an excuse to steal away our CPU time. In other words,
// the code path to get an item into or out of the queue should result in as little blocking or mutex lock
// contention as possible. While we can't avoid R/W to the data store, we can have producers and consumers
// 'queue' as much work as possible so when we are active, we get as much done as quickly as possible.
//
// If a single queue becomes a bottle neck for throughput, users should create more queues.
//
// ### The Write Path
// The best cast scenario is that each item will have 3 writes.
// * 1st Write - Add item to the Queue
// * 2nd Write - Fetch Reservable items from the queue and mark them as Reserved
// * 3rd Write - Write the item in the queue as complete (delete the item)
//
// In addition, our single threaded synchronized design may allow for several optimizations and probably
// some I'm not thinking of.
// * Pre-fetching reservable items from the queue into memory, if we know items are in high demand.
// * Reserving items as soon as they are produced (avoiding the second write)
//
// ### Queue Groups
// Allow users to combine multiple queues by creating a "Queue Group". Then a client can
// produce/reserve from the queue group, and the queue group round robins the request to each queue
// in the group. Doing so destroys any ordering of queued items, but in some applications ordering might
// not be important to the consumer, but Almost Exactly Once Delivery is still desired.
//
// ### Audit Trail
// If users need to track which client picked up the item, we should implement an optional audit log which
// follows the progress of each item in the queue. In this way, the audit log shows which clients picked up the
// item and for how long. This especially helpful in understanding how an item got into the dead letter queue.
// Implementing this adds an additional write burden on our disk, as such it should be optional.
func (q *Queue) processQueues() {
	defer q.wg.Done()

	// TODO: Refactor this into a smaller loop with handler functions which pass around a state object with
	//  State.AddToWPR() and State.AddToWRR() etc....
	wrr := waitingReserveRequests{
		Requests: make(map[string]*ReserveRequest, 5_000),
	}
	wpr := waitingProduceRequests{
		Requests: make([]*ProduceRequest, 0, 5_000),
	}
	wcr := waitingCompleteRequests{
		Requests: make([]*CompleteRequest, 0, 5_000),
	}
	var timerCh <-chan time.Time

	addToWPR := func(req *ProduceRequest) {
		wpr.Requests = append(wpr.Requests, req)
		wpr.TotalItems += len(req.Items)
	}

	addToWCR := func(req *CompleteRequest) {
		wcr.Requests = append(wcr.Requests, req)
		wcr.TotalIDs += len(req.Ids)
	}

	addToWRR := func(req *ReserveRequest) {
		if _, ok := wrr.Requests[req.ClientID]; ok {
			req.err = ErrDuplicateClientID
			close(req.readyCh)
			return
		}
		wrr.Requests[req.ClientID] = req
		wrr.Total += int(req.BatchSize)
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
			ch := make(chan *ProduceRequest, queueChSize)
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
			//  step, in addition, we can back fill reservable items into memory when processQueues() isn't
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
			// new channel allows any incoming wrr that occur while this routine
			// is running to be queued into the new wait queue without interfering with
			// anything we are doing here.
			ch := make(chan *ReserveRequest, queueChSize)
			q.reserveQueueCh.Store(&ch)
			close(*reserveQueueCh)

			addToWRR(req)
			for req := range *reserveQueueCh {
				addToWRR(req)
			}

			// NOTE: V0 doing the simplest thing that works now, we can get fancy & efficient later.

			// Remove any clients that have timed out
			_ = findNextTimeout(&wrr)

			// Fetch all the reservable items in the store
			ctx, cancel := context.WithTimeout(context.Background(), q.opts.QueueStore.Options().ReadTimeout)
			var items []*store.Item
			if err := q.opts.QueueStore.Reserve(ctx, &items, store.ReserveOptions{
				ReserveDeadline: time.Now().UTC().Add(q.opts.ReserveTimeout),
				Limit:           wrr.Total,
			}); err != nil {
				q.opts.Logger.Warn("while calling QueueStore.Reserve()", "error", err)
				cancel()
				continue
			}
			cancel()

			// Distribute the items evenly to all the reserve clients
			keys := maps.Keys(wrr.Requests)
			var idx int
			for _, item := range items {
				req = wrr.Requests[keys[idx]]
				req.Items = append(req.Items, item)
				if idx > len(keys)-1 {
					idx = 0
				}
			}

			// Inform clients they have reservations ready
			for _, req := range wrr.Requests {
				if len(req.Items) != 0 {
					close(req.readyCh)
					wrr.Total -= len(req.Items)
					delete(wrr.Requests, req.ClientID)
				}
			}

			// Set a timer for remaining open reserve requests
			// Or when next reservation will expire
			next := findNextTimeout(&wrr)
			if next.Nanoseconds() != 0 {
				timerCh = time.After(next)
			}

		// -----------------------------------------------
		case req := <-*completeQueueCh:
			ch := make(chan *CompleteRequest, queueChSize)
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

			if err := q.opts.QueueStore.Delete(req.Context, ids); err != nil {
				req.err = fmt.Errorf("during complete QueueStore.Delete(): %w", err)
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
					req.err = fmt.Errorf("during inspect QueueStore.Read(): %w", err)
				}
			} else {
				if err := q.opts.QueueStore.Read(req.Context, &req.Items, req.Pivot, req.Limit); err != nil {
					req.err = fmt.Errorf("during list QueueStore.Read(): %w", err)
				}
			}
			close(req.readyCh)

		// -----------------------------------------------
		case req := <-q.shutdownCh:
			for _, r := range wpr.Requests {
				r.err = ErrQueueShutdown
				close(r.readyCh)
			}
			for _, r := range wrr.Requests {
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
			// Find the next request that will time out, and notify any clients of expired wrr
			next := findNextTimeout(&wrr)
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

	req := &ShutdownRequest{
		readyCh: make(chan struct{}),
		Context: ctx,
	}

	// Wait until q.processQueues() shutdown is complete or until
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
	var soon *ReserveRequest

	for _, req := range r.Requests {
		// If request has already expired
		if time.Now().UTC().After(req.requestDeadline) {
			// Inform our waiting client
			req.err = ErrRequestTimeout
			close(req.readyCh)
			r.Total -= int(req.BatchSize)
			delete(r.Requests, req.ClientID)
			continue
		}

		// If client has gone away
		if req.Context.Err() != nil {
			close(req.readyCh)
			r.Total -= int(req.BatchSize)
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
	Requests map[string]*ReserveRequest
	Total    int
}

type waitingProduceRequests struct {
	Requests   []*ProduceRequest
	TotalItems int
}

type waitingCompleteRequests struct {
	Requests []*CompleteRequest
	TotalIDs int
}
