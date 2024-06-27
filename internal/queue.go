package internal

import (
	"context"
	"errors"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal/maps"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
	"sync/atomic"
	"time"
)

var (
	ErrDuplicateClientID = errors.New("duplicate client id")
	ErrRequestTimeout    = errors.New("request timeout")
)

const (
	queueChSize = 20_000
	// TODO: This should be based on the minimum request timeout provided by the client
	dataStoreTimeout  = time.Minute
	maxRequestTimeout = 15 * time.Minute
	minWriteTimeout   = 5 * time.Second
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

	// QueueStorage is the store interface used to persist items for this specific queue
	QueueStorage store.Queue
}

// Queue manages job is to evenly distribute and consume items from a single queue. Ensuring consumers and
// producers are handled fairly and efficiently. Since a Queue is the synchronization point for R/W
// there can ONLY BE ONE instance of a Queue running anywhere in the cluster at any given time. All consume
// and produce requests MUST go through this queue singleton.
type Queue struct {
	reserveQueueCh atomic.Pointer[chan *ReserveRequest]
	produceQueueCh atomic.Pointer[chan *ProduceRequest]
	shutdownCh     chan struct{}
	opts           QueueOptions
}

func NewQueue(opts QueueOptions) (*Queue, error) {
	set.Default(&opts.Logger, slog.Default())
	if opts.QueueStorage == nil {
		return nil, errors.New("QueueOptions.Queue cannot be nil")
	}

	q := &Queue{
		shutdownCh: make(chan struct{}),
	}

	// Assign new wait queue
	ch := make(chan *ReserveRequest, queueChSize)
	q.reserveQueueCh.Store(&ch)

	return q, nil
}

// TODO(thrawn01): Consider creating a pool of ReserveRequest and QueueItem to avoid GC

type ReserveRequest struct {
	// The number of items requested from the queue.
	BatchSize int32
	// How long the caller expects Reserve() to block before returning
	// if no items are available to be reserved. Max duration is 5 minutes.
	RequestTimeout time.Duration
	// The id of the client
	ClientID string
	// The context of the requesting client
	Context context.Context
	// The result of the reservation
	Items []*store.QueueItem

	// Is calculated from RequestTimeout and specifies a time in the future when this request
	// should be cancelled.
	requestDeadline time.Time
	// Used by Reserve() to wait for this request to complete
	readyCh chan struct{}
	// The error to be returned to the caller
	err error
}

type ProduceRequest struct {
	// How long the caller expects Produce() to block before returning
	// if no items are available to be reserved. Max duration is 5 minutes.
	RequestTimeout time.Duration
	// The context of the requesting client
	Context context.Context
	// The items to produce
	Items []*store.QueueItem

	// Is calculated from RequestTimeout and specifies a time in the future when this request
	// should be cancelled.
	requestDeadline time.Time
	// Used by Produce() to wait for this request to complete
	readyCh chan struct{}
	// The error to be returned to the caller
	err error
}

// Produce is called by clients who wish to produce an item to the queue. This
// call will block until the item has been written to the queue or until the request
// is cancelled via the passed context or RequestTimeout is reached.
func (q *Queue) Produce(ctx context.Context, req *ProduceRequest) error {
	req.readyCh = make(chan struct{})

	// --- Verify all the things ---
	req.requestDeadline = time.Now().Add(req.RequestTimeout)
	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidRequest("RequestTimeout '%s' is invalid; maximum timeout is '15m'",
			req.RequestTimeout.String())
	}

	if req.RequestTimeout == time.Duration(0) {
		req.RequestTimeout = maxRequestTimeout
	}
	// -----------------------------

	// The context is passed to processQueues which will check
	// for context cancel and do the right thing.
	req.Context = ctx
	*q.produceQueueCh.Load() <- req

	// Wait until the request has been processed
	select {
	case <-req.readyCh:
		return req.err
	}
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
func (q *Queue) Reserve(req *ReserveRequest) error {
	req.readyCh = make(chan struct{})

	if req.ClientID == "" {
		return transport.NewInvalidRequest("ClientID is invalid; cannot be empty")
	}

	if req.RequestTimeout > maxRequestTimeout {
		return transport.NewInvalidRequest("RequestTimeout '%s' is invalid; maximum timeout is '15m'", req.RequestTimeout.String())
	}

	if req.RequestTimeout == time.Duration(0) {
		req.RequestTimeout = maxRequestTimeout
	}

	req.requestDeadline = time.Now().Add(req.RequestTimeout)
	*q.reserveQueueCh.Load() <- req

	// Wait until the request has been processed
	select {
	case <-req.readyCh:
		return req.err
	}
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

	// TODO: Refactor this into a smaller loop with handler functions which pass around a state object with
	//  State.AddToWPR() and State.AddToWRR() etc....
	wrr := waitingReserveRequests{
		Requests: make(map[string]*ReserveRequest, 5_000),
	}
	wpr := waitingProduceRequests{
		Requests: make([]*ProduceRequest, 5_000),
	}
	var timerCh <-chan time.Time

	addToWPR := func(req *ProduceRequest) {
		wpr.Requests = append(wpr.Requests, req)
		wpr.Total += len(req.Items)
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
		select {

		// Process the produce queue into the wpr list
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
			produceQueueCh = nil

			// FUTURE: Inspect the Waiting Reserve Requests, and attempt to assign produced items with
			//  waiting reserve requests if our queue is caught up.
			//  (Check for cancel or expire reserve requests first)

			// FUTURE: Buffer the produced items at the top of the queue into memory, so we don't need
			//  to query them from the database when we reserve items later. Doing so avoids the ListReservable()
			//  step, in addition, we can back fill reservable items into memory when processQueues() isn't
			//  actively producing or consuming.

			// Place all the queued produce requests in to the `items` list, which is a complete list of
			// all the items each producer provided us with.
			items := make([]*store.QueueItem, wpr.Total)
			writeTimeout := 15 * time.Minute
			for _, req := range wpr.Requests {
				// Cancel any produce requests that have timed out
				if time.Now().After(req.requestDeadline) {
					// TODO: Change to NewRequestRetry (DUH-RPC Update)
					req.err = transport.NewRequestFailed("request timeout while producing item; try again")
					close(req.readyCh)
				}
				for _, item := range req.Items {
					// Record the appropriate dead deadline just before we write to the data store
					item.DeadDeadline = time.Now().Add(q.opts.DeadTimeout)
					items = append(items, item)
				}
				// Attempt to get a timeout that is within the request
				// with the least amount of time left to wait
				timeLeft := time.Now().Sub(req.requestDeadline)
				if timeLeft < writeTimeout {
					writeTimeout = timeLeft
				}
			}

			// If we allow a calculated write timeout to be a few milliseconds, then the store.Write()
			// is almost guaranteed to fail, so we ensure the write timeout is something reasonable.
			if writeTimeout < q.opts.QueueStorage.Options().MinWriteTimeout {
				// MinWriteTimeout comes from the storage implementation as the user who configured the
				// storage option should know a reasonable timeout value for the configuration chosen.
				writeTimeout = q.opts.QueueStorage.Options().MinWriteTimeout
			}

			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			if err := q.opts.QueueStorage.Write(ctx, items); err != nil {
				q.opts.Logger.Warn("while calling QueueStorage.Write()", "error", err)
				cancel()
				// TODO: Let clients that are timed out, know we are done with them.
				continue
			}
			cancel()

			// Tell the waiting clients the items have been produced
			for _, req := range wpr.Requests {
				close(req.readyCh)
			}
			wpr.Total = 0

		// Process the reserve queue into the waiting reserve request (wrr) list.
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
			reserveQueueCh = nil

			// NOTE: V0 doing the simplest thing that works now, we can get fancy & efficient later.

			// Remove any clients that have expired
			_ = findNextTimeout(&wrr)

			// Fetch all the reservable items in the store
			ctx, cancel := context.WithTimeout(context.Background(), dataStoreTimeout)
			var items []*store.QueueItem
			if err := q.opts.QueueStorage.Reserve(ctx, &items, store.ReserveOptions{
				ReserveDeadline: time.Now().Add(q.opts.ReserveTimeout),
				Limit:           wrr.Total,
			}); err != nil {
				q.opts.Logger.Warn("while calling Queue.Reserve", "error", err)
				continue
			}
			cancel()

			// Distribute the records evenly to all the reserve clients
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

		case <-q.shutdownCh:
			// TODO: Tell all the clients are are done, and flush everything then exit
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

func findNextTimeout(r *waitingReserveRequests) time.Duration {
	var soon *ReserveRequest

	for _, req := range r.Requests {
		// If request has already expired
		if req.requestDeadline.After(time.Now()) {
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

		// Will this happen soon?
		if req.requestDeadline.Before(soon.requestDeadline) {
			soon = req
		}
	}

	// No sooner than now =)
	if soon == nil {
		return time.Duration(0)
	}

	// How soon is it? =)
	return soon.requestDeadline.Sub(time.Now())
}

type waitingReserveRequests struct {
	Requests map[string]*ReserveRequest
	Total    int
}

type waitingProduceRequests struct {
	Requests []*ProduceRequest
	Total    int
}
