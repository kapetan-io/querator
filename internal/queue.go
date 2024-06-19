package internal

import (
	"context"
	"errors"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal/maps"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
	"sync/atomic"
	"time"
)

var (
	ErrDuplicateClientID     = errors.New("duplicate client id")
	ErrReserveRequestExpired = errors.New("reserve request expired")
)

const (
	queueChSize = 20_000
	// TODO: This should be based on the reservation timeout set on the queue.
	dataStoreTimeout = time.Minute
)

type QueueOptions struct {
	Logger duh.StandardLogger

	// ReserveTimeout is how long the reservation is valid for.
	ReserveTimeout time.Duration

	// DeadQueue is the name of the dead letter queue for this queue.
	DeadQueue string

	// DeadLine is how long the message can wait in the queue regardless
	// of attempts before it is moved to the dead letter queue.
	DeadLine time.Duration
}

// Queue manages job is to evenly distribute and consume items from a single queue. Ensuring consumers and
// producers are handled fairly and efficiently. Since a Queue is the synchronization point for R/W
// there can ONLY BE ONE instance of a Queue running anywhere in the cluster at any given time. All consume
// and produce requests MUST go through this queue singleton.
type Queue struct {
	reserveQueueCh atomic.Pointer[chan *ReserveRequest]
	produceQueueCh atomic.Pointer[chan *ProduceRequest]
	store          store.QueueStorage
	shutdownCh     chan struct{}
	opts           QueueOptions
	name           string
}

func NewQueue(opts QueueOptions) *Queue {
	set.Default(&opts.Logger, slog.Default())

	q := &Queue{
		// TODO: Init the QueueStorage
		shutdownCh: make(chan struct{}),
	}

	// Assign new wait queue
	ch := make(chan *ReserveRequest, queueChSize)
	q.reserveQueueCh.Store(&ch)

	return q
}

// TODO(thrawn01): Consider creating a pool of ReserveRequest and QueueItem to avoid GC

type ReserveRequest struct {
	// The number of messages requested from the queue.
	BatchSize int32
	// A time in the future when the caller expects Reserve() to return
	// if no items are available to be reserved.
	TimeoutDeadline time.Time
	// The id of the client
	ClientID string
	// The context of the requesting client
	Context context.Context
	// The result of the reservation
	Items []*store.QueueItem

	// Used by Reserve() to wait for this request to complete
	readyCh chan struct{}
	// The error to be returned to the caller
	err error
}

type ProduceRequest struct {
	// The id of the client
	ClientID string
	// The context of the requesting client
	Context context.Context
	// The items to produce
	Items []*store.QueueItem

	// Used by Produce() to wait for this request to complete
	readyCh chan struct{}
	// The error to be returned to the caller
	err error
}

func (q *Queue) Produce(req *ReserveRequest) error {
	req.readyCh = make(chan struct{})

	*q.reserveQueueCh.Load() <- req

	// Wait until the request has been processed
	select {
	case <-req.readyCh:
		return req.err
	}
}

// Reserve is called by clients wanting to reserve a new item from the queue. This call
// will block until the request is cancelled via the passed context or the TimeoutDeadline
// is reached.
//
// ### Context
// DO NOT use with context.WithTimeout() or context.WithDeadline(). Reserve() will block
// until the duration provided via ReserveRequest.WaitTimeout has been reached. You may cancel
// the context if the client has gone away. In this case Queue will abort the reservation
// request. If the context is cancelled after reservation has been written to the data store
// then those requests will be lost until they can be offered to another client after the
// `reserve_timeout` has been reached.
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
// ### Consumer Groups
// Allow users to combine multiple queues by creating a "Consumer Group". Then a client can
// produce/reserve from the consumer group, and the consumer group round robins the request to each queue
// in the group. Doing so destroys any ordering of queued items, but in some applications ordering might
// not be important to the consumer, but Exactly Once Delivery is still desired.
//
// ### Audit Trail
// If users need to track which client picked up the item, we should implement an optional audit log which
// follows the progress of each item in the queue. In this way, the audit log shows which clients picked up the
// item and for how long. This especially helpful in understanding how an item got into the dead letter queue.
func (q *Queue) processQueues() {

	// TODO: Refeactor this into a smaller loop with handler functions which pass around a state object with
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

			// FUTURE: Inspect the Waiting Reserve Requests, and attempt to assign produced messages with
			//  waiting reserve requests if our queue is caught up.
			//  (Check for cancel or expire reserve requests first)

			// FUTURE: Buffer the produced messages at the top of the queue into memory, so we don't need
			//  to query them from the database when we reserve messages later. Doing so avoids the ListReservable()
			//  step, in addition, we can back fill reservable items into memory when processQueues() isn't
			//  actively producing or consuming.

			items := make([]*store.QueueItem, wpr.Total)
			for _, req := range wpr.Requests {
				items = append(items, req.Items...)
			}

			ctx, cancel := context.WithTimeout(context.Background(), dataStoreTimeout)
			if err := q.store.Write(ctx, items); err != nil {
				q.opts.Logger.Warn("while calling QueueStorage.Write()", "error", err)
				cancel()
				continue
			}
			cancel()

			// TODO: Tell the waiting clients the items have been produced
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
			if err := q.store.Reserve(ctx, &items, store.ReserveOptions{
				ReserveExpireAt: time.Now().Add(q.opts.ReserveTimeout),
				Limit:           wrr.Total,
			}); err != nil {
				q.opts.Logger.Warn("while calling QueueStorage.Reserve", "error", err)
				continue
			}
			cancel()

			// Distribute the records evenly to all the reserve clients
			keys := maps.Keys(wrr.Requests)
			var idx int
			for _, item := range items {
				req = wrr.Requests[keys[idx]]
				req.Items = append(req.Items, item)
				// QueueStorage each item assigned to be updated in the database with the reservation
				item.ClientID = req.ClientID
				if idx > len(keys)-1 {
					idx = 0
				}
				// TODO: Update the ExpiredAt and IsReserved field
			}

			// Now mark the records as reserved
			//ctx, cancel = context.WithTimeout(context.Background(), dataStoreTimeout)
			//if err := q.store.Write(ctx, q.name, items); err != nil {
			//	q.opts.Logger.Warn("while calling QueueStorage.MarkAsReserved()", "error", err)
			//	continue
			//}
			//cancel()

			// Inform clients they have reservations ready
			for _, req := range wrr.Requests {
				if len(req.Items) != 0 {
					close(req.readyCh)
					wrr.Total -= len(req.Items)
					delete(wrr.Requests, req.ClientID)
				}
			}

			// Set a timer for remaining open reserve requests
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
		if req.TimeoutDeadline.After(time.Now()) {
			// Inform our waiting client
			req.err = ErrReserveRequestExpired
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
		if req.TimeoutDeadline.Before(soon.TimeoutDeadline) {
			soon = req
		}
	}

	// No sooner than now =)
	if soon == nil {
		return time.Duration(0)
	}

	// How soon is it? =)
	return soon.TimeoutDeadline.Sub(time.Now())
}

type waitingReserveRequests struct {
	Requests map[string]*ReserveRequest
	Total    int
}

type waitingProduceRequests struct {
	Requests []*ProduceRequest
	Total    int
}
