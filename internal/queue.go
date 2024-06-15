package internal

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrDuplicateClientID     = errors.New("duplicate client id")
	ErrReserveRequestExpired = errors.New("reserve request expired")
)

// TODO: When a message has been deferred, it could be queued into a different partition than
//  what it's message id suggests. We should either NOT include the partition in the message id.
//  which means we need to include the partition during /queue.complete OR we change the message id
//  everytime it's deferred... I think if the user wanted a unique id through which it can track the
//  status of a queued item all through it's life time, we should introduce a new field for that purpose.

// Queue manages a set of partitions which make up the queue. Its job is to evenly distribute and
// consume items from the queue. Ensuring consumers and producers are handled fairly and efficiently.
type Queue struct {
	waitCh            atomic.Pointer[chan *ReserveRequest]
	partitionsReadyCh chan struct{}
	shutdownCh        chan struct{}
}

func NewQueue() *Queue {
	q := &Queue{
		partitionsReadyCh: make(chan struct{}),
		shutdownCh:        make(chan struct{}),
	}

	// Assign new wait queue
	ch := make(chan *ReserveRequest, 20_000)
	q.waitCh.Store(&ch)

	return q
}

type ReserveRequest struct {
	// The number of messages requested from the queue.
	BatchSize int32
	// A time in the future when the caller expects Reserve() to return
	// if no items are available to be reserved.
	TimeoutDeadline time.Time
	// The id of the client reserving
	ClientID string
	// The context of the requesting client
	Context context.Context
	// The result of the reservation
	Items []ReservedItem

	// Used by Reserve() to wait for this reserve request to complete
	readyCh chan struct{}
	// The error to be returned to the caller
	err error
}

type ReservedItem struct {
	ClientID string
}

// Reserve is called by clients wanting to reserve a new item from the queue. This call
// will block until the request is cancelled via the passed context.
//
// ### Context
// DO NOT use with context.WithTimeout() or context.WithDeadline(). Reserve() will block
// until the duration provided via ReserveRequest.WaitTimeout has been reached. You may cancel
// the context if the client has gone away. In this case Queue will abort the reservation
// request. If the context is cancelled after reservation has been written to the database
// then those requests will be lost until they can be offered to another client after the
// `reserve_timeout` has been reached.
//
// Unique Requests
// ClientID must NOT be empty and each request must be unique, or it will be rejected with ErrDuplicateClientID
func (q *Queue) Reserve(req *ReserveRequest) error {
	req.readyCh = make(chan struct{})

	*q.waitCh.Load() <- req

	// Wait until the request has been processed
	select {
	case <-req.readyCh:
		return req.err
	}
}

// processReserveRequests processes the queue of waiting clients in waitCh. It adds every
// client waiting to the 'requests' list. Then it tries its best to find data for every client.
//
// The idea here is that when this go routine is woken up by the scheduler we do as much as
// possible without giving the scheduler an excuse to steal away our CPU time. In other words,
// the code path to get an item from the queue should not do anything that blocks. Items in
// partitions should already be in a queue waiting for this go routine to pick them up.
//
// This allows for some really cool FUTURE optimizations where items produced to the queue
// can be reserved before they are written to the database. It also allows batching reads and
// writes with the database. Batching reduces IOPs required and increases efficiency.
//
// This routine does one of a few things:
//   - New client requests are added to the waiting 'requests' list. If there is
//     data waiting in the partitions, we distribute the data between all the
//     waiting requests
//   - The timer fires, and we look for any clients which have expired setting the
//     appropriate time out error.
//   - The partitions tell us there is data ready, and we process 'requests' list
//     giving out candy to any client asking.
//   - Close all the things, we are in shutdown!
func (q *Queue) processReserveRequests() {
	var timerCh <-chan time.Time
	// TODO: Adjust the size of the requests (maybe)?
	requests := make(map[string]*ReserveRequest, 100_000)
	addToRequests := func(req *ReserveRequest) {
		if _, ok := requests[req.ClientID]; ok {
			req.err = ErrDuplicateClientID
			close(req.readyCh)
			return
		}
		requests[req.ClientID] = req
	}

	for {
		waitCh := q.waitCh.Load()
		select {

		case req := <-*waitCh:
			// Swap the wait channel, so we can process the wait queue. Creating a
			// new channel allows any incoming requests that occur while this routine
			// is running to be queued into the new wait queue without interfering with
			// anything we are doing here.
			ch := make(chan *ReserveRequest, 20_000)
			q.waitCh.Store(&ch)
			close(*waitCh)

			// Consume the wait queue
			addToRequests(req)
			for req := range *waitCh {
				addToRequests(req)
			}
			waitCh = nil

			// If there is nothing to do, then check for request timeouts.
			if !q.partitionsReady() {
				next := findNextTimeout(requests)
				if next.Nanoseconds() != 0 {
					timerCh = time.After(next)
				}
			}
			// TODO: Attempt to fill each request from all the partitions

			// TODO: close(req.readyCh) each request with reservations

			// TODO: Set a timer for the request that will expire next

		// partitionsReadyCh acts like a semaphore. If any partition receives new data, it
		// will attempt to queue into the channel, thus waking up this go routine.
		case <-q.partitionsReadyCh:
		// TODO: Process any waiting reservation requests
		case <-q.shutdownCh:
			// TODO: Tell all the clients are are done, and tell all the partitions to flush everything and exit
		case <-timerCh:
			// Find the next request that will time out, and notify any clients of expired requests
			next := findNextTimeout(requests)
			if next.Nanoseconds() != 0 {
				timerCh = time.After(next)
			}

		}
	}
}

// Returns true of one of the partitions is ready to be read
func (q *Queue) partitionsReady() bool {
	select {
	case <-q.partitionsReadyCh:
		return true
	default:
		return false
	}
}

func findNextTimeout(m map[string]*ReserveRequest) time.Duration {
	var soon *ReserveRequest

	for _, req := range m {
		// If request has already expired
		if req.TimeoutDeadline.After(time.Now()) {
			// Inform our waiting client
			req.err = ErrReserveRequestExpired
			close(req.readyCh)
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
