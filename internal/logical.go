package internal

import (
	"context"
	"fmt"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

type MethodKind int

const (
	maxRequestTimeout = 15 * clock.Minute
	minRequestTimeout = 10 * clock.Millisecond

	MethodStorageItemsList MethodKind = iota
	MethodStorageItemsImport
	MethodStorageItemsDelete
	MethodQueueStats
	MethodQueuePause
	MethodQueueClear
	MethodUpdateInfo
	MethodUpdatePartitions
	MethodProduce
	MethodReserve
	MethodComplete
	MethodLifeCycle

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
	Log *slog.Logger
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
	// The storage partitions handled by the LogicalQueue
	StoragePartitions []store.Partition
	// Manager is the QueuesManager used by partition life cycles to move items to a dead letter
	Manager *QueuesManager
}

// Logical produces and consumes items from the many partitions ensuring consumers and producers are handled fairly
// and efficiently. Since a Logical is the synchronization point for R/W there can ONLY BE ONE instance of a
// Logical running anywhere in the cluster at any given time. All consume and produce requests for the partitions
// assigned to this Logical instance MUST go through this singleton.
type Logical struct {
	// TODO: Failures should be used to indicate all partitions for this Logical Queue have failed. The QueueManager
	//  can check for this flag and avoid routing clients to this Logical Queue.
	// Failures      atomic.Bool

	// Hot request channel is for requests this is included in congestion detection
	hotRequestCh chan *Request
	// Cold request channel is for requests that is NOT included in congestion detection
	coldRequestCh chan *Request

	shutdownCh chan *types.ShutdownRequest
	wg         sync.WaitGroup
	conf       LogicalConfig
	log        *slog.Logger
	// inFlight is a counter of number of hot requests that are in flight waiting
	// for a response to their request. It is used by shutdown to gracefully shut down
	// all outstanding requests.
	inFlight   atomic.Int32
	inShutdown atomic.Bool
	instanceID string
}

func SpawnLogicalQueue(conf LogicalConfig) (*Logical, error) {
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.MaxReserveBatchSize, DefaultMaxReserveBatchSize)
	set.Default(&conf.MaxProduceBatchSize, DefaultMaxProduceBatchSize)
	set.Default(&conf.MaxCompleteBatchSize, DefaultMaxCompleteBatchSize)
	set.Default(&conf.MaxRequestsPerQueue, DefaultMaxRequestsPerQueue)
	set.Default(&conf.Clock, clock.NewProvider())

	l := &Logical{
		// Shutdowns require special handling in the sync loop
		shutdownCh: make(chan *types.ShutdownRequest),
		// Logical requests are any request that doesn't require special batch processing
		coldRequestCh: make(chan *Request),
		instanceID:    random.Alpha("", 10),
		conf:          conf,
	}
	l.log = conf.Log.With("code.namespace", "Logical", "queue", conf.Name, "instance-id", l.instanceID)

	// These are request queues that queue requests from clients until the sync loop has
	// time to process them. When they get processed, every request in the queue is handled
	// in a batch.
	l.hotRequestCh = make(chan *Request, conf.MaxRequestsPerQueue)

	l.log.LogAttrs(context.Background(), LevelDebugAll, "logical queue started")
	l.wg.Add(1)
	go l.requestLoop()
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

	// TODO(schedule): Ignore Schedule dates in the past and enqueue immediately.
	// TODO(schedule): If Scheduled is set, and if the future date is less than 1 second from now, then skip schedule
	//  and enqueue the item immediately. This avoids unnecessary work for something that will be moved into
	//  the queue less than 1 second from now. If someone queued work with the intention that is be be scheduled
	//  now or within a few seconds we should optimize for this common mistake. Programmers may want to be
	//  "complete", so they may include a Scheduled time which is set to now or near to now, due to clock drift
	//  between systems.

	req.RequestDeadline = l.conf.Clock.Now().UTC().Add(req.RequestTimeout)
	req.ReadyCh = make(chan struct{})
	req.Context = ctx

	select {
	case l.hotRequestCh <- &Request{
		Method:  MethodProduce,
		Request: req,
	}:
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
	case l.hotRequestCh <- &Request{
		Method:  MethodReserve,
		Request: req,
	}:
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
	case l.hotRequestCh <- &Request{
		Method:  MethodComplete,
		Request: req,
	}:
	default:
		return transport.NewRetryRequest(MsgQueueOverLoaded)
	}

	// Wait until the request has been processed
	<-req.ReadyCh
	return req.Err
}

func (l *Logical) LifeCycle(ctx context.Context, req *types.LifeCycleRequest) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	r := Request{
		ReadyCh: make(chan struct{}),
		Method:  MethodLifeCycle,
		Context: ctx,
		Request: req,
	}

	select {
	case l.hotRequestCh <- &r:
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

// QueueStats retrieves stats about the queue and items in storage
func (l *Logical) QueueStats(ctx context.Context, stats *types.LogicalStats) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	r := Request{
		Method:  MethodQueueStats,
		Request: stats,
	}
	return l.queueRequest(ctx, &r)
}

// Pause pauses processing of produce, reserve, complete and defer operations until the pause is cancelled.
// It is only used for testing and not exposed to the user via API, as such it is not considered apart of
// the public API.
func (l *Logical) Pause(ctx context.Context, req *types.PauseRequest) error {
	r := Request{
		Method:  MethodQueuePause,
		Request: req,
	}
	return l.queueRequest(ctx, &r)
}

// Clear clears enqueued items from the queue
func (l *Logical) Clear(ctx context.Context, req *types.ClearRequest) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if !req.Queue && !req.Defer && !req.Scheduled {
		return transport.NewInvalidOption("invalid clear request; one of 'queue', 'defer'," +
			" 'scheduled' must be true")
	}

	r := Request{
		Method:  MethodQueueClear,
		Request: req,
	}
	return l.queueRequest(ctx, &r)
}

// UpdateInfo is called whenever QueueInfo changes. It is called during initialization of Logical struct and
// is intended to be called whenever queue configuration changes.
func (l *Logical) UpdateInfo(ctx context.Context, info types.QueueInfo) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}
	r := Request{
		Method:  MethodUpdateInfo,
		Request: info,
	}
	return l.queueRequest(ctx, &r)
}

// UpdatePartitions is called whenever the list of partitions this Logical Queue is responsible for changes.
// It is called during initialization of Logical struct and is intended to be called whenever the QueueManager
// rebalances partitions
func (l *Logical) UpdatePartitions(ctx context.Context, p []store.Partition) error {
	r := Request{
		Method:  MethodUpdatePartitions,
		Request: p,
	}
	return l.queueRequest(ctx, &r)
}

// -------------------------------------------------
// Methods to manage queue storage
// -------------------------------------------------

func (l *Logical) StorageItemsList(ctx context.Context, partition int, items *[]*types.Item, opts types.ListOptions) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}
	req := StorageRequest{
		Partition: partition,
		Items:     items,
		Options:   opts,
	}

	// TODO: Test for invalid pivot

	r := Request{
		Method:  MethodStorageItemsList,
		Request: req,
	}
	return l.queueRequest(ctx, &r)
}

func (l *Logical) StorageItemsImport(ctx context.Context, partition int, items *[]*types.Item) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}
	// TODO: Test for empty list
	r := Request{
		Method: MethodStorageItemsImport,
		Request: StorageRequest{
			Partition: partition,
			Items:     items,
		},
	}
	return l.queueRequest(ctx, &r)
}

func (l *Logical) StorageItemsDelete(ctx context.Context, partition int, ids []types.ItemID) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}
	if len(ids) == 0 {
		return transport.NewInvalidOption("ids is invalid; cannot be empty")
	}

	r := Request{
		Method: MethodStorageItemsDelete,
		Request: StorageRequest{
			Partition: partition,
			IDs:       ids,
		},
	}
	return l.queueRequest(ctx, &r)
}

// prepareQueueState is called whenever partition assignment changes
func (l *Logical) prepareQueueState(state *QueueState) {

	if state.Producers.Requests == nil {
		state.Producers.Requests = make([]*types.ProduceRequest, 0, 5_000)
	}

	if state.Reservations.Requests == nil {
		state.Reservations.Requests = make([]*types.ReserveRequest, 0, 5_000)
	}

	if state.Completes.Requests == nil {
		state.Completes.Requests = make([]*types.CompleteRequest, 0, 5_000)
	}

	// TODO: Validate each partition hasn't been modified since last loaded by a Logical queue.
	//  we do this by saving he last item id in the partition into QueueInfo.StoragePartitions along with
	//  the the QueueInfo.StoragePartitions.Count of items in the partition. When loading the partition,
	//  If the last item id saved in QueueInfo.StoragePartitions agrees with what is in the actual partition,
	//  we should trust that the partition was not modified and the count saved in
	//  QueueInfo.StoragePartitions.Count is accurate. If it's not accurate, we MUST re-count all the items
	//  in the partition in order to accurately distribute items.

	// TODO(rebalance): This needs to preserve partitions counts and current in flight reserve requests
	//  when a partition is added or removed. Do the simple thing right now, in case this all changes
	//  later.
	state.Partitions = make([]*Partition, len(l.conf.StoragePartitions))

	for i, p := range l.conf.StoragePartitions {
		o := &Partition{
			Info:  p.Info(),
			Store: p,
		}
		o.LifeCycle = NewLifeCycle(LifeCycleConfig{
			Clock:     l.conf.Clock,
			Partition: o,
			Storage:   p,
			Logical:   l,
		})
		o.Failures.Store(-1)
		state.Partitions[i] = o

		// TODO(rebalance): We need to shutdown existing life cycles when partitions are added or removed

		ctx, cancel := context.WithTimeout(context.Background(), l.conf.ReadTimeout)
		if err := o.LifeCycle.Start(ctx); err != nil {
			l.log.Warn("partition unavailable; while waiting for life cycle to start",
				"partition", p.Info().PartitionNum,
				"timeout", l.conf.ReadTimeout,
				"error", err.Error())
		}
		cancel()
	}

	slices.SortFunc(state.Partitions, func(a, b *Partition) int {
		if a.Count < b.Count {
			return -1
		}
		if a.Count > b.Count {
			return +1
		}
		return 0
	})

}

// -------------------------------------------------
// Main Loop and Handlers
// See doc/adr/0003-rw-sync-point.md for an explanation of this design
// -------------------------------------------------

type QueueState struct {
	LifeCycles        types.Batch[types.LifeCycleRequest]
	Completes         types.Batch[types.CompleteRequest]
	Producers         types.Batch[types.ProduceRequest]
	Reservations      types.ReserveBatch
	NextMaintenanceCh <-chan clock.Time
	Partitions        []*Partition
}

func (l *Logical) requestLoop() {
	defer l.wg.Done()
	var state QueueState

	l.prepareQueueState(&state)

	// TODO(lifecycle): When adding new scheduled items to the partition, requestLoop needs to notify the life cycle
	//  of the new items via state.Maintenance.NotifyScheduledItem(date)

	for {
		l.log.LogAttrs(context.Background(), LevelDebugAll, "requestLoop()",
			slog.Int("InChannel", len(l.hotRequestCh)),
			slog.Int("RBlocked", len(state.Reservations.Requests)),
			slog.Int("PBlocked", len(state.Producers.Requests)),
			slog.Int("CBlocked", len(state.Completes.Requests)),
			slog.Int("InFlight", int(l.inFlight.Load())),
		)

		select {
		case req := <-l.hotRequestCh:
			l.handleHotRequests(&state, req)

		case req := <-l.coldRequestCh:
			l.handleColdRequests(&state, req)
			// If a cold request initiated a shutdown, exit immediately
			if l.inShutdown.Load() {
				return
			}

		case req := <-l.shutdownCh:
			l.handleShutdown(&state, req)
			return

		case <-state.NextMaintenanceCh:
			l.handleRequestTimeouts(&state)
		}
	}
}

func (l *Logical) handleHotRequests(state *QueueState, req *Request) {
	// If request is empty, do not consume more requests, the caller wants us to
	// only process requests currently waiting in 'state'
	if req != nil {
		// Consume all requests from the hot channel, adding the requests to
		// the current state.
		l.consumeHotCh(state, req)
	}

	// TODO: Perhaps we can move all of these into the same method?
	l.distributeLifeCycles(state)
	l.distributeProduceRequests(state)
	// FUTURE: distributeReserveRequests() should inspect the produce requests, and
	// attempt to assign produced items with waiting reserve requests if our
	// queue is caught up. (Check for cancel or expire reserve requests first)
	l.distributeReserveRequests(state)
	l.distributeCompleteRequests(state)

	// write distributed requests to each partition according to assignment.
	_ = l.writeToPartitions(state)

	// TODO: If an error occurred, and it's recoverable, then
	//  attempt to re-distribute remaining requests to other partitions

	// TODO: Some time may have passed while talking to the partition.
	//  If a partition has failed, and we can't re-distribute to other partitions
	//  we need to let clients know they  are timed out.
	//  Perhaps scheduled maint should do this or just let `handleRequestTimeouts()` do it?

	// ----------------------------------------
	// Inform clients their requests are done
	// ----------------------------------------
	for _, req := range state.Producers.Requests {
		close(req.ReadyCh)
	}
	for _, req := range state.Completes.Requests {
		close(req.ReadyCh)
	}

	for i, req := range state.Reservations.Requests {
		if req == nil {
			continue
		}
		if len(req.Items) != 0 || req.Err != nil {
			state.Reservations.MarkNil(i)
			close(req.ReadyCh)
		}
	}

	// Reset the partition assignments
	for _, p := range state.Partitions {
		if p.NumReserved != 0 {
			// Indexing LifeCycles here only works because LifeCycles and Partitions have the
			// same number of entries, consider merging them in the future.
			p.LifeCycle.Notify(p.MostRecentDeadline)
		}
		p.Reset()
	}

	state.Reservations.FilterNils()
	state.Producers.Reset()
	state.Completes.Reset()

	l.handleRequestTimeouts(state)
}

func (l *Logical) consumeHotCh(state *QueueState, req *Request) {
	// TODO: Check for request timeout, it's possible the request has been sitting in
	//  the channel for a while
	// Cancel any produce requests that have timed out
	//if l.conf.Clock.Now().UTC().After(req.RequestDeadline) {
	//	req.Err = ErrRequestTimeout
	//	state.Producers.Remove(req)
	//	close(req.ReadyCh)
	//	continue
	//}
	//
	// Remove any clients that have timed out
	// l.nextTimeout(&state.Reservations)
	//
	//if l.conf.Clock.Now().UTC().After(req.RequestDeadline) {
	//	req.Err = ErrRequestTimeout
	//	state.Completes.Remove(req)
	//	close(req.ReadyCh)
	//	continue
	//}

	switch req.Method {
	case MethodProduce:
		state.Producers.Add(req.Request.(*types.ProduceRequest))
	case MethodComplete:
		state.Completes.Add(req.Request.(*types.CompleteRequest))
	case MethodLifeCycle:
		state.LifeCycles.Add(req.Request.(*types.LifeCycleRequest))
	case MethodReserve:
		addIfUnique(&state.Reservations, req.Request.(*types.ReserveRequest))
	default:
		panic(fmt.Sprintf("undefined request method '%d'", req.Method))
	}
EMPTY:
	for {
		select {
		case req := <-l.hotRequestCh:
			// TODO(thrawn01): Ensure we don't go beyond our max number of state.Producers, state.Completes, etc..
			//  If we do then we must inform the client that we are overloaded, and they must try again. We
			//  cannot hold on to a request and let it sit in the channel, else we break the
			//  request_timeout contract we have with the client.
			switch req.Method {
			case MethodProduce:
				state.Producers.Add(req.Request.(*types.ProduceRequest))
			case MethodComplete:
				state.Completes.Add(req.Request.(*types.CompleteRequest))
			case MethodLifeCycle:
				state.LifeCycles.Add(req.Request.(*types.LifeCycleRequest))
			case MethodReserve:
				addIfUnique(&state.Reservations, req.Request.(*types.ReserveRequest))
			default:
				panic(fmt.Sprintf("undefined request method '%d'", req.Method))
			}
		default:
			break EMPTY
		}
	}
}

func (l *Logical) distributeLifeCycles(state *QueueState) {
	for _, req := range state.LifeCycles.Requests {
		p := state.GetPartition(req.PartitionNum)
		if p == nil {
			l.log.Warn("LifeCycleRequest received for invalid partition; skipping request",
				"req-partition", req.PartitionNum)
			continue
		}
		p.LifeCycleRequests.Add(req)
	}
	// TODO: Write a new PartitionStore method to update the items
}

func (l *Logical) distributeProduceRequests(state *QueueState) {
	for _, req := range state.Producers.Requests {
		// Assign a ExpireTimeout to each item
		for _, item := range req.Items {
			item.ExpireDeadline = l.conf.Clock.Now().UTC().Add(l.conf.ExpireTimeout)
		}

		l.log.LogAttrs(req.Context, LevelDebug, "Produce",
			slog.Int("partition", state.Partitions[0].Store.Info().PartitionNum),
			slog.Int("items", len(req.Items)))

		// Preform Opportunistic Partition Distribution
		// See doc/adr/0019-partition-items-distribution.md for details
		state.Partitions[0].Produce(req)
		slices.SortFunc(state.Partitions, func(a, b *Partition) int {
			if a.Count < b.Count {
				return -1
			}
			if a.Count > b.Count {
				return +1
			}
			return 0
		})
	}
}

func (l *Logical) distributeReserveRequests(state *QueueState) {
	// Assign Reservation Requests to StoragePartitions
	for _, req := range state.Reservations.Requests {
		l.log.LogAttrs(req.Context, LevelDebug, "Reserve",
			slog.Int("partition", state.Partitions[len(state.Partitions)-1].Store.Info().PartitionNum),
			slog.Int("num_requested", req.NumRequested),
			slog.String("client_id", req.ClientID))

		// Preform Opportunistic Partition Distribution
		// See doc/adr/0019-partition-items-distribution.md for details
		state.Partitions[len(state.Partitions)-1].Reserve(req)
		slices.SortFunc(state.Partitions, func(a, b *Partition) int {
			if a.Count < b.Count {
				return -1
			}
			if a.Count > b.Count {
				return +1
			}
			return 0
		})
	}
}

func (l *Logical) distributeCompleteRequests(state *QueueState) {
nextRequest:
	for _, req := range state.Completes.Requests {
		// Find the partition in the distribution
		var found bool
		for i, p := range state.Partitions {
			// Assign the request to the partition
			if req.Partition == p.Store.Info().PartitionNum {
				if p.Failures.Load() != 0 {
					req.Err = transport.NewRetryRequest("partition not available;"+
						" partition '%d' is failing, retry again", req.Partition)
					//close(req.ReadyCh)
					continue nextRequest
				}
				state.Partitions[i].CompleteRequests.Add(req)
				found = true
			}
		}
		if !found {
			req.Err = transport.NewRetryRequest("partition not found;"+
				" partition '%d' may have moved, retry again", req.Partition)
			//close(req.ReadyCh)
		}
	}
}

// TODO: handleDist() needs to indicate if a partition failed, which one, and if we should re-distribute
//	requests to other partitions.

// writeToPartitions is intended to be used as a place where we can optimise interacting with the
// data store. If the data store supports batching multiple actions in a single round trip request
// we should make that possible here.
func (l *Logical) writeToPartitions(state *QueueState) error {
	// TODO: Future: Querator should handle Partition failure by redistributing the batches to partitions which have
	//  not failed. (note: make sure we reduce the Partitions[].Count when re-assigning)
	// TODO: Identify a degradation vs a failure and set Partition[].Failures as appropriate.
	// TODO: Adjust the item count in state.Partitions for failed partitions
	// TODO: If no new produce requests come in, this may never try again. We need the maintenance
	//  handler to poke the partition again at some reasonable time in the future, in case it recovered

	// TODO: Interaction with each partition should probably be async so we can send multiple
	//  writes simultaneously. If one of the partitions is slow, it won't hold up the other partitions

	// TODO: Partition should accept batched writes if such a thing is supported.

	for _, p := range state.Partitions {
		ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
		// ----------------------------------------
		// Produce
		// ----------------------------------------
		if len(p.ProduceRequests.Requests) != 0 {
			if err := p.Store.Produce(ctx, p.ProduceRequests); err != nil {
				l.log.Error("while calling store.Partition.Produce()",
					"partition", p.Store.Info().PartitionNum, "error", err)
				// TODO: Handle degradation
			}
			// Only if Produce was successful
			p.MostRecentDeadline = l.conf.Clock.Now().UTC().Add(l.conf.ExpireTimeout)
		}

		// ----------------------------------------
		// Reserve
		// ----------------------------------------
		if len(p.ReserveRequests.Requests) != 0 {
			reserveDeadline := l.conf.Clock.Now().UTC().Add(l.conf.ReserveTimeout)
			if err := p.Store.Reserve(ctx, p.ReserveRequests, store.ReserveOptions{
				ReserveDeadline: reserveDeadline,
			}); err != nil {
				l.log.Error("while calling store.Partition.Reserve()",
					"partition", p.Store.Info().PartitionNum,
					"error", err)
				// TODO: Handle degradation
			}
			// Only if Reserve was successful
			p.MostRecentDeadline = reserveDeadline
		}

		// ----------------------------------------
		// Complete
		// ----------------------------------------
		if err := p.Store.Complete(ctx, p.CompleteRequests); err != nil {
			l.log.Error("while calling store.Partition.Complete()",
				"partition", p.Store.Info().PartitionNum,
				"queueName", l.conf.Name,
				"error", err)

			// Completes are a bit different, in that we can't assign them to different
			// partitions, a complete request is FOR a specific partition, if there was
			// an error talking to that partition we need to tell the client.
			for _, req := range p.CompleteRequests.Requests {
				// TODO: We likely want to inform the client they should retry
				//  instead of return an internal error
				req.Err = ErrInternalRetry
			}
			// TODO: Handle degradation
		}
		// ----------------------------------------
		// LifeCycle
		// ----------------------------------------
		if err := p.Store.TakeAction(ctx, p.LifeCycleRequests); err != nil {
			l.log.Error("while calling store.Partition.TakeAction()",
				"partition", p.Store.Info().PartitionNum,
				"queueName", l.conf.Name,
				"error", err)

		}
		cancel()
	}
	return nil
}

func (l *Logical) handleRequestTimeouts(state *QueueState) {
	// TODO: Consider state.nextDeadLine which should be the next time a reservation will expire
	next := l.nextTimeout(&state.Reservations)
	if next.Nanoseconds() != 0 {
		l.log.LogAttrs(context.Background(), LevelDebug, "next maintenance",
			slog.String("duration", next.String()))
		state.NextMaintenanceCh = l.conf.Clock.After(next)
	}
}

func (l *Logical) Shutdown(ctx context.Context) error {
	if l.inShutdown.Swap(true) {
		return nil
	}

	req := &types.ShutdownRequest{
		ReadyCh: make(chan struct{}),
		Context: ctx,
	}

	// Wait until l.requestLoop() shutdown is complete or until
	// our context is cancelled.
	select {
	case l.shutdownCh <- req:
		l.wg.Wait()
		return req.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Logical) queueRequest(ctx context.Context, r *Request) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	r.ReadyCh = make(chan struct{})
	r.Context = ctx

	select {
	case l.coldRequestCh <- r:
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
		//l.prepareQueueState(state)
		close(req.ReadyCh)
	case MethodUpdatePartitions:
		p := req.Request.([]store.Partition)
		l.conf.StoragePartitions = p
		close(req.ReadyCh)
	default:
		panic(fmt.Sprintf("undefined request method '%d'", req.Method))
	}
}

func (l *Logical) handleClear(_ *QueueState, req *Request) {
	// NOTE: When clearing a queue, ensure we flush any cached items. As of this current
	// version (V0), there is no cached data to sync, but this will likely change in the future.
	cr := req.Request.(*types.ClearRequest)

	if cr.Queue {
		// Ask the store to clean up any items in the data store which are not currently out for reservation
		if err := l.conf.StoragePartitions[0].Clear(req.Context, cr.Destructive); err != nil {
			req.Err = err
		}
	}
	// TODO(thrawn01): Support clearing defer and scheduled
	close(req.ReadyCh)
}

func (l *Logical) handleStorageRequests(req *Request) {
	sr := req.Request.(StorageRequest)

	switch req.Method {
	case MethodStorageItemsList:
		if err := l.conf.StoragePartitions[sr.Partition].List(req.Context, sr.Items, sr.Options); err != nil {
			req.Err = err
		}
	case MethodStorageItemsImport:
		if err := l.conf.StoragePartitions[sr.Partition].Add(req.Context, *sr.Items); err != nil {
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
	qs.ReserveWaiting = len(state.Reservations.Requests)
	qs.ProduceWaiting = len(state.Producers.Requests)
	qs.CompleteWaiting = len(state.Completes.Requests)
	qs.InFlight = int(l.inFlight.Load())

	for _, p := range l.conf.StoragePartitions {
		var ps types.PartitionStats
		if err := p.Stats(r.Context, &ps); err != nil {
			r.Err = err
		}
		qs.Partitions = append(qs.Partitions, ps)
	}
	close(r.ReadyCh)
}

// handlePause places Logical into a special loop where operations none of the // produce,
// reserve, complete, defer operations will be processed until we leave the loop.
func (l *Logical) handlePause(state *QueueState, r *Request) {
	pr := r.Request.(*types.PauseRequest)

	if !pr.Pause {
		close(r.ReadyCh)
		return
	}
	close(r.ReadyCh)

	l.log.Debug("paused", "logical", l.instanceID)
	defer l.log.Debug("un-paused", "logical", l.instanceID)

	for {
		select {
		case req := <-l.coldRequestCh:
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
		case req := <-l.hotRequestCh:
			l.consumeHotCh(state, req)
		}
	}
}
func (l *Logical) handleShutdown(state *QueueState, req *types.ShutdownRequest) {
	// Cancel any open reservations
	for _, r := range state.Reservations.Requests {
		if r != nil {
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		}
	}

	// Consume all requests currently in flight
	for l.inFlight.Load() != 0 {
		for {
			select {
			case r := <-l.hotRequestCh:
				switch r.Method {
				case MethodProduce:
					o := r.Request.(*types.ProduceRequest)
					o.Err = ErrQueueShutdown
					close(o.ReadyCh)
				case MethodReserve:
					o := r.Request.(*types.ReserveRequest)
					o.Err = ErrQueueShutdown
					close(o.ReadyCh)
				case MethodComplete:
					o := r.Request.(*types.CompleteRequest)
					o.Err = ErrQueueShutdown
					close(o.ReadyCh)
				default:
					panic(fmt.Sprintf("undefined request method '%d'", r.Method))
				}
			default:
				// If there is a hot request still in flight, it is possible
				// it hasn't been added to the channel yet, so we try until there
				// are no more waiting requests.
				if l.inFlight.Load() == 0 {
					break
				}
			}
		}
	}

	for _, p := range state.Partitions {
		l.log.LogAttrs(req.Context, LevelDebugAll, "shutdown lifecycle",
			slog.Int("partition", p.Store.Info().PartitionNum))
		if err := p.LifeCycle.Shutdown(req.Context); err != nil {
			l.log.Error("during lifecycle shutdown", "error", err)
			req.Err = err
		}
	}

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
			r.TotalRequested -= req.NumRequested
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
	r.TotalRequested += req.NumRequested
	r.Requests = append(r.Requests, req)
}
