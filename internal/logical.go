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

const (
	maxRequestTimeout      = 15 * clock.Minute
	minRequestTimeout      = 10 * clock.Millisecond
	MethodStorageItemsList = iota
	MethodStorageItemsImport
	MethodStorageItemsDelete
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
	// The initial partitions provided to the LogicalQueue at initialization.
	Partitions []store.Partition
}

// Logical produces and consumes items from the many partitions ensuring consumers and producers are handled fairly
// and efficiently. Since a Logical is the synchronization point for R/W there can ONLY BE ONE instance of a
// Logical running anywhere in the cluster at any given time. All consume and produce requests for the partitions
// assigned to this Logical instance MUST go through this singleton.
type Logical struct {
	// TODO: InFailure should be used to indicate all partitions for this Logical Queue have failed. The QueueManager
	//  can check for this flag and avoid routing clients to this Logical Queue.
	// InFailure      atomic.Bool

	reserveQueueCh  chan *types.ReserveRequest
	produceQueueCh  chan *types.ProduceRequest
	completeQueueCh chan *types.CompleteRequest

	shutdownCh     chan *types.ShutdownRequest
	queueRequestCh chan *QueueRequest
	wg             sync.WaitGroup
	conf           LogicalConfig
	inFlight       atomic.Int32
	inShutdown     atomic.Bool
	instanceID     string
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
		queueRequestCh: make(chan *QueueRequest),
		instanceID:     random.Alpha("", 10),
		conf:           conf,
	}
	l.conf.Log = conf.Log.With("queue", conf.Name, "instance-id", l.instanceID)

	// These are request queues that queue requests from clients until the sync loop has
	// time to process them. When they get processed, every request in the queue is handled
	// in a batch.
	l.reserveQueueCh = make(chan *types.ReserveRequest, conf.MaxRequestsPerQueue)
	l.produceQueueCh = make(chan *types.ProduceRequest, conf.MaxRequestsPerQueue)
	l.completeQueueCh = make(chan *types.CompleteRequest, conf.MaxRequestsPerQueue)

	l.conf.Log.LogAttrs(context.Background(), slog.LevelDebug, "logical queue started")
	l.wg.Add(1)
	go l.syncLoop()
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
func (l *Logical) QueueStats(ctx context.Context, stats *types.LogicalStats) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

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
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

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
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}
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

	r := QueueRequest{
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
	r := QueueRequest{
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

	r := QueueRequest{
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
	//  we do this by saving he last item id in the partition into QueueInfo.Partitions along with
	//  the the QueueInfo.Partitions.Count of items in the partition. When loading the partition,
	//  If the last item id saved in QueueInfo.Partitions agrees with what is in the actual partition,
	//  we should trust that the partition was not modified and the count saved in
	//  QueueInfo.Partitions.Count is accurate. If it's not accurate, we MUST re-count all the items
	//  in the partition in order to accurately distribute items.

	state.Distributions = make([]PartitionDistribution, len(l.conf.Partitions))
	for i, p := range l.conf.Partitions {
		state.Distributions[i] = PartitionDistribution{
			Partition: p,
		}

		ctx, cancel := context.WithTimeout(context.Background(), l.conf.ReadTimeout)
		var stats types.PartitionStats
		if err := p.Stats(ctx, &stats); err != nil {
			state.Distributions[i].InFailure = true
			// TODO: Maintenance needs to partitions that are marked as InFailure until the partition is reassigned
			//  or no longer fails.

			l.conf.Log.Warn("unable to query partition stats while preparing partition state; "+
				" items will not be produced to partition until it is available",
				"partition", p.Info().Partition,
				"error", err)
		}
		state.Distributions[i].Count = stats.Total - stats.TotalReserved
		cancel()
	}

	slices.SortFunc(state.Distributions, func(a, b PartitionDistribution) int {
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

func (l *Logical) syncLoop() {
	defer l.wg.Done()
	var state QueueState

	l.prepareQueueState(&state)

	for {
		l.conf.Log.LogAttrs(context.Background(), slog.LevelDebug, "syncLoop()")
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

	// Assign a DeadTimeout and Calculate an appropriate write timeout such that we respect RequestDeadline
	// if we are experiencing a saturation event
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

		l.conf.Log.LogAttrs(req.Context, slog.LevelDebug, "Produce",
			slog.Int("partition", state.Distributions[0].Partition.Info().Partition),
			slog.Int("items", len(req.Items)))

		// Preform Opportunistic Partition Distribution
		// See doc/adr/0019-partition-items-distribution.md for details
		state.Distributions[0].Produce(req)
		slices.SortFunc(state.Distributions, func(a, b PartitionDistribution) int {
			if a.Count < b.Count {
				return -1
			}
			if a.Count > b.Count {
				return +1
			}
			return 0
		})
	}

	// TODO: Future: Querator should handle Partition failure by redistributing the batches to partitions which have
	//  not failed. (note: make sure we reduce the Distributions[].Count when re-assigning)

	for i, p := range state.Distributions {
		ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
		if err := p.Partition.Produce(ctx, p.ProduceRequests); err != nil {
			l.conf.Log.Error("while calling Partition.Produce()",
				"partition", p.Partition.Info().Partition, "error", err)

			// Some time may have passed while talking to the partition.
			// let clients that are timed out, know we are done with them.
			for _, req := range state.Producers.Requests {
				if l.conf.Clock.Now().UTC().After(req.RequestDeadline) {
					req.Err = ErrRequestTimeout
					close(req.ReadyCh)
					continue
				}
			}
			// We get here if there was an internal error with the data store
			// TODO: Identify a degradation vs a failure and set PartitionDistribution[].InFailure as appropriate.
			// TODO: Adjust the item count in state.Distributions for failed partitions
			// TODO: If no new produce requests come in, this may never try again. We need the maintenance
			//  handler to poke the partition again at some reasonable time in the future, in case it recovered
		}
		// Reset the produce request assignments to this partition
		state.Distributions[i].ProduceRequests.Reset()
		cancel()
	}

	// Tell the waiting clients that items have been produced
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

	// Remove any clients that have timed out
	l.nextTimeout(&state.Reservations)

	// Assign Reservation Requests to Partitions
	for _, req := range state.Reservations.Requests {
		l.conf.Log.LogAttrs(req.Context, slog.LevelDebug, "Reserve",
			slog.Int("partition", state.Distributions[len(state.Distributions)-1].Partition.Info().Partition),
			slog.Int("num_requested", req.NumRequested),
			slog.String("client_id", req.ClientID))

		// Preform Opportunistic Partition Distribution
		// See doc/adr/0019-partition-items-distribution.md for details
		state.Distributions[len(state.Distributions)-1].Reserve(req)
		slices.SortFunc(state.Distributions, func(a, b PartitionDistribution) int {
			if a.Count < b.Count {
				return -1
			}
			if a.Count > b.Count {
				return +1
			}
			return 0
		})
	}

	// Fulfill the assigned reservation requests
	for i, p := range state.Distributions {
		ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
		if err := p.Partition.Reserve(ctx, p.ReserveRequests, store.ReserveOptions{
			ReserveDeadline: l.conf.Clock.Now().UTC().Add(l.conf.ReserveTimeout),
		}); err != nil {
			l.conf.Log.Error("while calling Partition.Reserve()",
				"partition", p.Partition.Info().Partition,
				"error", err)

			// We get here if there was an internal error with the data store
			// TODO: Identify a degradation vs a failure and set PartitionDistribution[].InFailure as appropriate.
			// TODO: Adjust the item count in state.Distributions for failed partitions
			// TODO: If no new reserve requests come in, this may never try again. We need the maintenance
			//  handler to poke the partition again at some reasonable time in the future, in case it recovered
		}
		// Reset the reserve request assignments to this partition
		state.Distributions[i].ReserveRequests.Reset()
		cancel()
	}

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

nextRequest:
	for _, req := range state.Completes.Requests {
		// Cancel requests that have timed out
		if l.conf.Clock.Now().UTC().After(req.RequestDeadline) {
			req.Err = ErrRequestTimeout
			state.Completes.Remove(req)
			close(req.ReadyCh)
			continue
		}

		// Find the partition in the distribution
		var found bool
		for i, p := range state.Distributions {
			// Assign the request to the partition
			if req.Partition == p.Partition.Info().Partition {
				if p.InFailure {
					req.Err = transport.NewRetryRequest("partition not available;"+
						" partition '%d' is failing, retry again", req.Partition)
					close(req.ReadyCh)
					continue nextRequest
				}
				state.Distributions[i].CompleteRequests.Add(req)
				found = true
			}
		}
		if !found {
			req.Err = transport.NewRetryRequest("partition not found;"+
				" partition '%d' may have moved, retry again", req.Partition)
			close(req.ReadyCh)
		}
	}

	// Fulfill the assigned complete requests
	for i, p := range state.Distributions {
		ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
		var err error
		if err = p.Partition.Complete(ctx, p.CompleteRequests); err != nil {
			l.conf.Log.Error("while calling Partition.Complete()",
				"partition", p.Partition.Info().Partition,
				"queueName", l.conf.Name,
				"error", err)

			// We get here if there was an internal error with the data store
			// TODO: Identify a degradation vs a failure and set PartitionDistribution[].InFailure as appropriate.
			// TODO: Adjust the item count in state.Distributions for failed partitions
			// TODO: If no new reserve requests come in, this may never try again. We need the maintenance
			//  handler to poke the partition again at some reasonable time in the future, in case it recovered
			// TODO: We may want to retry on behalf of the client in the future.
		}
		// Tell the waiting clients that items have been marked as complete for this partition
		for _, req := range p.CompleteRequests.Requests {
			if err != nil {
				// TODO: We likely want to inform the client they should retry instead of return an internal error
				req.Err = ErrInternalRetry
			}
			close(req.ReadyCh)
			// Reset the complete request assignments to this partition
			state.Distributions[i].CompleteRequests.Reset()
		}
		cancel()
	}

	state.Completes.Reset()
}

// stateCleanUp is responsible for cleaning the QueueState by removing clients that have timed out,
// and finding the next reserve request that will time out and wetting the wakeup timer.
func (l *Logical) stateCleanUp(state *QueueState) {
	next := l.nextTimeout(&state.Reservations)
	if next.Nanoseconds() != 0 {
		l.conf.Log.LogAttrs(context.Background(), slog.LevelDebug, "next maintenance",
			slog.String("duration", next.String()))
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

	// Wait until l.syncLoop() shutdown is complete or until
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
		l.prepareQueueState(state)
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

	switch req.Method {
	case MethodStorageItemsList:
		if err := l.conf.Partitions[sr.Partition].List(req.Context, sr.Items, sr.Options); err != nil {
			req.Err = err
		}
	case MethodStorageItemsImport:
		if err := l.conf.Partitions[sr.Partition].Add(req.Context, *sr.Items); err != nil {
			req.Err = err
		}
	case MethodStorageItemsDelete:
		if err := l.conf.Partitions[sr.Partition].Delete(req.Context, sr.IDs); err != nil {
			req.Err = err
		}
	default:
		panic(fmt.Sprintf("unknown storage request method '%d'", req.Method))
	}
	close(req.ReadyCh)
}

func (l *Logical) handleStats(state *QueueState, r *QueueRequest) {
	qs := r.Request.(*types.LogicalStats)
	qs.ProduceWaiting = len(l.produceQueueCh)
	qs.ReserveWaiting = len(l.reserveQueueCh)
	qs.CompleteWaiting = len(l.completeQueueCh)
	qs.ReserveBlocked = len(state.Reservations.Requests)
	qs.InFlight = int(l.inFlight.Load())

	for _, p := range l.conf.Partitions {
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
func (l *Logical) handlePause(state *QueueState, r *QueueRequest) {
	pr := r.Request.(*types.PauseRequest)

	if !pr.Pause {
		close(r.ReadyCh)
		return
	}
	close(r.ReadyCh)

	l.conf.Log.Debug("paused", "logical", l.instanceID)
	defer l.conf.Log.Debug("un-paused", "logical", l.instanceID)

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
	l.stateCleanUp(state)

	// Cancel any open reservations
	for _, r := range state.Reservations.Requests {
		r.Err = ErrQueueShutdown
		close(r.ReadyCh)
	}

	// Consume all requests currently in flight
	for l.inFlight.Load() != 0 {
		select {
		case r := <-l.produceQueueCh:
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		case r := <-l.reserveQueueCh:
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		case r := <-l.completeQueueCh:
			r.Err = ErrQueueShutdown
			close(r.ReadyCh)
		case <-l.conf.Clock.After(100 * clock.Millisecond):
			// all time for the closed requests handlers to exit
		case <-req.Context.Done():
			req.Err = req.Context.Err()
			return
		}
	}

	for _, p := range l.conf.Partitions {
		l.conf.Log.LogAttrs(req.Context, slog.LevelDebug, "close partition",
			slog.Int("partition", p.Info().Partition))
		if err := l.conf.Partitions[0].Close(req.Context); err != nil {
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
