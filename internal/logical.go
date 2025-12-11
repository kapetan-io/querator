package internal

import (
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/kapetan-io/tackle/retry"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	MethodLease
	MethodComplete
	MethodRetry
	MethodReload
	MethodNotify
	MethodNotifyScheduled

	DefaultMaxLeaseBatchSize        = 1_000
	DefaultMaxProduceBatchSize      = 1_000
	DefaultMaxCompleteBatchSize     = 1_000
	DefaultMaxRequestsPerQueue      = 100
	DefaultMaxConcurrentConnections = 1_000

	MsgRequestTimeout    = "request timeout; no items are in the queue, try again"
	MsgDuplicateClientID = "duplicate client id; a client cannot make multiple leased requests to the same queue"
	MsgQueueInShutdown   = "queue is shutting down"
	MsgQueueOverLoaded   = "queue is overloaded; try again later"
)

var (
	ErrQueueShutdown  = transport.NewRequestFailed(MsgQueueInShutdown)
	ErrRequestTimeout = transport.NewRetryRequest(MsgRequestTimeout)
	ErrInternalRetry  = transport.NewRetryRequest("internal error, try your request again")

	lifecycleBackOff = retry.IntervalBackOff{
		Min:    500 * time.Millisecond,
		Max:    5 * time.Second,
		Factor: 1.5,
		Jitter: 0.2,
	}
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
	// MaxLeaseBatchSize is the maximum number of items a client can request in a single lease request
	MaxLeaseBatchSize int
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
	// Manager is the QueuesManager used by lifecycle processing to move items to a dead letter queue
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

	requestCh chan *Request

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
	set.Default(&conf.MaxLeaseBatchSize, DefaultMaxLeaseBatchSize)
	set.Default(&conf.MaxProduceBatchSize, DefaultMaxProduceBatchSize)
	set.Default(&conf.MaxCompleteBatchSize, DefaultMaxCompleteBatchSize)
	set.Default(&conf.MaxRequestsPerQueue, DefaultMaxRequestsPerQueue)
	set.Default(&conf.Clock, clock.NewProvider())

	l := &Logical{
		shutdownCh: make(chan *types.ShutdownRequest),
		requestCh:  make(chan *Request, conf.MaxRequestsPerQueue),
		instanceID: random.Alpha("", 10),
		conf:       conf,
	}
	l.log = conf.Log.With("code.namespace", "Logical", "queue", conf.Name, "instance-id", l.instanceID)

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

	req.RequestDeadline = l.conf.Clock.Now().UTC().Add(req.RequestTimeout)
	req.ReadyCh = make(chan struct{})
	req.Context = ctx

	select {
	case l.requestCh <- &Request{
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

// Lease is called by clients wanting to lease a new item from the queue. This call
// will block until the request is cancelled via the passed context or the RequestTimeout
// is reached.
//
// # Context Cancellation
// IT IS NOT recommend to cancel wit context.WithTimeout() or context.WithDeadline() on Lease() since
// Lease() will block until the duration provided via LeaseRequest.RequestTimeout has been reached.
// Callers SHOULD cancel the context if the client has gone away, in this case Logical will abort the lease
// request. If the context is cancelled after lease has been written to the data store
// then those leases will remain leased until they can be offered to another client after the
// LeaseDeadline has been reached. See docs/adr/0009-client-timeouts.md
//
// # Unique Requests
// ClientID must NOT be empty and each request must be unique, Non-unique requests will be rejected with
// MsgDuplicateClientID. See docs/adr/0007-encourage-simple-clients.md for an explanation.
func (l *Logical) Lease(ctx context.Context, req *types.LeaseRequest) error {
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

	if req.NumRequested > l.conf.MaxLeaseBatchSize {
		return transport.NewInvalidOption("invalid batch size; max_lease_batch_size is %d, "+
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
	case l.requestCh <- &Request{
		Method:  MethodLease,
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
	case l.requestCh <- &Request{
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

func (l *Logical) Retry(ctx context.Context, req *types.RetryRequest) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}
	l.inFlight.Add(1)
	defer l.inFlight.Add(-1)

	if len(req.Items) == 0 {
		return transport.NewInvalidOption("items is invalid; list of items cannot be empty")
	}

	if len(req.Items) > l.conf.MaxCompleteBatchSize {
		return transport.NewInvalidOption("items is invalid; max_complete_batch_size is"+
			" %d but received %d", l.conf.MaxCompleteBatchSize, len(req.Items))
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
	case l.requestCh <- &Request{
		Method:  MethodRetry,
		Request: req,
	}:
	default:
		return transport.NewRetryRequest(MsgQueueOverLoaded)
	}

	// Wait until the request has been processed
	<-req.ReadyCh
	return req.Err
}

// ProduceInternal writes items directly to storage without going through the request channel.
// This method is used internally by the QueuesManager for DLQ item movement.
// It bypasses client validation and writes directly to the selected partition.
func (l *Logical) ProduceInternal(ctx context.Context, items []*types.Item) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if len(items) == 0 {
		return nil
	}

	// Use first partition for DLQ writes (simple approach)
	if len(l.conf.StoragePartitions) == 0 {
		return errors.New("no storage partitions available")
	}

	partition := l.conf.StoragePartitions[0]

	// Assign ExpireTimeout to each item
	now := l.conf.Clock.Now().UTC()
	for _, item := range items {
		item.ExpireDeadline = now.Add(l.conf.ExpireTimeout)
	}

	// Create a produce batch for storage
	batch := types.ProduceBatch{}
	batch.Add(&types.ProduceRequest{
		Items: items,
	})

	// Write directly to storage
	if err := partition.Produce(ctx, batch, now); err != nil {
		return errors.Errorf("failed to produce items to partition: %w", err)
	}

	return nil
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

// Pause pauses processing of produce, lease, complete and retry operations until the pause is cancelled.
// It is only used for testing and not exposed to the user via API, as such it is not considered apart of
// the public API.
func (l *Logical) Pause(ctx context.Context, req *types.PauseRequest) error {
	r := Request{
		Method:  MethodQueuePause,
		Request: req,
	}
	return l.queueRequest(ctx, &r)
}

// ReloadPartitions reloads all partitions from storage. Queue operation
// is paused while reload completes.
func (l *Logical) ReloadPartitions(ctx context.Context, req *types.ReloadRequest) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	r := Request{
		Method:  MethodReload,
		Request: req,
	}
	return l.queueRequest(ctx, &r)
}

// Clear clears enqueued items from the queue
func (l *Logical) Clear(ctx context.Context, req *types.ClearRequest) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if !req.Queue && !req.Retry && !req.Scheduled {
		return transport.NewInvalidOption("invalid clear request; one of 'queue', 'retry'," +
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
// It is called during initialization of Logical struct.
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

func (l *Logical) StorageItemsList(ctx context.Context, req StorageRequest) error {

	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}
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

// prepareQueueState is called whenever partition assignment changes
func (l *Logical) prepareQueueState(state *QueueState) {
	now := l.conf.Clock.Now().UTC()

	if state.Producers.Requests == nil {
		state.Producers.Requests = make([]*types.ProduceRequest, 0, 5_000)
	}

	if state.Leases.Requests == nil {
		state.Leases.Requests = make([]*types.LeaseRequest, 0, 5_000)
	}

	if state.Completes.Requests == nil {
		state.Completes.Requests = make([]*types.CompleteRequest, 0, 5_000)
	}

	// TODO: Validate each partition hasn't been modified since last loaded by a Logical queue.
	//  we do this by saving he last item id in the partition into QueueInfo.StoragePartitions along with
	//  the the QueueInfo.StoragePartitions.UnLeased of items in the partition. When loading the partition,
	//  If the last item id saved in QueueInfo.StoragePartitions agrees with what is in the actual partition,
	//  we should trust that the partition was not modified and the count saved in
	//  QueueInfo.StoragePartitions.UnLeased is accurate. If it's not accurate, we MUST re-count all the items
	//  in the partition in order to accurately distribute items.

	state.Partitions = make([]*Partition, len(l.conf.StoragePartitions))

	for i, p := range l.conf.StoragePartitions {
		partition := &Partition{
			Info:  p.Info(),
			Store: p,
			Lifecycle: PartitionLifecycleState{
				NextLifecycleRun: now,
				NextScheduledRun: now.Add(humanize.LongTime),
				Failures:         0,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), l.conf.ReadTimeout)
		var stats types.PartitionStats
		err := p.Stats(ctx, &stats, now)
		cancel()

		if err != nil {
			l.log.Warn("partition unavailable; recovery will retry",
				"partition", p.Info().PartitionNum,
				"error", err)
			partition.State.Failures = 1
			partition.Lifecycle.Failures = 1
		} else {
			partition.State.Failures = 0
			partition.State.UnLeased = stats.Total - stats.NumLeased
			partition.State.NumLeased = stats.NumLeased
		}

		state.Partitions[i] = partition
	}

	state.LifecycleTimer = l.conf.Clock.NewTimer(0)
	state.ScheduledTimer = l.conf.Clock.NewTimer(humanize.LongTime)

	sortPartitionsByLoad(state.Partitions)

}

// -------------------------------------------------
// Main Loop and Handlers
// See docs/adr/0003-rw-sync-point.md for an explanation of this design
// -------------------------------------------------

type QueueState struct {
	LifeCycles        types.LifeCycleBatch
	Completes         types.CompleteBatch
	Retries           types.RetryBatch
	Producers         types.ProduceBatch
	Leases            types.LeaseBatch
	NextMaintenanceCh <-chan clock.Time
	LifecycleTimer    clock.Timer
	ScheduledTimer    clock.Timer
	Partitions        []*Partition
}

func (q *QueueState) GetPartition(num int) *Partition {
	for _, p := range q.Partitions {
		if p.Info.PartitionNum == num {
			return p
		}
	}
	return nil
}

func (l *Logical) requestLoop() {
	defer l.wg.Done()
	var state QueueState

	l.prepareQueueState(&state)

	for {
		l.log.LogAttrs(context.Background(), LevelDebugAll, "Logical.requestLoop()",
			slog.Int("Requests", len(l.requestCh)),
			slog.Int("Leases", len(state.Leases.Requests)),
			slog.Int("Producers", len(state.Producers.Requests)),
			slog.Int("Completes", len(state.Completes.Requests)),
			slog.Int("LifeCycles", len(state.LifeCycles.Requests)),
			slog.Int("InFlight", int(l.inFlight.Load())),
		)

		select {
		case req := <-l.requestCh:
			l.handleRequest(&state, req)
			if l.inShutdown.Load() {
				return
			}

		case req := <-l.shutdownCh:
			l.handleShutdown(&state, req)
			return

		case <-state.LifecycleTimer.C():
			l.log.LogAttrs(context.Background(), LevelDebugAll, "lifecycle timer fired")
			l.runLifecycle(&state)
			state.LifecycleTimer.Reset(l.minNextLifecycleRun(&state))

		case <-state.ScheduledTimer.C():
			l.log.LogAttrs(context.Background(), LevelDebugAll, "scheduled timer fired")
			l.runScheduled(&state)
			state.ScheduledTimer.Reset(l.minNextScheduledRun(&state))

		case <-state.NextMaintenanceCh:
			l.handleRequestTimeouts(&state)
		}
	}
}

func (l *Logical) handleRequest(state *QueueState, req *Request) {
	switch req.Method {
	case MethodProduce, MethodLease, MethodComplete, MethodRetry:
		l.handleHotRequests(state, req)
	case MethodStorageItemsList, MethodStorageItemsImport, MethodStorageItemsDelete,
		MethodQueueStats, MethodQueuePause, MethodQueueClear, MethodUpdateInfo,
		MethodUpdatePartitions, MethodReload:
		l.handleColdRequests(state, req)
	default:
		panic(fmt.Sprintf("undefined request method '%d'", req.Method))
	}
}

func (l *Logical) handleRequestTimeouts(state *QueueState) {
	// TODO: Consider state.nextDeadLine which should be the next time a lease will expire
	next := l.nextTimeout(&state.Leases)
	if next.Nanoseconds() != 0 {
		l.log.LogAttrs(context.Background(), LevelDebug, "next maintenance",
			slog.String("duration", next.String()))
		state.NextMaintenanceCh = l.conf.Clock.After(next)
	}
}

func (l *Logical) handleHotRequests(state *QueueState, req *Request) {
	// If request is empty, do not consume more requests, the caller only wants us
	// to process requests currently waiting in 'state'
	if req != nil {
		// Consume all requests from the hot channel, adding the requests to
		// the client request state.
		l.consumeHotCh(state, req)
	}

	// Assign all the client requests in 'state' to appropriate partitions
	// according to request type.
	l.assignToPartitions(state)

	// Apply the requests to each partition according their assignment.
	l.applyToPartitions(state)

	// TODO: If an error occurred with one of the partitions and it's recoverable,
	//  then attempt to re-distribute un-applied requests to other partitions, else
	//  we let the client request remain until it times out.

	// TODO: Some time may have passed while talking to the partition, due to
	//  storage degradation, ensure we handle timed out clients.

	// If the request was fulfilled, Finalize the requests, else leave the
	// request in the state until our next run
	l.finalizeRequests(state)

	// Reset the partition assignments
	lifecycleUpdated := false
	for _, p := range state.Partitions {
		if p.State.NumLeased != 0 {
			// MostRecentDeadline is updated when we apply leases to our
			// partitions. Update the partition lifecycle state so it knows
			// when a lease is likely to expire next.
			now := l.conf.Clock.Now().UTC()
			if p.State.MostRecentDeadline.After(now) && p.State.MostRecentDeadline.Before(p.Lifecycle.NextLifecycleRun) {
				p.Lifecycle.NextLifecycleRun = p.State.MostRecentDeadline
				lifecycleUpdated = true
			}
		}
		p.Reset()
	}

	// Reset lifecycle timer if any partition's NextLifecycleRun was updated
	if lifecycleUpdated {
		state.LifecycleTimer.Reset(l.minNextLifecycleRun(state))
	}

	state.LifeCycles.Reset()
	state.Leases.FilterNils()
	state.Producers.Reset()
	state.Completes.Reset()
	state.Retries.Reset()

	// Handle any request timeouts and update our next run timer
	l.handleRequestTimeouts(state)
}

func (l *Logical) isHotRequest(req *Request) bool {
	switch req.Method {
	case MethodProduce, MethodLease, MethodComplete, MethodRetry:
		return true
	default:
		return false
	}
}


func (l *Logical) consumeHotCh(state *QueueState, req *Request) {
	// Add the first request from the channel to the client request state
	l.reqToState(req, state)

EMPTY:
	// Add any hot requests in the requestCh to the client requests state
	for {
		select {
		case req := <-l.requestCh:
			// Only consume hot requests (produce, lease, complete, retry, lifecycle)
			if l.isHotRequest(req) {
				l.reqToState(req, state)
			} else {
				// Put cold request back on channel for later processing
				select {
				case l.requestCh <- req:
				default:
					// Channel full, handle it now
					l.handleColdRequests(state, req)
				}
				break EMPTY
			}
		default:
			break EMPTY
		}
	}
}

func (l *Logical) reqToState(req *Request, state *QueueState) {
	// TODO: Check for request timeout, it's possible the request has been sitting in
	//  the channel for a while

	// TODO(thrawn01): Ensure we don't go beyond our max number of state.Producers, state.Completes, etc..
	//  If we do then we must inform the client that we are overloaded, and they must try again. We
	//  cannot hold on to a request and let it sit in the channel, else we break the
	//  request_timeout contract we have with the client.

	switch req.Method {
	case MethodProduce:
		state.Producers.Add(req.Request.(*types.ProduceRequest))
	case MethodComplete:
		state.Completes.Add(req.Request.(*types.CompleteRequest))
	case MethodRetry:
		state.Retries.Add(req.Request.(*types.RetryRequest))
	case MethodLease:
		addIfUnique(&state.Leases, req.Request.(*types.LeaseRequest))
	default:
		panic(fmt.Sprintf("undefined request method '%d'", req.Method))
	}
}


// -------------------------------------------------
// Handlers for the main and pause loops
// -------------------------------------------------

func (l *Logical) queueRequest(ctx context.Context, r *Request) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	r.ReadyCh = make(chan struct{})
	r.Context = ctx

	select {
	case l.requestCh <- r:
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

// TODO: I don't think we should pass by ref. We should use escape analysis to decide
func (l *Logical) nextTimeout(r *types.LeaseBatch) clock.Duration {
	var soon *types.LeaseRequest

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

// Useful for debugging, do not remove thrawn(2025-03-31)
// func (l *Logical) dumpPartitionOrder(state *QueueState) string {
// 	out := strings.Builder{}
// 	out.WriteString("Partition Order:\n")
// 	for _, req := range state.Partitions {
// 		fmt.Fprintf(&out, "  %02d - UnLeased: %d NumLeased: %d Failures: %d\n",
// 			req.Info.PartitionNum, req.State.UnLeased, req.State.NumLeased,
// 			req.State.Failures)
// 	}
// 	return out.String()
// }

// addIfUnique adds a LeaseRequest to the batch. Returns false if the LeaseRequest.ClientID is a duplicate
// and the request was not added to the batch
func addIfUnique(r *types.LeaseBatch, req *types.LeaseRequest) {
	if req == nil {
		return
	}
	for _, existing := range r.Requests {
		if existing != nil && existing.ClientID == req.ClientID {
			req.Err = transport.NewInvalidOption(MsgDuplicateClientID)
			close(req.ReadyCh)
			return
		}
	}
	r.TotalRequested += req.NumRequested
	r.Requests = append(r.Requests, req)
}
