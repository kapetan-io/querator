package internal

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/kapetan-io/tackle/retry"
	"github.com/kapetan-io/tackle/set"
	"github.com/kapetan-io/tiger/assert"
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
	MethodProduceInternal

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
	ErrQueueShutdown  = reply.NewRequestFailed(MsgQueueInShutdown)
	ErrRequestTimeout = reply.NewRetryRequest(MsgRequestTimeout)
	ErrInternalRetry  = reply.NewRetryRequest("internal error, try your request again")

	lifecycleBackOff = retry.IntervalBackOff{
		Min:    500 * time.Millisecond,
		Max:    5 * time.Second,
		Factor: 1.5,
		Jitter: 0.2,
	}
)

const lifecycleBackOffAttemptLimit = 32

type LogicalConfig struct {
	types.QueueInfo

	Log                  *slog.Logger
	WriteTimeout         clock.Duration
	ReadTimeout          clock.Duration
	MaxLeaseBatchSize    int
	MaxProduceBatchSize  int
	MaxCompleteBatchSize int
	MaxRequestsPerQueue  int
	Clock                *clock.Provider
	StoragePartitions    []store.Partition
	Manager              *QueuesManager
}

type Logical struct {
	requestCh requestInbox

	shutdownCh   shutdownRequests
	wg           sync.WaitGroup
	conf         LogicalConfig
	log          *slog.Logger
	inFlight     atomic.Int32
	inFlightCold atomic.Int32
	inShutdown   atomic.Bool
	instanceID   string
}

func SpawnLogicalQueue(conf LogicalConfig) (*Logical, error) {
	set.Default(&conf.Log, slog.Default())
	set.Default(&conf.MaxLeaseBatchSize, DefaultMaxLeaseBatchSize)
	set.Default(&conf.MaxProduceBatchSize, DefaultMaxProduceBatchSize)
	set.Default(&conf.MaxCompleteBatchSize, DefaultMaxCompleteBatchSize)
	set.Default(&conf.MaxRequestsPerQueue, DefaultMaxRequestsPerQueue)
	set.Default(&conf.Clock, clock.NewProvider())

	l := &Logical{
		shutdownCh: make(shutdownRequests),
		requestCh:  make(requestInbox, conf.MaxRequestsPerQueue),
		instanceID: random.Alpha("", 10),
		conf:       conf,
	}
	l.log = conf.Log.With("code.namespace", "Logical", "queue", conf.Name, "instance-id", l.instanceID)

	l.log.LogAttrs(context.Background(), LevelDebugAll, "logical queue started")
	l.wg.Go(l.requestLoop)
	return l, nil
}

func (l *Logical) Produce(ctx context.Context, req *types.ProduceRequest) error {
	l.inFlight.Add(1)
	defer l.inFlight.Add(-1)
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if req.RequestTimeout == clock.Duration(0) {
		return reply.NewInvalidOption("request timeout is required; '5m' is recommended, 15m is the maximum")
	}

	if req.RequestTimeout >= maxRequestTimeout {
		return reply.NewInvalidOption("request timeout is invalid; maximum timeout is '15m' but"+
			" '%s' was requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout <= minRequestTimeout {
		return reply.NewInvalidOption("request timeout is invalid; minimum timeout is '10ms' but"+
			" '%s' was requested", req.RequestTimeout.String())
	}

	if len(req.Items) == 0 {
		return reply.NewInvalidOption("items cannot be empty; at least one item is required")
	}

	if len(req.Items) > l.conf.MaxProduceBatchSize {
		return reply.NewInvalidOption("items is invalid; max_produce_batch_size is"+
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
		return reply.NewRetryRequest(MsgQueueOverLoaded)
	}

	select {
	case <-req.ReadyCh:
		return req.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Logical) Lease(ctx context.Context, req *types.LeaseRequest) error {
	l.inFlight.Add(1)
	defer l.inFlight.Add(-1)
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if strings.TrimSpace(req.ClientID) == "" {
		return reply.NewInvalidOption("invalid client id; cannot be empty")
	}

	if req.NumRequested <= 0 {
		return reply.NewInvalidOption("invalid batch size; must be greater than zero")
	}

	if req.NumRequested > l.conf.MaxLeaseBatchSize {
		return reply.NewInvalidOption("invalid batch size; max_lease_batch_size is %d, "+
			"but %d was requested", l.conf.MaxLeaseBatchSize, req.NumRequested)
	}

	if req.RequestTimeout == clock.Duration(0) {
		return reply.NewInvalidOption("request timeout is required; '5m' is recommended, 15m is the maximum")
	}

	if req.RequestTimeout > maxRequestTimeout {
		return reply.NewInvalidOption("request timeout is invalid; maximum timeout is '15m' but '%s' "+
			"was requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout <= minRequestTimeout {
		return reply.NewInvalidOption("request timeout is invalid; minimum timeout is '10ms' but"+
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
		return reply.NewRetryRequest(MsgQueueOverLoaded)
	}

	select {
	case <-req.ReadyCh:
		return req.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Logical) Complete(ctx context.Context, req *types.CompleteRequest) error {
	l.inFlight.Add(1)
	defer l.inFlight.Add(-1)
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if len(req.Ids) == 0 {
		return reply.NewInvalidOption("ids is invalid; list of ids cannot be empty")
	}

	if len(req.Ids) > l.conf.MaxCompleteBatchSize {
		return reply.NewInvalidOption("ids is invalid; max_complete_batch_size is"+
			" %d but received %d", l.conf.MaxCompleteBatchSize, len(req.Ids))
	}

	if req.RequestTimeout > maxRequestTimeout {
		return reply.NewInvalidOption("request timeout is invalid; maximum timeout is '15m' but '%s' "+
			"was requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout == clock.Duration(0) {
		return reply.NewInvalidOption("request timeout is required; '5m' is recommended, 15m is the maximum")
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
		return reply.NewRetryRequest(MsgQueueOverLoaded)
	}

	select {
	case <-req.ReadyCh:
		return req.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Logical) Retry(ctx context.Context, req *types.RetryRequest) error {
	l.inFlight.Add(1)
	defer l.inFlight.Add(-1)
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if len(req.Items) == 0 {
		return reply.NewInvalidOption("items is invalid; list of items cannot be empty")
	}

	if len(req.Items) > l.conf.MaxCompleteBatchSize {
		return reply.NewInvalidOption("items is invalid; max_complete_batch_size is"+
			" %d but received %d", l.conf.MaxCompleteBatchSize, len(req.Items))
	}

	if req.RequestTimeout > maxRequestTimeout {
		return reply.NewInvalidOption("request timeout is invalid; maximum timeout is '15m' but '%s' "+
			"was requested", req.RequestTimeout.String())
	}

	if req.RequestTimeout == clock.Duration(0) {
		return reply.NewInvalidOption("request timeout is required; '5m' is recommended, 15m is the maximum")
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
		return reply.NewRetryRequest(MsgQueueOverLoaded)
	}

	select {
	case <-req.ReadyCh:
		return req.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Logical) ProduceDeadLetter(ctx context.Context, req *types.ProduceRequest) error {
	l.inFlight.Add(1)
	defer l.inFlight.Add(-1)
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if len(req.Items) == 0 {
		return nil
	}

	req.ReadyCh = make(chan struct{})
	req.Context = ctx
	req.DeadLetterHandOff = true

	select {
	case l.requestCh <- &Request{
		Method:  MethodProduceInternal,
		Request: req,
	}:
	default:
		return reply.NewRetryRequest(MsgQueueOverLoaded)
	}

	select {
	case <-req.ReadyCh:
		return req.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

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

func (l *Logical) Pause(ctx context.Context, req *types.PauseRequest) error {
	r := Request{
		Method:  MethodQueuePause,
		Request: req,
	}
	return l.queueRequest(ctx, &r)
}

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

func (l *Logical) Clear(ctx context.Context, req *types.ClearRequest) error {
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	if !req.Queue && !req.Scheduled {
		return reply.NewInvalidOption("invalid clear request; one of 'queue'," +
			" 'scheduled' must be true")
	}

	r := Request{
		Method:  MethodQueueClear,
		Request: req,
	}
	return l.queueRequest(ctx, &r)
}

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

func (l *Logical) UpdatePartitions(ctx context.Context, p []store.Partition) error {
	r := Request{
		Method:  MethodUpdatePartitions,
		Request: p,
	}
	return l.queueRequest(ctx, &r)
}

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
		return reply.NewInvalidOption("ids is invalid; cannot be empty")
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

	select {
	case l.shutdownCh <- req:
		l.wg.Wait()
		return req.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

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

	state.Partitions = make([]*Partition, len(l.conf.StoragePartitions))

	for i, p := range l.conf.StoragePartitions {
		partition := &Partition{
			Info:  p.Info(),
			Store: p,
			Lifecycle: PartitionLifecycleState{
				NextLifecycleRun: now,
				NextScheduledRun: now,
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

	l.runScheduled(state)
	state.ScheduledTimer = l.conf.Clock.NewTimer(l.minNextScheduledRun(state))

	sortPartitionsByLoad(state.Partitions)

}

type QueueState struct {
	LifeCycles        types.LifeCycleBatch
	Completes         types.CompleteBatch
	Retries           types.RetryBatch
	Producers         types.ProduceBatch
	Leases            types.LeaseBatch
	NextMaintenanceCh requestTimeoutAlarm
	LifecycleTimer    clock.Timer
	ScheduledTimer    clock.Timer
	Partitions        []*Partition
	Paused            bool
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
			slog.Bool("Paused", state.Paused),
		)

		lifecycleCh, scheduledCh, maintenanceCh := state.LifecycleTimer.C(), state.ScheduledTimer.C(), state.NextMaintenanceCh
		if state.Paused {
			lifecycleCh, scheduledCh, maintenanceCh = nil, nil, nil
		}

		if !state.Paused && l.timedRunDue(&state) {
			for range len(l.requestCh) {
				select {
				case req := <-l.requestCh:
					l.handleRequest(&state, req)
				default:
				}
			}
			select {
			case req := <-l.shutdownCh:
				l.handleShutdown(&state, req)
				return
			case <-maintenanceCh:
				l.handleRequestTimeouts(&state)
			default:
			}
			if !state.Paused {
				l.runLifecycle(&state)
				l.runScheduled(&state)
				l.leaseToWaitingConsumers(&state)
			}
			state.LifecycleTimer.Reset(l.minNextLifecycleRun(&state))
			state.ScheduledTimer.Reset(l.minNextScheduledRun(&state))
			continue
		}

		select {
		case req := <-l.requestCh:
			l.handleRequest(&state, req)

		case req := <-l.shutdownCh:
			l.handleShutdown(&state, req)
			return

		case <-lifecycleCh:
			l.log.LogAttrs(context.Background(), LevelDebugAll, "lifecycle timer fired")
			l.runLifecycle(&state)
			l.leaseToWaitingConsumers(&state)
			state.LifecycleTimer.Reset(l.minNextLifecycleRun(&state))

		case <-scheduledCh:
			l.log.LogAttrs(context.Background(), LevelDebugAll, "scheduled timer fired")
			l.runScheduled(&state)
			l.leaseToWaitingConsumers(&state)
			state.ScheduledTimer.Reset(l.minNextScheduledRun(&state))
			state.LifecycleTimer.Reset(l.minNextLifecycleRun(&state))

		case <-maintenanceCh:
			l.handleRequestTimeouts(&state)
		}
	}
}

func (l *Logical) leaseToWaitingConsumers(state *QueueState) {
	if len(state.Leases.Requests) == 0 {
		return
	}
	l.expireProduceRequests(&state.Producers)
	l.handleHotRequests(state, nil)
}

func (l *Logical) handleRequest(state *QueueState, req *Request) {
	switch req.Method {
	case MethodProduce, MethodProduceInternal, MethodLease, MethodComplete, MethodRetry:
		if state.Paused {
			state.Producers.DropAbandonedHandOffs()
			l.reqToState(req, state)
			return
		}
		if cold := l.handleHotRequests(state, req); cold != nil {
			l.handleColdRequests(state, cold)
		}
	case MethodStorageItemsList, MethodStorageItemsImport, MethodStorageItemsDelete,
		MethodQueueStats, MethodQueuePause, MethodQueueClear, MethodUpdateInfo,
		MethodUpdatePartitions, MethodReload:
		l.handleColdRequests(state, req)
	default:
		assert.Unreachable(fmt.Sprintf("undefined request method '%d'", req.Method))
	}
}

func (l *Logical) handleRequestTimeouts(state *QueueState) {
	next := l.nextTimeout(&state.Leases)
	if produce := l.expireProduceRequests(&state.Producers); produce != 0 {
		if next == 0 || produce < next {
			next = produce
		}
	}
	if next.Nanoseconds() != 0 {
		l.log.LogAttrs(context.Background(), LevelDebug, "next maintenance",
			slog.String("duration", next.String()))
		state.NextMaintenanceCh = l.conf.Clock.After(next)
	}
}

func (l *Logical) handleHotRequests(state *QueueState, req *Request) *Request {
	var cold *Request
	if req != nil {
		cold = l.consumeHotCh(state, req)
		l.expireProduceRequests(&state.Producers)
	}

	l.assignToPartitions(state)

	l.applyToPartitions(state)

	l.finalizeRequests(state)

	lifecycleUpdated := false
	for _, p := range state.Partitions {
		if p.State.NumLeased != 0 {
			now := l.conf.Clock.Now().UTC()
			if p.State.MostRecentDeadline.After(now) && p.Lifecycle.RunLifecycleBy(p.State.MostRecentDeadline) {
				lifecycleUpdated = true
			}
		}
		p.Reset()
	}

	if lifecycleUpdated {
		state.LifecycleTimer.Reset(l.minNextLifecycleRun(state))
	}

	state.LifeCycles.Reset()
	state.Leases.FilterNils()
	state.Producers.Reset()
	state.Completes.Reset()
	state.Retries.Reset()

	l.handleRequestTimeouts(state)
	return cold
}

func (l *Logical) isHotRequest(req *Request) bool {
	switch req.Method {
	case MethodProduce, MethodProduceInternal, MethodLease, MethodComplete, MethodRetry:
		return true
	case MethodStorageItemsList, MethodStorageItemsImport, MethodStorageItemsDelete,
		MethodQueueStats, MethodQueuePause, MethodQueueClear, MethodUpdateInfo,
		MethodUpdatePartitions, MethodReload:
		return false
	default:
		assert.Unreachable(fmt.Sprintf("undefined request method '%d'", req.Method))
	}
	return false
}

func (l *Logical) consumeHotCh(state *QueueState, req *Request) *Request {
	l.reqToState(req, state)

	for range len(l.requestCh) {
		select {
		case next := <-l.requestCh:
			if !l.isHotRequest(next) {
				return next
			}
			l.reqToState(next, state)
		default:
			return nil
		}
	}
	return nil
}

func (l *Logical) reqToState(req *Request, state *QueueState) {

	switch req.Method {
	case MethodProduce, MethodProduceInternal:
		state.Producers.Add(req.Request.(*types.ProduceRequest))
	case MethodComplete:
		state.Completes.Add(req.Request.(*types.CompleteRequest))
	case MethodRetry:
		state.Retries.Add(req.Request.(*types.RetryRequest))
	case MethodLease:
		addIfUnique(&state.Leases, req.Request.(*types.LeaseRequest))
	default:
		assert.Unreachable(fmt.Sprintf("undefined request method '%d'", req.Method))
	}
}

func (l *Logical) queueRequest(ctx context.Context, r *Request) error {
	l.inFlightCold.Add(1)
	defer l.inFlightCold.Add(-1)
	if l.inShutdown.Load() {
		return ErrQueueShutdown
	}

	r.ReadyCh = make(readySignal)
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

func (l *Logical) nextTimeout(r *types.LeaseBatch) clock.Duration {
	var soon *types.LeaseRequest

	for i, req := range r.Requests {
		if req == nil {
			continue
		}

		reqExpired := l.conf.Clock.Now().UTC().After(req.RequestDeadline)
		clientGone := req.Context.Err() != nil

		if reqExpired || clientGone {
			if reqExpired {
				req.Err = ErrRequestTimeout
			} else {
				req.Err = req.Context.Err()
			}
			close(req.ReadyCh)
			r.MarkNil(i)
			continue
		}

		if soon == nil {
			soon = req
			continue
		}

		if req.RequestDeadline.Before(soon.RequestDeadline) {
			soon = req
		}
	}

	if soon == nil {
		return clock.Duration(0)
	}

	return soon.RequestDeadline.Sub(l.conf.Clock.Now().UTC())
}

func (l *Logical) expireProduceRequests(r *types.ProduceBatch) clock.Duration {
	now := l.conf.Clock.Now().UTC()
	var soon clock.Time
	n := 0

	for _, req := range r.Requests {
		if req.DeadLetterHandOff {
			r.Requests[n] = req
			n++
			continue
		}

		if !now.Before(req.RequestDeadline) {
			req.Err = ErrInternalRetry
			close(req.ReadyCh)
			continue
		}
		if req.Context.Err() != nil {
			req.Err = req.Context.Err()
			close(req.ReadyCh)
			continue
		}

		if soon.IsZero() || req.RequestDeadline.Before(soon) {
			soon = req.RequestDeadline
		}
		r.Requests[n] = req
		n++
	}
	r.Requests = r.Requests[:n]

	if soon.IsZero() {
		return clock.Duration(0)
	}
	return soon.Sub(now)
}

func addIfUnique(r *types.LeaseBatch, req *types.LeaseRequest) {
	if req == nil {
		return
	}
	for _, existing := range r.Requests {
		if existing != nil && existing.ClientID == req.ClientID {
			req.Err = reply.NewInvalidOption(MsgDuplicateClientID)
			close(req.ReadyCh)
			return
		}
	}
	r.TotalRequested += req.NumRequested
	r.Requests = append(r.Requests, req)
}
