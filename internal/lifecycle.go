package internal

import (
	"context"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
	"log/slog"
	"sync"
	"sync/atomic"
)

const (
	Years = (24 * clock.Hour) * 365
)

var (
	ErrLifeCycleRunning  = errors.New("life cycle is already running")
	ErrLifeCycleShutdown = errors.New("life cycle is shutdown")
)

type LifeCycleConfig struct {
	Distribution *PartitionDistribution
	Storage      store.Partition
	Clock        clock.Provider
	Logical      *Logical
}

// LifeCycle manages the life cycle of items in a partition.
// A life cycle:
// - Moves scheduled items into the queue
// - Handles reserved items that are past their completion deadline
// - Moves expired items out of the queue into the dead letter
// - Decides if a partition is available or not
// - Sets the initial partition counts if none exist
type LifeCycle struct {
	conf       LifeCycleConfig
	wg         sync.WaitGroup
	manager    *QueuesManager
	runCh      chan Request
	notifyCh   chan Request
	shutdownCh chan Request
	log        *slog.Logger
	running    atomic.Bool
	logical    *Logical
}

func NewLifeCycle(conf LifeCycleConfig) *LifeCycle {
	return &LifeCycle{
		log: conf.Logical.conf.Log.With(errors.OtelCodeNamespace, "LifeCycle").
			With("instance-id", conf.Logical.instanceID).
			With("queue", conf.Logical.conf.Name),
		manager:    conf.Logical.conf.Manager,
		notifyCh:   make(chan Request),
		runCh:      make(chan Request),
		shutdownCh: make(chan Request),
		logical:    conf.Logical,
		conf:       conf,
	}
}

// Start starts the life cycle by running a read phase upon the partition storage.
// If successful, the life cycle marks the LifeCycle.Distribution as available.
// Start() will block until the read phase is complete, if the context is cancelled
// before the read phase is complete, Start() returns, but the read phase continues
// until completion and the LifeCycle.Distribution is marked as available.
func (l *LifeCycle) Start(ctx context.Context) error {
	if !l.running.CompareAndSwap(false, true) {
		return ErrLifeCycleRunning
	}

	l.wg.Add(1)
	go l.requestLoop()

	select {
	case <-l.start():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *LifeCycle) start() chan struct{} {
	for {
		// Continues to call Run() until the requestLoop() go routine
		// is up and running and accepting requests.
		if ch := l.Run(); ch != nil {
			return ch
		}
		clock.Sleep(clock.Microsecond)
	}
}

// Run runs the lifecycle immediately, returning a channel that when closed indicates the run has completed.
func (l *LifeCycle) Run() chan struct{} {
	req := Request{
		ReadyCh: make(chan struct{}),
	}
	select {
	case l.runCh <- req:
	default:
		return nil
	}
	return req.ReadyCh
}

// Notify notifies the life cycle that it needs to consider running at the provided time.
// The life cycle will decide if a previous notify or action supersedes this notification
// time.
func (l *LifeCycle) Notify(t clock.Time) {
	req := Request{
		Request: t,
	}
	select {
	case l.notifyCh <- req:
	default:
	}
}

func (l *LifeCycle) requestLoop() {
	defer l.wg.Done()
	state := lifeCycleState{
		timer: clock.NewTimer(100 * Years),
	}

	// nolint
	for {
		// NOTE: Separate channels avoids missing a shutdown or run cycle requests
		// in the case where we have a ton of notify requests.
		select {
		case req := <-l.runCh:
			l.runLifeCycle(&state)
			close(req.ReadyCh)
		case req := <-l.notifyCh:
			l.handleNotify(&state, req.Request.(clock.Time))
		case req := <-l.shutdownCh:
			close(req.ReadyCh)
			return
		case <-state.timer.C():
			l.runLifeCycle(&state)
		}
	}
}

func (l *LifeCycle) handleNotify(state *lifeCycleState, notify clock.Time) {
	now := clock.Now().UTC()
	// Ignore notify times from the past
	if now.After(notify) {
		return
	}

	// Ignore if we already have a life cycle run scheduled that is before
	// the notify time.
	if !state.runDeadline.IsZero() && notify.After(state.runDeadline) {
		return
	}
	state.runDeadline = notify
	state.timer.Reset(notify.Sub(now))
}

func (l *LifeCycle) runLifeCycle(state *lifeCycleState) {
	// TODO: Find the next reservation which will expire
	// TODO: Find the next scheduled item which needs to be moved over
	// TODO: Set l.nextRun or set to nil if no items

	var stats types.PartitionStats
	ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.ReadTimeout)
	defer cancel()

	if err := l.conf.Storage.Stats(ctx, &stats); err != nil {
		l.log.Warn("unable to query partition stats while preparing partition state; "+
			" items will not be produced to partition until it is available",
			"partition", l.conf.Storage.Info().Partition,
			"error", err)
	}

	l.log.LogAttrs(context.Background(), slog.LevelDebug, "partition available",
		slog.Int("partition", l.conf.Distribution.Partition.Info().Partition))
	l.conf.Distribution.Count = stats.Total - stats.TotalReserved
	l.conf.Distribution.Available.Store(true)
}

func (l *LifeCycle) Shutdown(ctx context.Context) error {
	if !l.running.Load() {
		return ErrLifeCycleShutdown
	}

	// Request shutdown
	req := Request{
		ReadyCh: make(chan struct{}),
	}

	// Queue shutdown request
	select {
	case l.shutdownCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Wait for the response
	select {
	case <-req.ReadyCh:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Ensure go routine was shutdown
	done := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type lifeCycleState struct {
	runDeadline clock.Time
	timer       clock.Timer
}
