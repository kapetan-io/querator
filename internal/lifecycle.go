package internal

import (
	"context"
	"fmt"
	"github.com/duh-rpc/duh-go/retry"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Years = (24 * clock.Hour) * 365
)

var (
	ErrLifeCycleRunning  = errors.New("life cycle is already running")
	ErrLifeCycleShutdown = errors.New("life cycle is already shutdown")
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
		log: conf.Logical.conf.Log.With(errors.OtelCodeNamespace, "LifeCycle",
			"partition", conf.Distribution.Partition.Info().PartitionNum,
			"instance-id", conf.Logical.instanceID,
			"queue", conf.Logical.conf.Name),
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

// Notify proposes a time for the life cycle to consider when determining the next execution cycle.
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
	if !state.nextRunDeadline.IsZero() && notify.After(state.nextRunDeadline) {
		return
	}
	state.nextRunDeadline = notify
	state.timer.Reset(notify.Sub(now))
}

func (l *LifeCycle) runLifeCycle(state *lifeCycleState) {
	// Attempt to get the partition out of a failed state
	if l.conf.Distribution.Failures.Load() != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.ReadTimeout)
		var stats types.PartitionStats
		defer cancel()

		if err := l.conf.Storage.Stats(ctx, &stats); err != nil {
			retryIn := retry.DefaultBackOff.Next(int(l.conf.Distribution.Failures.Add(1)))
			state.nextRunDeadline = clock.Now().UTC().Add(retryIn)
			l.log.Warn("while fetching partition stats; retrying...",
				"retry-in", retryIn,
				"error", err)
		} else {
			l.log.LogAttrs(context.Background(), slog.LevelDebug, "partition available",
				slog.Int("partition", l.conf.Distribution.Partition.Info().PartitionNum))
			l.conf.Distribution.Count = stats.Total - stats.TotalReserved
			l.conf.Distribution.Failures.Store(0)
		}
	}

	lActions := make([]types.Action, 0, 1_000)
	mActions := make([]types.Action, 0, 1_000)
	// Separate the events into separate slices handling them in batches
	for e := range l.conf.Storage.LifeCycleActions(l.logical.conf.ReadTimeout) {
		switch e.Action {
		case types.ActionMoveScheduledItem, types.ActionResetReserved:
			lActions = append(lActions, e)
			if len(lActions) >= 1_000 {
				l.handleLogicalActions(state, lActions)
				lActions = lActions[:]
			}
		case types.ActionDeadLetter:
			mActions = append(mActions, e)
			if len(mActions) >= 1_000 {
				l.handleManagerEvents(state, mActions)
				mActions = mActions[:]
			}
		default:
			panic(fmt.Sprintf("undefined action '%d'", e.Action))
		}
	}

	if len(lActions) > 0 {
		l.handleLogicalActions(state, lActions)
	}

	if len(mActions) > 0 {
		l.handleManagerEvents(state, mActions)
	}

	ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.ReadTimeout)
	var info types.LifeCycleInfo
	defer cancel()

	if err := l.conf.Storage.LifeCycleInfo(ctx, &info); err != nil {
		retryIn := retry.DefaultBackOff.Next(int(l.conf.Distribution.Failures.Add(1)))
		state.nextRunDeadline = clock.Now().UTC().Add(retryIn)
		l.log.Warn("while fetching life cycle info from partition; retrying...",
			"retry-in", retryIn,
			"error", err)
	}

	if info.NextReserveExpiry.Before(state.nextRunDeadline) {
		state.nextRunDeadline = info.NextReserveExpiry
	}
}

func (l *LifeCycle) handleLogicalActions(state *lifeCycleState, actions []types.Action) {
	r := types.LifeCycleRequest{
		RequestTimeout: l.logical.conf.WriteTimeout,
		Actions:        actions,
	}
	ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.WriteTimeout+time.Second)
	defer cancel()
	if err := l.logical.LifeCycle(ctx, &r); err != nil {
		// Log a warning, a failure here isn't a big deal as the next life cycle run will find the same
		// items and attempt to handle them in the same way.

		retryIn := retry.DefaultBackOff.Next(int(l.conf.Distribution.Failures.Add(1)))
		state.nextRunDeadline = clock.Now().UTC().Add(retryIn)
		l.log.Warn("storage failure during logical life cycle; retrying...",
			"retry-in", retryIn,
			"error", err)
	}
}

func (l *LifeCycle) handleManagerEvents(state *lifeCycleState, actions []types.Action) {
	r := types.LifeCycleRequest{
		RequestTimeout: l.logical.conf.WriteTimeout,
		Actions:        actions,
	}
	ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.WriteTimeout+time.Second)
	defer cancel()
	if err := l.manager.LifeCycle(ctx, &r); err != nil {
		// It's possible the item was written, but we lost contact with a remote logical queue
		// in either case, we must assume it wasn't written and try again.
		retryIn := retry.DefaultBackOff.Next(int(l.conf.Distribution.Failures.Add(1)))
		state.nextRunDeadline = clock.Now().UTC().Add(retryIn)
		l.log.Warn("storage failure during logical life cycle; retrying...",
			"retry-in", retryIn,
			"error", err)
	}
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
	nextRunDeadline clock.Time
	timer           clock.Timer
}
