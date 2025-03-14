package internal

import (
	"context"
	"fmt"
	"github.com/duh-rpc/duh-go/retry"
	"github.com/dustin/go-humanize"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrLifeCycleRunning  = errors.New("life cycle is already running")
	ErrLifeCycleShutdown = errors.New("life cycle is already shutdown")
)

type LifeCycleConfig struct {
	Partition *Partition
	Storage   store.Partition
	Clock     *clock.Provider
	Logical   *Logical
}

// TODO: Moving scheduled items into the queue, This MIGHT not be handled by the lifecycle.

// LifeCycle manages the life cycle of items in a partition.
// A life cycle:
// - Handles reserved items that are past their completion deadline
// - Moves expired items out of the queue into the dead letter
// - Decides if a partition is available or not
// - Sets the initial partition counts if none exist
type LifeCycle struct {
	state      types.PartitionState
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
			"partition", conf.Partition.Store.Info().PartitionNum,
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
	go l.run()

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
		time.Sleep(clock.Microsecond)
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

func (l *LifeCycle) run() {
	defer l.wg.Done()
	state := lifeCycleState{
		timer:           l.conf.Clock.NewTimer(humanize.LongTime),
		nextRunDeadline: l.conf.Clock.Now().UTC().Add(humanize.LongTime),
	}

	// nolint
	for {
		// NOTE: Separate channels avoids missing a shutdown or run cycle requests
		// in the case where we have a ton of notify requests.
		select {
		case req := <-l.runCh:
			l.log.LogAttrs(context.Background(), LevelDebugAll, "life cycle run - requested")
			l.runLifeCycle(&state)
			close(req.ReadyCh)
		case req := <-l.notifyCh:
			l.handleNotify(&state, req.Request.(clock.Time))
		case req := <-l.shutdownCh:
			close(req.ReadyCh)
			state.timer.Stop()
			return
		case <-state.timer.C():
			l.log.LogAttrs(context.Background(), LevelDebugAll, "life cycle run - timer")
			l.runLifeCycle(&state)
		}

		now := l.conf.Clock.Now().UTC()
		state.timer.Reset(state.nextRunDeadline.Sub(now))
		l.log.LogAttrs(context.Background(), LevelDebugAll, "timer reset",
			slog.String("nextRunDeadline", state.nextRunDeadline.String()),
			slog.String("duration", state.nextRunDeadline.Sub(now).String()))
	}
}

func (l *LifeCycle) handleNotify(state *lifeCycleState, notify clock.Time) {
	now := l.conf.Clock.Now().UTC()
	fmt.Printf("handleNotify now '%s'\n", now.String())
	// Ignore notify times from the past
	if now.After(notify) {
		return
	}

	// Ignore if we already have a life cycle run scheduled that is before the notify time.
	if state.nextRunDeadline.After(now) && state.nextRunDeadline.Before(notify) {
		fmt.Printf("handleNotify deadline ignore '%s' - '%s'\n", state.nextRunDeadline.String(), notify.String())
		return
	}
	l.log.LogAttrs(context.Background(), LevelDebugAll, "life cycle notified",
		slog.String("next", notify.String()),
		slog.String("now", now.String()),
		slog.String("timer_reset", notify.Sub(now).String()))
	state.nextRunDeadline = notify
}

type handleActions func(state *lifeCycleState, actions []types.Action)

func (l *LifeCycle) runLifeCycle(state *lifeCycleState) {
	now := l.conf.Clock.Now().UTC()
	l.log.LogAttrs(context.Background(), LevelDebugAll, "life cycle run",
		slog.String("now", now.String()))

	defer func() {
		l.log.LogAttrs(context.Background(), LevelDebugAll, "life cycle run complete",
			slog.String("next-run", state.nextRunDeadline.String()))
	}()

	// Attempt to get the partition out of a failed state
	if l.state.Failures != 0 {
		if !l.recoverPartition(state) {
			return
		}
	}

	add := func(a types.Action, actions []types.Action, handle handleActions) []types.Action {
		actions = append(actions, a)
		if len(actions) >= 1_000 {
			handle(state, actions)
			actions = actions[:]
		}
		return actions
	}

	lActions := make([]types.Action, 0, 1_000)
	mActions := make([]types.Action, 0, 1_000)

	// Separate the events into separate slices handling them in batches
	for a := range l.conf.Storage.ScanForActions(l.logical.conf.ReadTimeout, now) {
		l.log.LogAttrs(context.Background(), LevelDebugAll, "new life cycle action",
			slog.String("reserve_deadline", a.Item.ReserveDeadline.String()),
			slog.String("expire_deadline", a.Item.ExpireDeadline.String()),
			slog.String("action", types.ActionToString(a.Action)),
			slog.String("id", string(a.Item.ID)))
		switch a.Action {
		case types.ActionQueueScheduledItem, types.ActionReserveExpired:
			lActions = add(a, lActions, l.writeLogicalActions)
		case types.ActionItemExpired, types.ActionItemMaxAttempts:
			// If there is no dead letter queue defined
			if l.conf.Partition.Info.Queue.DeadQueue == "" {
				a.Action = types.ActionDeleteItem
				lActions = add(a, lActions, l.writeLogicalActions)
				continue
			}
			mActions = add(a, mActions, l.writeManagerActions)
		default:
			panic(fmt.Sprintf("undefined action '%d' received from partition storage", a.Action))
		}
	}

	if len(lActions) > 0 {
		l.writeLogicalActions(state, lActions)
	}

	if len(mActions) > 0 {
		l.writeManagerActions(state, mActions)
	}

	ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.ReadTimeout)
	var info types.LifeCycleInfo
	defer cancel()

	if err := l.conf.Storage.LifeCycleInfo(ctx, &info); err != nil {
		// TODO(no-write): Update the Logical of the Failures count
		l.state.Failures++
		retryIn := retry.DefaultBackOff.Next(l.state.Failures)
		state.nextRunDeadline = now.Add(retryIn)
		l.log.Warn("while fetching life cycle info from partition; retrying...",
			"retry", retryIn,
			"error", err)
		return
	}

	// TODO: if there is nothing in the queue, we need to set a new nextRunDeadline to some time
	//  in the future? Actually this will happen when the next thing comes in... right?
	if info.NextReserveExpiry.After(now) && info.NextReserveExpiry.Before(state.nextRunDeadline) {
		state.nextRunDeadline = info.NextReserveExpiry
		fmt.Printf("NextReseveExpiry: %s\n", info.NextReserveExpiry)
	}

	// If there is no known future run then set the time to some time in the future
	// This can happen when the queue becomes empty or all reserved items in the queue are
	// expired.
	if state.nextRunDeadline.Before(now) {
		state.nextRunDeadline = now.Add(humanize.LongTime)
	}
}

func (l *LifeCycle) partitionStateChange(failures int, numReserved int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.WriteTimeout+time.Second)
	defer cancel()
	if err := l.logical.PartitionStateChange(ctx, l.conf.Partition.Info.PartitionNum,
		types.PartitionState{
			Failures:    failures,
			NumReserved: numReserved,
		}); err != nil {
		if errors.Is(err, ErrQueueShutdown) {
			return false
		}
		l.log.Warn("while updating partition state; continuing...", "error", err)
		return false
	}
	return true
}

func (l *LifeCycle) writeLogicalActions(state *lifeCycleState, actions []types.Action) {
	r := types.LifeCycleRequest{
		PartitionNum:   l.conf.Partition.Info.PartitionNum,
		RequestTimeout: l.logical.conf.WriteTimeout,
		Actions:        actions,
	}
	ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.WriteTimeout+time.Second)
	defer cancel()
	if err := l.logical.LifeCycle(ctx, &r); err != nil {
		if errors.Is(err, ErrQueueShutdown) {
			return
		}
		// Log a warning, a failure here isn't a big deal as the next life cycle run will find the same
		// items and attempt to handle them in the same way.
		l.state.Failures++
		retryIn := retry.DefaultBackOff.Next(l.state.Failures)
		state.nextRunDeadline = l.conf.Clock.Now().UTC().Add(retryIn)
		l.log.Warn("storage failure during logical life cycle; retrying...",
			"retry", retryIn,
			"error", err)
		l.partitionStateChange(l.state.Failures, types.UnSet)
	}
}

func (l *LifeCycle) writeManagerActions(state *lifeCycleState, actions []types.Action) {
	r := types.LifeCycleRequest{
		PartitionNum:   l.conf.Partition.Info.PartitionNum,
		RequestTimeout: l.logical.conf.WriteTimeout,
		Actions:        actions,
	}
	ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.WriteTimeout+time.Second)
	defer cancel()
	if err := l.manager.LifeCycle(ctx, &r); err != nil {
		if errors.Is(err, ErrQueueShutdown) {
			return
		}
		// It's possible the item was written, but we lost contact with a remote logical queue
		// in either case, we must assume it wasn't written and try again.
		l.state.Failures++
		retryIn := retry.DefaultBackOff.Next(l.state.Failures)
		state.nextRunDeadline = l.conf.Clock.Now().UTC().Add(retryIn)
		l.log.Warn("storage failure during manager life cycle; retrying...",
			"retry", retryIn,
			"error", err)

		l.partitionStateChange(l.state.Failures, types.UnSet)
	}
}

func (l *LifeCycle) recoverPartition(state *lifeCycleState) bool {
	ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.ReadTimeout)
	var stats types.PartitionStats
	defer cancel()

	// TODO(next): The state.Failures is not getting set to ZERO so the partition never comes up

	if err := l.conf.Storage.Stats(ctx, &stats); err != nil {
		l.state.Failures++
		retryIn := retry.DefaultBackOff.Next(l.state.Failures)
		state.nextRunDeadline = l.conf.Clock.Now().UTC().Add(retryIn)
		l.log.Warn("while fetching partition stats; retrying...",
			"retry", retryIn,
			"error", err)

		l.partitionStateChange(l.state.Failures, types.UnSet)
		return false
	}

	l.log.LogAttrs(context.Background(), LevelDebug, "partition available",
		slog.Int("partition", l.conf.Partition.Info.PartitionNum))

	if stats.Total-stats.TotalReserved < 0 {
		l.log.Error("assertion failed; total count of items in the partition "+
			"cannot be more than the total reserved count", "total", stats.Total,
			"reserved", stats.TotalReserved)
		return false
	}

	l.state.Failures = 0
	return l.partitionStateChange(0, stats.Total-stats.TotalReserved)
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
