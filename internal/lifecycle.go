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

type handleActions func(state *lifeCycleState, actions []types.Action)

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
	startCh    chan Request
	notifyCh   chan Request
	shutdownCh chan Request
	log        *slog.Logger
	running    atomic.Bool
	logical    *Logical
}

func NewLifeCycle(conf LifeCycleConfig) *LifeCycle {
	return &LifeCycle{
		log: conf.Logical.conf.Log.With(errors.OtelCodeNamespace, "LifeCycle",
			"partition", conf.Partition.Info.PartitionNum,
			"instance-id", conf.Logical.instanceID,
			"queue", conf.Logical.conf.Name),
		manager:    conf.Logical.conf.Manager,
		notifyCh:   make(chan Request),
		startCh:    make(chan Request),
		shutdownCh: make(chan Request),
		state: types.PartitionState{
			Failures: types.UnSet,
		},
		logical: conf.Logical,
		conf:    conf,
	}
}

// Start starts the partition life cycle kicking off a partition recovery if necessary.
// Partition recovery might take a while, as such the LifeCycle will signal the *Logical
// when the partition becomes available. The *Logical won't allow interaction with the
// partition until recovery has completed.
func (l *LifeCycle) Start() error {
	if !l.running.CompareAndSwap(false, true) {
		return ErrLifeCycleRunning
	}

	l.wg.Add(1)
	go l.requestLoop()

	// Signal the LifeCycle to start it's first run
	l.startCh <- Request{
		ReadyCh: make(chan struct{}),
	}
	return nil
}

// Notify proposes a time for the life cycle to consider when determining the next execution cycle.
func (l *LifeCycle) Notify(t clock.Time) {
	select {
	case l.notifyCh <- Request{Method: MethodNotify, Request: t}:
	default:
	}
}

// NotifyScheduled proposes a time for the life cycle to consider when determining the next scheduled cycle.
func (l *LifeCycle) NotifyScheduled(t clock.Time) {
	select {
	case l.notifyCh <- Request{Method: MethodNotifyScheduled, Request: t}:
	default:
	}
}

func (l *LifeCycle) requestLoop() {
	defer l.wg.Done()
	state := lifeCycleState{
		nextRunDeadline: l.conf.Clock.Now().UTC().Add(humanize.LongTime),
		scheduleTimer:   l.conf.Clock.NewTimer(humanize.LongTime),
		timer:           l.conf.Clock.NewTimer(humanize.LongTime),
	}

	for {
		select {
		case req := <-l.startCh:
			l.log.LogAttrs(context.Background(), LevelDebugAll, "life cycle run - start")
			l.runLifeCycle(&state)
			close(req.ReadyCh)
		case req := <-l.notifyCh:
			switch req.Method {
			case MethodNotify:
				l.handleNotify(&state, req.Request.(clock.Time))
			case MethodNotifyScheduled:
				l.handleScheduledNotify(&state, req.Request.(clock.Time))
			}
		case req := <-l.shutdownCh:
			close(req.ReadyCh)
			state.timer.Stop()
			return
		case <-state.timer.C():
			l.log.LogAttrs(context.Background(), LevelDebugAll, "life cycle run - timer fired")
			l.runLifeCycle(&state)
		case <-state.scheduleTimer.C():
			l.log.LogAttrs(context.Background(), LevelDebugAll, "scheduled run - timer fired")
			l.runScheduled(&state)
		}

		now := l.conf.Clock.Now().UTC()
		state.timer.Reset(state.nextRunDeadline.Sub(now))
		l.log.LogAttrs(context.Background(), LevelDebugAll, "life cycle timer reset",
			slog.String("nextRunDeadline", state.nextRunDeadline.String()),
			slog.String("duration", state.nextRunDeadline.Sub(now).String()))
	}
}

func (l *LifeCycle) handleScheduledNotify(state *lifeCycleState, notify clock.Time) {
	now := l.conf.Clock.Now().UTC()
	// Ignore notify times from the past
	if now.After(notify) {
		return
	}

	// Ignore if we already have a life cycle run scheduled that is before the notify time.
	if state.nextScheduleDeadline.After(now) && state.nextScheduleDeadline.Before(notify) {
		return
	}
	l.log.LogAttrs(context.Background(), LevelDebugAll, "scheduled notified",
		slog.String("next", notify.String()),
		slog.String("now", now.String()),
		slog.String("timer_reset", notify.Sub(now).String()))
	state.nextScheduleDeadline = notify
}

func (l *LifeCycle) runScheduled(state *lifeCycleState) {
	defer func() {
		l.log.LogAttrs(context.Background(), LevelDebugAll, "scheduled run complete",
			slog.String("next-run", state.nextScheduleDeadline.String()))
	}()

	now := l.conf.Clock.Now().UTC()
	actions := make([]types.Action, 0, 1_000)

	// ScanForScheduled() returns an iterator which allows us to batch the scheduled actions together
	// before writing the actions to the logical.
	for a := range l.conf.Storage.ScanForScheduled(l.logical.conf.ReadTimeout, now) {
		Assert(a.Action != types.ActionQueueScheduledItem, "must only return scheduled items")
		actions = l.batchAction(state, a, actions, l.writeLogicalActions)
	}

	if len(actions) > 0 {
		l.writeLogicalActions(state, actions)
	}
}

func (l *LifeCycle) handleNotify(state *lifeCycleState, notify clock.Time) {
	now := l.conf.Clock.Now().UTC()
	// Ignore notify times from the past
	if now.After(notify) {
		return
	}

	// Ignore if we already have a life cycle run scheduled that is before the notify time.
	if state.nextRunDeadline.After(now) && state.nextRunDeadline.Before(notify) {
		return
	}
	l.log.LogAttrs(context.Background(), LevelDebugAll, "life cycle notified",
		slog.String("next", notify.String()),
		slog.String("now", now.String()),
		slog.String("timer_reset", notify.Sub(now).String()))
	state.nextRunDeadline = notify
}

func (l *LifeCycle) runLifeCycle(state *lifeCycleState) {
	defer func() {
		l.log.LogAttrs(context.Background(), LevelDebugAll, "life cycle run complete",
			slog.String("next-run", state.nextRunDeadline.String()))
	}()
	now := l.conf.Clock.Now().UTC()

	// Attempt to get the partition out of a failed state
	if l.state.Failures != 0 {
		if !l.recoverPartition(state) {
			return
		}
	}

	lActions := make([]types.Action, 0, 1_000)
	mActions := make([]types.Action, 0, 1_000)

	// ScanForActions() returns an iterator which allows us to batch actions together
	// before writing the actions to the logical or manager.
	for a := range l.conf.Storage.ScanForActions(l.logical.conf.ReadTimeout, now) {
		switch a.Action {
		case types.ActionReserveExpired:
			// If attempts exhausted, then convert to a delete action
			if a.Item.Attempts >= l.conf.Partition.Info.Queue.MaxAttempts {
				a.Action = types.ActionDeleteItem
			}
			lActions = l.batchAction(state, a, lActions, l.writeLogicalActions)
		case types.ActionQueueScheduledItem:
			lActions = l.batchAction(state, a, lActions, l.writeLogicalActions)
		case types.ActionItemExpired, types.ActionItemMaxAttempts:
			// If there is no dead letter queue defined then convert to delete action
			if l.conf.Partition.Info.Queue.DeadQueue == "" {
				a.Action = types.ActionDeleteItem
				lActions = l.batchAction(state, a, lActions, l.writeLogicalActions)
				continue
			}
			mActions = l.batchAction(state, a, mActions, l.writeManagerActions)
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
		l.state.Failures++
		l.partitionStateChange(l.state.Failures, types.UnSet)
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
	}

	// If there is no known future run then set the time to some time in the future
	// This can happen when the queue becomes empty or all reserved items in the queue are
	// expired.
	if state.nextRunDeadline.Before(now) {
		state.nextRunDeadline = now.Add(humanize.LongTime)
	}
}

func (l *LifeCycle) batchAction(state *lifeCycleState, a types.Action, actions []types.Action, handle handleActions) []types.Action {
	l.log.LogAttrs(context.Background(), LevelDebugAll, "new life cycle action",
		slog.String("action", types.ActionToString(a.Action)),
		slog.String("reserve_deadline", a.Item.ReserveDeadline.String()),
		slog.String("expire_deadline", a.Item.ExpireDeadline.String()),
		slog.String("id", string(a.Item.ID)))
	actions = append(actions, a)
	if len(actions) >= 1_000 {
		handle(state, actions)
		actions = actions[:]
	}
	return actions
}

// partitionStateChange notifies the logical that a partition changed state
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
		l.state.Failures++
		l.partitionStateChange(l.state.Failures, types.UnSet)
		retryIn := retry.DefaultBackOff.Next(l.state.Failures)
		state.nextRunDeadline = l.conf.Clock.Now().UTC().Add(retryIn)

		// Only log a warning as a failure here isn't a big deal. The next life cycle
		// run will find the same items and attempt to handle them in the same way.
		l.log.Warn("partition storage failure; retrying...",
			"retry", retryIn,
			"error", err)
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
		l.state.Failures++
		l.partitionStateChange(l.state.Failures, types.UnSet)
		retryIn := retry.DefaultBackOff.Next(l.state.Failures)
		state.nextRunDeadline = l.conf.Clock.Now().UTC().Add(retryIn)

		// It's possible the item was written, but there was some issue with the manager.
		// In either case, we must assume it wasn't written and try again.
		l.log.Warn("storage failure during manager life cycle; retrying...",
			"retry", retryIn,
			"error", err)
	}
}

func (l *LifeCycle) recoverPartition(state *lifeCycleState) bool {
	ctx, cancel := context.WithTimeout(context.Background(), l.logical.conf.ReadTimeout)
	var stats types.PartitionStats
	defer cancel()

	if err := l.conf.Storage.Stats(ctx, &stats); err != nil {
		l.state.Failures++
		l.partitionStateChange(l.state.Failures, types.UnSet)
		retryIn := retry.DefaultBackOff.Next(l.state.Failures)
		state.nextRunDeadline = l.conf.Clock.Now().UTC().Add(retryIn)

		l.log.Warn("recover partition attempt failed; retrying...",
			"retry", retryIn,
			"error", err)
		return false
	}

	if stats.Total-stats.NumReserved < 0 {
		l.log.Error("assertion failed; total count of items in the partition "+
			"cannot be more than the total reserved count", "total", stats.Total,
			"reserved", stats.NumReserved)
		return false
	}

	// TODO(thrawn01): Change this to info, if this is the first time the partition has become available.
	l.log.LogAttrs(context.Background(), slog.LevelWarn, "partition recovered",
		slog.Int("partition", l.conf.Partition.Info.PartitionNum))

	l.state.Failures = 0
	l.state.NumReserved = stats.NumReserved
	return l.partitionStateChange(0, stats.Total-stats.NumReserved)
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
	scheduleTimer        clock.Timer
	timer                clock.Timer
	nextScheduleDeadline clock.Time
	nextRunDeadline      clock.Time
}
