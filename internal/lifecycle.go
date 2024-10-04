package internal

import (
	"context"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	LifeCycleMethodRun = iota
	LifeCycleMethodScheduled
	LifeCycleMethodShutdown
)

var (
	ErrLifeCycleRunning  = errors.New("life cycle is already running")
	ErrLifeCycleShutdown = errors.New("life cycle is shutdown")
)

type LifeCycleConfig struct {
	Distribution *PartitionDistribution
	Storage      store.Partition
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
	conf      LifeCycleConfig
	wg        sync.WaitGroup
	requestCh chan Request
	running   atomic.Bool
	manager   *QueuesManager
	logical   *Logical
	log       *slog.Logger
}

func NewLifeCycle(conf LifeCycleConfig) *LifeCycle {
	return &LifeCycle{
		log: conf.Logical.conf.Log.With(errors.OtelCodeNamespace, "LifeCycle").
			With("instance-id", conf.Logical.instanceID).
			With("queue", conf.Logical.conf.Name),
		manager:   conf.Logical.conf.Manager,
		requestCh: make(chan Request),
		logical:   conf.Logical,
		conf:      conf,
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
		time.Sleep(time.Microsecond)
	}
}

func (l *LifeCycle) Run() chan struct{} {
	req := Request{
		ReadyCh: make(chan struct{}),
		Method:  LifeCycleMethodRun,
	}
	select {
	case l.requestCh <- req:
	default:
		return nil
	}
	return req.ReadyCh
}

func (l *LifeCycle) NotifyScheduledItem(t time.Time) chan struct{} {
	req := Request{
		ReadyCh: make(chan struct{}),
		Method:  LifeCycleMethodScheduled,
	}
	select {
	case l.requestCh <- req:
	default:
		return nil
	}
	return req.ReadyCh
}

func (l *LifeCycle) requestLoop() {
	defer l.wg.Done()

	// nolint
	for {
		select {
		case req := <-l.requestCh:
			switch req.Method {
			case LifeCycleMethodRun:
				l.runLifeCycle()
			case LifeCycleMethodScheduled:
				// TODO
			case LifeCycleMethodShutdown:
				close(req.ReadyCh)
				return
			}
			close(req.ReadyCh)
		}
	}
}

func (l *LifeCycle) runLifeCycle() {
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
		Method:  LifeCycleMethodShutdown,
	}

	// Queue shutdown request
	select {
	case l.requestCh <- req:
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
