package service_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLifecycle(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}
	mongo := mongoTestSetup{}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
		Lifetime storageLifetime
	}{
		{
			Name: "InMemory",
			Setup: func() store.Config {
				return setupMemoryStorage(store.Config{})
			},
			TearDown: func() {},
		},
		{
			Name: "BadgerDB",
			Setup: func() store.Config {
				return badgerdb.Setup(store.BadgerConfig{})
			},
			TearDown: func() {
				badgerdb.Teardown()
			},
			Lifetime: outlivesDaemon,
		},
		{
			Name: "PostgreSQL",
			Setup: func() store.Config {
				return postgres.Setup(store.PostgresConfig{})
			},
			TearDown: func() {
				postgres.Teardown()
			},
			Lifetime: outlivesDaemon,
		},
		{
			Name: "MongoDB",
			Setup: func() store.Config {
				return mongo.Setup(store.MongoConfig{})
			},
			TearDown: func() {
				mongo.Teardown()
			},
			Lifetime: outlivesDaemon,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testLifecycle(t, tc.Setup, tc.TearDown, tc.Lifetime)
		})
	}
}

// pageSize is the most items one lifecycle store call reads. A partition larger than this
// takes more than one page to read.
const pageSize = 1_000

// largePartition holds one and a half pages of items, so a lifecycle pass over it reads two pages.
const largePartition = pageSize + pageSize/2

// requestSettle is how long a test waits for a request it started to reach the queue.
const requestSettle = 200 * clock.Millisecond

// pageWriteTimeout is the real time a lifecycle write gets. A page of 1,000 requeued items on
// MongoDB or PostgreSQL can take longer than the 3s default under -race on a busy machine, and a
// write that times out backs off on the frozen clock, which never moves to retry it.
const pageWriteTimeout = 30 * clock.Second

// waitForPageWritten waits, for as long as a page write may take, until the next lifecycle run of
// the queue's only partition is in the future again, which happens only after a due run has
// finished and rescheduled itself.
func waitForPageWritten(t *testing.T, ctx context.Context, c *querator.Client, queueName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var stats pb.QueueStatsResponse
		if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
			return false
		}
		if len(stats.LogicalQueues) != 1 || len(stats.LogicalQueues[0].Partitions) != 1 {
			return false
		}
		next, err := clock.ParseDuration(stats.LogicalQueues[0].Partitions[0].NextLifecycleRun)
		return err == nil && next > 0
	}, pageWriteTimeout, 20*clock.Millisecond)
}

func testLifecycle(t *testing.T, setup NewStorageFunc, tearDown func(), lifetime storageLifetime) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("LargePartition", func(t *testing.T) {
		t.Run("ExpiredLeasesAreRequeued", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now, WriteTimeout: pageWriteTimeout})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			createOnePartitionQueue(t, ctx, c, queueName)
			produceInOrder(t, ctx, c, queueName, largePartition)
			require.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)

			now.Advance(clock.Minute + clock.Second)
			waitForPageWritten(t, ctx, c, queueName)
			assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)
		})

		t.Run("ExpiredItemsAreRemoved", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now, WriteTimeout: pageWriteTimeout})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			createOnePartitionQueue(t, ctx, c, queueName)
			produceInOrder(t, ctx, c, queueName, largePartition)
			require.Equal(t, int32(largePartition), partitionTotal(t, ctx, c, queueName))

			now.Advance(24*clock.Hour + clock.Second)
			require.Eventually(t, func() bool {
				return partitionTotal(t, ctx, c, queueName) == 0
			}, 10*clock.Second, 50*clock.Millisecond)
		})

		t.Run("MaxAttemptsMoveToDeadLetter", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now, WriteTimeout: pageWriteTimeout})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			dlqName := random.String("dlq-", 10)
			createOnePartitionQueue(t, ctx, c, dlqName)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       "24h0m0s",
				MaxAttempts:         1,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			})
			produceInOrder(t, ctx, c, queueName, largePartition)
			require.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)

			now.Advance(clock.Minute + clock.Second)
			require.Eventually(t, func() bool {
				return partitionTotal(t, ctx, c, dlqName) == largePartition
			}, 10*clock.Second, 50*clock.Millisecond)
			assert.Equal(t, int32(0), partitionTotal(t, ctx, c, queueName))
		})

		t.Run("ScheduledItemsAreEnqueued", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now, WriteTimeout: pageWriteTimeout})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			createOnePartitionQueue(t, ctx, c, queueName)
			enqueueAt := timestamppb.New(now.Now().Add(clock.Minute))
			for done := 0; done < largePartition; done += pageSize {
				items := make([]*pb.QueueProduceItem, min(pageSize, largePartition-done))
				for i := range items {
					items[i] = &pb.QueueProduceItem{
						Reference: fmt.Sprintf("item-%05d", done+i),
						Bytes:     []byte("x"),
						EnqueueAt: enqueueAt,
					}
				}
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          items,
				}))
			}

			now.Advance(clock.Minute + clock.Second)
			require.Eventually(t, func() bool {
				var stats pb.QueueStatsResponse
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
					return false
				}
				return stats.LogicalQueues[0].Partitions[0].Scheduled == 0
			}, 10*clock.Second, 50*clock.Millisecond)
			assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)
		})
	})

	t.Run("LeaseAndExpiryBothPassed", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		produceInOrder(t, ctx, c, queueName, 1)
		require.Equal(t, "item-00000", leaseOne(t, ctx, c, queueName).Reference)

		// Both deadlines pass before the lifecycle runs, so the run that gives the lease back finds
		// an item that has already expired.
		now.Advance(24*clock.Hour + clock.Second)
		require.Eventually(t, func() bool {
			return partitionTotal(t, ctx, c, queueName) == 0
		}, 5*clock.Second, 20*clock.Millisecond)
	})

	t.Run("ScheduledWriteFailingPartwayRemovesExpiredItem", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		// Both items expire after 24 hours but are not enqueued until 48 hours, so no lifecycle pass
		// sees them while they are scheduled.
		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		produceScheduled(t, ctx, c, queueName, 2, now.Now().UTC().Add(48*clock.Hour))
		now.Advance(24*clock.Hour + clock.Second)
		waitForPageWritten(t, ctx, c, queueName)

		// The scheduled write enqueues item-00000 and then fails, so the item is no longer scheduled
		// and its expire deadline has passed. Only a lifecycle pass removes it.
		spy.FailScheduledWritesAfterFirstItem()
		now.Advance(24*clock.Hour + clock.Second)
		require.Eventually(t, func() bool {
			return scheduledCount(ctx, c, queueName) == 1
		}, 5*clock.Second, 20*clock.Millisecond)
		require.Eventually(t, func() bool {
			return partitionTotal(t, ctx, c, queueName) == 0
		}, 5*clock.Second, 20*clock.Millisecond)
	})

	t.Run("RequestsAreServedBetweenPages", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		produceInOrder(t, ctx, c, queueName, largePartition)
		require.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)

		// A produce reaches the queue while the pass reads its first page, and must be served before
		// the second. It adds a full page of items, and the pass still ends at the item that was last when it began.
		var produceErr error
		spy.HoldFirstPage()
		defer spy.Release()
		now.Advance(clock.Minute + clock.Second)
		spy.WhileFirstPageHeld(t, ctx, func() {
			produceErr = c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          produceRandomItems(pageSize),
			})
		})
		waitForPageWritten(t, ctx, c, queueName)
		require.NoError(t, produceErr)

		events := spy.Events()
		require.GreaterOrEqual(t, len(events), 3)
		assert.Equal(t, []string{"page:start", "produce", "page:resume"}, events[:3])
		assert.Equal(t, 2, spy.Pages())
		for _, n := range spy.ActionCounts() {
			assert.LessOrEqual(t, n, pageSize)
		}
		assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition+pageSize), largePartition+pageSize)
	})

	t.Run("LeaseGrantedDuringPassExpires", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		produceInOrder(t, ctx, c, queueName, largePartition)
		require.Equal(t, "item-00000", leaseOne(t, ctx, c, queueName).Reference)

		// The expired lease on item-00000 starts a pass. Between its two pages, item-00001 is
		// leased. It sits behind the pass's pivot, so the pass never reads its lease deadline.
		var leased pb.QueueLeaseResponse
		var leaseErr error
		spy.HoldFirstPage()
		defer spy.Release()
		now.Advance(clock.Minute + clock.Second)
		spy.WhileFirstPageHeld(t, ctx, func() {
			leaseErr = c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &leased)
		})
		waitForPageWritten(t, ctx, c, queueName)
		require.NoError(t, leaseErr)
		require.Len(t, leased.Items, 1)
		require.Equal(t, "item-00001", leased.Items[0].Reference)
		require.Equal(t, 2, spy.Pages())
		// The next run is due when item-00001's lease expires, not later.
		require.LessOrEqual(t, nextLifecycleRun(t, ctx, c, queueName), clock.Minute)

		now.Advance(clock.Minute + clock.Second)
		waitForPageWritten(t, ctx, c, queueName)
		assert.Contains(t, leaseUpTo(t, ctx, c, queueName, largePartition), "item-00001")
	})

	t.Run("PauseDuringPassLosesNoWork", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		produceInOrder(t, ctx, c, queueName, largePartition)
		require.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)

		var pauseErr error
		spy.HoldFirstPage()
		defer spy.Release()
		now.Advance(clock.Minute + clock.Second)
		spy.WhileFirstPageHeld(t, ctx, func() {
			pauseErr = d.Service().PauseQueue(ctx, queueName)
		})
		require.NoError(t, pauseErr)

		// While paused, the pass stops after its first page. The stats reply comes from the
		// request loop, which would run the next page first if the pause did not hold it.
		nextLifecycleRun(t, ctx, c, queueName)
		assert.Equal(t, 1, spy.Pages())

		require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
		waitForPageWritten(t, ctx, c, queueName)
		assert.Equal(t, []string{"page:start", "page:resume"}, spy.Events())
		assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)
	})

	// Only storage that outlives the daemon can show what a restart finds.
	if lifetime == outlivesDaemon {
		t.Run("ShutdownDuringPassLosesNoWork", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			conf := setup()
			defer tearDown()
			spy := &lifecycleSpy{}
			first := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(conf), Clock: now, WriteTimeout: pageWriteTimeout})
			stopped := false
			defer func() {
				if !stopped {
					first.Shutdown(t)
				}
			}()
			c, ctx := first.MustClient(), first.Context()

			queueName := random.String("queue-", 10)
			createOnePartitionQueue(t, ctx, c, queueName)
			produceInOrder(t, ctx, c, queueName, largePartition)
			require.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)

			// A pause that reaches the queue during the first page stops the pass after it, so the
			// shutdown lands in the middle of the pass.
			var pauseErr error
			spy.HoldFirstPage()
			defer spy.Release()
			now.Advance(clock.Minute + clock.Second)
			spy.WhileFirstPageHeld(t, ctx, func() {
				pauseErr = first.Service().PauseQueue(ctx, queueName)
			})
			require.NoError(t, pauseErr)
			require.Equal(t, 1, spy.Pages())
			first.Shutdown(t)
			stopped = true

			second := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: conf, Clock: now, WriteTimeout: pageWriteTimeout})
			defer second.Shutdown(t)
			c, ctx = second.MustClient(), second.Context()

			// The restarted queue loads on its first request, and its first lifecycle run waits for
			// the clock to move.
			nextLifecycleRun(t, ctx, c, queueName)
			now.Advance(clock.Millisecond)
			assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)
		})
	}

	t.Run("ReloadDuringPassStartsAgain", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		produceInOrder(t, ctx, c, queueName, largePartition)
		require.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)

		var reloadErr error
		spy.HoldFirstPage()
		defer spy.Release()
		now.Advance(clock.Minute + clock.Second)
		spy.WhileFirstPageHeld(t, ctx, func() {
			reloadErr = c.QueueReload(ctx, &pb.QueueReloadRequest{QueueName: queueName})
		})
		waitForPageWritten(t, ctx, c, queueName)
		require.NoError(t, reloadErr)

		// The reload ends the pass after its first page, and the next page starts at the first item.
		events := spy.Events()
		require.GreaterOrEqual(t, len(events), 2)
		assert.Equal(t, []string{"page:start", "page:start"}, events[:2])
		assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)
	})

	t.Run("FailingScanBacksOff", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          []*pb.QueueProduceItem{{Reference: "leased", Bytes: []byte("x")}},
		}))
		require.Equal(t, "leased", leaseOne(t, ctx, c, queueName).Reference)

		// Every scan fails while Stats keeps working, so each run recovers the partition and
		// fails again. The wait between runs grows to the 5s cap.
		spy.FailScans()
		now.Advance(clock.Minute + clock.Second)
		var waits []clock.Duration
		for range 7 {
			waitForPageWritten(t, ctx, c, queueName)
			waits = append(waits, nextLifecycleRun(t, ctx, c, queueName))
			if len(waits) < 7 {
				now.Advance(waits[len(waits)-1])
			}
		}
		for i := 1; i < len(waits); i++ {
			assert.GreaterOrEqual(t, waits[i], waits[i-1])
		}
		assert.Greater(t, waits[len(waits)-1], 3*clock.Second)
		assert.LessOrEqual(t, waits[len(waits)-1], 5*clock.Second)

		// The first scan that works gives the lease back and the waits return to normal. Scans
		// stop failing before the clock reaches the next run, so that run is the one that works.
		spy.StopFailingScans()
		now.Advance(waits[len(waits)-1])
		waitForPageWritten(t, ctx, c, queueName)
		assert.Eventually(t, func() bool {
			return isUnLeased(ctx, c, queueName, "leased")
		}, 5*clock.Second, 20*clock.Millisecond)
		assert.Greater(t, nextLifecycleRun(t, ctx, c, queueName), clock.Hour)
	})

	t.Run("CompleteIsServedBetweenPages", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		produceInOrder(t, ctx, c, queueName, largePartition)
		require.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)

		// The lease on "held" outlives the leases that start the pass, so it can be completed
		// while the pass is between its two pages.
		now.Advance(10 * clock.Second)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          []*pb.QueueProduceItem{{Reference: "held", Bytes: []byte("x")}},
		}))
		held := leaseOne(t, ctx, c, queueName)
		require.Equal(t, "held", held.Reference)

		var completeErr error
		spy.HoldFirstPage()
		defer spy.Release()
		now.Advance(55 * clock.Second)
		spy.WhileFirstPageHeld(t, ctx, func() {
			completeErr = c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Ids:            []string{held.Id},
			})
		})
		waitForPageWritten(t, ctx, c, queueName)
		require.NoError(t, completeErr)

		events := spy.Events()
		require.GreaterOrEqual(t, len(events), 3)
		assert.Equal(t, []string{"page:start", "complete", "page:resume"}, events[:3])
		assert.Equal(t, int32(largePartition), partitionTotal(t, ctx, c, queueName))
		assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)
	})

	t.Run("RetryDuringPassIsNotMissed", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "10m0s",
			RequestedPartitions: 1,
		})

		// "retried" expires a second before the page of items produced after it. A retry moves
		// it to the tail, behind that page, so the pass below reaches it on its second page.
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          []*pb.QueueProduceItem{{Reference: "retried", Bytes: []byte("x")}},
		}))
		now.Advance(clock.Second)
		produceInOrder(t, ctx, c, queueName, pageSize)
		first := leaseOne(t, ctx, c, queueName)
		require.Equal(t, "retried", first.Reference)
		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Items:     []*pb.QueueRetryItem{{Id: first.Id}},
		}))
		leased := leaseUpTo(t, ctx, c, queueName, pageSize)
		require.Len(t, leased, pageSize)
		require.NotContains(t, leased, "retried")

		now.Advance(10 * clock.Second)
		second := leaseOne(t, ctx, c, queueName)
		require.Equal(t, "retried", second.Reference)

		// The expired leases on the page start a pass. Between its pages "retried" is retried
		// again, which moves it past the item the pass ends at, so the pass never reads it.
		var retryErr error
		spy.HoldFirstPage()
		defer spy.Release()
		now.Advance(55 * clock.Second)
		spy.WhileFirstPageHeld(t, ctx, func() {
			retryErr = c.QueueRetry(ctx, &pb.QueueRetryRequest{
				QueueName: queueName,
				Items:     []*pb.QueueRetryItem{{Id: second.Id}},
			})
		})
		waitForPageWritten(t, ctx, c, queueName)
		require.NoError(t, retryErr)

		events := spy.Events()
		require.GreaterOrEqual(t, len(events), 3)
		assert.Equal(t, []string{"page:start", "retry", "page:resume"}, events[:3])

		// "retried" is removed when its own expire deadline arrives, before the page's.
		now.Advance(8*clock.Minute + 54*clock.Second)
		assert.Eventually(t, func() bool {
			return partitionTotal(t, ctx, c, queueName) == pageSize
		}, 5*clock.Second, 20*clock.Millisecond)
	})

	t.Run("PauseDuringScheduledPassLosesNoWork", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{watchScheduled: true}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		produceScheduled(t, ctx, c, queueName, largePartition, now.Now().Add(clock.Minute))

		var pauseErr error
		spy.HoldFirstPage()
		defer spy.Release()
		now.Advance(clock.Minute + clock.Second)
		spy.WhileFirstPageHeld(t, ctx, func() {
			pauseErr = d.Service().PauseQueue(ctx, queueName)
		})
		require.NoError(t, pauseErr)

		// While paused, the scheduled pass stops after its first page. The stats reply comes from
		// the request loop, which would run the next page first if the pause did not hold it.
		nextLifecycleRun(t, ctx, c, queueName)
		assert.Equal(t, 1, spy.Pages())

		require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
		require.Eventually(t, func() bool {
			return scheduledCount(ctx, c, queueName) == 0
		}, 5*clock.Second, 20*clock.Millisecond)
		assert.Equal(t, []string{"page:start", "page:resume"}, spy.Events())
		assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)
	})

	t.Run("ReloadDuringScheduledPassStartsAgain", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{watchScheduled: true}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		produceScheduled(t, ctx, c, queueName, largePartition, now.Now().Add(clock.Minute))

		var reloadErr error
		spy.HoldFirstPage()
		defer spy.Release()
		now.Advance(clock.Minute + clock.Second)
		spy.WhileFirstPageHeld(t, ctx, func() {
			reloadErr = c.QueueReload(ctx, &pb.QueueReloadRequest{QueueName: queueName})
		})
		require.NoError(t, reloadErr)
		require.Eventually(t, func() bool {
			return scheduledCount(ctx, c, queueName) == 0
		}, 5*clock.Second, 20*clock.Millisecond)

		// The reload ends the scheduled pass after its first page, and the next page starts at
		// the first item.
		events := spy.Events()
		require.GreaterOrEqual(t, len(events), 2)
		assert.Equal(t, []string{"page:start", "page:start"}, events[:2])
		assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)
	})

	t.Run("LargePayloadsAreRequeued", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)

		// BadgerDB keeps a value under 1 MiB in the transaction that writes it, and one
		// transaction holds about 10 MiB, so 16 of these do not fit in one write.
		const count, perRequest, payload = 16, 4, 1_000_000
		for done := 0; done < count; done += perRequest {
			items := make([]*pb.QueueProduceItem, perRequest)
			for i := range items {
				items[i] = &pb.QueueProduceItem{
					Reference: fmt.Sprintf("item-%05d", done+i),
					Bytes:     []byte(strings.Repeat("x", payload)),
				}
			}
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          items,
			}))
		}
		require.Len(t, leaseUpTo(t, ctx, c, queueName, count), count)

		now.Advance(clock.Minute + clock.Second)
		waitForPageWritten(t, ctx, c, queueName)
		assert.Len(t, leaseUpTo(t, ctx, c, queueName, count), count)
	})

	t.Run("DeadLetterWriteFailingPartwayMovesEachItemOnce", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		dlqName := random.String("dlq-", 10)
		createOnePartitionQueue(t, ctx, c, dlqName)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			MaxAttempts:         1,
			DeadQueue:           dlqName,
			RequestedPartitions: 1,
		})
		const count = 10
		produceInOrder(t, ctx, c, queueName, count)
		require.Len(t, leaseUpTo(t, ctx, c, queueName, count), count)

		// The first move stores item-00000 in the dead letter queue, says so, and then fails. The
		// source deletes only item-00000, so a consumer that completes it before the next pass does
		// not see it moved again.
		spy.FailNextDeadLetterWriteAfterFirstItem()
		now.Advance(clock.Minute + clock.Second)
		require.Eventually(t, func() bool {
			return partitionTotal(t, ctx, c, queueName) == count-1
		}, 5*clock.Second, 20*clock.Millisecond)
		assert.Equal(t, int32(1), partitionTotal(t, ctx, c, dlqName))

		moved := leaseOne(t, ctx, c, dlqName)
		require.Equal(t, "item-00000", moved.Reference)
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      dlqName,
			RequestTimeout: "5s",
			Ids:            []string{moved.Id},
		}))

		now.Advance(10 * clock.Second)
		require.Eventually(t, func() bool {
			return partitionTotal(t, ctx, c, queueName) == 0
		}, 5*clock.Second, 20*clock.Millisecond)

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &list, &querator.ListOptions{Limit: 2 * count}))
		references := make([]string, 0, len(list.Items))
		for _, item := range list.Items {
			references = append(references, item.Reference)
		}
		assert.ElementsMatch(t, []string{
			"item-00001", "item-00002", "item-00003", "item-00004",
			"item-00005", "item-00006", "item-00007", "item-00008", "item-00009",
		}, references)
	})
}

// produceInOrder produces count items named item-00000, item-00001 and so on, in that order.
func produceInOrder(t *testing.T, ctx context.Context, c *querator.Client, queueName string, count int) {
	t.Helper()
	for done := 0; done < count; done += pageSize {
		items := make([]*pb.QueueProduceItem, min(pageSize, count-done))
		for i := range items {
			items[i] = &pb.QueueProduceItem{
				Reference: fmt.Sprintf("item-%05d", done+i),
				Bytes:     []byte("x"),
			}
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          items,
		}))
	}
}

// produceScheduled produces count items named item-00000, item-00001 and so on, each to be
// enqueued at enqueueAt.
func produceScheduled(t *testing.T, ctx context.Context, c *querator.Client, queueName string, count int, enqueueAt clock.Time) {
	t.Helper()
	for done := 0; done < count; done += pageSize {
		items := make([]*pb.QueueProduceItem, min(pageSize, count-done))
		for i := range items {
			items[i] = &pb.QueueProduceItem{
				Reference: fmt.Sprintf("item-%05d", done+i),
				Bytes:     []byte("x"),
				EnqueueAt: timestamppb.New(enqueueAt),
			}
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          items,
		}))
	}
}

// scheduledCount returns how many scheduled items wait in the queue's only partition. It takes
// no t and returns -1 on any error so it can be polled from require.Eventually.
func scheduledCount(ctx context.Context, c *querator.Client, queueName string) int32 {
	var stats pb.QueueStatsResponse
	if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
		return -1
	}
	if len(stats.LogicalQueues) != 1 || len(stats.LogicalQueues[0].Partitions) != 1 {
		return -1
	}
	return stats.LogicalQueues[0].Partitions[0].Scheduled
}

// leaseUpTo leases until it holds want items or a lease request comes back empty, and returns
// the references it leased.
func leaseUpTo(t *testing.T, ctx context.Context, c *querator.Client, queueName string, want int) []string {
	t.Helper()
	var leased []string
	for range want {
		if len(leased) >= want {
			return leased
		}
		var resp pb.QueueLeaseResponse
		err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "2s",
			QueueName:      queueName,
			BatchSize:      int32(min(pageSize, want-len(leased))),
		}, &resp)
		if err != nil && strings.Contains(err.Error(), "no items are in the queue") {
			return leased
		}
		require.NoError(t, err)
		if len(resp.Items) == 0 {
			return leased
		}
		for _, item := range resp.Items {
			leased = append(leased, item.Reference)
		}
	}
	return leased
}

func partitionTotal(t *testing.T, ctx context.Context, c *querator.Client, queueName string) int32 {
	t.Helper()
	var stats pb.QueueStatsResponse
	require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
	require.Len(t, stats.LogicalQueues, 1)
	require.Len(t, stats.LogicalQueues[0].Partitions, 1)
	return stats.LogicalQueues[0].Partitions[0].Total
}
