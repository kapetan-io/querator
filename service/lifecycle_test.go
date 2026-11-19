package service_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
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

func testLifecycle(t *testing.T, setup NewStorageFunc, tearDown func(), lifetime storageLifetime) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("LargePartition", func(t *testing.T) {
		t.Run("ExpiredLeasesAreRequeued", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
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
			waitForLifecycleRun(t, ctx, c, queueName)
			assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)
		})

		t.Run("ExpiredItemsAreRemoved", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
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

			d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
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

			d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
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

		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
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

	t.Run("RequestsAreServedBetweenPages", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now})
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
		waitForLifecycleRun(t, ctx, c, queueName)
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
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now})
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
		waitForLifecycleRun(t, ctx, c, queueName)
		require.NoError(t, leaseErr)
		require.Len(t, leased.Items, 1)
		require.Equal(t, "item-00001", leased.Items[0].Reference)
		require.Equal(t, 2, spy.Pages())
		// The next run is due when item-00001's lease expires, not later.
		require.LessOrEqual(t, nextLifecycleRun(t, ctx, c, queueName), clock.Minute)

		now.Advance(clock.Minute + clock.Second)
		waitForLifecycleRun(t, ctx, c, queueName)
		assert.Contains(t, leaseUpTo(t, ctx, c, queueName, largePartition), "item-00001")
	})

	t.Run("PauseDuringPassLosesNoWork", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now})
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
		waitForLifecycleRun(t, ctx, c, queueName)
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
			first := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(conf), Clock: now})
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

			second := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: conf, Clock: now})
			defer second.Shutdown(t)
			c, ctx = second.MustClient(), second.Context()
			assert.Len(t, leaseUpTo(t, ctx, c, queueName, largePartition), largePartition)
		})
	}

	t.Run("ReloadDuringPassStartsAgain", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now})
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
		waitForLifecycleRun(t, ctx, c, queueName)
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
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now})
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
			waitForLifecycleRun(t, ctx, c, queueName)
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
		waitForLifecycleRun(t, ctx, c, queueName)
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
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now})
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
		waitForLifecycleRun(t, ctx, c, queueName)
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
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now})
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
		waitForLifecycleRun(t, ctx, c, queueName)
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
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now})
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
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(setup()), Clock: now})
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

		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
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
		waitForLifecycleRun(t, ctx, c, queueName)
		assert.Len(t, leaseUpTo(t, ctx, c, queueName, count), count)
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

// lifecycleSpy wraps a partition store. It records each lifecycle page read and each produce,
// complete and retry in the order the partition sees them, can hold the first page of a pass
// until the test releases it, and can make the lifecycle's reads fail. With watchScheduled set
// it records and holds the scheduled pass's pages instead of the lifecycle's.
type lifecycleSpy struct {
	mu             sync.Mutex
	watchScheduled bool
	armed          bool
	reached        chan struct{}
	release        chan struct{}
	released       sync.Once
	events         []string
	actions        []int
	pages          int
	failScans      atomic.Bool
}

func (s *lifecycleSpy) Wrap(conf store.Config) store.Config {
	for i := range conf.PartitionStorage {
		conf.PartitionStorage[i].PartitionStore = spyStore{PartitionStore: conf.PartitionStorage[i].PartitionStore, spy: s}
	}
	return conf
}

// HoldFirstPage starts recording and makes the next page read wait until Release.
func (s *lifecycleSpy) HoldFirstPage() {
	defer s.mu.Unlock()
	s.mu.Lock()
	s.armed = true
	s.reached = make(chan struct{})
	s.release = make(chan struct{})
}

// WaitForFirstPage returns once the lifecycle is waiting to read the held page.
func (s *lifecycleSpy) WaitForFirstPage(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-s.reached:
	case <-ctx.Done():
		require.FailNow(t, "the lifecycle never read its first page")
	}
}

// WhileFirstPageHeld runs call while the lifecycle waits to read the held page, gives the request
// call makes time to reach the queue, then lets the pass go on. The goroutine starts in a named
// function because Tiger's TS-C02 does not see a Wait that follows Go inside a function literal.
func (s *lifecycleSpy) WhileFirstPageHeld(t *testing.T, ctx context.Context, call func()) {
	t.Helper()
	s.WaitForFirstPage(t, ctx)
	var calls sync.WaitGroup
	calls.Go(call)
	clock.Sleep(requestSettle)
	s.Release()
	calls.Wait()
}

func (s *lifecycleSpy) Release() {
	s.released.Do(func() { close(s.release) })
}

func (s *lifecycleSpy) FailScans() {
	s.failScans.Store(true)
}

func (s *lifecycleSpy) StopFailingScans() {
	s.failScans.Store(false)
}

func (s *lifecycleSpy) Events() []string {
	defer s.mu.Unlock()
	s.mu.Lock()
	return append([]string(nil), s.events...)
}

func (s *lifecycleSpy) ActionCounts() []int {
	defer s.mu.Unlock()
	s.mu.Lock()
	return append([]int(nil), s.actions...)
}

func (s *lifecycleSpy) Pages() int {
	defer s.mu.Unlock()
	s.mu.Lock()
	return s.pages
}

func (s *lifecycleSpy) beforePage(ctx context.Context, scan *types.LifeCycleScan) {
	s.mu.Lock()
	if !s.armed {
		s.mu.Unlock()
		return
	}
	s.pages++
	first := s.pages == 1
	if scan.MidPass() {
		s.events = append(s.events, "page:resume")
	} else {
		s.events = append(s.events, "page:start")
	}
	s.mu.Unlock()
	if first {
		close(s.reached)
		select {
		case <-s.release:
		case <-ctx.Done():
		}
	}
}

func (s *lifecycleSpy) afterPage(scan *types.LifeCycleScan) {
	defer s.mu.Unlock()
	s.mu.Lock()
	if s.armed {
		s.actions = append(s.actions, len(scan.Actions))
	}
}

func (s *lifecycleSpy) produce() {
	s.record("produce")
}

func (s *lifecycleSpy) record(event string) {
	defer s.mu.Unlock()
	s.mu.Lock()
	if s.armed {
		s.events = append(s.events, event)
	}
}

type spyStore struct {
	store.PartitionStore
	spy *lifecycleSpy
}

func (s spyStore) Get(info types.PartitionInfo) store.Partition {
	return spyPartition{Partition: s.PartitionStore.Get(info), spy: s.spy}
}

type spyPartition struct {
	store.Partition
	spy *lifecycleSpy
}

func (p spyPartition) ScanForActions(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	if p.spy.failScans.Load() {
		return errors.New("scan failed by test")
	}
	if p.spy.watchScheduled {
		return p.Partition.ScanForActions(ctx, scan, now)
	}
	p.spy.beforePage(ctx, scan)
	if err := p.Partition.ScanForActions(ctx, scan, now); err != nil {
		return err
	}
	p.spy.afterPage(scan)
	return nil
}

func (p spyPartition) Produce(ctx context.Context, batch types.ProduceBatch, now clock.Time) error {
	p.spy.produce()
	return p.Partition.Produce(ctx, batch, now)
}

func (p spyPartition) ScanForScheduled(ctx context.Context, scan *types.LifeCycleScan, now clock.Time) error {
	if !p.spy.watchScheduled {
		return p.Partition.ScanForScheduled(ctx, scan, now)
	}
	p.spy.beforePage(ctx, scan)
	if err := p.Partition.ScanForScheduled(ctx, scan, now); err != nil {
		return err
	}
	p.spy.afterPage(scan)
	return nil
}

func (p spyPartition) Complete(ctx context.Context, batch types.CompleteBatch) error {
	if len(batch.Requests) != 0 {
		p.spy.record("complete")
	}
	return p.Partition.Complete(ctx, batch)
}

func (p spyPartition) Retry(ctx context.Context, batch types.RetryBatch) error {
	if len(batch.Requests) != 0 {
		p.spy.record("retry")
	}
	return p.Partition.Retry(ctx, batch)
}
