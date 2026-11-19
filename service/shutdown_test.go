package service_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShutdown(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}
	mongo := mongoTestSetup{}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "BadgerDB",
			Setup: func() store.Config {
				return badgerdb.Setup(store.BadgerConfig{})
			},
			TearDown: func() {
				badgerdb.Teardown()
			},
		},
		{
			Name: "PostgreSQL",
			Setup: func() store.Config {
				return postgres.Setup(store.PostgresConfig{})
			},
			TearDown: func() {
				postgres.Teardown()
			},
		},
		{
			Name: "MongoDB",
			Setup: func() store.Config {
				return mongo.Setup(store.MongoConfig{})
			},
			TearDown: func() {
				mongo.Teardown()
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testShutdown(t, tc.Setup, tc.TearDown)
		})
	}
}

func testShutdown(t *testing.T, setup NewStorageFunc, tearDown func()) {

	t.Run("ProduceThenShutdown", func(t *testing.T) {

		const numItems = 5
		queueName := random.String("queue-", 10)
		storage := setup()

		// First daemon: produce items
		d1 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
		c1, ctx1 := d1.MustClient(), d1.Context()
		createQueueAndWait(t, ctx1, c1, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 1,
		})

		require.NoError(t, c1.QueueProduce(ctx1, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(numItems),
		}))

		d1.Shutdown(t)

		// Second daemon: verify items persisted
		d2 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
		defer func() {
			d2.Shutdown(t)
			tearDown()
		}()
		c2, ctx2 := d2.MustClient(), d2.Context()

		var listResp pb.StorageItemsListResponse
		require.NoError(t, c2.StorageItemsList(ctx2, queueName, 0, &listResp, &querator.ListOptions{Limit: 100}))

		assert.Len(t, listResp.Items, numItems)
	})

	t.Run("LeaseThenShutdown", func(t *testing.T) {
		const numItems = 5
		queueName := random.String("queue-", 10)
		storage := setup()

		// First daemon: produce and lease items
		d1 := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
		c1, ctx1 := d1.MustClient(), d1.Context()
		createQueueAndWait(t, ctx1, c1, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 1,
		})

		require.NoError(t, c1.QueueProduce(ctx1, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(numItems),
		}))

		var leaseResp pb.QueueLeaseResponse
		require.NoError(t, c1.QueueLease(ctx1, &pb.QueueLeaseRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			BatchSize:      int32(numItems),
			ClientId:       random.String("client-", 10),
		}, &leaseResp))
		require.Len(t, leaseResp.Items, numItems)

		d1.Shutdown(t)

		// Second daemon: verify items still exist in storage (leased state persists)
		d2 := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
		defer func() {
			d2.Shutdown(t)
			tearDown()
		}()
		c2, ctx2 := d2.MustClient(), d2.Context()

		// Items should still exist in storage - use StorageItemsList to verify persistence
		// regardless of lease state (which requires lifecycle loop to process)
		var listResp pb.StorageItemsListResponse
		require.NoError(t, c2.StorageItemsList(ctx2, queueName, 0, &listResp, &querator.ListOptions{Limit: 100}))
		assert.Len(t, listResp.Items, numItems)
	})

	t.Run("PartialCompleteThenShutdown", func(t *testing.T) {
		const numItems = 10
		const completeCount = 5
		queueName := random.String("queue-", 10)
		storage := setup()

		// First daemon: produce, lease, partially complete
		d1 := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
		c1, ctx1 := d1.MustClient(), d1.Context()
		createQueueAndWait(t, ctx1, c1, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 1,
		})

		require.NoError(t, c1.QueueProduce(ctx1, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(numItems),
		}))

		var leaseResp pb.QueueLeaseResponse
		require.NoError(t, c1.QueueLease(ctx1, &pb.QueueLeaseRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			BatchSize:      int32(numItems),
			ClientId:       random.String("client-", 10),
		}, &leaseResp))
		require.Len(t, leaseResp.Items, numItems)

		// Complete only first 5 items
		idsToComplete := make([]string, completeCount)
		for i := 0; i < completeCount; i++ {
			idsToComplete[i] = leaseResp.Items[i].Id
		}

		require.NoError(t, c1.QueueComplete(ctx1, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Ids:            idsToComplete,
		}))

		d1.Shutdown(t)

		// Second daemon: verify only uncompleted items remain in storage
		d2 := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
		defer func() {
			d2.Shutdown(t)
			tearDown()
		}()
		c2, ctx2 := d2.MustClient(), d2.Context()

		// Use StorageItemsList to verify completed items were removed from storage
		// This doesn't depend on lifecycle loop timing
		var listResp pb.StorageItemsListResponse
		require.NoError(t, c2.StorageItemsList(ctx2, queueName, 0, &listResp, &querator.ListOptions{Limit: 100}))

		// Only the 5 uncompleted items should remain in storage
		assert.Len(t, listResp.Items, numItems-completeCount)
	})

	t.Run("PauseThenShutdown", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		storage := setup()
		defer tearDown()

		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})

		defer d.cancel()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 1,
		})

		require.NoError(t, d.Service().PauseQueue(ctx, queueName))

		// Shutdown must complete while the queue is paused
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, d.d.Shutdown(shutdownCtx))
	})

	t.Run("PauseWithWaitingProduceThenShutdown", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		storage := setup()
		defer tearDown()

		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})

		defer d.cancel()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 1,
		})

		require.NoError(t, d.Service().PauseQueue(ctx, queueName))

		var produceErr error
		returned := make(chan struct{})
		backgroundCall{
			Call: func() {
				produceErr = c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "20s",
					Items:          produceRandomItems(1),
				})
				close(returned)
			},
			While: func() {
				// Wait until the paused request loop has consumed the produce request
				require.Eventually(t, func() bool {
					var resp pb.QueueStatsResponse
					require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &resp))
					return resp.LogicalQueues[0].ProduceWaiting == 1
				}, 10*time.Second, 100*time.Millisecond)

				// Shutdown must complete while the queue is paused with a waiting producer
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				require.NoError(t, d.d.Shutdown(shutdownCtx))

				select {
				case <-returned:
				case <-time.After(5 * time.Second):
					t.Fatal("produce request did not return after shutdown")
				}
			},
		}.run()
		require.Error(t, produceErr)
	})

	t.Run("PauseWithWaitingHotRequestsThenShutdown", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		storage := setup()
		defer tearDown()

		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
		defer d.cancel()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 1,
		})

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(2),
		}))

		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      2,
		}, &lease))
		require.Len(t, lease.Items, 2)

		require.NoError(t, d.Service().PauseQueue(ctx, queueName))

		heldCtx, cancelHeld := context.WithCancel(ctx)
		defer cancelHeld()

		errs := make([]error, 4)
		duringCalls(cancelHeld, func() {
			// Wait until the paused request loop holds all four requests
			require.Eventually(t, func() bool {
				var resp pb.QueueStatsResponse
				require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &resp))
				return resp.LogicalQueues[0].InFlight == 4
			}, 10*time.Second, 100*time.Millisecond)

			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			require.NoError(t, d.d.Shutdown(shutdownCtx))
		}, func() {
			errs[0] = c.QueueProduce(heldCtx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "20s",
				Items:          produceRandomItems(1),
			})
		}, func() {
			errs[1] = c.QueueLease(heldCtx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "20s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &pb.QueueLeaseResponse{})
		}, func() {
			errs[2] = c.QueueComplete(heldCtx, &pb.QueueCompleteRequest{
				QueueName:      queueName,
				Partition:      lease.Partition,
				RequestTimeout: "20s",
				Ids:            []string{lease.Items[0].Id},
			})
		}, func() {
			errs[3] = c.QueueRetry(heldCtx, &pb.QueueRetryRequest{
				QueueName: queueName,
				Partition: lease.Partition,
				Items:     []*pb.QueueRetryItem{{Id: lease.Items[1].Id}},
			})
		})

		// Every held request is cancelled, none is applied
		for _, err := range errs {
			assert.ErrorContains(t, err, "queue is shutting down")
		}

		// A second daemon sees the two leased items untouched
		d2 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
		defer d2.Shutdown(t)
		c2, ctx2 := d2.MustClient(), d2.Context()

		var list pb.StorageItemsListResponse
		require.NoError(t, c2.StorageItemsList(ctx2, queueName, 0, &list, &querator.ListOptions{Limit: 100}))
		require.Len(t, list.Items, 2)
		for _, item := range list.Items {
			assert.True(t, item.IsLeased)
		}
	})

	t.Run("ShutdownDuringUnpauseDrain", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		storage := setup()
		defer tearDown()

		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})

		defer d.cancel()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 1,
		})

		require.NoError(t, d.Service().PauseQueue(ctx, queueName))

		// Buffer enough produce requests that the un-pause drain takes long enough
		// for Shutdown to land while the request loop is still mid-request.
		const numProducers = 10
		const numItems = 1_000
		produceErrs := make([]error, numProducers)
		returned := make(chan struct{})
		backgroundCall{
			Call: func() {
				concurrently(numProducers, func(i int) {
					produceErrs[i] = c.QueueProduce(ctx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "20s",
						Items:          produceRandomItems(numItems),
					})
				})
				close(returned)
			},
			While: func() {
				require.Eventually(t, func() bool {
					var resp pb.QueueStatsResponse
					require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &resp))
					return resp.LogicalQueues[0].ProduceWaiting == numProducers
				}, 10*time.Second, 100*time.Millisecond)

				// ResumeQueue returns as soon as the loop accepts the resume; the loop
				// then applies the buffered requests to storage before it selects again.
				require.NoError(t, d.Service().ResumeQueue(ctx, queueName))

				// Shutdown must be delivered to the loop after the drain instead of the loop
				// exiting on the shutdown flag alone and leaving the partitions open.
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				require.NoError(t, d.d.Shutdown(shutdownCtx))

				select {
				case <-returned:
				case <-time.After(5 * time.Second):
					t.Fatal("produce requests did not return after shutdown")
				}
			},
		}.run()

		// The drain completed before shutdown was handled, so every producer succeeded
		for _, err := range produceErrs {
			require.NoError(t, err)
		}
	})

	t.Run("ShutdownAnswersStatsRequestsWaitingBehindDrain", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		storage := setup()
		defer tearDown()

		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
		defer d.cancel()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 1,
		})

		require.NoError(t, d.Service().PauseQueue(ctx, queueName))

		const numProducers = 10
		const numStats = 8
		statsErrs := make([]error, numStats)
		produceDone := make(chan struct{})
		statsDone := make(chan struct{})
		statsCtx, cancelStats := context.WithTimeout(ctx, 30*time.Second)
		defer cancelStats()

		go func() {
			defer close(produceDone)
			concurrently(numProducers, func(int) {
				_ = c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "20s",
					Items:          produceRandomItems(1_000),
				})
			})
		}()

		require.Eventually(t, func() bool {
			var resp pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &resp))
			return resp.LogicalQueues[0].ProduceWaiting == numProducers
		}, 10*time.Second, 100*time.Millisecond)

		// The loop applies the buffered produces after the resume is accepted, so stats
		// requests sent now wait in the request channel behind that work.
		require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
		go func() {
			defer close(statsDone)
			concurrently(numStats, func(i int) {
				statsErrs[i] = c.QueueStats(statsCtx, &pb.QueueStatsRequest{QueueName: queueName},
					&pb.QueueStatsResponse{})
			})
		}()

		// Stats has no observable "queued" state, so give the requests time to reach the channel.
		time.Sleep(300 * time.Millisecond)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, d.d.Shutdown(shutdownCtx))

		select {
		case <-statsDone:
		case <-time.After(5 * time.Second):
			t.Fatal("stats requests did not return after shutdown")
		}
		<-produceDone

		for _, err := range statsErrs {
			if err != nil {
				assert.ErrorContains(t, err, "is shutting down")
			}
		}
	})

	t.Run("ShutdownDuringActiveRequests", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		storage := setup()
		defer tearDown()

		// Use a shorter context since this test explicitly triggers shutdown
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
		c, ctx := d.MustClient(), d.Context()
		// Ensure cleanup even if test fails early - use fresh context since d.ctx may be canceled
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = d.d.Shutdown(cleanupCtx)
		}()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 1,
		})

		const numRequests = 20
		results := make([]error, numRequests)
		done := make(chan struct{})

		backgroundCall{
			// Launch concurrent requests
			Call: func() {
				concurrently(numRequests, func(i int) {
					results[i] = c.QueueProduce(ctx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "5s",
						Items:          produceRandomItems(1),
					})
				})
				close(done)
			},
			While: func() {
				// Let some requests start
				time.Sleep(10 * time.Millisecond)

				// Trigger shutdown while requests are in flight - don't use d.Shutdown(t)
				// because that will fail if the context times out during shutdown
				if err := d.d.Shutdown(d.ctx); err != nil {
					t.Logf("shutdown while requests are in flight: %v", err)
				}
				d.cancel()

				// Wait for all requests to complete
				select {
				case <-done:
					// All requests completed - good
				case <-time.After(5 * time.Second):
					t.Fatal("requests did not complete within timeout - possible hang")
				}
			},
		}.run()

		// Verify at least some requests received shutdown errors
		shutdownErrCount := 0
		successCount := 0
		for _, err := range results {
			if err == nil {
				successCount++
			} else {
				shutdownErrCount++
				errStr := err.Error()
				isShutdown := strings.Contains(errStr, "shutting down") ||
					strings.Contains(errStr, "context canceled") ||
					strings.Contains(errStr, "EOF") ||
					strings.Contains(errStr, "connection reset by peer")
				assert.True(t, isShutdown, "expected shutdown error, got: %v", err)
			}
		}

		// Some requests succeeded before shutdown, others got shutdown errors
		t.Logf("Results: %d succeeded, %d shutdown errors", successCount, shutdownErrCount)
	})
}

// TestShutdownWithAdminRequestWaiting sits outside TestShutdown because it wraps the in-memory
// partition store so a storage list can hold the request loop while other requests queue behind
// it. When the loop is released, both the queued requests and the shutdown are ready and Go picks
// one at random, so the scenario runs several times to reach the shutdown-first order.
func TestShutdownWithAdminRequestWaiting(t *testing.T) {
	const attempts = 10
	for range attempts {
		shutdownWithAdminRequestWaiting(t)
	}
}

func shutdownWithAdminRequestWaiting(t *testing.T) {
	const settle = 100 * clock.Millisecond
	entered, release := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool

	conf := setupMemoryStorage(store.Config{})
	conf.PartitionStorage[0].PartitionStore = heldListStore{
		PartitionStore: conf.PartitionStorage[0].PartitionStore,
		hold: func(ctx context.Context) error {
			if !armed.CompareAndSwap(true, false) {
				return nil
			}
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
	c, ctx := d.MustClient(), d.Context()
	var calls sync.WaitGroup
	defer func() {
		d.cancel()
		calls.Wait()
	}()

	queueName := random.String("queue-", 10)
	createQueueAndWait(t, ctx, c, &pb.QueueInfo{
		QueueName:           queueName,
		LeaseTimeout:        "1m0s",
		ExpireTimeout:       "24h0m0s",
		RequestedPartitions: 1,
	})
	armed.Store(true)

	var listErr, statsErr, produceErr, shutdownErr error
	calls.Go(func() {
		listErr = c.StorageItemsList(ctx, queueName, 0, &pb.StorageItemsListResponse{}, &querator.ListOptions{Limit: 10})
	})
	select {
	case <-entered:
	case <-ctx.Done():
		require.FailNow(t, "storage list never reached the partition")
	}

	// The loop is held, so the stats request waits in the queue ahead of the produce, and the
	// shutdown waits behind both.
	calls.Go(func() {
		statsErr = c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &pb.QueueStatsResponse{})
	})
	clock.Sleep(settle)
	calls.Go(func() {
		produceErr = c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(1),
		})
	})
	clock.Sleep(settle)
	calls.Go(func() {
		shutdownErr = d.d.Shutdown(ctx)
	})
	clock.Sleep(settle)
	close(release)
	calls.Wait()

	require.NoError(t, listErr)
	require.NoError(t, shutdownErr)
	for _, err := range []error{statsErr, produceErr} {
		if err != nil {
			assert.ErrorContains(t, err, "queue is shutting down")
		}
	}
}

type heldListStore struct {
	store.PartitionStore
	hold func(context.Context) error
}

func (s heldListStore) Get(info types.PartitionInfo) store.Partition {
	return heldListPartition{Partition: s.PartitionStore.Get(info), hold: s.hold}
}

type heldListPartition struct {
	store.Partition
	hold func(context.Context) error
}

func (p heldListPartition) List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error {
	if err := p.hold(ctx); err != nil {
		return err
	}
	return p.Partition.List(ctx, items, opts)
}
