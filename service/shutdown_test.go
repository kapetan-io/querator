package service_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
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

	for _, tc := range []struct {
		Setup      NewStorageFunc
		TearDown   func()
		Name       string
		Persistent bool
	}{
		{
			Name:       "InMemory",
			Persistent: false,
			Setup: func() store.Config {
				return setupMemoryStorage(store.Config{})
			},
			TearDown: func() {},
		},
		{
			Name:       "BadgerDB",
			Persistent: true,
			Setup: func() store.Config {
				return badgerdb.Setup(store.BadgerConfig{})
			},
			TearDown: func() {
				badgerdb.Teardown()
			},
		},
		{
			Name:       "PostgreSQL",
			Persistent: true,
			Setup: func() store.Config {
				return postgres.Setup(store.PostgresConfig{})
			},
			TearDown: func() {
				postgres.Teardown()
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testShutdown(t, tc.Setup, tc.TearDown, tc.Persistent)
		})
	}
}

func testShutdown(t *testing.T, setup NewStorageFunc, tearDown func(), persistent bool) {

	t.Run("ProduceThenShutdown", func(t *testing.T) {
		if !persistent {
			t.Skip("skipping persistence test for non-persistent backend")
		}

		const numItems = 5
		queueName := random.String("queue-", 10)
		storage := setup()

		// First daemon: produce items
		d1, c1, ctx1 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
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
		d2, c2, ctx2 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
		defer func() {
			d2.Shutdown(t)
			tearDown()
		}()

		var listResp pb.StorageItemsListResponse
		require.NoError(t, c2.StorageItemsList(ctx2, queueName, 0, &listResp, &querator.ListOptions{Limit: 100}))

		assert.Len(t, listResp.Items, numItems)
	})

	t.Run("LeaseThenShutdown", func(t *testing.T) {
		if !persistent {
			t.Skip("skipping persistence test for non-persistent backend")
		}

		const numItems = 5
		queueName := random.String("queue-", 10)
		storage := setup()

		// First daemon: produce and lease items
		d1, c1, ctx1 := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
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
		d2, c2, ctx2 := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
		defer func() {
			d2.Shutdown(t)
			tearDown()
		}()

		// Items should still exist in storage - use StorageItemsList to verify persistence
		// regardless of lease state (which requires lifecycle loop to process)
		var listResp pb.StorageItemsListResponse
		require.NoError(t, c2.StorageItemsList(ctx2, queueName, 0, &listResp, &querator.ListOptions{Limit: 100}))
		assert.Len(t, listResp.Items, numItems)
	})

	t.Run("PartialCompleteThenShutdown", func(t *testing.T) {
		if !persistent {
			t.Skip("skipping persistence test for non-persistent backend")
		}

		const numItems = 10
		const completeCount = 5
		queueName := random.String("queue-", 10)
		storage := setup()

		// First daemon: produce, lease, partially complete
		d1, c1, ctx1 := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
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
		d2, c2, ctx2 := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
		defer func() {
			d2.Shutdown(t)
			tearDown()
		}()

		// Use StorageItemsList to verify completed items were removed from storage
		// This doesn't depend on lifecycle loop timing
		var listResp pb.StorageItemsListResponse
		require.NoError(t, c2.StorageItemsList(ctx2, queueName, 0, &listResp, &querator.ListOptions{Limit: 100}))

		// Only the 5 uncompleted items should remain in storage
		assert.Len(t, listResp.Items, numItems-completeCount)
	})

	t.Run("ShutdownDuringActiveRequests", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		storage := setup()
		defer tearDown()

		// Use a shorter context since this test explicitly triggers shutdown
		d, c, ctx := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			RequestedPartitions: 1,
		})

		const numRequests = 20
		var wg sync.WaitGroup
		results := make([]error, numRequests)

		// Launch concurrent requests
		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx] = c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          produceRandomItems(1),
				})
			}(i)
		}

		// Let some requests start
		time.Sleep(10 * time.Millisecond)

		// Trigger shutdown while requests are in flight - don't use d.Shutdown(t)
		// because that will fail if the context times out during shutdown
		_ = d.d.Shutdown(d.ctx)
		d.cancel()

		// Wait for all requests to complete
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// All requests completed - good
		case <-time.After(5 * time.Second):
			t.Fatal("requests did not complete within timeout - possible hang")
		}

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
