package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/duh-rpc/duh-go/retry"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRetry(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}
	mongo := mongoTestSetup{}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
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
		// {
		// 	Name: "SurrealDB",
		// },
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testRetry(t, tc.Setup, tc.TearDown)
		})
	}
}

func testRetry(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("QueueRetry", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		// Create a queue
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})

		// Produce items for retry testing
		ref1 := random.String("ref-", 10)
		ref2 := random.String("ref-", 10)
		ref3 := random.String("ref-", 10)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{
				{
					Reference: ref1,
					Encoding:  "utf8",
					Kind:      "immediate-retry",
					Bytes:     []byte("immediate retry test"),
				},
				{
					Reference: ref2,
					Encoding:  "utf8",
					Kind:      "scheduled-retry",
					Bytes:     []byte("scheduled retry test"),
				},
				{
					Reference: ref3,
					Encoding:  "utf8",
					Kind:      "dead-letter",
					Bytes:     []byte("dead letter test"),
				},
			},
		}))

		// Lease the items
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      3,
		}, &lease))

		require.Equal(t, 3, len(lease.Items))
		item1 := findInLeaseResp(ref1, &lease)
		item2 := findInLeaseResp(ref2, &lease)
		item3 := findInLeaseResp(ref3, &lease)
		require.NotNil(t, item1)
		require.NotNil(t, item2)
		require.NotNil(t, item3)

		t.Run("ImmediateRetry", func(t *testing.T) {
			// Retry item1 immediately (no RetryAt timestamp)
			require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
				QueueName: queueName,
				Partition: lease.Partition,
				Items: []*pb.QueueRetryItem{
					{
						Id:   item1.Id,
						Dead: false,
						// No RetryAt means immediate retry
					},
				},
			}))

			// Verify item is immediately available for lease again with incremented attempts
			var retryLease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &retryLease))

			require.Equal(t, 1, len(retryLease.Items))
			retriedItem := retryLease.Items[0]
			assert.Equal(t, ref1, retriedItem.Reference)
			assert.Equal(t, int32(2), retriedItem.Attempts)

			// Complete the retried item to clean up
			require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				QueueName:      queueName,
				Partition:      retryLease.Partition,
				RequestTimeout: "5s",
				Ids:            []string{retriedItem.Id},
			}))
		})

		t.Run("DeadLetter", func(t *testing.T) {
			// Mark item3 as dead
			require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
				QueueName: queueName,
				Partition: lease.Partition,
				Items: []*pb.QueueRetryItem{
					{
						Id:   item3.Id,
						Dead: true,
					},
				},
			}))

			// Verify item is no longer available for lease (dead items are removed)
			var noLease pb.QueueLeaseResponse
			err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "1s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &noLease)
			require.Error(t, err)
			var e duh.Error
			require.True(t, errors.As(err, &e))
			assert.Equal(t, duh.CodeRetryRequest, e.Code())
		})
	})

	t.Run("MaxAttemptsFinalLeaseCanBeRetried", func(t *testing.T) {
		// Test that an item that reached MaxAttempts but still under lease
		// can be retried even if lifecycle processing is triggered by another item
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		dlqName := random.String("dlq-", 10)
		queueName := random.String("queue-", 10)
		d := newDaemon(t, 60*clock.Second, svc.Config{
			StorageConfig: setup(),
			Clock:         now,
		})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           dlqName,
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			RequestedPartitions: 1,
		})
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "5m0s",
			ExpireTimeout:       "1h0m0s",
			RequestedPartitions: 1,
			MaxAttempts:         2,
			DeadQueue:           dlqName,
		})

		ref := random.String("ref-", 10)
		triggerRef := random.String("trigger-", 10)

		// Produce first item and lease it
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{{
				Reference: ref,
				Encoding:  "test-encoding",
				Kind:      "test-kind",
				Bytes:     []byte("final lease retry"),
			}},
		}))

		firstLease := leaseOne(t, ctx, c, queueName)
		require.Equal(t, ref, firstLease.Reference)
		require.Equal(t, int32(1), firstLease.Attempts)

		now.Advance(1 * clock.Minute)
		// Produce a second item
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{{
				Reference: triggerRef,
				Encoding:  "test-encoding",
				Kind:      "lifecycle-trigger",
				Bytes:     []byte("trigger lifecycle scan"),
			}},
		}))

		// Expire first item lease and wait for lifecycle to re-queue it
		now.Advance(4*clock.Minute + clock.Second)
		waitForLifecycleRun(t, ctx, c, queueName)
		require.Eventually(t, func() bool {
			return isUnLeased(ctx, c, queueName, itemReference(ref))
		}, 5*clock.Second, 20*clock.Millisecond)

		// Lease second item
		triggerLease := leaseOne(t, ctx, c, queueName)
		require.Equal(t, triggerRef, triggerLease.Reference)
		require.Equal(t, int32(1), triggerLease.Attempts)

		// Lease first item for its final attempt
		now.Advance(1 * clock.Minute)
		var finalLeaseResponse pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &finalLeaseResponse))
		require.Len(t, finalLeaseResponse.Items, 1)
		finalLease := finalLeaseResponse.Items[0]
		require.Equal(t, ref, finalLease.Reference)
		require.Equal(t, int32(2), finalLease.Attempts)

		// Second item lease expires, triggering lifecycle scan
		now.Advance(4*clock.Minute + clock.Second)
		waitForLifecycleRun(t, ctx, c, queueName)
		require.Eventually(t, func() bool {
			return isUnLeased(ctx, c, queueName, itemReference(triggerRef))
		}, 5*clock.Second, 20*clock.Millisecond)

		// Verify first item is still leased
		var sourceResp pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, int(finalLeaseResponse.Partition), &sourceResp, nil))
		item := findInStorageList(ref, &sourceResp)
		require.NotNil(t, item)
		assert.True(t, item.IsLeased)
		assert.True(t, item.LeaseDeadline.AsTime().After(now.Now()))
		assert.Equal(t, int32(2), item.Attempts)

		// Verify first item was not incorrectly moved to the DLQ
		var dlqResp pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
		assert.Empty(t, dlqResp.Items)

		// Verify first item can be retried
		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: finalLeaseResponse.Partition,
			Items:     []*pb.QueueRetryItem{{Id: finalLease.Id}},
		}))

		// Verify the first item was successfully requeued
		require.NoError(t, c.StorageItemsList(ctx, queueName, int(finalLeaseResponse.Partition), &sourceResp, nil))
		item = findInStorageList(ref, &sourceResp)
		require.NotNil(t, item)
		assert.False(t, item.IsLeased)
		assert.Equal(t, int32(2), item.Attempts)

		require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
		assert.Empty(t, dlqResp.Items)
	})

	t.Run("ScheduledRetry", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 60*clock.Second, svc.Config{
			StorageConfig: setup(),
			Clock:         now,
		})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			RequestedPartitions: 1,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			QueueName:           queueName,
		})

		// Produce an item to retry with scheduled time
		reference := random.String("ref-", 10)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items: []*pb.QueueProduceItem{
				{
					Reference: reference,
					Encoding:  "utf8",
					Kind:      "scheduled-retry-test",
					Bytes:     []byte("scheduled retry lifecycle test"),
				},
			},
		}))

		// Lease the item
		var initialLease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &initialLease))
		require.Len(t, initialLease.Items, 1)
		leasedItem := initialLease.Items[0]
		assert.Equal(t, int32(1), leasedItem.Attempts)

		// Retry with future RetryAt
		retryAt := now.Now().Add(1 * clock.Minute)
		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: initialLease.Partition,
			Items: []*pb.QueueRetryItem{
				{
					Id:      leasedItem.Id,
					RetryAt: timestamppb.New(retryAt),
				},
			},
		}))

		// Verify item appears in StorageScheduledList
		var scheduledList pb.StorageItemsListResponse
		err := c.StorageScheduledList(ctx, queueName, 0, &scheduledList, &querator.ListOptions{Limit: 10})
		require.NoError(t, err)
		require.Len(t, scheduledList.Items, 1)

		// Verify item does NOT appear in StorageItemsList
		var itemsList pb.StorageItemsListResponse
		err = c.StorageItemsList(ctx, queueName, 0, &itemsList, nil)
		require.NoError(t, err)
		require.Empty(t, itemsList.Items)

		// Advance clock past RetryAt time
		now.Advance(2 * clock.Minute)

		// Wait for item to appear in StorageItemsList
		retryPolicy := retry.Policy{Interval: retry.Sleep(100 * clock.Millisecond), Attempts: 50}
		err = retry.On(ctx, retryPolicy, func(ctx context.Context, i int) error {
			var resp pb.StorageItemsListResponse
			if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
				return err
			}
			if len(resp.Items) != 1 {
				return fmt.Errorf("expected 1 item, got %d", len(resp.Items))
			}
			return nil
		})
		require.NoError(t, err)

		// Verify item removed from StorageScheduledList
		err = c.StorageScheduledList(ctx, queueName, 0, &scheduledList, nil)
		require.NoError(t, err)
		require.Empty(t, scheduledList.Items)

		// Lease item again and verify Attempts incremented
		var retryLease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &retryLease))
		require.Len(t, retryLease.Items, 1)
		retriedItem := retryLease.Items[0]
		assert.Equal(t, reference, retriedItem.Reference)
		assert.Equal(t, int32(2), retriedItem.Attempts)
	})

	t.Run("RetryWithPastTimestamp", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 60*clock.Second, svc.Config{
			StorageConfig: setup(),
			Clock:         now,
		})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			RequestedPartitions: 1,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			QueueName:           queueName,
		})

		// Produce an item
		reference := random.String("ref-", 10)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items: []*pb.QueueProduceItem{
				{
					Reference: reference,
					Encoding:  "utf8",
					Kind:      "past-retry-test",
					Bytes:     []byte("past retry timestamp test"),
				},
			},
		}))

		// Lease the item
		var initialLease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &initialLease))
		require.Len(t, initialLease.Items, 1)
		leasedItem := initialLease.Items[0]

		// Retry with past RetryAt timestamp
		retryAt := now.Now().Add(-1 * clock.Minute)
		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: initialLease.Partition,
			Items: []*pb.QueueRetryItem{
				{
					Id:      leasedItem.Id,
					RetryAt: timestamppb.New(retryAt),
				},
			},
		}))

		// Verify item NOT in StorageScheduledList
		var scheduledList pb.StorageItemsListResponse
		err := c.StorageScheduledList(ctx, queueName, 0, &scheduledList, nil)
		require.NoError(t, err)
		require.Empty(t, scheduledList.Items)

		// Verify item immediately available for lease
		var retryLease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &retryLease))
		require.Len(t, retryLease.Items, 1)

		// Verify Attempts incremented
		retriedItem := retryLease.Items[0]
		assert.Equal(t, reference, retriedItem.Reference)
		assert.Equal(t, int32(2), retriedItem.Attempts)
	})

	t.Run("RetryMovesToTail", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})

		// Produce three items A, B, C in FIFO order
		refA := random.String("ref-A-", 10)
		refB := random.String("ref-B-", 10)
		refC := random.String("ref-C-", 10)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{
				{Reference: refA, Encoding: "utf8", Kind: "tail-test", Bytes: []byte("item-a")},
				{Reference: refB, Encoding: "utf8", Kind: "tail-test", Bytes: []byte("item-b")},
				{Reference: refC, Encoding: "utf8", Kind: "tail-test", Bytes: []byte("item-c")},
			},
		}))

		// Lease batch of 1 — receive A (first item in FIFO order)
		var firstLease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &firstLease))

		require.Len(t, firstLease.Items, 1)
		itemA := firstLease.Items[0]
		assert.Equal(t, refA, itemA.Reference)
		originalIDofA := itemA.Id

		// Retry A immediately (no RetryAt)
		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: firstLease.Partition,
			Items: []*pb.QueueRetryItem{
				{
					Id:   itemA.Id,
					Dead: false,
				},
			},
		}))

		// Lease batch of 3 — order must be B, C, A (A moved to tail)
		var secondLease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      3,
		}, &secondLease))

		require.Len(t, secondLease.Items, 3)
		assert.Equal(t, refB, secondLease.Items[0].Reference)
		assert.Equal(t, refC, secondLease.Items[1].Reference)
		assert.Equal(t, refA, secondLease.Items[2].Reference)

		// Verify A has a new ID and Attempts == 2
		retriedA := secondLease.Items[2]
		assert.NotEqual(t, originalIDofA, retriedA.Id)
		assert.Equal(t, int32(2), retriedA.Attempts)
	})

	t.Run("LeaseExpiryMovesToTail", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 60*clock.Second, svc.Config{
			StorageConfig: setup(),
			Clock:         now,
		})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        "300ms",
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})

		// Produce three items A, B, C in FIFO order
		refA := random.String("ref-A-", 10)
		refB := random.String("ref-B-", 10)
		refC := random.String("ref-C-", 10)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{
				{Reference: refA, Encoding: "utf8", Kind: "expiry-test", Bytes: []byte("item-a")},
				{Reference: refB, Encoding: "utf8", Kind: "expiry-test", Bytes: []byte("item-b")},
				{Reference: refC, Encoding: "utf8", Kind: "expiry-test", Bytes: []byte("item-c")},
			},
		}))

		// Lease batch of 1 — receive A, store original ID
		var firstLease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &firstLease))

		require.Len(t, firstLease.Items, 1)
		assert.Equal(t, refA, firstLease.Items[0].Reference)
		originalIDofA := firstLease.Items[0].Id

		// Advance the clock past the lease timeout to trigger lease expiry
		now.Advance(2 * clock.Second)

		// Poll until A is no longer leased AND has a new ID (lifecycle has processed the expiry)
		err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
			var resp pb.StorageItemsListResponse
			if err := c.StorageItemsList(ctx, queueName, 0, &resp, &querator.ListOptions{Limit: 100}); err != nil {
				return err
			}
			item := findInStorageList(refA, &resp)
			if item == nil {
				return fmt.Errorf("item A not found in storage")
			}
			if item.IsLeased {
				return fmt.Errorf("item A still leased")
			}
			if item.Id == originalIDofA {
				return fmt.Errorf("item A still has original ID, lifecycle not yet processed")
			}
			return nil
		})
		require.NoError(t, err)

		// Lease batch of 3 — order must be B, C, A (A moved to tail after expiry)
		var secondLease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      3,
		}, &secondLease))

		require.Len(t, secondLease.Items, 3)
		assert.Equal(t, refB, secondLease.Items[0].Reference)
		assert.Equal(t, refC, secondLease.Items[1].Reference)
		assert.Equal(t, refA, secondLease.Items[2].Reference)

		// Verify A has a new ID
		retriedA := secondLease.Items[2]
		assert.NotEqual(t, originalIDofA, retriedA.Id)
	})

	t.Run("RetryRequestsInOneBatch", func(t *testing.T) {
		// Retry requests buffered while the queue is paused reach the partition as one batch.
		// A request whose items fail must get its own error without stopping the requests after it.
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{
				{Reference: "gone", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
				{Reference: "retried", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
				{Reference: "dead", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
			},
		}))

		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      3,
		}, &lease))
		require.Len(t, lease.Items, 3)
		require.Equal(t, "gone", lease.Items[0].Reference)
		require.Equal(t, "retried", lease.Items[1].Reference)
		require.Equal(t, "dead", lease.Items[2].Reference)

		// Dead-letter one item now, so its id no longer exists when the batch runs
		gone := lease.Items[0].Id
		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: lease.Partition,
			Items:     []*pb.QueueRetryItem{{Id: gone, Dead: true}},
		}))

		// An item that was never leased
		notLeased := writeRandomItems(t, ctx, c, queueName, 1)
		require.Len(t, notLeased, 1)

		require.NoError(t, d.Service().PauseQueue(ctx, queueName))

		requests := []*pb.QueueRetryRequest{
			{
				QueueName: queueName,
				Partition: lease.Partition,
				Items:     []*pb.QueueRetryItem{{Id: lease.Items[1].Id}},
			},
			{
				QueueName: queueName,
				Partition: lease.Partition,
				Items:     []*pb.QueueRetryItem{{Id: gone}},
			},
			{
				QueueName: queueName,
				Partition: lease.Partition,
				Items:     []*pb.QueueRetryItem{{Id: notLeased[0].Id}},
			},
			{
				QueueName: queueName,
				Partition: lease.Partition,
				Items:     []*pb.QueueRetryItem{{Id: lease.Items[2].Id, Dead: true}},
			},
		}

		errs := retryWhilePaused(ctx, c, requests, func() {
			// Wait until every retry request is buffered in the paused queue
			assert.Eventually(t, func() bool {
				var stats pb.QueueStatsResponse
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
					return false
				}
				return len(stats.LogicalQueues) == 1 && int(stats.LogicalQueues[0].InFlight) == len(requests)
			}, 10*clock.Second, 100*clock.Millisecond)
			assert.NoError(t, d.Service().ResumeQueue(ctx, queueName))
		})

		assert.NoError(t, errs[0])
		var e duh.Error
		require.True(t, errors.As(errs[1], &e))
		assert.Contains(t, e.Message(), "invalid storage id; '"+gone+"' does not exist")
		assert.Equal(t, duh.CodeBadRequest, e.Code())
		require.True(t, errors.As(errs[2], &e))
		assert.Contains(t, e.Message(), "item(s) cannot be retried; '"+notLeased[0].Id+"' is not marked as leased")
		assert.Equal(t, duh.CodeBadRequest, e.Code())
		assert.NoError(t, errs[3])

		// The never-leased item remains and the retried item is back in the queue un-leased;
		// the dead-lettered item is gone.
		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		require.Len(t, list.Items, 2)
		assert.Equal(t, notLeased[0].Id, list.Items[0].Id)
		assert.Equal(t, "retried", list.Items[1].Reference)
		assert.False(t, list.Items[1].IsLeased)
	})

	t.Run("Errors", func(t *testing.T) {
		storage := setup()
		defer tearDown()

		t.Run("QueueRetry", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			d := newDaemon(t, 5*clock.Second, svc.Config{StorageConfig: storage})
			defer d.Shutdown(t)
			c, ctx := d.MustClient(), d.Context()

			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				QueueName:           queueName,
				RequestedPartitions: 1,
			})

			for _, tc := range []struct {
				Name string
				Req  *pb.QueueRetryRequest
				Msg  string
				Code int
			}{
				{
					Name: "EmptyRequest",
					Req:  &pb.QueueRetryRequest{},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "EmptyItems",
					Req: &pb.QueueRetryRequest{
						QueueName: queueName,
						Items:     []*pb.QueueRetryItem{},
					},
					Msg:  "items is invalid; list of items cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidPartition",
					Req: &pb.QueueRetryRequest{
						QueueName: queueName,
						Partition: 65234,
						Items: []*pb.QueueRetryItem{
							{Id: "test-id"},
						},
					},
					Msg:  "partition is invalid; '65234' is not a valid partition",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidId",
					Req: &pb.QueueRetryRequest{
						QueueName: queueName,
						Items: []*pb.QueueRetryItem{
							{Id: "invalid-id"},
						},
					},
					Msg:  "invalid storage id; 'invalid-id'",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "DoesNotExist",
					Req: &pb.QueueRetryRequest{
						QueueName: queueName,
						Items: []*pb.QueueRetryItem{
							{Id: ksuid.New().String()},
						},
					},
					Msg:  "invalid storage id;",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(tc.Name, func(t *testing.T) {
					err := c.QueueRetry(ctx, tc.Req)
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Contains(t, e.Message(), tc.Msg)
					assert.Equal(t, tc.Code, e.Code())
					if e.Message() == "" {
						t.Logf("Error: %s", e.Error())
					}
				})
			}
		})

		t.Run("RetryNotLeased", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			d := newDaemon(t, 5*clock.Second, svc.Config{StorageConfig: storage})
			defer d.Shutdown(t)
			c, ctx := d.MustClient(), d.Context()

			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				QueueName:           queueName,
				RequestedPartitions: 1,
			})

			// Write items directly to storage (IsLeased = false by default)
			items := writeRandomItems(t, ctx, c, queueName, 1)
			require.Len(t, items, 1)

			// Attempt to retry an item that is not leased
			err := c.QueueRetry(ctx, &pb.QueueRetryRequest{
				QueueName: queueName,
				Items: []*pb.QueueRetryItem{
					{Id: items[0].Id},
				},
			})

			require.Error(t, err)
			var e duh.Error
			require.True(t, errors.As(err, &e))
			assert.Contains(t, e.Message(), "item(s) cannot be retried;")
			assert.Contains(t, e.Message(), "is not marked as leased")
			assert.Equal(t, duh.CodeBadRequest, e.Code())
		})
	})
}

// retryWhilePaused sends every request at once and calls unpause while they wait in the
// paused queue. The goroutines start in a named function because Tiger's TS-C02
// does not see a Wait that follows wg.Go inside a function literal.
func retryWhilePaused(ctx context.Context, c *querator.Client, requests []*pb.QueueRetryRequest, unpause func()) []error {
	errs := make([]error, len(requests))
	var wg sync.WaitGroup
	for i := range requests {
		wg.Go(func() {
			errs[i] = c.QueueRetry(ctx, requests[i])
		})
	}
	unpause()
	wg.Wait()
	return errs
}
