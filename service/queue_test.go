package service_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/duh-rpc/duh-go/retry"
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

func TestQueue(t *testing.T) {
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
			testQueue(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueue(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("ProduceAndLease", func(t *testing.T) {
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

		// Produce a single message
		ref := random.String("ref-", 10)
		enc := random.String("enc-", 10)
		kind := random.String("kind-", 10)
		payload := []byte("I didn't learn a thing. I was right all along")
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{
				{
					Reference: ref,
					Encoding:  enc,
					Kind:      kind,
					Bytes:     payload,
				},
			},
		}))

		// Lease a single message
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &lease))

		// Ensure we got the item we produced
		assert.Equal(t, 1, len(lease.Items))
		item := lease.Items[0]
		assert.Equal(t, ref, item.Reference)
		assert.Equal(t, enc, item.Encoding)
		assert.Equal(t, kind, item.Kind)
		assert.Equal(t, int32(1), item.Attempts)
		assert.Equal(t, payload, item.Bytes)

		// Partition storage should have only one item
		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 10}))
		require.Equal(t, 1, len(list.Items))

		inspect := list.Items[0]
		assert.Equal(t, ref, inspect.Reference)
		assert.Equal(t, kind, inspect.Kind)
		assert.Equal(t, int32(1), inspect.Attempts)
		assert.Equal(t, payload, inspect.Payload)
		assert.Equal(t, item.Id, inspect.Id)
		assert.Equal(t, true, inspect.IsLeased)

		// Mark the item as complete
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Ids: []string{
				item.Id,
			},
		}))

		// Partition storage should be empty
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 10}))
		assert.Equal(t, 0, len(list.Items))

		// Remove queue
		require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: queueName}))
		var queues pb.QueuesListResponse
		require.NoError(t, c.QueuesList(ctx, &queues, &querator.ListOptions{Limit: 10}))
		for _, q := range queues.Items {
			assert.NotEqual(t, q.QueueName, queueName)
		}
	})

	t.Run("Produce", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			Reference:           "rainbow@dash.com",
			ExpireTimeout:       "20h0m0s",
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			MaxAttempts:         256,
			RequestedPartitions: 1,
		})

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{
				{
					Reference: random.String("ref-", 10),
					Encoding:  random.String("enc-", 10),
					Kind:      random.String("kind-", 10),
					Bytes:     []byte("first item"),
				},
			},
		}))

		// GetByPartition the last item in the queue, so the following tests know where to begin their assertions.
		var last pb.StorageItemsListResponse
		err := c.StorageItemsList(ctx, queueName, 0, &last, nil)
		require.NoError(t, err)
		lastItem := &pb.StorageItem{}
		if len(last.Items) != 0 {
			lastItem = last.Items[len(last.Items)-1]
		}

		t.Run("InheritsQueueInfo", func(t *testing.T) {
			now := clock.Now().UTC()
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: "flutter@shy.com",
						Encoding:  "friendship",
						Kind:      "yes",
						Bytes:     []byte("You're...going...TO LOVE ME!"),
					},
					{
						Reference: "",
						Encoding:  "application/json",
						Kind:      "no",
						Bytes:     []byte("It needs to be about 20% cooler"),
					},
				}}))
			expireDeadline := clock.Now().UTC().Add(20 * clock.Hour)

			var list pb.StorageItemsListResponse
			err := c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 20, Pivot: lastItem.Id})
			require.NoError(t, err)
			produced := list.Items[1:]

			assert.Equal(t, 2, len(produced))
			assert.Equal(t, "flutter@shy.com", produced[0].Reference)
			assert.Equal(t, "friendship", produced[0].Encoding)
			assert.Equal(t, "yes", produced[0].Kind)
			assert.True(t, produced[0].LeaseDeadline.AsTime().IsZero())
			assert.False(t, produced[0].ExpireDeadline.AsTime().IsZero())
			assert.True(t, produced[0].ExpireDeadline.AsTime().After(now))
			assert.True(t, produced[0].ExpireDeadline.AsTime().Before(expireDeadline))

			assert.Equal(t, "", produced[1].Reference)
			assert.Equal(t, "application/json", produced[1].Encoding)
			assert.Equal(t, "no", produced[1].Kind)
			assert.True(t, produced[1].LeaseDeadline.AsTime().IsZero())
			assert.False(t, produced[1].ExpireDeadline.AsTime().IsZero())
			assert.True(t, produced[1].ExpireDeadline.AsTime().After(now))
			assert.True(t, produced[1].ExpireDeadline.AsTime().Before(expireDeadline))
			lastItem = list.Items[len(list.Items)-1]
		})

		// Note: MaxAttempts, ExpireTimeout, DeadQueue tests are in Lifecycle section below

		t.Run("Bytes", func(t *testing.T) {
			var items []*pb.QueueProduceItem
			for i := 0; i < 10; i++ {
				items = append(items, &pb.QueueProduceItem{
					Reference: random.String("ref-", 10),
					Encoding:  random.String("enc-", 10),
					Kind:      random.String("kind-", 10),
					Bytes:     []byte(fmt.Sprintf("message-%d", i)),
				})
			}

			now := clock.Now().UTC()
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items:          items,
			}))
			expireDeadline := clock.Now().UTC().Add(24 * clock.Hour)

			// Ensure the items produced are in the data store
			var list pb.StorageItemsListResponse
			err := c.StorageItemsList(ctx, queueName, 0, &list,
				&querator.ListOptions{Pivot: lastItem.Id, Limit: 20})
			require.NoError(t, err)
			assert.Equal(t, len(items), len(list.Items[1:]))
			produced := list.Items[1:]

			require.Len(t, produced, 10)
			for i := range produced {
				assert.True(t, produced[i].CreatedAt.AsTime().After(now))

				// ExpireDeadline should be after we produced the item, but before the dead clockout
				assert.True(t, produced[i].LeaseDeadline.AsTime().IsZero())
				assert.False(t, produced[i].ExpireDeadline.AsTime().IsZero())
				assert.True(t, produced[i].ExpireDeadline.AsTime().After(now))
				assert.True(t, produced[i].ExpireDeadline.AsTime().Before(expireDeadline))

				assert.Equal(t, false, produced[i].IsLeased)
				assert.Equal(t, int32(0), produced[i].Attempts)
				assert.Equal(t, items[i].Reference, produced[i].Reference)
				assert.Equal(t, items[i].Encoding, produced[i].Encoding)
				assert.Equal(t, items[i].Bytes, produced[i].Payload)
				assert.Equal(t, items[i].Kind, produced[i].Kind)
			}
			lastItem = list.Items[len(list.Items)-1]
		})

		t.Run("Utf8", func(t *testing.T) {
			var items []*pb.QueueProduceItem
			for i := 0; i < 100; i++ {
				items = append(items, &pb.QueueProduceItem{
					Reference: random.String("ref-", 10),
					Encoding:  random.String("enc-", 10),
					Kind:      random.String("kind-", 10),
					Utf8:      fmt.Sprintf("message-%d", i),
				})
			}

			now := clock.Now().UTC()
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items:          items,
			}))
			expireDeadline := clock.Now().UTC().Add(24 * clock.Hour)

			// List all the items we just produced
			var list pb.StorageItemsListResponse
			err := c.StorageItemsList(ctx, queueName, 0, &list,
				&querator.ListOptions{Pivot: lastItem.Id, Limit: 101})
			require.NoError(t, err)

			require.Len(t, items, 100)

			// Remember the pivot is included in the results, so we remove the pivot from the results
			assert.Equal(t, len(items), len(list.Items[1:]))
			produced := list.Items[1:]

			for i := range produced {
				assert.True(t, produced[i].CreatedAt.AsTime().After(now))

				// ExpireDeadline should be after we produced the item, but before the dead clockout
				assert.False(t, produced[i].ExpireDeadline.AsTime().IsZero())
				assert.True(t, produced[i].ExpireDeadline.AsTime().After(now))
				assert.True(t, produced[i].ExpireDeadline.AsTime().Before(expireDeadline))

				assert.Equal(t, false, produced[i].IsLeased)
				assert.Equal(t, int32(0), produced[i].Attempts)
				assert.Equal(t, items[i].Reference, produced[i].Reference)
				assert.Equal(t, items[i].Encoding, produced[i].Encoding)
				assert.Equal(t, []byte(items[i].Utf8), produced[i].Payload)
				assert.Equal(t, items[i].Kind, produced[i].Kind)
			}
			lastItem = list.Items[len(list.Items)-1]
		})
	})

	t.Run("Produce/Scheduled", func(t *testing.T) {
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

		const numItems = 100
		enqueueAt := now.Now().Add(1 * clock.Minute)
		var items []*pb.QueueProduceItem
		for i := 0; i < numItems; i++ {
			items = append(items, &pb.QueueProduceItem{
				Reference: random.String("ref-", 10),
				Encoding:  random.String("enc-", 10),
				Kind:      random.String("kind-", 10),
				Utf8:      fmt.Sprintf("message-%d", i),
				EnqueueAt: timestamppb.New(enqueueAt),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items:          items,
		}))

		// Items should appear in StorageScheduledList
		var scheduled pb.StorageItemsListResponse
		err := c.StorageScheduledList(ctx, queueName, 0, &scheduled, &querator.ListOptions{Limit: 150})
		require.NoError(t, err)
		require.Len(t, scheduled.Items, numItems)

		// Items should NOT appear in StorageItemsList
		var list pb.StorageItemsListResponse
		err = c.StorageItemsList(ctx, queueName, 0, &list, nil)
		require.NoError(t, err)
		require.Empty(t, list.Items)

		// Advance clock past EnqueueAt
		now.Advance(2 * clock.Minute)

		// Wait for items to appear in StorageItemsList
		retryPolicy := retry.Policy{Interval: retry.Sleep(100 * clock.Millisecond), Attempts: 50}
		err = retry.On(ctx, retryPolicy, func(ctx context.Context, i int) error {
			var resp pb.StorageItemsListResponse
			if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
				return err
			}
			if len(resp.Items) != numItems {
				return fmt.Errorf("expected %d items, got %d", numItems, len(resp.Items))
			}
			return nil
		})
		require.NoError(t, err)

		// Items should be removed from StorageScheduledList
		err = c.StorageScheduledList(ctx, queueName, 0, &scheduled, nil)
		require.NoError(t, err)
		require.Empty(t, scheduled.Items)

		// Items should now be leasable
		var lease pb.QueueLeaseResponse
		err = c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      numItems,
		}, &lease)
		require.NoError(t, err)
		require.Len(t, lease.Items, numItems)

		// Complete all leased items
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			RequestTimeout: "5s",
			QueueName:      queueName,
			Ids:            querator.CollectIDs(lease.Items),
		}))

		// Verify items are removed from queue
		err = c.StorageItemsList(ctx, queueName, 0, &list, nil)
		require.NoError(t, err)
		assert.Empty(t, list.Items)
	})

	t.Run("Produce/Scheduled/Linearizability", func(t *testing.T) {
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

		const numItems = 10
		enqueueAt := now.Now().Add(1 * clock.Minute)
		var items []*pb.QueueProduceItem
		for i := 0; i < numItems; i++ {
			items = append(items, &pb.QueueProduceItem{
				Reference: fmt.Sprintf("ref-%d", i),
				Encoding:  "utf8",
				Kind:      "linearizability-test",
				Utf8:      fmt.Sprintf("message-%d", i),
				EnqueueAt: timestamppb.New(enqueueAt),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items:          items,
		}))

		// Items should appear in StorageScheduledList
		var scheduled pb.StorageItemsListResponse
		err := c.StorageScheduledList(ctx, queueName, 0, &scheduled, &querator.ListOptions{Limit: 20})
		require.NoError(t, err)
		require.Len(t, scheduled.Items, numItems)

		// Advance clock past EnqueueAt
		now.Advance(2 * clock.Minute)

		// Wait for items to appear in StorageItemsList
		retryPolicy := retry.Policy{Interval: retry.Sleep(100 * clock.Millisecond), Attempts: 50}
		err = retry.On(ctx, retryPolicy, func(ctx context.Context, i int) error {
			var resp pb.StorageItemsListResponse
			if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
				return err
			}
			if len(resp.Items) != numItems {
				return fmt.Errorf("expected %d items, got %d", numItems, len(resp.Items))
			}
			return nil
		})
		require.NoError(t, err)

		// Lease all items in batch
		var lease pb.QueueLeaseResponse
		err = c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      numItems,
		}, &lease)
		require.NoError(t, err)
		require.Len(t, lease.Items, numItems)

		// Verify items leased in FIFO order (by Reference matching production order)
		for i := 0; i < numItems; i++ {
			assert.Equal(t, fmt.Sprintf("ref-%d", i), lease.Items[i].Reference)
		}
	})

	t.Run("Produce/Scheduled/NearImmediate", func(t *testing.T) {
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

		// Produce item with EnqueueAt = now + 50ms (< 100ms optimization threshold)
		enqueueAt := now.Now().Add(50 * clock.Millisecond)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items: []*pb.QueueProduceItem{
				{
					Reference: "near-immediate",
					Encoding:  "utf8",
					Kind:      "test",
					Utf8:      "near immediate test",
					EnqueueAt: timestamppb.New(enqueueAt),
				},
			},
		}))

		// Item should appear in StorageItemsList immediately (count == 1)
		var list pb.StorageItemsListResponse
		err := c.StorageItemsList(ctx, queueName, 0, &list, nil)
		require.NoError(t, err)
		require.Len(t, list.Items, 1)

		// Item should NOT be in StorageScheduledList (count == 0)
		var scheduled pb.StorageItemsListResponse
		err = c.StorageScheduledList(ctx, queueName, 0, &scheduled, nil)
		require.NoError(t, err)
		require.Empty(t, scheduled.Items)

		// Item should be immediately leasable
		var lease pb.QueueLeaseResponse
		err = c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &lease)
		require.NoError(t, err)
		require.Len(t, lease.Items, 1)
		assert.Equal(t, "near-immediate", lease.Items[0].Reference)
	})

	t.Run("Produce/Scheduled/PastTimestamp", func(t *testing.T) {
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

		// Produce item with EnqueueAt in the past
		enqueueAt := now.Now().Add(-1 * clock.Minute)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items: []*pb.QueueProduceItem{
				{
					Reference: "past-timestamp",
					Encoding:  "utf8",
					Kind:      "test",
					Utf8:      "past timestamp test",
					EnqueueAt: timestamppb.New(enqueueAt),
				},
			},
		}))

		// Item should appear in StorageItemsList immediately
		var list pb.StorageItemsListResponse
		err := c.StorageItemsList(ctx, queueName, 0, &list, nil)
		require.NoError(t, err)
		require.Len(t, list.Items, 1)

		// Item should NOT be in StorageScheduledList
		var scheduled pb.StorageItemsListResponse
		err = c.StorageScheduledList(ctx, queueName, 0, &scheduled, nil)
		require.NoError(t, err)
		require.Empty(t, scheduled.Items)

		// Item should be immediately leasable
		var lease pb.QueueLeaseResponse
		err = c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &lease)
		require.NoError(t, err)
		require.Len(t, lease.Items, 1)
		assert.Equal(t, "past-timestamp", lease.Items[0].Reference)
	})

	t.Run("Produce/Scheduled/Pagination", func(t *testing.T) {
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

		// Produce 50 scheduled items with future EnqueueAt
		const numItems = 50
		enqueueAt := now.Now().Add(1 * clock.Minute)
		var items []*pb.QueueProduceItem
		for i := 0; i < numItems; i++ {
			items = append(items, &pb.QueueProduceItem{
				Reference: fmt.Sprintf("page-ref-%d", i),
				Encoding:  "utf8",
				Kind:      "pagination-test",
				Utf8:      fmt.Sprintf("message-%d", i),
				EnqueueAt: timestamppb.New(enqueueAt),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items:          items,
		}))

		// Verify pagination works by fetching all items in pages
		// Use a larger limit to get all items in one or two calls
		var allItems pb.StorageItemsListResponse
		err := c.StorageScheduledList(ctx, queueName, 0, &allItems, &querator.ListOptions{Limit: 100})
		require.NoError(t, err)
		require.Len(t, allItems.Items, numItems)

		// Now test pagination with smaller page size
		const pageSize = 20
		var page1 pb.StorageItemsListResponse
		err = c.StorageScheduledList(ctx, queueName, 0, &page1, &querator.ListOptions{Limit: pageSize})
		require.NoError(t, err)
		require.Len(t, page1.Items, pageSize)

		// Verify all items have unique IDs
		seenIDs := make(map[string]bool)
		for _, item := range page1.Items {
			require.False(t, seenIDs[item.Id], "Duplicate ID in first page: %s", item.Id)
			seenIDs[item.Id] = true
		}

		// Second page using pivot
		var page2 pb.StorageItemsListResponse
		err = c.StorageScheduledList(ctx, queueName, 0, &page2, &querator.ListOptions{
			Limit: pageSize,
			Pivot: page1.Items[len(page1.Items)-1].Id,
		})
		require.NoError(t, err)
		require.NotEmpty(t, page2.Items)

		// Verify second page has different items (may include pivot or not, but should have new items)
		newItemsFound := 0
		for _, item := range page2.Items {
			if !seenIDs[item.Id] {
				newItemsFound++
				seenIDs[item.Id] = true
			}
		}
		require.Greater(t, newItemsFound, 0, "Second page should have at least some new items")
	})

	t.Run("Lease", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		clientID := random.String("client-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			LeaseTimeout:        "2m0s",
			RequestedPartitions: 1,
		})
		items := writeRandomItems(t, ctx, c, queueName, 10_000)
		require.Len(t, items, 10_000)

		expire := clock.Now().UTC().Add(2_000 * clock.Minute)
		var leased, secondLease pb.QueueLeaseResponse
		var list pb.StorageItemsListResponse

		t.Run("TenItems", func(t *testing.T) {
			req := pb.QueueLeaseRequest{
				ClientId:       clientID,
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			}

			now := clock.Now().UTC()
			require.NoError(t, c.QueueLease(ctx, &req, &leased))
			require.Equal(t, 10, len(leased.Items))
			leaseDeadline := clock.Now().UTC().Add(2 * clock.Minute)

			// Ensure the items leased are marked as leased in the database
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 10_000}))

			for i := range leased.Items {
				assert.Equal(t, list.Items[i].Id, leased.Items[i].Id)
				assert.Equal(t, true, list.Items[i].IsLeased)

				// LeaseDeadline should be after we leased the item, but before the lease clockout
				assert.False(t, list.Items[i].LeaseDeadline.AsTime().IsZero())
				assert.True(t, list.Items[i].LeaseDeadline.AsTime().After(now))
				assert.True(t, list.Items[i].LeaseDeadline.AsTime().Before(leaseDeadline))
				assert.True(t, list.Items[i].LeaseDeadline.AsTime().Before(expire))
			}
		})

		t.Run("AnotherTenItems", func(t *testing.T) {
			req := pb.QueueLeaseRequest{
				ClientId:       clientID,
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			}

			require.NoError(t, c.QueueLease(ctx, &req, &secondLease))
			require.Equal(t, 10, len(leased.Items))

			var combined []*pb.QueueLeaseItem
			combined = append(combined, leased.Items...)
			combined = append(combined, secondLease.Items...)

			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 10_000}))
			assert.NotEqual(t, leased.Items[0].Id, secondLease.Items[0].Id)
			assert.Equal(t, combined[0].Id, list.Items[0].Id)
			require.Equal(t, 20, len(combined))
			require.Equal(t, 10_000, len(list.Items))

			// Ensure all the items leased are marked as leased in the database
			for i := range combined {
				assert.Equal(t, list.Items[i].Id, combined[i].Id)
				assert.Equal(t, true, list.Items[i].IsLeased)
				assert.True(t, list.Items[i].LeaseDeadline.AsTime().Before(expire))
			}
		})

		t.Run("DistributeNumRequested", func(t *testing.T) {
			requests := []*pb.QueueLeaseRequest{
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      5,
					RequestTimeout: "1m",
				},
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      10,
					RequestTimeout: "1m",
				},
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      20,
					RequestTimeout: "1m",
				},
			}
			responses := pauseAndLease(t, ctx, d.Service(), c, queueName, requests)

			assert.Equal(t, int32(5), requests[0].BatchSize)
			assert.Equal(t, 5, len(responses[0].Items))
			assert.Equal(t, int32(10), requests[1].BatchSize)
			assert.Equal(t, 10, len(responses[1].Items))
			assert.Equal(t, int32(20), requests[2].BatchSize)
			assert.Equal(t, 20, len(responses[2].Items))

			// Fetch items from storage, ensure items are leased
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 10_000}))
			require.Equal(t, 10_000, len(list.Items))

			var found int
			for _, item := range list.Items {
				// Find the leased item in the batch request
				if findInResponses(responses, item.Reference) {
					found++
					// Ensure the item is leased
					require.Equal(t, true, item.IsLeased)
				}
			}
			assert.Equal(t, 35, found, "expected to find 35 leased items, got %d", found)
		})

		t.Run("DistributeNotEnoughItems", func(t *testing.T) {
			// Clear all the items from the queue
			require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
				QueueName:   queueName,
				Destructive: true,
				Queue:       true,
			}))

			// Write a limited number of items into the queue
			items := writeRandomItems(t, ctx, c, queueName, 23)
			require.Len(t, items, 23)
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
			require.Equal(t, 23, len(list.Items))

			requests := []*pb.QueueLeaseRequest{
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      20,
					RequestTimeout: "1m",
				},
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      6,
					RequestTimeout: "1m",
				},
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      1,
					RequestTimeout: "1m",
				},
			}
			responses := pauseAndLease(t, ctx, d.Service(), c, queueName, requests)

			// Lease() should fairly distribute items across all requests
			assert.Equal(t, int32(20), requests[0].BatchSize)
			assert.Equal(t, 16, len(responses[0].Items))
			assert.Equal(t, int32(6), requests[1].BatchSize)
			assert.Equal(t, 6, len(responses[1].Items))
			assert.Equal(t, int32(1), requests[2].BatchSize)
			assert.Equal(t, 1, len(responses[2].Items))
		})

		t.Run("DistributeNoItems", func(t *testing.T) {
			// Clear all the items from the queue
			require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
				QueueName:   queueName,
				Destructive: true,
				Queue:       true,
			}))

			requests := []*pb.QueueLeaseRequest{
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      20,
					RequestTimeout: "2s",
				},
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      6,
					RequestTimeout: "2s",
				},
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      1,
					RequestTimeout: "3s",
				},
			}
			responses := pauseAndLease(t, ctx, d.Service(), c, queueName, requests)

			// Lease() should return no items, and
			assert.Equal(t, int32(20), requests[0].BatchSize)
			assert.Equal(t, 0, len(responses[0].Items))
			assert.Equal(t, int32(6), requests[1].BatchSize)
			assert.Equal(t, 0, len(responses[1].Items))
			assert.Equal(t, int32(1), requests[2].BatchSize)
			assert.Equal(t, 0, len(responses[2].Items))
		})
	})

	t.Run("Complete", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		clientID := random.String("client-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		}))

		t.Run("Success", func(t *testing.T) {
			items := writeRandomItems(t, ctx, c, queueName, 10)
			require.Len(t, items, 10)

			var leased pb.QueueLeaseResponse
			var list pb.StorageItemsListResponse

			req := pb.QueueLeaseRequest{
				ClientId:       clientID,
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			}

			require.NoError(t, c.QueueLease(ctx, &req, &leased))
			require.Equal(t, 10, len(leased.Items))

			require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				Ids:            querator.CollectIDs(leased.Items),
				QueueName:      queueName,
				RequestTimeout: "1m",
			}))

			// Fetch items from storage, ensure items are no longer available
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
			require.Equal(t, 0, len(list.Items))
		})

		t.Run("NotLeased", func(t *testing.T) {
			// Attempt to complete an item that has not been leased
			items := writeRandomItems(t, ctx, c, queueName, 15)
			require.Len(t, items, 15)

			var ids []string
			for _, i := range items {
				ids = append(ids, i.Id)
			}

			err := c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Ids:            ids,
			})

			require.Error(t, err)
			var e duh.Error
			require.True(t, errors.As(err, &e))
			assert.Contains(t, e.Message(), "item(s) cannot be completed;")
			assert.Contains(t, e.Message(), " is not marked as leased")
			assert.Equal(t, 400, e.Code())
		})

		t.Run("InvalidID", func(t *testing.T) {
			err := c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Ids: []string{
					"another-invalid-id",
					"invalid-id",
				},
			})

			require.Error(t, err)
			var e duh.Error
			require.True(t, errors.As(err, &e))
			assert.Contains(t, e.Message(), "invalid storage id; 'another-invalid-id'")
			assert.Equal(t, 400, e.Code())
		})
	})

	t.Run("Stats", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		clientID := random.String("client-", 10)
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

		produce := pb.QueueProduceRequest{
			Items:          produceRandomItems(500),
			QueueName:      queueName,
			RequestTimeout: "1m",
		}
		require.NoError(t, c.QueueProduce(ctx, &produce))

		// TODO: Reload the partition now that new items have been added to the underlying storage.
		// c.QueueReload(ctx, queueName)

		req := pb.QueueLeaseRequest{
			ClientId:       clientID,
			QueueName:      queueName,
			BatchSize:      15,
			RequestTimeout: "1m",
		}

		var leased pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &req, &leased))
		require.Equal(t, 15, len(leased.Items))

		var stats pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))

		stat := stats.LogicalQueues[0]
		p := stat.Partitions[0]
		assert.Equal(t, int32(500), p.Total)
		assert.Equal(t, int32(15), p.TotalLeased)
		assert.Equal(t, int32(0), p.Failures)
		assert.NotEmpty(t, p.AverageAge)
		assert.NotEmpty(t, p.AverageLeasedAge)
		t.Logf("total: %d average-age: %s leased %d average-leased: %s",
			p.Total, p.AverageAge, p.TotalLeased, p.AverageLeasedAge)
	})

	t.Run("Stats/Scheduled", func(t *testing.T) {
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

		const numScheduled = 50
		enqueueAt := now.Now().Add(1 * clock.Minute)
		var items []*pb.QueueProduceItem
		for i := 0; i < numScheduled; i++ {
			items = append(items, &pb.QueueProduceItem{
				Reference: random.String("ref-", 10),
				Encoding:  random.String("enc-", 10),
				Kind:      random.String("kind-", 10),
				Utf8:      fmt.Sprintf("scheduled-%d", i),
				EnqueueAt: timestamppb.New(enqueueAt),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items:          items,
		}))

		// Verify scheduled count in stats
		var stats pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
		require.Len(t, stats.LogicalQueues, 1)
		require.Len(t, stats.LogicalQueues[0].Partitions, 1)
		p := stats.LogicalQueues[0].Partitions[0]
		assert.Equal(t, int32(numScheduled), p.Scheduled)
		assert.Equal(t, int32(0), p.Total)

		// Advance clock past EnqueueAt
		now.Advance(2 * clock.Minute)

		// Wait for items to move to ready queue
		retryPolicy := retry.Policy{Interval: retry.Sleep(100 * clock.Millisecond), Attempts: 50}
		err := retry.On(ctx, retryPolicy, func(ctx context.Context, i int) error {
			var resp pb.StorageItemsListResponse
			if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
				return err
			}
			if len(resp.Items) != numScheduled {
				return fmt.Errorf("expected %d items, got %d", numScheduled, len(resp.Items))
			}
			return nil
		})
		require.NoError(t, err)

		// Verify scheduled count is now 0
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
		p = stats.LogicalQueues[0].Partitions[0]
		assert.Equal(t, int32(0), p.Scheduled)
		assert.Equal(t, int32(numScheduled), p.Total)
	})

	t.Run("QueueClear", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		var leased []*pb.StorageItem
		var list pb.StorageItemsListResponse
		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		}))

		// Write some items to the queue
		_ = writeRandomItems(t, ctx, c, queueName, 500)
		// Ensure the items exist
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		assert.Equal(t, 500, len(list.Items))

		expire := clock.Now().UTC().Add(random.Duration(10*clock.Second, clock.Minute))
		leased = append(leased, &pb.StorageItem{
			ExpireDeadline: timestamppb.New(expire),
			LeaseDeadline:  timestamppb.New(expire),
			Attempts:       int32(rand.Intn(10)),
			Reference:      random.String("ref-", 10),
			Encoding:       random.String("enc-", 10),
			Kind:           random.String("kind-", 10),
			Payload:        []byte("Leased 1"),
			IsLeased:       true,
		})
		leased = append(leased, &pb.StorageItem{
			ExpireDeadline: timestamppb.New(expire),
			LeaseDeadline:  timestamppb.New(expire),
			Attempts:       int32(rand.Intn(10)),
			Reference:      random.String("ref-", 10),
			Encoding:       random.String("enc-", 10),
			Kind:           random.String("kind-", 10),
			Payload:        []byte("Leased 2"),
			IsLeased:       true,
		})

		// Add some leased items
		var resp pb.StorageItemsImportResponse
		err := c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{Items: leased, QueueName: queueName}, &resp)
		require.NoError(t, err)
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		assert.Equal(t, 502, len(list.Items))

		t.Run("NonDestructive", func(t *testing.T) {
			require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true}))
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
			assert.Equal(t, 2, len(list.Items))
		})

		t.Run("Destructive", func(t *testing.T) {
			_ = writeRandomItems(t, ctx, c, queueName, 200)
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
			assert.Equal(t, 202, len(list.Items))
			require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
				QueueName:   queueName,
				Destructive: true,
				Queue:       true}))
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
			assert.Equal(t, 0, len(list.Items))
		})

	})

	t.Run("QueueClear/Scheduled", func(t *testing.T) {
		nowSched := clock.NewProvider()
		nowSched.Freeze(clock.Now())
		defer nowSched.UnFreeze()

		var scheduledQueueName = random.String("queue-", 10)
		dSched := newDaemon(t, 60*clock.Second, svc.Config{
			StorageConfig: setup(),
			Clock:         nowSched,
		})
		defer func() {
			dSched.Shutdown(t)
			tearDown()
		}()
		cSched, ctxSched := dSched.MustClient(), dSched.Context()

		createQueueAndWait(t, ctxSched, cSched, &pb.QueueInfo{
			RequestedPartitions: 1,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			QueueName:           scheduledQueueName,
		})

		// Produce regular items AND scheduled items
		const numRegular = 10
		const numScheduled = 20
		enqueueAt := nowSched.Now().Add(5 * clock.Minute)

		var regularItems []*pb.QueueProduceItem
		for i := 0; i < numRegular; i++ {
			regularItems = append(regularItems, &pb.QueueProduceItem{
				Reference: random.String("regular-", 10),
				Encoding:  random.String("enc-", 10),
				Kind:      random.String("kind-", 10),
				Utf8:      fmt.Sprintf("regular-%d", i),
			})
		}
		require.NoError(t, cSched.QueueProduce(ctxSched, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      scheduledQueueName,
			Items:          regularItems,
		}))

		var scheduledItems []*pb.QueueProduceItem
		for i := 0; i < numScheduled; i++ {
			scheduledItems = append(scheduledItems, &pb.QueueProduceItem{
				Reference: random.String("scheduled-", 10),
				Encoding:  random.String("enc-", 10),
				Kind:      random.String("kind-", 10),
				Utf8:      fmt.Sprintf("scheduled-%d", i),
				EnqueueAt: timestamppb.New(enqueueAt),
			})
		}
		require.NoError(t, cSched.QueueProduce(ctxSched, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      scheduledQueueName,
			Items:          scheduledItems,
		}))

		// Verify scheduled items exist
		var scheduledList pb.StorageItemsListResponse
		require.NoError(t, cSched.StorageScheduledList(ctxSched, scheduledQueueName, 0, &scheduledList, nil))
		assert.Equal(t, numScheduled, len(scheduledList.Items))

		// Verify regular items exist
		var itemsList pb.StorageItemsListResponse
		require.NoError(t, cSched.StorageItemsList(ctxSched, scheduledQueueName, 0, &itemsList, nil))
		assert.Equal(t, numRegular, len(itemsList.Items))

		// Clear only scheduled items
		require.NoError(t, cSched.QueueClear(ctxSched, &pb.QueueClearRequest{
			QueueName: scheduledQueueName,
			Scheduled: true,
		}))

		// Verify scheduled items removed
		require.NoError(t, cSched.StorageScheduledList(ctxSched, scheduledQueueName, 0, &scheduledList, nil))
		assert.Equal(t, 0, len(scheduledList.Items))

		// Verify regular items remain
		require.NoError(t, cSched.StorageItemsList(ctxSched, scheduledQueueName, 0, &itemsList, nil))
		assert.Equal(t, numRegular, len(itemsList.Items))

		// Verify stats.Scheduled is 0
		var stats pb.QueueStatsResponse
		require.NoError(t, cSched.QueueStats(ctxSched, &pb.QueueStatsRequest{QueueName: scheduledQueueName}, &stats))
		require.Len(t, stats.LogicalQueues, 1)
		require.Len(t, stats.LogicalQueues[0].Partitions, 1)
		p := stats.LogicalQueues[0].Partitions[0]
		assert.Equal(t, int32(0), p.Scheduled)
		assert.Equal(t, int32(numRegular), p.Total)
	})

	t.Run("QueueClear/ScheduledAndQueue", func(t *testing.T) {
		nowCombined := clock.NewProvider()
		nowCombined.Freeze(clock.Now())
		defer nowCombined.UnFreeze()

		var combinedQueueName = random.String("queue-", 10)
		dCombined := newDaemon(t, 60*clock.Second, svc.Config{
			StorageConfig: setup(),
			Clock:         nowCombined,
		})
		defer func() {
			dCombined.Shutdown(t)
			tearDown()
		}()
		cCombined, ctxCombined := dCombined.MustClient(), dCombined.Context()

		createQueueAndWait(t, ctxCombined, cCombined, &pb.QueueInfo{
			RequestedPartitions: 1,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			QueueName:           combinedQueueName,
		})

		// Produce regular items AND scheduled items
		const numRegular = 15
		const numScheduled = 25
		enqueueAt := nowCombined.Now().Add(5 * clock.Minute)

		var regularItems []*pb.QueueProduceItem
		for i := 0; i < numRegular; i++ {
			regularItems = append(regularItems, &pb.QueueProduceItem{
				Reference: random.String("regular-", 10),
				Encoding:  random.String("enc-", 10),
				Kind:      random.String("kind-", 10),
				Utf8:      fmt.Sprintf("regular-%d", i),
			})
		}
		require.NoError(t, cCombined.QueueProduce(ctxCombined, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      combinedQueueName,
			Items:          regularItems,
		}))

		var scheduledItems []*pb.QueueProduceItem
		for i := 0; i < numScheduled; i++ {
			scheduledItems = append(scheduledItems, &pb.QueueProduceItem{
				Reference: random.String("scheduled-", 10),
				Encoding:  random.String("enc-", 10),
				Kind:      random.String("kind-", 10),
				Utf8:      fmt.Sprintf("scheduled-%d", i),
				EnqueueAt: timestamppb.New(enqueueAt),
			})
		}
		require.NoError(t, cCombined.QueueProduce(ctxCombined, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      combinedQueueName,
			Items:          scheduledItems,
		}))

		// Verify both exist
		var scheduledList pb.StorageItemsListResponse
		require.NoError(t, cCombined.StorageScheduledList(ctxCombined, combinedQueueName, 0, &scheduledList, nil))
		assert.Equal(t, numScheduled, len(scheduledList.Items))

		var itemsList pb.StorageItemsListResponse
		require.NoError(t, cCombined.StorageItemsList(ctxCombined, combinedQueueName, 0, &itemsList, nil))
		assert.Equal(t, numRegular, len(itemsList.Items))

		// Clear both scheduled and queue items
		require.NoError(t, cCombined.QueueClear(ctxCombined, &pb.QueueClearRequest{
			QueueName: combinedQueueName,
			Scheduled: true,
			Queue:     true,
		}))

		// Verify both removed
		require.NoError(t, cCombined.StorageScheduledList(ctxCombined, combinedQueueName, 0, &scheduledList, nil))
		assert.Equal(t, 0, len(scheduledList.Items))

		require.NoError(t, cCombined.StorageItemsList(ctxCombined, combinedQueueName, 0, &itemsList, nil))
		assert.Equal(t, 0, len(itemsList.Items))

		// Verify stats
		var stats pb.QueueStatsResponse
		require.NoError(t, cCombined.QueueStats(ctxCombined, &pb.QueueStatsRequest{QueueName: combinedQueueName}, &stats))
		require.Len(t, stats.LogicalQueues, 1)
		require.Len(t, stats.LogicalQueues[0].Partitions, 1)
		p := stats.LogicalQueues[0].Partitions[0]
		assert.Equal(t, int32(0), p.Scheduled)
		assert.Equal(t, int32(0), p.Total)
	})

	t.Run("QueueClear/DestructiveKeepsScheduled", func(t *testing.T) {
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

		const numRegular = 10
		const numScheduled = 20
		var regular []*pb.QueueProduceItem
		for i := 0; i < numRegular; i++ {
			regular = append(regular, &pb.QueueProduceItem{
				Reference: random.String("regular-", 10),
				Utf8:      fmt.Sprintf("regular-%d", i),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items:          regular,
		}))

		var scheduled []*pb.QueueProduceItem
		for i := 0; i < numScheduled; i++ {
			scheduled = append(scheduled, &pb.QueueProduceItem{
				Reference: random.String("scheduled-", 10),
				Utf8:      fmt.Sprintf("scheduled-%d", i),
				EnqueueAt: timestamppb.New(now.Now().Add(5 * clock.Minute)),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items:          scheduled,
		}))

		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      2,
		}, &lease))
		require.Len(t, lease.Items, 2)

		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
			QueueName:   queueName,
			Queue:       true,
			Destructive: true,
		}))

		var items pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &items, nil))
		assert.Len(t, items.Items, 0)

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageScheduledList(ctx, queueName, 0, &list, nil))
		assert.Len(t, list.Items, numScheduled)
	})

	t.Run("QueueClear/QueueKeepsDueScheduled", func(t *testing.T) {
		// The daemon's clock runs an hour behind real time, so the item's time has passed
		// by real time but the lifecycle never moves it. A backend that compares against
		// real time instead of asking whether the item has moved picks the wrong set.
		now := clock.NewProvider()
		now.Freeze(clock.Now().Add(-clock.Hour))
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

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items: []*pb.QueueProduceItem{
				{Reference: random.String("regular-", 10), Utf8: "regular"},
			},
		}))
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items: []*pb.QueueProduceItem{{
				Reference: random.String("scheduled-", 10),
				Utf8:      "scheduled",
				EnqueueAt: timestamppb.New(now.Now().Add(5 * clock.Minute)),
			}},
		}))

		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
			QueueName: queueName,
			Queue:     true,
		}))

		var items pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &items, nil))
		assert.Len(t, items.Items, 0)

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageScheduledList(ctx, queueName, 0, &list, nil))
		assert.Len(t, list.Items, 1)
	})

	t.Run("QueueClear/ScheduledRemovesDueScheduled", func(t *testing.T) {
		// The daemon's clock runs an hour behind real time, so the item's time has passed
		// by real time but the lifecycle never moves it. A backend that compares against
		// real time instead of asking whether the item has moved picks the wrong set.
		now := clock.NewProvider()
		now.Freeze(clock.Now().Add(-clock.Hour))
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

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items: []*pb.QueueProduceItem{
				{Reference: random.String("regular-", 10), Utf8: "regular"},
			},
		}))
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items: []*pb.QueueProduceItem{{
				Reference: random.String("scheduled-", 10),
				Utf8:      "scheduled",
				EnqueueAt: timestamppb.New(now.Now().Add(5 * clock.Minute)),
			}},
		}))

		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
			QueueName: queueName,
			Scheduled: true,
		}))

		var items pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &items, nil))
		assert.Len(t, items.Items, 1)

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageScheduledList(ctx, queueName, 0, &list, nil))
		assert.Len(t, list.Items, 0)
	})

	t.Run("QueueClear/ScheduledAndQueueKeepsLeased", func(t *testing.T) {
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

		const numRegular = 10
		const numScheduled = 20
		var regular []*pb.QueueProduceItem
		for i := 0; i < numRegular; i++ {
			regular = append(regular, &pb.QueueProduceItem{
				Reference: random.String("regular-", 10),
				Utf8:      fmt.Sprintf("regular-%d", i),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items:          regular,
		}))

		var scheduled []*pb.QueueProduceItem
		for i := 0; i < numScheduled; i++ {
			scheduled = append(scheduled, &pb.QueueProduceItem{
				Reference: random.String("scheduled-", 10),
				Utf8:      fmt.Sprintf("scheduled-%d", i),
				EnqueueAt: timestamppb.New(now.Now().Add(5 * clock.Minute)),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items:          scheduled,
		}))

		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      2,
		}, &lease))
		require.Len(t, lease.Items, 2)

		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
			QueueName: queueName,
			Scheduled: true,
			Queue:     true,
		}))

		var items pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &items, nil))
		require.Len(t, items.Items, 2)
		for _, item := range items.Items {
			assert.True(t, item.IsLeased)
		}

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageScheduledList(ctx, queueName, 0, &list, nil))
		assert.Len(t, list.Items, 0)
	})

	t.Run("QueueClear/ScheduledAndQueueDestructive", func(t *testing.T) {
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

		const numRegular = 10
		const numScheduled = 20
		var regular []*pb.QueueProduceItem
		for i := 0; i < numRegular; i++ {
			regular = append(regular, &pb.QueueProduceItem{
				Reference: random.String("regular-", 10),
				Utf8:      fmt.Sprintf("regular-%d", i),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items:          regular,
		}))

		var scheduled []*pb.QueueProduceItem
		for i := 0; i < numScheduled; i++ {
			scheduled = append(scheduled, &pb.QueueProduceItem{
				Reference: random.String("scheduled-", 10),
				Utf8:      fmt.Sprintf("scheduled-%d", i),
				EnqueueAt: timestamppb.New(now.Now().Add(5 * clock.Minute)),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			RequestTimeout: "1m",
			QueueName:      queueName,
			Items:          scheduled,
		}))

		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      2,
		}, &lease))
		require.Len(t, lease.Items, 2)

		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
			QueueName:   queueName,
			Scheduled:   true,
			Queue:       true,
			Destructive: true,
		}))

		var items pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &items, nil))
		assert.Len(t, items.Items, 0)

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageScheduledList(ctx, queueName, 0, &list, nil))
		assert.Len(t, list.Items, 0)
	})

	t.Run("QueueClear/LeaseAfterNonDestructive", func(t *testing.T) {
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

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items:          []*pb.QueueProduceItem{{Reference: "A"}, {Reference: "B"}},
		}))
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &lease))
		require.Len(t, lease.Items, 1)
		assert.Equal(t, "A", lease.Items[0].Reference)

		// Clear removes B, the next item a lease would have taken, and keeps the leased A
		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true}))

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items:          []*pb.QueueProduceItem{{Reference: "C"}},
		}))
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      2,
		}, &lease))
		require.Len(t, lease.Items, 1)
		assert.Equal(t, "C", lease.Items[0].Reference)
	})

	t.Run("QueueClear/LeaseAfterDestructive", func(t *testing.T) {
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

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items:          []*pb.QueueProduceItem{{Reference: "A"}, {Reference: "B"}},
		}))
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &lease))
		require.Len(t, lease.Items, 1)
		assert.Equal(t, "A", lease.Items[0].Reference)

		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
			QueueName:   queueName,
			Destructive: true,
			Queue:       true,
		}))

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items:          []*pb.QueueProduceItem{{Reference: "C"}, {Reference: "D"}},
		}))
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      3,
		}, &lease))
		require.Len(t, lease.Items, 2)
		assert.Equal(t, "C", lease.Items[0].Reference)
		assert.Equal(t, "D", lease.Items[1].Reference)
	})

	t.Run("StorageItemsImport/LeaseAfterImport", func(t *testing.T) {
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

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items:          []*pb.QueueProduceItem{{Reference: "A"}},
		}))
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &lease))
		require.Len(t, lease.Items, 1)
		assert.Equal(t, "A", lease.Items[0].Reference)

		// The partition has nothing left to lease when the imported items arrive
		expire := timestamppb.New(clock.Now().UTC().Add(24 * clock.Hour))
		var res pb.StorageItemsImportResponse
		require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
			QueueName: queueName,
			Items: []*pb.StorageItem{
				{ExpireDeadline: expire, Reference: "B"},
				{
					ExpireDeadline: expire,
					LeaseDeadline:  timestamppb.New(clock.Now().UTC().Add(clock.Hour)),
					IsLeased:       true,
					Attempts:       1,
					Reference:      "C",
				},
				{ExpireDeadline: expire, Reference: "D"},
			},
		}, &res))

		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      3,
		}, &lease))
		require.Len(t, lease.Items, 2)
		assert.Equal(t, "B", lease.Items[0].Reference)
		assert.Equal(t, "D", lease.Items[1].Reference)
	})

	t.Run("QueueReload", func(t *testing.T) {
		t.Run("BasicReload", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				RequestedPartitions: 1,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				QueueName:           queueName,
			})

			// Call QueueReload on empty queue
			require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{
				QueueName: queueName,
			}))

			// Verify stats show empty queue
			var stats pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
			require.Len(t, stats.LogicalQueues, 1)
			require.Len(t, stats.LogicalQueues[0].Partitions, 1)
			p := stats.LogicalQueues[0].Partitions[0]
			assert.Equal(t, int32(0), p.Total)
			assert.Equal(t, int32(0), p.TotalLeased)
		})

		t.Run("ReloadPreservesItems", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				RequestedPartitions: 1,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				QueueName:           queueName,
			})

			// Produce 50 items
			const numItems = 50
			_ = writeRandomItems(t, ctx, c, queueName, numItems)

			// Get stats before reload
			var statsBefore pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &statsBefore))
			require.Len(t, statsBefore.LogicalQueues, 1)
			require.Len(t, statsBefore.LogicalQueues[0].Partitions, 1)
			pBefore := statsBefore.LogicalQueues[0].Partitions[0]
			assert.Equal(t, int32(numItems), pBefore.Total)
			assert.Equal(t, int32(0), pBefore.TotalLeased)

			// Call QueueReload
			require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{
				QueueName: queueName,
			}))

			// Verify stats unchanged - items preserved
			var statsAfter pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &statsAfter))
			require.Len(t, statsAfter.LogicalQueues, 1)
			require.Len(t, statsAfter.LogicalQueues[0].Partitions, 1)
			pAfter := statsAfter.LogicalQueues[0].Partitions[0]
			assert.Equal(t, int32(numItems), pAfter.Total)
			assert.Equal(t, int32(0), pAfter.TotalLeased)

			// Verify items still exist in storage
			var list pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
			assert.Equal(t, numItems, len(list.Items))
		})

		t.Run("StorageSyncAfterImport", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				RequestedPartitions: 1,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				QueueName:           queueName,
			})

			// Produce 100 items normally
			const numProduced = 100
			_ = writeRandomItems(t, ctx, c, queueName, numProduced)

			// Verify stats show 100 items
			var statsBefore pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &statsBefore))
			require.Len(t, statsBefore.LogicalQueues, 1)
			require.Len(t, statsBefore.LogicalQueues[0].Partitions, 1)
			pBefore := statsBefore.LogicalQueues[0].Partitions[0]
			assert.Equal(t, int32(numProduced), pBefore.Total)
			assert.Equal(t, int32(0), pBefore.TotalLeased)

			// Import 50 items directly via StorageItemsImport (bypasses normal flow)
			const numImported = 50
			var importItems []*pb.StorageItem
			expire := clock.Now().UTC().Add(24 * clock.Hour)
			for i := 0; i < numImported; i++ {
				importItems = append(importItems, &pb.StorageItem{
					ExpireDeadline: timestamppb.New(expire),
					Reference:      random.String("import-", 10),
					Encoding:       random.String("enc-", 10),
					Kind:           random.String("kind-", 10),
					Payload:        []byte(fmt.Sprintf("imported-%d", i)),
				})
			}
			var importResp pb.StorageItemsImportResponse
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: queueName,
				Partition: 0,
				Items:     importItems,
			}, &importResp))

			// Stats Total is always read from storage, so shows 150
			var statsAfterImport pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &statsAfterImport))
			require.Len(t, statsAfterImport.LogicalQueues, 1)
			require.Len(t, statsAfterImport.LogicalQueues[0].Partitions, 1)
			pAfterImport := statsAfterImport.LogicalQueues[0].Partitions[0]
			assert.Equal(t, int32(numProduced+numImported), pAfterImport.Total)

			// Call QueueReload to ensure in-memory counts are synced
			require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{
				QueueName: queueName,
			}))

			// Verify we can lease all 150 items
			var leaseResp pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				RequestTimeout: "1m",
				QueueName:      queueName,
				ClientId:       random.String("client-", 10),
				BatchSize:      numProduced + numImported,
			}, &leaseResp))
			assert.Len(t, leaseResp.Items, numProduced+numImported)
		})

		t.Run("ReloadWithLeasedItems", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				RequestedPartitions: 1,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				QueueName:           queueName,
			})

			// Produce 100 items
			const numItems = 100
			_ = writeRandomItems(t, ctx, c, queueName, numItems)

			// Lease 20 items
			const numLeased = 20
			var leaseResp pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				RequestTimeout: "1m",
				QueueName:      queueName,
				ClientId:       random.String("client-", 10),
				BatchSize:      numLeased,
			}, &leaseResp))
			require.Len(t, leaseResp.Items, numLeased)

			// Call QueueReload - should preserve leased items
			require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{
				QueueName: queueName,
			}))

			// Verify stats show correct counts
			var statsAfterReload pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &statsAfterReload))
			require.Len(t, statsAfterReload.LogicalQueues, 1)
			require.Len(t, statsAfterReload.LogicalQueues[0].Partitions, 1)
			pAfterReload := statsAfterReload.LogicalQueues[0].Partitions[0]
			assert.Equal(t, int32(numItems), pAfterReload.Total)

			// Complete 10 leased items
			const numCompleted = 10
			var completedIDs []string
			for i := 0; i < numCompleted; i++ {
				completedIDs = append(completedIDs, leaseResp.Items[i].Id)
			}
			require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				RequestTimeout: "1m",
				QueueName:      queueName,
				Ids:            completedIDs,
			}))

			// Call QueueReload again
			require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{
				QueueName: queueName,
			}))

			// Verify stats updated after completions
			var statsFinal pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &statsFinal))
			require.Len(t, statsFinal.LogicalQueues, 1)
			require.Len(t, statsFinal.LogicalQueues[0].Partitions, 1)
			pFinal := statsFinal.LogicalQueues[0].Partitions[0]
			assert.Equal(t, int32(numItems-numCompleted), pFinal.Total)
		})
	})

	t.Run("ContextCancel", func(t *testing.T) {
		// A client which cancels its context during Produce, Complete or Retry must be
		// released even when the queue is stalled. The pause loop buffers hot requests
		// without processing them, so only context cancellation can release the caller.
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

		// Produce and lease two items so Complete and Retry reference valid leased ids
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{
				{Reference: "complete-me", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
				{Reference: "retry-me", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
			},
		}))

		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      2,
		}, &lease))
		require.Len(t, lease.Items, 2)

		// Pause the queue so hot requests are buffered but never processed
		require.NoError(t, d.Service().PauseQueue(ctx, queueName))

		for _, test := range []struct {
			Name string
			Call func(ctx context.Context) error
		}{
			{
				Name: "Produce",
				Call: func(ctx context.Context) error {
					return c.QueueProduce(ctx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "1m",
						Items: []*pb.QueueProduceItem{
							{Reference: "cancelled", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
						},
					})
				},
			},
			{
				Name: "Complete",
				Call: func(ctx context.Context) error {
					return c.QueueComplete(ctx, &pb.QueueCompleteRequest{
						QueueName:      queueName,
						Partition:      lease.Partition,
						RequestTimeout: "1m",
						Ids:            []string{lease.Items[0].Id},
					})
				},
			},
			{
				Name: "Retry",
				Call: func(ctx context.Context) error {
					return c.QueueRetry(ctx, &pb.QueueRetryRequest{
						QueueName: queueName,
						Partition: lease.Partition,
						Items:     []*pb.QueueRetryItem{{Id: lease.Items[1].Id}},
					})
				},
			},
		} {
			t.Run(test.Name, func(t *testing.T) {
				cancelCtx, cancel := context.WithCancel(context.Background())
				defer cancel()

				var callErr error
				returned := make(chan struct{})
				backgroundCall{
					Call: func() {
						callErr = test.Call(cancelCtx)
						close(returned)
					},
					While: func() {
						// Wait until the request is in flight inside the logical queue
						require.Eventually(t, func() bool {
							var stats pb.QueueStatsResponse
							if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
								return false
							}
							return len(stats.LogicalQueues) == 1 && stats.LogicalQueues[0].InFlight == 1
						}, 10*clock.Second, 100*clock.Millisecond)

						cancel()

						// The call must return promptly with the context error
						select {
						case <-returned:
						case <-clock.After(5 * clock.Second):
							t.Fatal("call did not return after context cancellation")
						}
					},
				}.run()
				require.ErrorContains(t, callErr, "context canceled")

				// The handler goroutine must be released while the queue is still paused
				require.Eventually(t, func() bool {
					var stats pb.QueueStatsResponse
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
						return false
					}
					return len(stats.LogicalQueues) == 1 && stats.LogicalQueues[0].InFlight == 0
				}, 10*clock.Second, 100*clock.Millisecond)
			})
		}

		// Un-pause so the buffered requests drain and shutdown can proceed
		require.NoError(t, d.Service().ResumeQueue(ctx, queueName))

		// The request loop still applies the buffered requests even though the callers
		// are gone (see docs/adr/0009-client-timeouts.md): the complete removes one item,
		// the retry returns the other to the queue and the produce adds a new item.
		// Waiting for the drain also quiesces the loop before shutdown.
		require.Eventually(t, func() bool {
			var list pb.StorageItemsListResponse
			if err := c.StorageItemsList(ctx, queueName, 0, &list, nil); err != nil {
				return false
			}
			refs := make([]string, 0, len(list.Items))
			for _, item := range list.Items {
				refs = append(refs, item.Reference)
			}
			return len(refs) == 2 && slices.Contains(refs, "retry-me") && slices.Contains(refs, "cancelled")
		}, 10*clock.Second, 100*clock.Millisecond)
	})

	t.Run("ContextCancelLease", func(t *testing.T) {
		// A lease caller which cancels its context must be released while the queue is paused,
		// even though its request is held in the request loop and never answered.
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup()})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})

		require.NoError(t, d.Service().PauseQueue(ctx, queueName))

		cancelCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var leaseErr error
		duringCalls(cancel, func() {
			require.Eventually(t, func() bool {
				var stats pb.QueueStatsResponse
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
					return false
				}
				return len(stats.LogicalQueues) == 1 && stats.LogicalQueues[0].InFlight == 1 &&
					stats.LogicalQueues[0].LeaseWaiting == 1
			}, 10*clock.Second, 100*clock.Millisecond)
			cancel()
		}, func() {
			leaseErr = c.QueueLease(cancelCtx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "1m",
				QueueName:      queueName,
				BatchSize:      1,
			}, &pb.QueueLeaseResponse{})
		})
		require.ErrorContains(t, leaseErr, "context canceled")

		// The server side of the lease must be released while the queue is still paused
		require.Eventually(t, func() bool {
			var stats pb.QueueStatsResponse
			if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
				return false
			}
			return len(stats.LogicalQueues) == 1 && stats.LogicalQueues[0].InFlight == 0
		}, 10*clock.Second, 100*clock.Millisecond)

		require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
	})

	t.Run("Pause/HoldsProduceUntilUnpaused", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup()})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})

		// Un-pausing a queue which is not paused changes nothing
		require.NoError(t, d.Service().ResumeQueue(ctx, queueName))

		// A second pause is a no-op, so a single un-pause must resume the queue
		require.NoError(t, d.Service().PauseQueue(ctx, queueName))
		require.NoError(t, d.Service().PauseQueue(ctx, queueName))

		produceCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var produceErr error
		duringCalls(cancel, func() {
			require.Eventually(t, func() bool {
				var stats pb.QueueStatsResponse
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
					return false
				}
				return len(stats.LogicalQueues) == 1 && stats.LogicalQueues[0].ProduceWaiting == 1
			}, 10*clock.Second, 100*clock.Millisecond)

			// Storage requests are still answered while paused, and the held produce is not yet written
			var list pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
			assert.Empty(t, list.Items)

			require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
		}, func() {
			produceErr = c.QueueProduce(produceCtx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{Reference: "held", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
				},
			})
		})
		require.NoError(t, produceErr)

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		require.Len(t, list.Items, 1)
		assert.Equal(t, "held", list.Items[0].Reference)
	})

	t.Run("CompleteRequestsInOneBatch", func(t *testing.T) {
		// Complete requests buffered while the queue is paused reach the partition as one batch.
		// A request whose ids fail must get its own error without stopping the requests after it.
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
				{Reference: "first", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
				{Reference: "last", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
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
		require.Equal(t, "first", lease.Items[1].Reference)
		require.Equal(t, "last", lease.Items[2].Reference)

		// Complete one item now, so its id no longer exists when the batch runs
		gone := lease.Items[0].Id
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      lease.Partition,
			RequestTimeout: "1m",
			Ids:            []string{gone},
		}))

		// An item that was never leased
		notLeased := writeRandomItems(t, ctx, c, queueName, 1)
		require.Len(t, notLeased, 1)

		require.NoError(t, d.Service().PauseQueue(ctx, queueName))

		requests := []*pb.QueueCompleteRequest{
			{
				QueueName:      queueName,
				Partition:      lease.Partition,
				RequestTimeout: "1m",
				Ids:            []string{lease.Items[1].Id},
			},
			{
				QueueName:      queueName,
				Partition:      lease.Partition,
				RequestTimeout: "1m",
				Ids:            []string{gone},
			},
			{
				QueueName:      queueName,
				Partition:      lease.Partition,
				RequestTimeout: "1m",
				Ids:            []string{notLeased[0].Id},
			},
			{
				QueueName:      queueName,
				Partition:      lease.Partition,
				RequestTimeout: "1m",
				Ids:            []string{lease.Items[2].Id},
			},
		}

		errs := completeWhilePaused(ctx, c, requests, func() {
			// Wait until every complete request is buffered in the paused queue
			assert.Eventually(t, func() bool {
				var stats pb.QueueStatsResponse
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
					return false
				}
				return len(stats.LogicalQueues) == 1 && int(stats.LogicalQueues[0].CompleteWaiting) == len(requests)
			}, 10*clock.Second, 100*clock.Millisecond)
			assert.NoError(t, d.Service().ResumeQueue(ctx, queueName))
		})

		assert.NoError(t, errs[0])
		var e duh.Error
		require.True(t, errors.As(errs[1], &e))
		assert.Contains(t, e.Message(), "invalid storage id; '"+gone+"' does not exist")
		assert.Equal(t, duh.CodeBadRequest, e.Code())
		require.True(t, errors.As(errs[2], &e))
		assert.Contains(t, e.Message(), "item(s) cannot be completed; '"+notLeased[0].Id+"' is not marked as leased")
		assert.Equal(t, duh.CodeBadRequest, e.Code())
		assert.NoError(t, errs[3])

		// Only the item that was never leased remains
		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		require.Len(t, list.Items, 1)
		assert.Equal(t, notLeased[0].Id, list.Items[0].Id)
	})

	t.Run("Errors", func(t *testing.T) {
		storage := setup()
		defer tearDown()

		t.Run("QueueClear", func(t *testing.T) {
			t.Run("StorageFails", func(t *testing.T) {
				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage)})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()

				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					QueueName:           queueName,
					RequestedPartitions: 2,
				})
				// Write to both partitions so a clear that stops at the failing one is visible.
				var before [2]pb.StorageItemsListResponse
				for partition := range 2 {
					var imported pb.StorageItemsImportResponse
					require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
						QueueName: queueName,
						Partition: int32(partition),
						Items: []*pb.StorageItem{
							{ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(clock.Hour)), Payload: []byte("item")},
						},
					}, &imported))
					require.NoError(t, c.StorageItemsList(ctx, queueName, partition, &before[partition], nil))
					require.NotEmpty(t, before[partition].Items)
				}

				// Partition 0 fails and partition 1 is healthy.
				fault.failingClear.Store(1)
				start := clock.Now().UTC()
				err := c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true, Destructive: true})
				require.Error(t, err)
				assert.Less(t, clock.Now().UTC().Sub(start), 5*clock.Second)
				var e duh.Error
				require.True(t, errors.As(err, &e))
				assert.Equal(t, duh.CodeInternalError, e.Code())

				// The failing partition keeps its items and the healthy one is cleared.
				var list pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
				assert.Len(t, list.Items, len(before[0].Items))
				require.NoError(t, c.StorageItemsList(ctx, queueName, 1, &list, nil))
				assert.Empty(t, list.Items)

				// A clear after storage recovers empties the partition that failed.
				fault.failingClear.Store(0)
				require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true, Destructive: true}))
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
				assert.Empty(t, list.Items)
			})
		})

		t.Run("QueueProduce", func(t *testing.T) {
			t.Run("WriteFails", func(t *testing.T) {
				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage)})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()

				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					QueueName:           queueName,
					RequestedPartitions: 1,
				})
				// An empty queue schedules its next lifecycle run far beyond the 24h ExpireTimeout.
				waitForLifecycleRun(t, ctx, c, queueName)

				fault.failingProduce.Store(true)
				err := c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          produceRandomItems(3),
				})
				require.Error(t, err)
				var e duh.Error
				require.True(t, errors.As(err, &e))
				assert.Equal(t, "internal error, try your request again", e.Message())
				assert.Equal(t, duh.CodeRetryRequest, e.Code())

				var list pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
				assert.Empty(t, list.Items)

				var stats pb.QueueStatsResponse
				require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
				assert.Equal(t, int32(0), stats.LogicalQueues[0].Partitions[0].UnLeased)
				// Items that were never stored do not pull the lifecycle forward to their expire deadline.
				assert.Greater(t, nextLifecycleRun(t, ctx, c, queueName), 24*clock.Hour)

				// A produce after storage recovers counts only its own items, so the failed write left nothing in un_leased.
				fault.failingProduce.Store(false)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          produceRandomItems(2),
				}))
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
				assert.Len(t, list.Items, 2)
				require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
				assert.Equal(t, int32(2), stats.LogicalQueues[0].Partitions[0].UnLeased)
			})

			t.Run("WriteFailsForEveryRequestInBatch", func(t *testing.T) {
				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage)})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()

				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					QueueName:           queueName,
					RequestedPartitions: 1,
				})
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          produceRandomItems(4),
				}))

				// Requests held while the queue is paused reach the partition in one pass, so the
				// lease counts the items of the failed write as un-leased before that write fails.
				fault.failingProduce.Store(true)
				require.NoError(t, d.Service().PauseQueue(ctx, queueName))

				callCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				var produceErrs [2]error
				var lease pb.QueueLeaseResponse
				var leaseErr error
				duringCalls(cancel, func() {
					require.Eventually(t, func() bool {
						var stats pb.QueueStatsResponse
						if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
							return false
						}
						return len(stats.LogicalQueues) == 1 &&
							stats.LogicalQueues[0].ProduceWaiting == 2 &&
							stats.LogicalQueues[0].LeaseWaiting == 1
					}, 5*clock.Second, 20*clock.Millisecond)
					require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
				}, func() {
					produceErrs[0] = c.QueueProduce(callCtx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "1m",
						Items:          produceRandomItems(2),
					})
				}, func() {
					produceErrs[1] = c.QueueProduce(callCtx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "1m",
						Items:          produceRandomItems(3),
					})
				}, func() {
					leaseErr = c.QueueLease(callCtx, &pb.QueueLeaseRequest{
						ClientId:       random.String("client-", 10),
						RequestTimeout: "1m",
						QueueName:      queueName,
						BatchSize:      6,
					}, &lease)
				})

				for _, err := range produceErrs {
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Equal(t, duh.CodeRetryRequest, e.Code())
				}
				require.NoError(t, leaseErr)
				assert.Len(t, lease.Items, 4)

				var list pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
				assert.Len(t, list.Items, 4)

				var stats pb.QueueStatsResponse
				require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
				assert.Equal(t, int32(0), stats.LogicalQueues[0].Partitions[0].UnLeased)
				assert.Equal(t, int32(4), stats.LogicalQueues[0].Partitions[0].TotalLeased)
			})

			t.Run("RequestTimeoutWhilePartitionFailed", func(t *testing.T) {
				now := clock.NewProvider()
				now.Freeze(clock.Now())
				defer now.UnFreeze()

				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage), Clock: now})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()

				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					QueueName:           queueName,
					RequestedPartitions: 1,
				})

				// The first lifecycle run waits for the clock to move. Its scan fails, which marks the
				// only partition failed, so a produce waits unassigned until the partition recovers and
				// another request arrives.
				fault.scanFailing.Store(&queueName)
				now.Advance(clock.Millisecond)
				var stats pb.QueueStatsResponse
				require.Eventually(t, func() bool {
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
						return false
					}
					return stats.LogicalQueues[0].Partitions[0].Failures != 0
				}, 5*clock.Second, 20*clock.Millisecond)

				callCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				var produceErr error
				duringCalls(cancel, func() {
					require.Eventually(t, func() bool {
						if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
							return false
						}
						return stats.LogicalQueues[0].ProduceWaiting == 1
					}, 5*clock.Second, 20*clock.Millisecond)

					// The partition stays failed past this advance because its lifecycle scan still fails.
					now.Advance(2 * clock.Second)
				}, func() {
					produceErr = c.QueueProduce(callCtx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "1s",
						Items:          produceRandomItems(3),
					})
				})
				var e duh.Error
				require.True(t, errors.As(produceErr, &e))
				assert.Equal(t, duh.CodeRetryRequest, e.Code())
				assert.Equal(t, "internal error, try your request again", e.Message())

				require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
				assert.Equal(t, int32(0), stats.LogicalQueues[0].ProduceWaiting)

				// Once the partition recovers, a new produce is written and the timed-out one is not.
				fault.scanFailing.Store(nil)
				require.Eventually(t, func() bool {
					now.Advance(clock.Second)
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
						return false
					}
					return stats.LogicalQueues[0].Partitions[0].Failures == 0
				}, 5*clock.Second, 20*clock.Millisecond)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          produceRandomItems(2),
				}))
				var list pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
				assert.Len(t, list.Items, 2)
			})

			t.Run("FirstPartitionFailed", func(t *testing.T) {
				now := clock.NewProvider()
				now.Freeze(clock.Now())
				defer now.UnFreeze()

				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage), Clock: now})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()

				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					QueueName:           queueName,
					RequestedPartitions: 2,
				})

				// Both partitions start empty and the first produce lands in partition 0, so partition 1
				// has the fewest unleased items and sorts first.
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          produceRandomItems(1),
				}))

				partitionFailures := func(num int32) int32 {
					var stats pb.QueueStatsResponse
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
						return -1
					}
					for _, p := range stats.LogicalQueues[0].Partitions {
						if p.Partition == num {
							return p.Failures
						}
					}
					return -1
				}
				itemCount := func(num int) int {
					var list pb.StorageItemsListResponse
					require.NoError(t, c.StorageItemsList(ctx, queueName, num, &list, nil))
					return len(list.Items)
				}

				fault.scanFailingOnly.Store(2)
				fault.scanFailing.Store(&queueName)
				now.Advance(clock.Millisecond)
				require.Eventually(t, func() bool { return partitionFailures(1) > 0 }, 5*clock.Second, 20*clock.Millisecond)

				produceCtx, cancel := context.WithTimeout(ctx, 3*clock.Second)
				defer cancel()
				require.NoError(t, c.QueueProduce(produceCtx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          produceRandomItems(2),
				}))
				assert.Equal(t, 3, itemCount(0))
				assert.Equal(t, 0, itemCount(1))

				// Once partition 1 recovers it has the fewest unleased items again and takes the next produce.
				fault.scanFailing.Store(nil)
				require.Eventually(t, func() bool {
					now.Advance(clock.Second)
					return partitionFailures(1) == 0
				}, 5*clock.Second, 20*clock.Millisecond)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          produceRandomItems(1),
				}))
				assert.Equal(t, 1, itemCount(1))
			})

			t.Run("ClientGoneWhilePartitionFailed", func(t *testing.T) {
				now := clock.NewProvider()
				now.Freeze(clock.Now())
				defer now.UnFreeze()

				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage), Clock: now})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()

				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					QueueName:           queueName,
					RequestedPartitions: 1,
				})

				// The first lifecycle run waits for the clock to move. Its scan fails, which marks the
				// only partition failed, so a produce waits unassigned until the partition recovers and
				// another request arrives.
				fault.scanFailing.Store(&queueName)
				now.Advance(clock.Millisecond)
				var stats pb.QueueStatsResponse
				require.Eventually(t, func() bool {
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
						return false
					}
					return stats.LogicalQueues[0].Partitions[0].Failures != 0
				}, 5*clock.Second, 20*clock.Millisecond)

				gone, cancel := context.WithCancel(ctx)
				defer cancel()
				var abandonedErr error
				duringCalls(cancel, func() {
					require.Eventually(t, func() bool {
						if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
							return false
						}
						return stats.LogicalQueues[0].ProduceWaiting == 1
					}, 5*clock.Second, 20*clock.Millisecond)
					cancel()
				}, func() {
					abandonedErr = c.QueueProduce(gone, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "10m",
						Items:          produceRandomItems(3),
					})
				})
				require.ErrorContains(t, abandonedErr, "context canceled")

				// The server has seen the client go once the abandoned produce call has returned.
				require.Eventually(t, func() bool {
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
						return false
					}
					return stats.LogicalQueues[0].InFlight == 0
				}, 5*clock.Second, 20*clock.Millisecond)

				// The next produce to arrive finds the abandoned one and drops it. That produce is answered
				// once its short request timeout passes, which proves it reached the queue, so nothing waits
				// afterwards. The abandoned produce's own request timeout is far beyond these advances.
				timeoutCtx, timeoutCancel := context.WithCancel(ctx)
				defer timeoutCancel()
				var timedOutErr error
				var answered atomic.Bool
				duringCalls(timeoutCancel, func() {
					require.Eventually(t, func() bool {
						now.Advance(clock.Second)
						return answered.Load()
					}, 5*clock.Second, 20*clock.Millisecond)
				}, func() {
					timedOutErr = c.QueueProduce(timeoutCtx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "1s",
						Items:          produceRandomItems(1),
					})
					answered.Store(true)
				})
				var e duh.Error
				require.True(t, errors.As(timedOutErr, &e))
				assert.Equal(t, duh.CodeRetryRequest, e.Code())
				require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
				assert.Equal(t, int32(0), stats.LogicalQueues[0].ProduceWaiting)

				// Once the partition recovers, a new produce writes the waiting one with it, and the
				// abandoned one is not written.
				callCtx, callCancel := context.WithCancel(ctx)
				defer callCancel()
				var waitingErr error
				duringCalls(callCancel, func() {
					require.Eventually(t, func() bool {
						if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
							return false
						}
						return stats.LogicalQueues[0].ProduceWaiting == 1
					}, 5*clock.Second, 20*clock.Millisecond)

					fault.scanFailing.Store(nil)
					require.Eventually(t, func() bool {
						now.Advance(clock.Second)
						if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
							return false
						}
						return stats.LogicalQueues[0].Partitions[0].Failures == 0
					}, 5*clock.Second, 20*clock.Millisecond)
					require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "5s",
						Items:          produceRandomItems(1),
					}))
				}, func() {
					waitingErr = c.QueueProduce(callCtx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "1m",
						Items:          produceRandomItems(2),
					})
				})
				require.NoError(t, waitingErr)
				var list pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
				assert.Len(t, list.Items, 3)
			})

			t.Run("RequestTimeoutBeforeRecoveryPass", func(t *testing.T) {
				now := clock.NewProvider()
				now.Freeze(clock.Now())
				defer now.UnFreeze()

				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage), Clock: now})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()

				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					QueueName:           queueName,
					RequestedPartitions: 1,
				})

				// The first lifecycle run waits for the clock to move. Its scan fails, which marks the
				// only partition failed.
				fault.scanFailing.Store(&queueName)
				now.Advance(clock.Millisecond)
				var stats pb.QueueStatsResponse
				require.Eventually(t, func() bool {
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
						return false
					}
					return stats.LogicalQueues[0].Partitions[0].Failures != 0
				}, 5*clock.Second, 20*clock.Millisecond)

				// With a lease waiting, the lifecycle run that recovers the partition goes on to assign
				// waiting produces. This advance makes that run and the produce's request timeout due
				// together, and whichever the queue handles first, the produce must not be written.
				produceCtx, produceCancel := context.WithCancel(ctx)
				defer produceCancel()
				leaseCtx, leaseCancel := context.WithCancel(ctx)
				defer leaseCancel()
				var produceErr, leaseErr error
				duringCalls(func() {
					produceCancel()
					leaseCancel()
				}, func() {
					require.Eventually(t, func() bool {
						if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
							return false
						}
						return stats.LogicalQueues[0].ProduceWaiting == 1 && stats.LogicalQueues[0].LeaseWaiting == 1
					}, 5*clock.Second, 20*clock.Millisecond)

					fault.scanFailing.Store(nil)
					now.Advance(2 * clock.Second)
					require.Eventually(t, func() bool {
						if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
							return false
						}
						return stats.LogicalQueues[0].ProduceWaiting == 0
					}, 5*clock.Second, 20*clock.Millisecond)
					leaseCancel()
				}, func() {
					produceErr = c.QueueProduce(produceCtx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "1s",
						Items:          produceRandomItems(3),
					})
				}, func() {
					var lease pb.QueueLeaseResponse
					leaseErr = c.QueueLease(leaseCtx, &pb.QueueLeaseRequest{
						ClientId:       random.String("client-", 10),
						RequestTimeout: "5m",
						QueueName:      queueName,
						BatchSize:      1,
					}, &lease)
				})

				var e duh.Error
				require.True(t, errors.As(produceErr, &e))
				assert.Equal(t, duh.CodeRetryRequest, e.Code())
				// The lease found nothing to take and waited until it was cancelled.
				require.ErrorContains(t, leaseErr, "context canceled")
				var list pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
				assert.Empty(t, list.Items)
			})

			t.Run("WaitingProduceStoredOnRecoveryWithoutLease", func(t *testing.T) {
				now := clock.NewProvider()
				now.Freeze(clock.Now())
				defer now.UnFreeze()

				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage), Clock: now})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()

				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					QueueName:           queueName,
					RequestedPartitions: 1,
				})

				fault.scanFailing.Store(&queueName)
				now.Advance(clock.Millisecond)
				var stats pb.QueueStatsResponse
				require.Eventually(t, func() bool {
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
						return false
					}
					return stats.LogicalQueues[0].Partitions[0].Failures != 0
				}, 5*clock.Second, 20*clock.Millisecond)

				// No lease is waiting, so only the lifecycle run that recovers the partition can
				// assign the waiting produce.
				produceCtx, produceCancel := context.WithCancel(ctx)
				defer produceCancel()
				var produceErr error
				duringCalls(produceCancel, func() {
					require.Eventually(t, func() bool {
						if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
							return false
						}
						return stats.LogicalQueues[0].ProduceWaiting == 1
					}, 5*clock.Second, 20*clock.Millisecond)

					fault.scanFailing.Store(nil)
					require.Eventually(t, func() bool {
						now.Advance(6 * clock.Second)
						if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
							return false
						}
						return stats.LogicalQueues[0].ProduceWaiting == 0
					}, 5*clock.Second, 20*clock.Millisecond)
				}, func() {
					produceErr = c.QueueProduce(produceCtx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "10m",
						Items:          produceRandomItems(3),
					})
				})
				require.NoError(t, produceErr)
				var list pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
				assert.Len(t, list.Items, 3)
			})

			var queueName = random.String("queue-", 10)
			d := newDaemon(t, 5*clock.Second, svc.Config{StorageConfig: storage})
			defer d.Shutdown(t)
			c, ctx := d.MustClient(), d.Context()
			maxItems := produceRandomItems(1_001)

			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				QueueName:           queueName,
				RequestedPartitions: 1,
			}))

			for _, test := range []struct {
				Name string
				Req  *pb.QueueProduceRequest
				Msg  string
				Code int
			}{
				{
					Name: "EmptyRequest",
					Req:  &pb.QueueProduceRequest{},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidQueue",
					Req: &pb.QueueProduceRequest{
						QueueName:      "invalid~queue",
						RequestTimeout: "1m",
					},
					Msg:  "queue name is invalid; 'invalid~queue' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "NoItemsNilPointer",
					Req: &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "1m",
						Items:          nil,
					},
					Msg:  "items cannot be empty; at least one item is required",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "NoItemsEmptyList",
					Req: &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "1m",
						Items:          []*pb.QueueProduceItem{},
					},
					Msg:  "items cannot be empty; at least one item is required",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ItemsWithNoPayloadAreOkay",
					Req: &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "5m",
						Items: []*pb.QueueProduceItem{
							{},
							{},
						},
					},
					Code: duh.CodeOK,
				},
				{
					Name: "RequestTimeoutIsRequired",
					Req: &pb.QueueProduceRequest{
						QueueName: queueName,
						Items: []*pb.QueueProduceItem{
							{},
						},
					},
					Msg:  "request timeout is required; '5m' is recommended, 15m is the maximum",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "RequestTimeoutTooLong",
					Req: &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "16m",
						Items: []*pb.QueueProduceItem{
							{},
						},
					},
					Msg:  "request timeout is invalid; maximum timeout is '15m' but '16m0s' was requested",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidRequestTimeout",
					Req: &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "foo",
						Items: []*pb.QueueProduceItem{
							{},
						},
					},
					Msg: "request timeout is invalid; time: invalid duration \"foo\"" +
						" - expected format: 900ms, 5m or 15m",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "MaxNumberOfItems",
					Req: &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "1m",
						Items:          maxItems,
					},
					Msg:  "items is invalid; max_produce_batch_size is 1000 but received 1001",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					err := c.QueueProduce(ctx, test.Req)
					if test.Code != duh.CodeOK {
						var e duh.Error
						require.True(t, errors.As(err, &e))
						assert.Equal(t, test.Msg, e.Message())
						assert.Equal(t, test.Code, e.Code())
					}
				})
			}
		})

		t.Run("QueueLeaseWriteFails", func(t *testing.T) {
			t.Run("TimesOutEmpty", func(t *testing.T) {
				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage)})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()
				queueName := produceLeaseFaultItems(t, ctx, c)

				fault.failingLease.Store(true)
				var lease pb.QueueLeaseResponse
				err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "1s",
					QueueName:      queueName,
					BatchSize:      2,
				}, &lease)
				var e duh.Error
				require.True(t, errors.As(err, &e))
				assert.Equal(t, querator.MsgRequestTimeout, e.Message())
				assert.Empty(t, lease.Items)
				assert.False(t, isLeased(t, ctx, c, queueName, "A"))
				assert.False(t, isLeased(t, ctx, c, queueName, "B"))
				assert.Equal(t, leaseCounts{UnLeased: 2, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))

				fault.failingLease.Store(false)
				assert.Equal(t, []string{"A", "B"}, leaseAndCompleteReferences(t, ctx, c, queueName))
			})

			t.Run("LaterPassAnswersWaitingLease", func(t *testing.T) {
				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage)})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()
				queueName := produceLeaseFaultItems(t, ctx, c)

				fault.failingLease.Store(true)
				var lease pb.QueueLeaseResponse
				var leaseErr error
				backgroundCall{
					Call: func() {
						leaseErr = c.QueueLease(ctx, &pb.QueueLeaseRequest{
							ClientId:       random.String("client-", 10),
							RequestTimeout: "10s",
							QueueName:      queueName,
							BatchSize:      2,
						}, &lease)
					},
					While: func() {
						require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 1))
						assert.False(t, isLeased(t, ctx, c, queueName, "A"))
						assert.False(t, isLeased(t, ctx, c, queueName, "B"))

						// Resuming runs another pass that tries the waiting request again and fails
						// again, and neither pass moves the counts.
						require.NoError(t, d.Service().PauseQueue(ctx, queueName))
						require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
						require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 1))
						assert.Equal(t, leaseCounts{UnLeased: 2, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))

						// A produce runs a later pass, which answers the waiting request.
						fault.failingLease.Store(false)
						require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
							QueueName:      queueName,
							RequestTimeout: "5s",
							Items:          []*pb.QueueProduceItem{{Reference: "C", Bytes: []byte("c")}},
						}))
					},
				}.run()
				require.NoError(t, leaseErr)

				var references []string
				for _, item := range lease.Items {
					references = append(references, item.Reference)
					assert.True(t, isLeased(t, ctx, c, queueName, itemReference(item.Reference)))
				}
				assert.Equal(t, []string{"A", "B"}, references)
				assert.False(t, isLeased(t, ctx, c, queueName, "C"))
				assert.Equal(t, leaseCounts{UnLeased: 1, TotalLeased: 2}, partitionLeaseCounts(t, ctx, c, queueName))
			})

			t.Run("SeveralWaitingLeases", func(t *testing.T) {
				var fault partitionFault
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage)})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()
				queueName := produceLeaseFaultItems(t, ctx, c)

				fault.failingLease.Store(true)
				require.NoError(t, d.Service().PauseQueue(ctx, queueName))
				leases := make([]pb.QueueLeaseResponse, 2)
				errs := make([]error, 2)
				backgroundCall{
					Call: func() {
						concurrently(2, func(i int) {
							errs[i] = c.QueueLease(ctx, &pb.QueueLeaseRequest{
								ClientId:       random.String("client-", 10),
								RequestTimeout: "10s",
								QueueName:      queueName,
								BatchSize:      1,
							}, &leases[i])
						})
					},
					While: func() {
						require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 2))
						// Resuming runs one pass that assigns both requests to the partition, and
						// the failed write leaves both waiting without moving the counts.
						require.NoError(t, d.Service().ResumeQueue(ctx, queueName))
						require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 2))
						assert.Equal(t, leaseCounts{UnLeased: 2, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))

						fault.failingLease.Store(false)
						require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
							QueueName:      queueName,
							RequestTimeout: "5s",
							Items:          []*pb.QueueProduceItem{{Reference: "C", Bytes: []byte("c")}},
						}))
					},
				}.run()

				var references []string
				for i := range leases {
					require.NoError(t, errs[i])
					require.Len(t, leases[i].Items, 1)
					references = append(references, leases[i].Items[0].Reference)
					assert.True(t, isLeased(t, ctx, c, queueName, itemReference(leases[i].Items[0].Reference)))
				}
				slices.Sort(references)
				assert.Equal(t, []string{"A", "B"}, references)
			})
		})

		t.Run("QueueLease", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			var clientID = random.String("client-", 10)
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
			defer d.Shutdown(t)
			c, ctx := d.MustClient(), d.Context()

			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				QueueName:           queueName,
				RequestedPartitions: 1,
			}))

			for _, tc := range []struct {
				Name string
				Req  *pb.QueueLeaseRequest
				Msg  string
				Code int
			}{
				{
					Name: "EmptyRequest",
					Req:  &pb.QueueLeaseRequest{},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ClientIdMissing",
					Req: &pb.QueueLeaseRequest{
						QueueName: queueName,
					},
					Msg:  "invalid client id; cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "BatchSizeCannotBeEmpty",
					Req: &pb.QueueLeaseRequest{
						QueueName: queueName,
						ClientId:  clientID,
					},
					Msg:  "invalid batch size; must be greater than zero",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "BatchSizeMaximum",
					Req: &pb.QueueLeaseRequest{
						QueueName: queueName,
						ClientId:  clientID,
						BatchSize: 1_001,
					},
					Msg:  "invalid batch size; max_lease_batch_size is 1000, but 1001 was requested",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "RequestTimeoutRequired",
					Req: &pb.QueueLeaseRequest{
						QueueName: queueName,
						ClientId:  clientID,
						BatchSize: 111,
					},
					Msg:  "request timeout is required; '5m' is recommended, 15m is the maximum",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "RequestTimeoutTooLong",
					Req: &pb.QueueLeaseRequest{
						QueueName:      queueName,
						ClientId:       clientID,
						BatchSize:      1_000,
						RequestTimeout: "16m",
					},
					Msg:  "request timeout is invalid; maximum timeout is '15m' but '16m0s' was requested",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "RequestTimeoutInvalid",
					Req: &pb.QueueLeaseRequest{
						QueueName:      queueName,
						ClientId:       clientID,
						BatchSize:      1_000,
						RequestTimeout: "foo",
					},
					Msg: "request timeout is invalid; time: invalid duration \"foo\"" +
						" - expected format: 900ms, 5m or 15m",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "MinimumRequestTimeoutIsAllowed",
					Req: &pb.QueueLeaseRequest{
						QueueName:      queueName,
						ClientId:       clientID,
						BatchSize:      1_000,
						RequestTimeout: "10ms",
					},
					Code: duh.CodeOK,
				},
				{
					Name: "RequestTimeoutIsTooShort",
					Req: &pb.QueueLeaseRequest{
						QueueName:      queueName,
						ClientId:       clientID,
						BatchSize:      1_000,
						RequestTimeout: "1ms",
					},
					Msg:  "request timeout is invalid; minimum timeout is '10ms' but '1ms' was requested",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(tc.Name, func(t *testing.T) {
					var res pb.QueueLeaseResponse
					err := c.QueueLease(ctx, tc.Req, &res)
					if tc.Code != duh.CodeOK {
						var e duh.Error
						require.True(t, errors.As(err, &e))
						assert.Equal(t, tc.Msg, e.Message())
						assert.Equal(t, tc.Code, e.Code())
						if e.Message() == "" {
							t.Logf("Error: %s", e.Error())
						}
					}
				})
			}

			t.Run("DuplicateClientId", func(t *testing.T) {
				clientID := random.String("client-", 10)
				var waitingErr error
				backgroundCall{
					Call: func() {
						var res pb.QueueLeaseResponse
						waitingErr = c.QueueLease(ctx, &pb.QueueLeaseRequest{
							QueueName:      queueName,
							ClientId:       clientID,
							RequestTimeout: "2s",
							BatchSize:      1,
						}, &res)
					},
					While: func() {
						// Wait until there is one lease client blocking on the queue.
						require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 1))

						// Should fail immediately
						var res pb.QueueLeaseResponse
						err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
							QueueName:      queueName,
							ClientId:       clientID,
							RequestTimeout: "2s",
							BatchSize:      1,
						}, &res)

						require.Error(t, err)
						var e duh.Error
						require.True(t, errors.As(err, &e))
						assert.Equal(t, querator.MsgDuplicateClientID, e.Message())
						assert.Equal(t, duh.CodeBadRequest, e.Code())
					},
				}.run()
				require.Error(t, waitingErr)
				var e duh.Error
				require.True(t, errors.As(waitingErr, &e))
				assert.Equal(t, querator.MsgRequestTimeout, e.Message())
				assert.Equal(t, duh.CodeRetryRequest, e.Code())
			})
		})
		t.Run("QueueComplete", func(t *testing.T) {
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

			// TODO: Produce and Lease some items to create actual ids
			listOfValidIds := []string{"valid-id"}

			for _, tc := range []struct {
				Name string
				Req  *pb.QueueCompleteRequest
				Msg  string
				Code int
			}{
				{
					Name: "EmptyRequest",
					Req:  &pb.QueueCompleteRequest{},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "IdsCannotBeEmpty",
					Req: &pb.QueueCompleteRequest{
						QueueName:      queueName,
						RequestTimeout: "1m0s",
					},
					Msg:  "ids is invalid; list of ids cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "RequestTimeoutRequired",
					Req: &pb.QueueCompleteRequest{
						Ids:       listOfValidIds,
						QueueName: queueName,
					},
					Msg:  "request timeout is required; '5m' is recommended, 15m is the maximum",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidPartition",
					Req: &pb.QueueCompleteRequest{
						Ids:            listOfValidIds,
						QueueName:      queueName,
						RequestTimeout: "1m0s",
						Partition:      65234,
					},
					Msg:  "partition is invalid; '65234' is not a valid partition",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "RequestTimeoutTooLong",
					Req: &pb.QueueCompleteRequest{
						Ids:            listOfValidIds,
						QueueName:      queueName,
						RequestTimeout: "16m0s",
					},
					Msg:  "request timeout is invalid; maximum timeout is '15m' but '16m0s' was requested",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "RequestTimeoutInvalid",
					Req: &pb.QueueCompleteRequest{
						Ids:            listOfValidIds,
						QueueName:      queueName,
						RequestTimeout: "foo",
					},
					Msg: "request timeout is invalid; time: invalid duration \"foo\"" +
						" - expected format: 900ms, 5m or 15m",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "RequestTimeoutTooShort",
					Req: &pb.QueueCompleteRequest{
						Ids:            listOfValidIds,
						QueueName:      queueName,
						RequestTimeout: "0ms",
					},
					Msg:  "request timeout is required; '5m' is recommended, 15m is the maximum",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidIds",
					Req: &pb.QueueCompleteRequest{
						Ids:            []string{"invalid-id", "invalid-ids"},
						QueueName:      queueName,
						RequestTimeout: "1m",
					},
					Msg:  "invalid storage id; 'invalid-id'",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "MaxNumberOfIds",
					Req: &pb.QueueCompleteRequest{
						QueueName:      queueName,
						RequestTimeout: "1m",
						Ids:            randomSliceStrings(1_001),
					},
					Msg:  "ids is invalid; max_complete_batch_size is 1000 but received 1001",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(tc.Name, func(t *testing.T) {
					err := c.QueueComplete(ctx, tc.Req)
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

		t.Run("DeadLetterMove", func(t *testing.T) {
			t.Run("WriteFailsKeepsItemUntilRetried", func(t *testing.T) {
				now := clock.NewProvider()
				now.Freeze(clock.Now())
				defer now.UnFreeze()

				var fault partitionFault
				d := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: fault.Wrap(storage), Clock: now})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()

				queueName := random.String("queue-", 10)
				dlqName := random.String("dlq-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqName,
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       ExpireTimeout,
					MaxAttempts:         1,
					DeadQueue:           dlqName,
					RequestedPartitions: 1,
				})
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          []*pb.QueueProduceItem{{Reference: "dead", Bytes: []byte("x")}},
				}))
				require.Equal(t, "dead", leaseOne(t, ctx, c, queueName).Reference)

				// The dead letter queue's storage refuses the write, so the move must not delete the source item.
				fault.failingProduce.Store(true)
				now.Advance(2 * clock.Minute)
				waitForLifecycleRun(t, ctx, c, queueName)
				assert.True(t, isLeased(t, ctx, c, queueName, "dead"))

				var resp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &resp, nil))
				assert.Empty(t, resp.Items)

				// A later lifecycle run moves the item once the dead letter queue's storage accepts writes.
				fault.failingProduce.Store(false)
				now.Advance(5 * clock.Second)
				require.Eventually(t, func() bool {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return false
					}
					return len(resp.Items) == 0
				}, 5*clock.Second, 20*clock.Millisecond)

				require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &resp, nil))
				require.Len(t, resp.Items, 1)
				assert.Equal(t, "dead", resp.Items[0].Reference)
			})
		})

		t.Run("QueueReload", func(t *testing.T) {
			d := newDaemon(t, 5*clock.Second, svc.Config{StorageConfig: storage})
			defer d.Shutdown(t)
			c, ctx := d.MustClient(), d.Context()

			t.Run("InvalidQueueName", func(t *testing.T) {
				err := c.QueueReload(ctx, &pb.QueueReloadRequest{
					QueueName: "invalid~queue",
				})
				var e duh.Error
				require.True(t, errors.As(err, &e))
				assert.Contains(t, e.Message(), "queue name is invalid")
				assert.Contains(t, e.Message(), "cannot contain '~' character")
				assert.Equal(t, duh.CodeBadRequest, e.Code())
			})

			t.Run("NonExistentQueue", func(t *testing.T) {
				err := c.QueueReload(ctx, &pb.QueueReloadRequest{
					QueueName: "does-not-exist",
				})
				var e duh.Error
				require.True(t, errors.As(err, &e))
				assert.Contains(t, e.Message(), "does not exist")
				assert.Equal(t, duh.CodeBadRequest, e.Code())
			})

			t.Run("StatsPastReadTimeoutFailsPartition", func(t *testing.T) {
				const (
					numItems  = 5
					numLeased = 2
				)
				now := clock.NewProvider()
				now.Freeze(clock.Now())
				defer now.UnFreeze()

				fault := &partitionFault{}
				d := newDaemon(t, 10*clock.Second, svc.Config{
					StorageConfig: fault.Wrap(setup()),
					ReadTimeout:   100 * clock.Millisecond,
					Clock:         now,
				})
				defer func() {
					d.Shutdown(t)
					tearDown()
				}()
				c, ctx := d.MustClient(), d.Context()

				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       "24h0m0s",
					QueueName:           queueName,
					RequestedPartitions: 1,
				})
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					Items:          produceRandomItems(numItems),
					QueueName:      queueName,
					RequestTimeout: "5s",
				}))
				var leased pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      numLeased,
					RequestTimeout: "5s",
				}, &leased))
				require.Len(t, leased.Items, numLeased)

				partition := func() *pb.QueuePartitionStats {
					var stats pb.QueueStatsResponse
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats); err != nil {
						return nil
					}
					return stats.LogicalQueues[0].Partitions[0]
				}

				// A hung backend holds the stats call only until the read timeout, so the reload returns
				// and marks the partition failed. Stats from /v1/queue.stats carry no deadline and still pass.
				fault.statsHung.Store(true)
				require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{QueueName: queueName}))
				t.Run("Reload", func(t *testing.T) {
					p := partition()
					require.NotNil(t, p)
					assert.Equal(t, int32(1), p.Failures)
				})

				// Recovery retries the stats call with the read timeout, so a failed attempt returns and
				// counts another failure.
				t.Run("Recovery", func(t *testing.T) {
					require.Eventually(t, func() bool {
						now.Advance(clock.Second)
						p := partition()
						return p != nil && p.Failures >= 2
					}, 5*clock.Second, 20*clock.Millisecond)

					fault.statsHung.Store(false)
					require.Eventually(t, func() bool {
						now.Advance(clock.Second)
						p := partition()
						return p != nil && p.Failures == 0
					}, 5*clock.Second, 20*clock.Millisecond)
					p := partition()
					require.NotNil(t, p)
					assert.Equal(t, int32(numItems-numLeased), p.UnLeased)
					assert.Equal(t, int32(numLeased), p.TotalLeased)
				})

				// A queue starting up calls its partition's stats with the read timeout too.
				fault.statsHung.Store(true)
				queueName = random.String("queue-", 10)
				require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       "24h0m0s",
					QueueName:           queueName,
					RequestedPartitions: 1,
				}))
				t.Run("StartUp", func(t *testing.T) {
					p := partition()
					require.NotNil(t, p)
					assert.Equal(t, int32(1), p.Failures)
				})
			})
		})
	})

	t.Run("Timeout", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		// Create a queue
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
			MaxAttempts:         2,
		})

		t.Run("LeaseTimeout", func(t *testing.T) {
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: "flutter@shy.com",
						Encoding:  "friendship",
						Kind:      "yes",
						Bytes:     []byte("Could, I hold you against your will for a bit?"),
					},
				}}))

			var lease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &lease))

			leased := lease.Items[0]
			assert.Equal(t, "flutter@shy.com", leased.Reference)

			var resp pb.StorageItemsListResponse
			err := c.StorageItemsList(ctx, queueName, 0, &resp,
				&querator.ListOptions{Pivot: leased.Id, Limit: 1})
			require.NoError(t, err)
			require.Equal(t, leased.Id, resp.Items[0].Id)
			require.Equal(t, true, resp.Items[0].IsLeased)

			for i := 0; i < 4; i++ {
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: "rainbow@dash.com",
							Encoding:  "friendship",
							Kind:      "yes",
							Bytes:     []byte("20% cooler"),
						},
					}}))
			}

			// Advance time til we meet the LeaseTime set by the queue
			now.Advance(2 * clock.Minute)

			// Wait until the item is no longer leased
			err = retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
				var resp pb.StorageItemsListResponse
				err := c.StorageItemsList(ctx, queueName, 0, &resp, nil)
				if err != nil {
					return err
				}
				item := findInStorageList("flutter@shy.com", &resp)
				require.NotNil(t, item)
				if item.IsLeased == false {
					return nil
				}
				return fmt.Errorf("expected leased item to be false, for '%s'", resp.Items[0].Id)
			})
			require.NoError(t, err)

			err = c.StorageItemsList(ctx, queueName, 0, &resp,
				&querator.ListOptions{Pivot: leased.Id, Limit: 5})
			require.NoError(t, err)
			item := findInStorageList("flutter@shy.com", &resp)
			require.NotNil(t, item)

			require.NotEqual(t, leased.Id, item.Id)
			assert.Equal(t, "friendship", leased.Encoding)
			assert.True(t, item.LeaseDeadline.AsTime().Before(now.Now()))
			require.Equal(t, false, item.IsLeased)
			assert.Equal(t, int32(1), item.Attempts)

			t.Run("AttemptComplete", func(t *testing.T) {
				// Attempt to complete the leased id using the original id
				err = c.QueueComplete(ctx, &pb.QueueCompleteRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Ids: []string{
						leased.Id,
					},
				})
				require.Error(t, err)
				assert.Contains(t, err.Error(), "does not exist")

				// Attempt to complete with the new id after it was re-queued after failure
				err = c.QueueComplete(ctx, &pb.QueueCompleteRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Ids: []string{
						item.Id,
					},
				})
				require.Error(t, err)
				assert.Contains(t, err.Error(), "is not marked as leased")
			})

			t.Run("CanLeaseAgain", func(t *testing.T) {
				// Lease the item again
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      5,
				}, &lease))

				requeued := findInLeaseResp("flutter@shy.com", &lease)
				require.NotNil(t, leased)
				// Items placed back into un-leased status should have a different id
				require.NotEqual(t, requeued.Id, leased.Id)

				// Ensure the item is marked as leased
				err = c.StorageItemsList(ctx, queueName, 0, &resp,
					&querator.ListOptions{Pivot: requeued.Id, Limit: 5})
				require.NoError(t, err)
				require.Equal(t, requeued.Id, resp.Items[0].Id)
				assert.True(t, resp.Items[0].ExpireDeadline.AsTime().After(now.Now()))
				require.Equal(t, true, resp.Items[0].IsLeased)

				// Mark it as complete
				err = c.QueueComplete(ctx, &pb.QueueCompleteRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Ids: []string{
						requeued.Id,
					},
				})
				require.NoError(t, err)

				// Ensure the item is removed from the queue
				err = c.StorageItemsList(ctx, queueName, 0, &resp,
					&querator.ListOptions{Pivot: requeued.Id, Limit: 5})
				require.NoError(t, err)
				require.Nil(t, findInStorageList("flutter@shy.com", &resp))
			})
		})

	})

	t.Run("Lifecycle", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 60*clock.Second, svc.Config{
			StorageConfig: setup(),
			Clock:         now,
		})

		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		t.Run("LeaseTimeoutRequeue", func(t *testing.T) {
			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
				MaxAttempts:         10,
			})

			// Produce a single item (Attempts = 0)
			ref := random.String("ref-", 10)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: ref,
						Encoding:  "test-encoding",
						Kind:      "test-kind",
						Bytes:     []byte("test payload"),
					},
				},
			}))

			// Verify item is in storage with Attempts = 0
			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			item := findInStorageList(ref, &resp)
			require.NotNil(t, item)
			assert.Equal(t, int32(0), item.Attempts)
			assert.False(t, item.IsLeased)

			// Lease the item (Attempts increments to 1)
			var lease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &lease))

			require.Len(t, lease.Items, 1)
			leased := lease.Items[0]
			assert.Equal(t, ref, leased.Reference)
			assert.Equal(t, int32(1), leased.Attempts)
			originalID := leased.Id

			// Verify item is leased in storage
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			item = findInStorageList(ref, &resp)
			require.NotNil(t, item)
			assert.True(t, item.IsLeased)
			assert.Equal(t, int32(1), item.Attempts)

			// Advance clock by 2 minutes (past LeaseTimeout of 1m)
			now.Advance(2 * clock.Minute)

			// Wait for item to be re-queued by lifecycle scanner
			err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return err
				}
				item := findInStorageList(ref, &resp)
				if item == nil {
					return fmt.Errorf("item not found in storage")
				}
				if item.IsLeased {
					return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
				}
				return nil
			})
			require.NoError(t, err)

			// Verify re-queued item state
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			item = findInStorageList(ref, &resp)
			require.NotNil(t, item)
			assert.False(t, item.IsLeased)
			assert.Equal(t, int32(1), item.Attempts)
			assert.NotEqual(t, originalID, item.Id)
			assert.True(t, item.LeaseDeadline.AsTime().Before(now.Now()))

			// Lease the item again (Attempts increments to 2)
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &lease))

			require.Len(t, lease.Items, 1)
			leased = lease.Items[0]
			assert.Equal(t, ref, leased.Reference)
			assert.Equal(t, int32(2), leased.Attempts)

			// Verify in storage
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			item = findInStorageList(ref, &resp)
			require.NotNil(t, item)
			assert.True(t, item.IsLeased)
			assert.Equal(t, int32(2), item.Attempts)
		})

		t.Run("MaxAttemptsWithDLQ", func(t *testing.T) {
			// Create DLQ first
			dlqName := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})

			// Create main queue referencing DLQ
			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
				MaxAttempts:         2,
				DeadQueue:           dlqName,
			})

			// Produce an item to main queue
			ref := random.String("ref-", 10)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: ref,
						Encoding:  "test-encoding",
						Kind:      "test-kind",
						Bytes:     []byte("test payload"),
					},
				},
			}))

			// Verify item is in storage with Attempts = 0
			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			item := findInStorageList(ref, &resp)
			require.NotNil(t, item)
			assert.Equal(t, int32(0), item.Attempts)

			// First lease/timeout cycle: Attempts becomes 1
			var lease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &lease))

			require.Len(t, lease.Items, 1)
			leased := lease.Items[0]
			assert.Equal(t, ref, leased.Reference)
			assert.Equal(t, int32(1), leased.Attempts)

			// Advance clock by 2 minutes (past LeaseTimeout of 1m)
			now.Advance(2 * clock.Minute)

			// Wait for item to be re-queued after first timeout
			err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return err
				}
				item := findInStorageList(ref, &resp)
				if item == nil {
					return fmt.Errorf("item not found in storage")
				}
				if item.IsLeased {
					return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
				}
				return nil
			})
			require.NoError(t, err)

			// Verify item is still in main queue with Attempts = 1
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			item = findInStorageList(ref, &resp)
			require.NotNil(t, item)
			assert.False(t, item.IsLeased)
			assert.Equal(t, int32(1), item.Attempts)

			// Second lease/timeout cycle: Attempts becomes 2, then routes to DLQ
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &lease))

			require.Len(t, lease.Items, 1)
			leased = lease.Items[0]
			assert.Equal(t, ref, leased.Reference)
			assert.Equal(t, int32(2), leased.Attempts)
			secondLeaseID := leased.Id

			// Advance clock by 2 minutes again (past LeaseTimeout)
			now.Advance(2 * clock.Minute)

			// Wait for item to be removed from main queue (routed to DLQ)
			err = retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return err
				}
				if findInStorageList(ref, &resp) != nil {
					return fmt.Errorf("expected item to be removed from main queue")
				}
				return nil
			})
			require.NoError(t, err)

			// Verify item is NOT in main queue
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			require.Nil(t, findInStorageList(ref, &resp))

			// Verify item IS in DLQ with correct properties
			var dlqResp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
			dlqItem := findInStorageList(ref, &dlqResp)
			require.NotNil(t, dlqItem)

			// Verify DLQ item has SourceID set to the item ID from the second lease
			assert.Equal(t, secondLeaseID, dlqItem.SourceId)
			// The dead-letter move clears the original ID so the dead-letter queue generates a
			// fresh one; the new ID must be non-empty and distinct from the SourceID.
			assert.NotEmpty(t, dlqItem.Id)
			assert.NotEqual(t, dlqItem.SourceId, dlqItem.Id)

			// Verify item fields are preserved
			assert.Equal(t, ref, dlqItem.Reference)
			assert.Equal(t, "test-encoding", dlqItem.Encoding)
			assert.Equal(t, "test-kind", dlqItem.Kind)
			assert.Equal(t, []byte("test payload"), dlqItem.Payload)

			// Verify item state in DLQ
			assert.False(t, dlqItem.IsLeased)
			assert.True(t, dlqItem.LeaseDeadline.AsTime().Before(now.Now()))
		})

		t.Run("SourceIDPreservedOnLeaseExpiry", func(t *testing.T) {
			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
				MaxAttempts:         10,
			})

			// Import an item carrying a SourceID (the provenance a dead-letter move records).
			sourceID := random.String("source-", 10)
			ref := random.String("ref-", 10)
			var importResp pb.StorageItemsImportResponse
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: queueName,
				Partition: 0,
				Items: []*pb.StorageItem{
					{
						SourceId:       sourceID,
						Reference:      ref,
						Encoding:       "test-encoding",
						Kind:           "test-kind",
						Payload:        []byte("test payload"),
						ExpireDeadline: timestamppb.New(now.Now().Add(1 * clock.Hour)),
					},
				},
			}, &importResp))
			require.Len(t, importResp.Items, 1)

			// SourceID is visible on the imported item before it is leased.
			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			item := findInStorageList(ref, &resp)
			require.NotNil(t, item)
			assert.Equal(t, sourceID, item.SourceId)

			// Lease the item, then let the lease expire so the lifecycle re-queues it to the tail.
			var lease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &lease))
			require.Len(t, lease.Items, 1)

			now.Advance(2 * clock.Minute)

			// Wait for the item to be re-queued after the lease expires (still under MaxAttempts),
			// capturing it so SourceID can be asserted without a second list call.
			var requeued *pb.StorageItem
			require.NoError(t, retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return err
				}
				found := findInStorageList(ref, &resp)
				if found == nil {
					return fmt.Errorf("item not found in storage")
				}
				if found.IsLeased {
					return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
				}
				requeued = found
				return nil
			}))

			// SourceID must survive the lease-expiry re-queue (insert-before-delete tail move).
			require.NotNil(t, requeued)
			assert.Equal(t, sourceID, requeued.SourceId)
		})

		t.Run("UnlimitedAttempts", func(t *testing.T) {
			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       "24h0m0s",
				RequestedPartitions: 1,
				MaxAttempts:         0,
			})

			// Produce a single item (Attempts = 0)
			ref := random.String("ref-", 10)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: ref,
						Encoding:  "test-encoding",
						Kind:      "test-kind",
						Bytes:     []byte("test payload"),
					},
				},
			}))

			// Verify item is in storage with Attempts = 0
			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			item := findInStorageList(ref, &resp)
			require.NotNil(t, item)
			assert.Equal(t, int32(0), item.Attempts)

			// Loop 5 times to prove unlimited behavior
			const numCycles = 5
			for cycle := 1; cycle <= numCycles; cycle++ {
				// Lease the item (Attempts increments)
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))

				require.Len(t, lease.Items, 1)
				leased := lease.Items[0]
				assert.Equal(t, ref, leased.Reference)
				assert.Equal(t, int32(cycle), leased.Attempts)

				// Advance clock by 2 minutes (past LeaseTimeout of 1m)
				now.Advance(2 * clock.Minute)

				// Wait for item to be re-queued by lifecycle scanner
				err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					item := findInStorageList(ref, &resp)
					if item == nil {
						return fmt.Errorf("item not found in storage")
					}
					if item.IsLeased {
						return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify item is still in queue with expected Attempts count
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				item = findInStorageList(ref, &resp)
				require.NotNil(t, item)
				assert.False(t, item.IsLeased)
				assert.Equal(t, int32(cycle), item.Attempts)
			}

			// After 5 cycles: Verify item persists with Attempts = 5
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			item = findInStorageList(ref, &resp)
			require.NotNil(t, item)
			assert.Equal(t, int32(5), item.Attempts)
			assert.False(t, item.IsLeased)
		})

		t.Run("MaxAttempts", func(t *testing.T) {
			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
				MaxAttempts:         2,
			})

			// Produce an item
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: "durp@pony.com",
						Encoding:  "friendship",
						Kind:      "dum",
						Bytes:     []byte("stares ground and sky simultaneously...."),
					},
				}}))

			for i := 0; i < 2; i++ {
				// Lease the item produced
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))

				leased := lease.Items[0]
				assert.Equal(t, "durp@pony.com", leased.Reference)

				// Advance time til we meet the LeaseTime set by the queue
				now.Advance(2 * clock.Minute)

				// If this isn't the final attempt
				if leased.Attempts != 2 {
					// Wait until the item is no longer leased
					err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
						var resp pb.StorageItemsListResponse
						if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
							return err
						}
						item := findInStorageList("durp@pony.com", &resp)
						require.NotNil(t, item)
						if item.IsLeased == false {
							return nil
						}
						return fmt.Errorf("expected leased item to be false, for '%s'", resp.Items[0].Id)
					})
					require.NoError(t, err)
				}
			}

			// Wait until the item is deleted
			err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return err
				}
				if item := findInStorageList("durp@pony.com", &resp); item != nil {
					return fmt.Errorf("expected leased item to be deleted, for '%s'", resp.Items[0].Reference)
				}
				return nil
			})
			require.NoError(t, err)
		})

		t.Run("ExpireTimeout", func(t *testing.T) {
			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "5s",
				ExpireTimeout:       "10s",
				RequestedPartitions: 1,
			})

			// Produce an item (without leasing it)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: "applejack@honesty.com",
						Encoding:  "apples",
						Kind:      "bucking",
						Bytes:     []byte("The truth will set you free"),
					},
				}}))

			// Verify item exists in storage
			var resp pb.StorageItemsListResponse
			err := c.StorageItemsList(ctx, queueName, 0, &resp, nil)
			require.NoError(t, err)
			item := findInStorageList("applejack@honesty.com", &resp)
			require.NotNil(t, item)

			// Advance time past ExpireDeadline (ExpireTimeout is 10s, advance 15s)
			now.Advance(15 * clock.Second)

			// Wait until the item is deleted
			err = retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return err
				}
				if findInStorageList("applejack@honesty.com", &resp) != nil {
					return fmt.Errorf("expected item to be deleted")
				}
				return nil
			})
			require.NoError(t, err)
		})

	})

	t.Run("LifecycleMissedWork", func(t *testing.T) {
		t.Run("LeaseExpiredAtExactDeadlineIsRequeued", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "boundary", Bytes: []byte("x")}},
			}))
			require.Equal(t, "boundary", leaseOne(t, ctx, c, queueName).Reference)

			// The clock lands exactly on the lease deadline and stays there.
			now.Advance(clock.Minute)
			require.Eventually(t, func() bool {
				return isUnLeased(ctx, c, queueName, "boundary")
			}, 5*clock.Second, 20*clock.Millisecond)
			assert.Equal(t, "boundary", leaseOne(t, ctx, c, queueName).Reference)
		})

		t.Run("StuckDeadLetterMoveDoesNotDelayLaterLeaseExpiry", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 20*clock.Second, svc.Config{
				StorageConfig: setup(),
				Clock:         now,
				WriteTimeout:  200 * clock.Millisecond,
			})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			dlqName := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         2,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			})

			// A's second lease reaches MaxAttempts=2. B is leased 30s later, so B's lease
			// deadline is 30s after A's.
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "A", Bytes: []byte("a")}},
			}))
			require.Equal(t, "A", leaseOne(t, ctx, c, queueName).Reference)
			now.Advance(clock.Minute + clock.Second)
			require.Eventually(t, func() bool {
				return isUnLeased(ctx, c, queueName, "A")
			}, 5*clock.Second, 20*clock.Millisecond)

			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "B", Bytes: []byte("b")}},
			}))
			require.Equal(t, "A", leaseOne(t, ctx, c, queueName).Reference)
			now.Advance(30 * clock.Second)
			require.Equal(t, "B", leaseOne(t, ctx, c, queueName).Reference)

			// The produce to a paused dead letter queue blocks past WriteTimeout, so moving A
			// fails and A stays in the source queue with its lease deadline in the past.
			require.NoError(t, d.Service().PauseQueue(ctx, dlqName))
			defer func() { require.NoError(t, d.Service().ResumeQueue(ctx, dlqName)) }()
			now.Advance(31 * clock.Second)
			waitForLifecycleRun(t, ctx, c, queueName)
			assert.True(t, isLeased(t, ctx, c, queueName, "A"))

			// Past B's deadline, B must be requeued even though A is still stuck.
			now.Advance(clock.Minute)
			assert.Eventually(t, func() bool {
				return isUnLeased(ctx, c, queueName, "B")
			}, 5*clock.Second, 20*clock.Millisecond)
		})

		t.Run("FailedDeadLetterMoveIsRetried", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 20*clock.Second, svc.Config{
				StorageConfig: setup(),
				Clock:         now,
				WriteTimeout:  200 * clock.Millisecond,
			})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			dlqName := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         1,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			})
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "dead", Bytes: []byte("x")}},
			}))
			require.Equal(t, "dead", leaseOne(t, ctx, c, queueName).Reference)

			// The produce to a paused dead letter queue blocks past WriteTimeout, so the move fails.
			require.NoError(t, d.Service().PauseQueue(ctx, dlqName))
			now.Advance(2 * clock.Minute)
			waitForLifecycleRun(t, ctx, c, queueName)
			assert.True(t, isLeased(t, ctx, c, queueName, "dead"))

			// Unlike DeadLetterQueue/.../RetainsItemOnFailedMove, this checks when the retry runs:
			// within lifecycleBackOff.Max (5s), not at the item's ExpireDeadline, and not at once.
			next := nextLifecycleRun(t, ctx, c, queueName)
			assert.Greater(t, next, clock.Duration(0))
			assert.LessOrEqual(t, next, 5*clock.Second)

			// Resuming lets the blocked produce land in the dead letter queue. The source item is
			// deleted only by a later lifecycle run, whose second produce the dead letter queue
			// drops because the SourceID matches, so it holds exactly one copy.
			require.NoError(t, d.Service().ResumeQueue(ctx, dlqName))
			now.Advance(5 * clock.Second)
			require.Eventually(t, func() bool {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return false
				}
				return len(resp.Items) == 0
			}, 5*clock.Second, 20*clock.Millisecond)

			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &resp, nil))
			require.Len(t, resp.Items, 1)
			assert.Equal(t, "dead", resp.Items[0].Reference)
		})

		t.Run("UnansweredMoveWaitsOnceInFailedDeadLetterQueue", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			var fault partitionFault
			d := newDaemon(t, 30*clock.Second, svc.Config{
				StorageConfig: fault.Wrap(setup()),
				Clock:         now,
				WriteTimeout:  200 * clock.Millisecond,
			})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			dlqName := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         1,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			})
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "dead", Bytes: []byte("x")}},
			}))
			require.Equal(t, "dead", leaseOne(t, ctx, c, queueName).Reference)

			// The dead letter queue's only partition fails its lifecycle scan, so a move waits there
			// unassigned and the source stops waiting for it at the write timeout.
			fault.scanFailing.Store(&dlqName)
			now.Advance(clock.Millisecond)
			var stats pb.QueueStatsResponse
			require.Eventually(t, func() bool {
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: dlqName}, &stats); err != nil {
					return false
				}
				return stats.LogicalQueues[0].Partitions[0].Failures != 0
			}, 5*clock.Second, 20*clock.Millisecond)

			now.Advance(2 * clock.Minute)
			waitForLifecycleRun(t, ctx, c, queueName)

			// Later passes of the source send nothing while the first move is unanswered.
			for range 3 {
				now.Advance(5 * clock.Second)
				waitForLifecycleRun(t, ctx, c, queueName)
			}
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: dlqName}, &stats))
			assert.Equal(t, int32(1), stats.LogicalQueues[0].ProduceWaiting)
			assert.True(t, isLeased(t, ctx, c, queueName, "dead"))

			// Once the partition recovers, the waiting move is stored and the source deletes the item.
			fault.scanFailing.Store(nil)
			require.Eventually(t, func() bool {
				now.Advance(clock.Second)
				return partitionTotal(t, ctx, c, queueName) == 0
			}, 10*clock.Second, 50*clock.Millisecond)

			var list pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &list, nil))
			require.Len(t, list.Items, 1)
			assert.Equal(t, "dead", list.Items[0].Reference)
		})

		t.Run("UnansweredMoveEndsWhenDeadLetterQueueIsDeleted", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 20*clock.Second, svc.Config{
				StorageConfig: setup(),
				Clock:         now,
				WriteTimeout:  200 * clock.Millisecond,
			})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			dlqName := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         1,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			})
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "dead", Bytes: []byte("x")}},
			}))
			require.Equal(t, "dead", leaseOne(t, ctx, c, queueName).Reference)

			// The move waits in the paused dead letter queue after the source stops waiting for it.
			require.NoError(t, d.Service().PauseQueue(ctx, dlqName))
			now.Advance(2 * clock.Minute)
			waitForLifecycleRun(t, ctx, c, queueName)
			assert.True(t, isLeased(t, ctx, c, queueName, "dead"))

			// Deleting the dead letter queue answers the waiting move, so the source moves the item
			// to the dead letter queue it names now.
			nextDLQ := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           nextDLQ,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
				QueueName: queueName,
				DeadQueue: new(nextDLQ),
			}))
			require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: dlqName}))
			require.Eventually(t, func() bool {
				now.Advance(clock.Second)
				return partitionTotal(t, ctx, c, queueName) == 0
			}, 10*clock.Second, 50*clock.Millisecond)

			var list pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, nextDLQ, 0, &list, nil))
			require.Len(t, list.Items, 1)
			assert.Equal(t, "dead", list.Items[0].Reference)
		})

		t.Run("ExpiredUnLeasedItemLeavesSourceQueue", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			dlqName := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       "1m0s",
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			})
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "expired", Bytes: []byte("x")}},
			}))

			// The item expires without ever being leased, so the move must delete an un-leased item.
			now.Advance(2 * clock.Minute)
			require.Eventually(t, func() bool {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return false
				}
				return len(resp.Items) == 0
			}, 5*clock.Second, 20*clock.Millisecond)

			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &resp, nil))
			require.Len(t, resp.Items, 1)
			assert.Equal(t, "expired", resp.Items[0].Reference)
		})

		t.Run("StuckDeadLetterMoveBacksOff", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 20*clock.Second, svc.Config{
				StorageConfig: setup(),
				Clock:         now,
				WriteTimeout:  200 * clock.Millisecond,
			})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			dlqName := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         1,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			})
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "dead", Bytes: []byte("x")}},
			}))
			require.Equal(t, "dead", leaseOne(t, ctx, c, queueName).Reference)

			// The dead letter queue stays paused, so every lifecycle run fails the move again.
			require.NoError(t, d.Service().PauseQueue(ctx, dlqName))
			defer func() { require.NoError(t, d.Service().ResumeQueue(ctx, dlqName)) }()
			now.Advance(2 * clock.Minute)
			waitForLifecycleRun(t, ctx, c, queueName)
			first := nextLifecycleRun(t, ctx, c, queueName)

			now.Advance(first)
			waitForLifecycleRun(t, ctx, c, queueName)
			second := nextLifecycleRun(t, ctx, c, queueName)

			now.Advance(second)
			waitForLifecycleRun(t, ctx, c, queueName)
			third := nextLifecycleRun(t, ctx, c, queueName)

			// Each failed run waits longer than the one before, up to the back-off's 5s maximum.
			assert.Greater(t, second, first)
			assert.Greater(t, third, second)
			assert.LessOrEqual(t, third, 5*clock.Second)
			assert.True(t, isLeased(t, ctx, c, queueName, "dead"))
		})

		t.Run("HandOffWaitsForFailedDeadLetterPartition", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			var fault partitionFault
			d := newDaemon(t, 20*clock.Second, svc.Config{
				StorageConfig: fault.Wrap(setup()),
				Clock:         now,
				// The source lifecycle waits this long, in real time, for a hand-off. It keeps the hand-off's
				// caller waiting through the steps below.
				WriteTimeout: 5 * clock.Second,
			})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			dlqName := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "100ms",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         1,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			})
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "dead", Bytes: []byte("x")}},
			}))
			require.Equal(t, "dead", leaseOne(t, ctx, c, queueName).Reference)

			// The dead letter queue's first lifecycle run waits for the clock to move. Its scan fails,
			// which marks the only partition failed until a later run succeeds.
			fault.scanFailing.Store(&dlqName)
			now.Advance(clock.Millisecond)
			var stats pb.QueueStatsResponse
			require.Eventually(t, func() bool {
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: dlqName}, &stats); err != nil {
					return false
				}
				return stats.LogicalQueues[0].Partitions[0].Failures != 0
			}, 5*clock.Second, 20*clock.Millisecond)

			// The source lease expires and its lifecycle hands the item off. A hand-off has no request
			// timeout, so it waits for the failed partition instead of being answered.
			now.Advance(200 * clock.Millisecond)
			require.Eventually(t, func() bool {
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: dlqName}, &stats); err != nil {
					return false
				}
				return stats.LogicalQueues[0].ProduceWaiting == 1
			}, 5*clock.Second, 20*clock.Millisecond)

			// Once the partition recovers, the next produce writes the item. The poll above proved the
			// hand-off waited, and the source ID keeps a later copy from storing it twice.
			fault.scanFailing.Store(nil)
			require.Eventually(t, func() bool {
				now.Advance(clock.Second)
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: dlqName}, &stats); err != nil {
					return false
				}
				return stats.LogicalQueues[0].Partitions[0].Failures == 0
			}, 5*clock.Second, 20*clock.Millisecond)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      dlqName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "direct", Bytes: []byte("y")}},
			}))
			var list pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &list, nil))
			var references []string
			for _, item := range list.Items {
				references = append(references, item.Reference)
			}
			assert.ElementsMatch(t, []string{"dead", "direct"}, references)
		})

		t.Run("PausedDeadLetterQueueHoldsOneHandOff", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			d := newDaemon(t, 20*clock.Second, svc.Config{
				StorageConfig: setup(),
				Clock:         now,
				WriteTimeout:  200 * clock.Millisecond,
			})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			queueName := random.String("queue-", 10)
			dlqName := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         1,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			})
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          []*pb.QueueProduceItem{{Reference: "dead", Bytes: []byte("x")}},
			}))
			require.Equal(t, "dead", leaseOne(t, ctx, c, queueName).Reference)

			// Each failed run sends a new hand-off to the paused dead letter queue after the
			// previous one's caller gave up at WriteTimeout.
			require.NoError(t, d.Service().PauseQueue(ctx, dlqName))
			now.Advance(2 * clock.Minute)
			waitForLifecycleRun(t, ctx, c, queueName)
			for range 3 {
				now.Advance(nextLifecycleRun(t, ctx, c, queueName))
				waitForLifecycleRun(t, ctx, c, queueName)
			}

			var stats pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: dlqName}, &stats))
			require.Len(t, stats.LogicalQueues, 1)
			assert.Equal(t, int32(1), stats.LogicalQueues[0].ProduceWaiting)

			// The one hand-off still held lands on resume.
			require.NoError(t, d.Service().ResumeQueue(ctx, dlqName))
			require.Eventually(t, func() bool {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, dlqName, 0, &resp, nil); err != nil {
					return false
				}
				return len(resp.Items) == 1
			}, 5*clock.Second, 20*clock.Millisecond)

			// The next lifecycle run, within the back-off's 5s maximum, deletes the source item. Its
			// new hand-off is dropped by SourceID, so the dead letter queue still holds one copy.
			now.Advance(5 * clock.Second)
			require.Eventually(t, func() bool {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return false
				}
				return len(resp.Items) == 0
			}, 5*clock.Second, 20*clock.Millisecond)

			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &resp, nil))
			require.Len(t, resp.Items, 1)
			assert.Equal(t, "dead", resp.Items[0].Reference)
		})
	})

	t.Run("SourceID", func(t *testing.T) {
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

		t.Run("ImportWithSourceID", func(t *testing.T) {
			sourceID := random.String("source-", 10)

			// Import an item with SourceID set
			var importResp pb.StorageItemsImportResponse
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: queueName,
				Partition: 0,
				Items: []*pb.StorageItem{
					{
						SourceId:       sourceID,
						Reference:      "test-ref",
						Encoding:       "test-enc",
						Kind:           "test-kind",
						Payload:        []byte("test payload"),
						ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(1 * clock.Hour)),
						MaxAttempts:    3,
					},
				},
			}, &importResp))

			// List items and verify SourceID is preserved
			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			require.Len(t, resp.Items, 1)
			assert.Equal(t, sourceID, resp.Items[0].SourceId)
			assert.Equal(t, "test-ref", resp.Items[0].Reference)
		})

		t.Run("DuplicateSourceIDIgnored", func(t *testing.T) {
			sourceID := random.String("source-", 10)

			// Import first item with SourceID
			var importResp1 pb.StorageItemsImportResponse
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: queueName,
				Partition: 0,
				Items: []*pb.StorageItem{
					{
						SourceId:       sourceID,
						Reference:      "first-ref",
						Encoding:       "test-enc",
						Kind:           "test-kind",
						Payload:        []byte("first payload"),
						ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(1 * clock.Hour)),
						MaxAttempts:    3,
					},
				},
			}, &importResp1))

			// Import second item with same SourceID
			var importResp2 pb.StorageItemsImportResponse
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: queueName,
				Partition: 0,
				Items: []*pb.StorageItem{
					{
						SourceId:       sourceID,
						Reference:      "second-ref",
						Encoding:       "test-enc",
						Kind:           "test-kind",
						Payload:        []byte("second payload"),
						ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(1 * clock.Hour)),
						MaxAttempts:    3,
					},
				},
			}, &importResp2))

			// Verify only one item exists (duplicate was ignored)
			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))

			// Find items with this SourceID
			var count int
			for _, item := range resp.Items {
				if item.SourceId == sourceID {
					count++
					assert.Equal(t, "first-ref", item.Reference)
				}
			}
			assert.Equal(t, 1, count)
		})

		t.Run("SourceIDPreservedOnList", func(t *testing.T) {
			sourceID := random.String("source-", 10)

			// Import item with SourceID
			var importResp pb.StorageItemsImportResponse
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: queueName,
				Partition: 0,
				Items: []*pb.StorageItem{
					{
						SourceId:       sourceID,
						Reference:      "preserve-test",
						Encoding:       "test-enc",
						Kind:           "test-kind",
						Payload:        []byte("test payload"),
						ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(1 * clock.Hour)),
						MaxAttempts:    3,
					},
				},
			}, &importResp))

			// List items via public API
			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))

			// Find and verify our item
			var found bool
			for _, item := range resp.Items {
				if item.Reference == "preserve-test" {
					found = true
					assert.Equal(t, sourceID, item.SourceId)
				}
			}
			require.True(t, found)
		})

		t.Run("ItemsWithoutSourceID", func(t *testing.T) {
			// Import item without SourceID
			var importResp pb.StorageItemsImportResponse
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: queueName,
				Partition: 0,
				Items: []*pb.StorageItem{
					{
						Reference:      "no-source-id",
						Encoding:       "test-enc",
						Kind:           "test-kind",
						Payload:        []byte("test payload"),
						ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(1 * clock.Hour)),
						MaxAttempts:    3,
					},
				},
			}, &importResp))

			// List items and verify SourceID is empty/nil for regular items
			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))

			// Find and verify our item
			var found bool
			for _, item := range resp.Items {
				if item.Reference == "no-source-id" {
					found = true
					assert.Empty(t, item.SourceId)
				}
			}
			require.True(t, found)
		})
	})

	t.Run("DeadLetterQueue", func(t *testing.T) {
		t.Run("MaxAttempts", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			var queueName = random.String("queue-", 10)
			var dlqName = random.String("dlq-", 10)
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			// Create DLQ queue (no DLQ of its own)
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        "1m",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         10,
				RequestedPartitions: 1,
			}))

			// Create source queue with DLQ configured, max_attempts = 2, lease_timeout = 1m
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         2,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			}))

			// Produce an item
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: "max-attempts-test",
						Encoding:  "text",
						Kind:      "test",
						Bytes:     []byte("test payload"),
					},
				},
			}))

			// Lease item (attempt 1)
			var lease1 pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				QueueName:      queueName,
				RequestTimeout: "5m",
				ClientId:       "client-1",
				BatchSize:      10,
			}, &lease1))
			require.Len(t, lease1.Items, 1)

			// Advance time to expire lease (attempt 1 expires)
			now.Advance(2 * clock.Minute)

			// Wait for item to be re-queued after lease expiry
			require.Eventually(t, func() bool {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return false
				}
				if len(resp.Items) == 0 {
					return false
				}
				// Item should no longer be leased and have 1 attempt
				return !resp.Items[0].IsLeased && resp.Items[0].Attempts == 1
			}, 5*clock.Second, 100*clock.Millisecond)

			// Lease item again (attempt 2)
			var lease2 pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				QueueName:      queueName,
				RequestTimeout: "5m",
				ClientId:       "client-2",
				BatchSize:      10,
			}, &lease2))
			require.Len(t, lease2.Items, 1)

			// Advance time to expire lease (attempt 2 expires, item now at max_attempts)
			now.Advance(2 * clock.Minute)

			// Wait for lifecycle to process the dead item and move to DLQ
			require.Eventually(t, func() bool {
				var dlqResp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil); err != nil {
					return false
				}
				return len(dlqResp.Items) == 1
			}, 5*clock.Second, 100*clock.Millisecond)

			// Verify item in DLQ has correct properties
			var dlqResp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
			require.Len(t, dlqResp.Items, 1)

			dlqItem := dlqResp.Items[0]
			assert.Equal(t, "max-attempts-test", dlqItem.Reference)
			assert.NotEmpty(t, dlqItem.SourceId)
			assert.False(t, dlqItem.IsLeased)
			assert.Equal(t, int32(0), dlqItem.Attempts)

			// Verify item is no longer in source queue
			var sourceResp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &sourceResp, nil))
			assert.Empty(t, sourceResp.Items)
		})

		t.Run("ScheduledMaxAttempts", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			var queueName = random.String("queue-", 10)
			var dlqName = random.String("dlq-", 10)
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			// Create DLQ queue (no DLQ of its own)
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        "1m",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         10,
				RequestedPartitions: 1,
			}))

			// Create source queue with DLQ, MaxAttempts=2, LeaseTimeout=1m
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         2,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			}))

			// Produce item with future EnqueueAt
			enqueueAt := now.Now().Add(1 * clock.Minute)
			reference := random.String("ref-", 10)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: reference,
						Encoding:  "text",
						Kind:      "scheduled-dlq-test",
						Bytes:     []byte("test payload"),
						EnqueueAt: timestamppb.New(enqueueAt),
					},
				},
			}))

			// Verify item in scheduled list
			var scheduledList pb.StorageItemsListResponse
			err := c.StorageScheduledList(ctx, queueName, 0, &scheduledList, &querator.ListOptions{Limit: 10})
			require.NoError(t, err)
			require.Len(t, scheduledList.Items, 1)

			// Advance clock past EnqueueAt, wait for item to be ready
			now.Advance(2 * clock.Minute)

			// Wait for item to appear in ready queue
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

			// Lease item (attempt 1)
			var lease1 pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				QueueName:      queueName,
				RequestTimeout: "5m",
				ClientId:       "client-1",
				BatchSize:      10,
			}, &lease1))
			require.Len(t, lease1.Items, 1)

			// Advance time to expire lease (attempt 1 expires)
			now.Advance(2 * clock.Minute)

			// Wait for item to be re-queued after lease expiry
			require.Eventually(t, func() bool {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return false
				}
				if len(resp.Items) == 0 {
					return false
				}
				return !resp.Items[0].IsLeased && resp.Items[0].Attempts == 1
			}, 5*clock.Second, 100*clock.Millisecond)

			// Lease item again (attempt 2)
			var lease2 pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				QueueName:      queueName,
				RequestTimeout: "5m",
				ClientId:       "client-2",
				BatchSize:      10,
			}, &lease2))
			require.Len(t, lease2.Items, 1)

			// Advance time to expire lease (attempt 2 expires, item now at MaxAttempts)
			now.Advance(2 * clock.Minute)

			// Wait for lifecycle to process the dead item and move to DLQ
			require.Eventually(t, func() bool {
				var dlqResp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil); err != nil {
					return false
				}
				return len(dlqResp.Items) == 1
			}, 5*clock.Second, 100*clock.Millisecond)

			// Verify item in DLQ has correct properties
			var dlqResp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
			require.Len(t, dlqResp.Items, 1)

			dlqItem := dlqResp.Items[0]
			assert.Equal(t, reference, dlqItem.Reference)
			assert.NotEmpty(t, dlqItem.SourceId)
			assert.False(t, dlqItem.IsLeased)
			assert.Equal(t, int32(0), dlqItem.Attempts)

			// Verify item removed from source queue
			var sourceResp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &sourceResp, nil))
			assert.Empty(t, sourceResp.Items)
		})

		t.Run("NoDLQConfigured", func(t *testing.T) {
			now := clock.NewProvider()
			now.Freeze(clock.Now())
			defer now.UnFreeze()

			var queueName = random.String("queue-", 10)
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			// Create queue with no DLQ configured, max_attempts = 2, lease_timeout = 1m
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m",
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         2,
				RequestedPartitions: 1,
			}))

			// Produce an item
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5m",
				Items: []*pb.QueueProduceItem{
					{
						Reference: "no-dlq-test",
						Encoding:  "text",
						Kind:      "test",
						Bytes:     []byte("test payload"),
					},
				},
			}))

			// Lease item (attempt 1)
			var lease1 pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				QueueName:      queueName,
				RequestTimeout: "5m",
				ClientId:       "client-1",
				BatchSize:      10,
			}, &lease1))
			require.Len(t, lease1.Items, 1)

			// Advance time to expire lease (attempt 1 expires)
			now.Advance(2 * clock.Minute)

			// Wait for item to be re-queued after lease expiry
			require.Eventually(t, func() bool {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return false
				}
				if len(resp.Items) == 0 {
					return false
				}
				return !resp.Items[0].IsLeased && resp.Items[0].Attempts == 1
			}, 5*clock.Second, 100*clock.Millisecond)

			// Lease item again (attempt 2)
			var lease2 pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				QueueName:      queueName,
				RequestTimeout: "5m",
				ClientId:       "client-2",
				BatchSize:      10,
			}, &lease2))
			require.Len(t, lease2.Items, 1)

			// Advance time to expire lease (attempt 2 expires, item now at max_attempts)
			now.Advance(2 * clock.Minute)

			// Wait for lifecycle to delete the item (no DLQ configured)
			require.Eventually(t, func() bool {
				var resp pb.StorageItemsListResponse
				if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
					return false
				}
				return len(resp.Items) == 0
			}, 5*clock.Second, 100*clock.Millisecond)
		})

		t.Run("Idempotency", func(t *testing.T) {
			var dlqName = random.String("dlq-", 10)
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
			defer func() {
				d.Shutdown(t)
				tearDown()
			}()
			c, ctx := d.MustClient(), d.Context()

			// Create DLQ queue
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           dlqName,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				MaxAttempts:         10,
				RequestedPartitions: 1,
			}))

			const sourceID = "source-item-123"

			// Import item with SourceID
			var importResp pb.StorageItemsImportResponse
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: dlqName,
				Items: []*pb.StorageItem{
					{
						Reference:      "idempotency-test-1",
						Encoding:       "text",
						Kind:           "test",
						Payload:        []byte("test payload 1"),
						SourceId:       sourceID,
						ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(1 * clock.Hour)),
						MaxAttempts:    3,
					},
				},
			}, &importResp))

			// Attempt to import another item with same SourceID
			var importResp2 pb.StorageItemsImportResponse
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: dlqName,
				Items: []*pb.StorageItem{
					{
						Reference:      "idempotency-test-2",
						Encoding:       "text",
						Kind:           "test",
						Payload:        []byte("test payload 2"),
						SourceId:       sourceID,
						ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(1 * clock.Hour)),
						MaxAttempts:    3,
					},
				},
			}, &importResp2))

			// Verify only one item exists
			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &resp, nil))
			assert.Len(t, resp.Items, 1, "should have exactly one item due to SourceID deduplication")

			// Verify it's the first item (second was ignored)
			if len(resp.Items) == 1 {
				assert.Equal(t, "idempotency-test-1", resp.Items[0].Reference)
				assert.Equal(t, sourceID, resp.Items[0].SourceId)
			}
		})

		// SingleWriter covers the behavioral and accounting guarantees that hold because
		// dead-letter production flows through the dead-letter queue's own requestLoop, the
		// sole writer of its partitions (ADR-0003): blocked consumers are woken, and the
		// queue's in-memory accounting stays consistent with storage.
		t.Run("SingleWriter", func(t *testing.T) {

			// WakesBlockedConsumer: a consumer blocked on QueueLease against the dead-letter
			// queue must be offered a newly dead-lettered item promptly. The dead-letter produce
			// runs on the dead-letter queue's loop, which re-evaluates waiting leases in the same
			// pass, so the blocked consumer is satisfied as soon as the item arrives rather than
			// waiting out its request timeout.
			t.Run("WakesBlockedConsumer", func(t *testing.T) {
				now := clock.NewProvider()
				now.Freeze(clock.Now())
				defer now.UnFreeze()

				queueName := random.String("queue-", 10)
				dlqName := random.String("dlq-", 10)
				d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
				defer func() {
					d.Shutdown(t)
					tearDown()
				}()
				c, ctx := d.MustClient(), d.Context()

				// Dead-letter queue with a long expiry so the dead-lettered item lingers.
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "24h0m0s",
					RequestedPartitions: 1,
				})
				// Source queue with a short expiry that drives the item to dead-letter on expiry.
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "1m",
					DeadQueue:           dlqName,
					RequestedPartitions: 1,
				})

				// A consumer blocks on the dead-letter queue before any item exists. The request
				// timeout is far longer than the clock advance below, so any wake must come from
				// the dead-letter produce, not from a request timeout.
				leaseCtx, leaseCancel := context.WithCancel(ctx)
				defer leaseCancel()
				var (
					leaseResp pb.QueueLeaseResponse
					leaseErr  error
				)
				returned := make(chan struct{})
				backgroundCall{
					Call: func() {
						leaseErr = c.QueueLease(leaseCtx, &pb.QueueLeaseRequest{
							QueueName:      dlqName,
							RequestTimeout: "15m",
							ClientId:       random.String("dlq-client-", 10),
							BatchSize:      10,
						}, &leaseResp)
						close(returned)
					},
					While: func() {
						// Wait until the consumer is registered as waiting on the dead-letter queue.
						require.NoError(t, untilLeaseClientWaiting(t, c, dlqName, 1))

						// Produce to the source queue, then expire it so the lifecycle dead-letters it.
						require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
							QueueName:      queueName,
							RequestTimeout: "1m",
							Items: []*pb.QueueProduceItem{
								{Reference: "wake-test", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
							},
						}))
						now.Advance(2 * clock.Minute)

						// The blocked consumer must receive the dead-lettered item promptly (wall-clock).
						select {
						case <-returned:
						case <-clock.After(10 * clock.Second):
							t.Fatal("blocked dead-letter consumer was not woken after the item was dead-lettered")
						}
					},
				}.run()
				require.NoError(t, leaseErr)
				require.Len(t, leaseResp.Items, 1)
				assert.Equal(t, "wake-test", leaseResp.Items[0].Reference)
			})

			// StatsReflectDeadLetteredItem: after an item is dead-lettered with no consumer
			// leasing it, the dead-letter partition's public stats must report unLeased == total
			// and a nextLifecycleRun advanced toward the item's expiry. Producing through the
			// dead-letter queue's loop updates State.UnLeased and schedules the lifecycle timer in
			// the same pass, so the in-memory accounting stays consistent with storage.
			t.Run("StatsReflectDeadLetteredItem", func(t *testing.T) {
				now := clock.NewProvider()
				now.Freeze(clock.Now())
				defer now.UnFreeze()

				const dlqExpire = "24h0m0s"
				queueName := random.String("queue-", 10)
				dlqName := random.String("dlq-", 10)
				d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
				defer func() {
					d.Shutdown(t)
					tearDown()
				}()
				c, ctx := d.MustClient(), d.Context()

				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       dlqExpire,
					RequestedPartitions: 1,
				})
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "1m",
					DeadQueue:           dlqName,
					RequestedPartitions: 1,
				})

				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{Reference: "drift-test", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
					},
				}))
				// Expire the source item; no consumer leases it on the dead-letter queue.
				now.Advance(2 * clock.Minute)

				// Wait for the item to land in the dead-letter partition's storage.
				require.Eventually(t, func() bool {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, dlqName, 0, &resp, nil); err != nil {
						return false
					}
					return len(resp.Items) == 1
				}, 10*clock.Second, 100*clock.Millisecond)

				// The dead-letter partition's in-memory accounting must match storage: unLeased ==
				// total, and nextLifecycleRun scheduled within the item's expiry window (a partition
				// with no scheduled lifecycle work reports humanize.LongTime, decades out).
				dlqExpireDur, err := clock.ParseDuration(dlqExpire)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					var stats pb.QueueStatsResponse
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: dlqName}, &stats); err != nil {
						return false
					}
					if len(stats.LogicalQueues) != 1 || len(stats.LogicalQueues[0].Partitions) != 1 {
						return false
					}
					p := stats.LogicalQueues[0].Partitions[0]
					if p.Total != 1 || p.UnLeased != p.Total {
						return false
					}
					next, perr := clock.ParseDuration(p.NextLifecycleRun)
					if perr != nil {
						return false
					}
					return next > 0 && next <= dlqExpireDur+clock.Minute
				}, 10*clock.Second, 100*clock.Millisecond)
			})

			// RetainsItemOnFailedMove covers delete-after-confirm and backpressure: when the
			// dead-letter produce cannot complete, the source item is retained (never deleted
			// before a confirmed produce), and a later pass moves it without duplication. The
			// dead-letter queue is paused to force the produce to block past WriteTimeout and
			// fail. A single-partition dead-letter queue keeps the no-duplicate assertion
			// deterministic via SourceID dedup.
			t.Run("RetainsItemOnFailedMove", func(t *testing.T) {
				now := clock.NewProvider()
				now.Freeze(clock.Now())
				defer now.UnFreeze()

				queueName := random.String("queue-", 10)
				dlqName := random.String("dlq-", 10)
				d := newDaemon(t, 30*clock.Second, svc.Config{
					StorageConfig: setup(),
					Clock:         now,
					// Short write timeout so the produce to the paused dead-letter queue fails fast.
					WriteTimeout: 500 * clock.Millisecond,
				})
				defer func() {
					d.Shutdown(t)
					tearDown()
				}()
				c, ctx := d.MustClient(), d.Context()

				// Dead-letter queue with a long expiry so the moved item never expires during the
				// clock advance used to trigger the source's later pass.
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "24h0m0s",
					RequestedPartitions: 1,
				})
				// Source queue: MaxAttempts=1 with a moderate expiry. Leasing then expiring the
				// lease drives the item to max-attempts dead-letter while keeping its ExpireDeadline
				// in the future, so a failed move reschedules to a reachable time (not LongTime).
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "30m",
					MaxAttempts:         1,
					DeadQueue:           dlqName,
					RequestedPartitions: 1,
				})

				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{Reference: "no-loss-test", Encoding: "text", Kind: "test", Bytes: []byte("payload")},
					},
				}))

				// Lease the item (attempt 1), reaching MaxAttempts.
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					QueueName:      queueName,
					RequestTimeout: "5m",
					ClientId:       "client-1",
					BatchSize:      10,
				}, &lease))
				require.Len(t, lease.Items, 1)

				// Pause the dead-letter queue so the internal produce cannot be applied.
				require.NoError(t, d.Service().PauseQueue(ctx, dlqName))

				// Expire the lease; the source lifecycle attempts to dead-letter the item but the
				// paused dead-letter queue cannot confirm the produce, so the move fails.
				now.Advance(2 * clock.Minute)

				// The dead-letter produce is buffered (waiting) in the paused dead-letter queue,
				// proving the move was attempted over its requestCh.
				require.Eventually(t, func() bool {
					var stats pb.QueueStatsResponse
					if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: dlqName}, &stats); err != nil {
						return false
					}
					return len(stats.LogicalQueues) == 1 && stats.LogicalQueues[0].ProduceWaiting == 1
				}, 10*clock.Second, 100*clock.Millisecond)

				// Delete-after-confirm: the source item is retained (the move never confirmed) and
				// nothing has been written to the dead-letter partition's storage.
				var srcList pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &srcList, nil))
				require.Len(t, srcList.Items, 1)
				assert.Equal(t, "no-loss-test", srcList.Items[0].Reference)

				var dlqList pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqList, nil))
				assert.Empty(t, dlqList.Items)

				// Un-pause the dead-letter queue, then advance the clock past the source item's
				// ExpireDeadline to trigger a later lifecycle pass that moves the item.
				require.NoError(t, d.Service().ResumeQueue(ctx, dlqName))
				now.Advance(29 * clock.Minute)

				// The item is moved exactly once: the dead-letter queue holds one copy (SourceID
				// dedup collapses the buffered flush and the retried move) and the source is empty.
				require.Eventually(t, func() bool {
					var dlqResp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil); err != nil {
						return false
					}
					var srcResp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &srcResp, nil); err != nil {
						return false
					}
					return len(dlqResp.Items) == 1 && len(srcResp.Items) == 0
				}, 15*clock.Second, 100*clock.Millisecond)

				// Confirm it is exactly one and stays one (no duplication).
				var finalDLQ pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &finalDLQ, nil))
				require.Len(t, finalDLQ.Items, 1)
				assert.Equal(t, "no-loss-test", finalDLQ.Items[0].Reference)
			})
		})
	})
}

// completeWhilePaused sends every request at once and calls unpause while they wait in the
// paused queue. The goroutines start in a named function because Tiger's TS-C02
// does not see a Wait that follows wg.Go inside a function literal.
func completeWhilePaused(ctx context.Context, c *querator.Client, requests []*pb.QueueCompleteRequest, unpause func()) []error {
	errs := make([]error, len(requests))
	var wg sync.WaitGroup
	for i := range requests {
		wg.Go(func() {
			errs[i] = c.QueueComplete(ctx, requests[i])
		})
	}
	unpause()
	wg.Wait()
	return errs
}

// duringCalls starts each call in its own goroutine, runs during, then waits for every call to
// return. If during exits early, as a failed require does, cancel releases the calls before the wait.
func duringCalls(cancel context.CancelFunc, during func(), calls ...func()) {
	var wg sync.WaitGroup
	finished := false
	defer func() {
		if !finished {
			cancel()
			wg.Wait()
		}
	}()
	for _, call := range calls {
		wg.Go(call)
	}
	during()
	wg.Wait()
	finished = true
}
