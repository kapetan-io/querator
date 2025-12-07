package querator_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/duh-rpc/duh-go/retry"
	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math/rand"
	"sync"
	"testing"
)

func TestQueue(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}

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
	defer goleak.VerifyNone(t)

	t.Run("ProduceAndLease", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

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
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Limit: 10}))
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
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Limit: 10}))
		assert.Equal(t, 0, len(list.Items))

		// Remove queue
		require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: queueName}))
		var queues pb.QueuesListResponse
		require.NoError(t, c.QueuesList(ctx, &queues, &que.ListOptions{Limit: 10}))
		for _, q := range queues.Items {
			assert.NotEqual(t, q.QueueName, queueName)
		}
	})

	t.Run("Produce", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

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
			err := c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Limit: 20, Pivot: lastItem.Id})
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

		t.Run("MaxAttempts", func(t *testing.T) {
			// TODO: Lease and retry one of the items multiple clocks until we exhaust the MaxAttempts,
			//  then assert item was deleted.
		})
		t.Run("LeaseTimeout", func(t *testing.T) {})
		t.Run("ExpireTimeout", func(t *testing.T) {
			// TODO: Fast Forward to the future, and ensure the item is removed after the dead clockout
		})
		t.Run("DeadQueue", func(t *testing.T) {
			// TODO: Create a new queue with a dead queue. Ensure an item produced in this queue is moved to
			//  the dead queue after all attempts are exhausted

			t.Run("ExpireTimeout", func(t *testing.T) {
				// TODO: Fast forward to the future, and ensure the item is moved to the dead queue after
				//  dead clockout
			})
		})

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
				&que.ListOptions{Pivot: lastItem.Id, Limit: 20})
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
				&que.ListOptions{Pivot: lastItem.Id, Limit: 101})
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
		d, c, ctx := newDaemon(t, 60*clock.Second, que.ServiceConfig{
			StorageConfig: setup(),
			Clock:         now,
		})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

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
		err := c.StorageScheduledList(ctx, queueName, 0, &scheduled, &que.ListOptions{Limit: 150})
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
	})

	t.Run("Lease", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		clientID := random.String("client-", 10)
		d, c, ctx := newDaemon(t, 30*clock.Second, que.ServiceConfig{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

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
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Limit: 10_000}))

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

			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Limit: 10_000}))
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
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Limit: 10_000}))
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
		d, c, ctx := newDaemon(t, 30*clock.Second, que.ServiceConfig{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

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
				Ids:            que.CollectIDs(leased.Items),
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

	// t.Run("Scheduled", func(t *testing.T) {
	// 	now := clock.NewProvider()
	// 	now.Freeze(clock.Now())
	// 	defer now.UnFreeze()
	//
	// 	var queueName = random.String("queue-", 10)
	// 	d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: setup(), Clock: now})
	// defer func() {
	// 	d.Shutdown(t)
	// 	tearDown()
	// }()
	//
	// 	// Create a queue
	// 	createQueueAndWait(t, ctx, c, &pb.QueueInfo{
	// 		LeaseTimeout:        LeaseTimeout,
	// 		ExpireTimeout:       ExpireTimeout,
	// 		QueueName:           queueName,
	// 		RequestedPartitions: 1,
	// 	})
	//
	// 	// Produce a single message
	// 	ref := random.String("ref-", 10)
	// 	enc := random.String("enc-", 10)
	// 	kind := random.String("kind-", 10)
	// 	payload := []byte("I didn't learn a thing. I was right all along")
	// 	require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
	// 		QueueName:      queueName,
	// 		RequestTimeout: "1m",
	// 		Items: []*pb.QueueProduceItem{
	// 			{
	// 				Reference: ref,
	// 				Encoding:  enc,
	// 				Kind:      kind,
	// 				Bytes:     payload,
	// 			},
	// 		},
	// 	}))
	//
	// 	// TODO(scheduled) Produce a scheduled item in the future
	// 	// TODO: Lease should only return the item produced, and no others
	// 	// TODO: Advance time until scheduled items are placed into the queue
	// 	// TODO: Should lease the items scheduled for produce.
	//
	// 	// Lease a single message
	// 	var lease pb.QueueLeaseResponse
	// 	require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
	// 		ClientId:       random.String("client-", 10),
	// 		RequestTimeout: "5s",
	// 		QueueName:      queueName,
	// 		BatchSize:      1,
	// 	}, &lease))
	//
	// 	// Ensure we got the item we produced
	// 	assert.Equal(t, 1, len(lease.Items))
	// 	item := lease.Items[0]
	// 	assert.Equal(t, ref, item.Reference)
	// 	assert.Equal(t, enc, item.Encoding)
	// 	assert.Equal(t, kind, item.Kind)
	// 	assert.Equal(t, int32(1), item.Attempts)
	// 	assert.Equal(t, payload, item.Bytes)
	//
	// 	// TODO(scheduled) Add a StorageScheduledList() client and endpoint
	//
	// 	// Partition storage should have scheduled items
	// 	// var list pb.StorageItemsListResponse
	// 	// require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Limit: 10}))
	// 	// require.Equal(t, 1, len(list.Items))
	//
	// 	// inspect := list.Items[0]
	// 	// assert.Equal(t, ref, inspect.Reference)
	// 	// assert.Equal(t, kind, inspect.Kind)
	// 	// assert.Equal(t, int32(1), inspect.Attempts)
	// 	// assert.Equal(t, payload, inspect.Payload)
	// 	// assert.Equal(t, item.Id, inspect.Id)
	// 	// assert.Equal(t, true, inspect.IsLeased)
	//
	// 	// Remove queue
	// 	require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: queueName}))
	// 	var queues pb.QueuesListResponse
	// 	require.NoError(t, c.QueuesList(ctx, &queues, &que.ListOptions{Limit: 10}))
	// 	for _, q := range queues.Items {
	// 		assert.NotEqual(t, q.QueueName, queueName)
	// 	}
	// })

	t.Run("Stats", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		clientID := random.String("client-", 10)
		d, c, ctx := newDaemon(t, 30*clock.Second, que.ServiceConfig{StorageConfig: setup()})
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

	t.Run("QueueClear", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

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
	t.Run("Errors", func(t *testing.T) {
		storage := setup()
		defer tearDown()

		t.Run("QueueProduce", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			d, c, ctx := newDaemon(t, 5*clock.Second, que.ServiceConfig{StorageConfig: storage})
			defer d.Shutdown(t)
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

		t.Run("QueueLease", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			var clientID = random.String("client-", 10)
			d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: storage})
			defer d.Shutdown(t)

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
					Msg:  "request timeout is invalid; maximum timeout is '15m' but '16m0s' requested",
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
				resultCh := make(chan error)
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					var res pb.QueueLeaseResponse
					resultCh <- c.QueueLease(ctx, &pb.QueueLeaseRequest{
						QueueName:      queueName,
						ClientId:       clientID,
						RequestTimeout: "2s",
						BatchSize:      1,
					}, &res)
					wg.Done()
				}()

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
				assert.Equal(t, que.MsgDuplicateClientID, e.Message())
				assert.Equal(t, duh.CodeBadRequest, e.Code())
				err = <-resultCh
				require.Error(t, err)
				require.True(t, errors.As(err, &e))
				assert.Equal(t, que.MsgRequestTimeout, e.Message())
				assert.Equal(t, duh.CodeRetryRequest, e.Code())
				wg.Wait()
			})
		})
		t.Run("QueueComplete", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			d, c, ctx := newDaemon(t, 5*clock.Second, que.ServiceConfig{StorageConfig: storage})
			defer d.Shutdown(t)

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
					Msg:  "request timeout is invalid; maximum timeout is '15m' but '16m0s' requested",
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
	})

	t.Run("Timeout", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: setup(), Clock: now})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

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
				&que.ListOptions{Pivot: leased.Id, Limit: 1})
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
				&que.ListOptions{Pivot: leased.Id, Limit: 5})
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
					&que.ListOptions{Pivot: requeued.Id, Limit: 5})
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
					&que.ListOptions{Pivot: requeued.Id, Limit: 5})
				require.NoError(t, err)
				require.Nil(t, findInStorageList("flutter@shy.com", &resp))
			})
		})

		// Create a queue
		queueName = random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
			MaxAttempts:         2,
		})

		t.Run("MaxAttempts", func(t *testing.T) {
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

		// Create a queue with short ExpireTimeout for testing
		queueName = random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        "5s",
			ExpireTimeout:       "10s",
			QueueName:           queueName,
			RequestedPartitions: 1,
		})

		t.Run("ExpireTimeout", func(t *testing.T) {
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

		// t.Run("UntilDeadLetter", func(t *testing.T) {
		// 	// Produce an item
		// 	// Lease it
		// 	// Wait for the Timeout
		// 	// Repeat until max attempts reached
		// })
	})

	// t.Run("RequestTimeouts", func(t *testing.T) {})
	// t.Run("ExpireTimeout", func(t *testing.T) {
	// 	// TODO: Test with and without a dead letter queue
	// })
}

// TODO: Start the Service, produce some items, then Shutdown the service
// TODO: Variations on this, produce/lease, shutdown, etc.... ensure all items are consumed.
// TODO: Attempt to shutdown the service while clients are still making requests
