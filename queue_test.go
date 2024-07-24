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
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math/rand"
	"sync"
	"testing"
	"time"
)

//func TestBrokenStorage(t *testing.T) {
//	var queueName = random.String("queue-", 10)
//	opts := &store.MockOptions{}
//	newStore := func() store.Storage {
//		return store.NewMockStorage(opts)
//	}
//
//	d, c, ctx := newDaemon(t, _store, 10*time.Second)
//	defer d.Shutdown(t)
//
//	opts.Methods["Queue.Produce"] = func(args []any) error {
//		return errors.New("unknown storage error")
//	}
//
//	// TODO: QueuesCreate should create a queue in storage
//	require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{Name: queueName}))
//
//}

var RetryTenTimes = retry.Policy{Interval: retry.Sleep(time.Second), Attempts: 10}

type NewStorageFunc func() store.Storage

func TestQueue(t *testing.T) {
	bdb := store.BoltDBTesting{Dir: t.TempDir()}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "InMemory",
			Setup: func() store.Storage {
				return store.NewMemoryStorage()
			},
			TearDown: func() {},
		},
		{
			Name: "BoltDB",
			Setup: func() store.Storage {
				return bdb.Setup(store.BoltOptions{})
			},
			TearDown: func() {
				bdb.Teardown()
			},
		},
		//{
		//	Name: "SurrealDB",
		//},
		//{
		//	Name: "PostgresSQL",
		//},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testQueue(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueue(t *testing.T, setup NewStorageFunc, tearDown func()) {
	_store := setup()
	defer tearDown()

	t.Run("ProduceAndConsume", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)

		// Create a queue
		require.NoError(t, c.QueueCreate(ctx, &pb.QueueInfo{QueueName: queueName}))

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

		// Reserve a single message
		var reserve pb.QueueReserveResponse
		require.NoError(t, c.QueueReserve(ctx, &pb.QueueReserveRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &reserve))

		// Ensure we got the item we produced
		assert.Equal(t, 1, len(reserve.Items))
		item := reserve.Items[0]
		assert.Equal(t, ref, item.Reference)
		assert.Equal(t, enc, item.Encoding)
		assert.Equal(t, kind, item.Kind)
		assert.Equal(t, int32(0), item.Attempts)
		assert.Equal(t, payload, item.Bytes)

		// Queue storage should have only one item
		var list pb.StorageQueueListResponse
		require.NoError(t, c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Limit: 10}))
		require.Equal(t, 1, len(list.Items))

		inspect := list.Items[0]
		assert.Equal(t, ref, inspect.Reference)
		assert.Equal(t, kind, inspect.Kind)
		assert.Equal(t, int32(0), inspect.Attempts)
		assert.Equal(t, payload, inspect.Payload)
		assert.Equal(t, item.Id, inspect.Id)
		assert.Equal(t, true, inspect.IsReserved)

		// Mark the item as complete
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Ids: []string{
				item.Id,
			},
		}))

		// Queue storage should be empty
		require.NoError(t, c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Limit: 10}))

		assert.Equal(t, 0, len(list.Items))

		// TODO: Delete the queue
	})

	t.Run("QueueClear", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)

		var reserved []*pb.StorageQueueItem
		var list pb.StorageQueueListResponse
		require.NoError(t, c.QueueCreate(ctx, &pb.QueueInfo{QueueName: queueName}))

		// Write some items to the queue
		_ = writeRandomItems(t, ctx, c, queueName, 500)
		// Ensure the items exist
		require.NoError(t, c.StorageQueueList(ctx, queueName, &list, nil))
		assert.Equal(t, 500, len(list.Items))

		expire := time.Now().UTC().Add(random.Duration(time.Minute))
		reserved = append(reserved, &pb.StorageQueueItem{
			DeadDeadline:    timestamppb.New(expire),
			ReserveDeadline: timestamppb.New(expire),
			Attempts:        int32(rand.Intn(10)),
			Reference:       random.String("ref-", 10),
			Encoding:        random.String("enc-", 10),
			Kind:            random.String("kind-", 10),
			Payload:         []byte("Reserved 1"),
			IsReserved:      true,
		})
		reserved = append(reserved, &pb.StorageQueueItem{
			DeadDeadline:    timestamppb.New(expire),
			ReserveDeadline: timestamppb.New(expire),
			Attempts:        int32(rand.Intn(10)),
			Reference:       random.String("ref-", 10),
			Encoding:        random.String("enc-", 10),
			Kind:            random.String("kind-", 10),
			Payload:         []byte("Reserved 2"),
			IsReserved:      true,
		})

		// Add some reserved items
		var resp pb.StorageQueueAddResponse
		err := c.StorageQueueAdd(ctx, &pb.StorageQueueAddRequest{Items: reserved, QueueName: queueName}, &resp)
		require.NoError(t, err)
		require.NoError(t, c.StorageQueueList(ctx, queueName, &list, nil))
		assert.Equal(t, 502, len(list.Items))

		t.Run("NonDestructive", func(t *testing.T) {
			require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true}))
			require.NoError(t, c.StorageQueueList(ctx, queueName, &list, nil))
			assert.Equal(t, 2, len(list.Items))
		})

		t.Run("Destructive", func(t *testing.T) {
			_ = writeRandomItems(t, ctx, c, queueName, 200)
			require.NoError(t, c.StorageQueueList(ctx, queueName, &list, nil))
			assert.Equal(t, 202, len(list.Items))
			require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
				QueueName:   queueName,
				Destructive: true,
				Queue:       true}))
			require.NoError(t, c.StorageQueueList(ctx, queueName, &list, nil))
			assert.Equal(t, 0, len(list.Items))
		})

	})

	t.Run("Produce", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)
		now := time.Now().UTC()

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueInfo{QueueName: queueName}))
		var lastItem *pb.StorageQueueItem
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

			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items:          items,
			}))

			// Ensure the items produced are in the data store
			var list pb.StorageQueueListResponse
			err := c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Limit: 20})
			require.NoError(t, err)

			require.Len(t, items, 10)
			assert.Equal(t, len(items), len(list.Items))
			for i := range items {
				assert.True(t, list.Items[i].CreatedAt.AsTime().After(now))
				// TODO: Ensure the DeadDeadline is set to something reasonable and then
				//  change this test to ensure the they are also Before the time we think it should be.
				assert.True(t, list.Items[i].DeadDeadline.AsTime().After(now))
				assert.True(t, list.Items[i].ReserveDeadline.AsTime().IsZero())

				assert.Equal(t, false, list.Items[i].IsReserved)
				assert.Equal(t, int32(0), list.Items[i].Attempts)
				assert.Equal(t, items[i].Reference, list.Items[i].Reference)
				assert.Equal(t, items[i].Encoding, list.Items[i].Encoding)
				assert.Equal(t, items[i].Bytes, list.Items[i].Payload)
				assert.Equal(t, items[i].Kind, list.Items[i].Kind)
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

			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items:          items,
			}))

			// Ensure the items produced are in the data store
			var list pb.StorageQueueListResponse
			err := c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Pivot: lastItem.Id, Limit: 101})
			require.NoError(t, err)

			require.Len(t, items, 100)

			// Remember the pivot is included in the results, so we remove the pivot from the results
			assert.Equal(t, len(items), len(list.Items[1:]))
			produced := list.Items[1:]

			for i := range produced {
				assert.True(t, produced[i].CreatedAt.AsTime().After(now))
				// TODO: Ensure the DeadDeadline is set to something reasonable and then
				//  change this test to ensure the they are also Before the time we think it should be.
				assert.True(t, produced[i].DeadDeadline.AsTime().After(now))
				assert.True(t, produced[i].ReserveDeadline.AsTime().IsZero())

				assert.Equal(t, false, produced[i].IsReserved)
				assert.Equal(t, int32(0), produced[i].Attempts)
				assert.Equal(t, items[i].Reference, produced[i].Reference)
				assert.Equal(t, items[i].Encoding, produced[i].Encoding)
				assert.Equal(t, []byte(items[i].Utf8), produced[i].Payload)
				assert.Equal(t, items[i].Kind, produced[i].Kind)
			}
		})
	})

	t.Run("Reserve", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		clientID := random.String("client-", 10)
		d, c, ctx := newDaemon(t, _store, 30*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueInfo{QueueName: queueName}))
		items := writeRandomItems(t, ctx, c, queueName, 10_000)
		require.Len(t, items, 10_000)

		expire := time.Now().UTC().Add(2_000 * time.Minute)
		var reserved, secondReserve pb.QueueReserveResponse
		var list pb.StorageQueueListResponse

		t.Run("TenItems", func(t *testing.T) {
			req := pb.QueueReserveRequest{
				ClientId:       clientID,
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			}

			require.NoError(t, c.QueueReserve(ctx, &req, &reserved))
			require.Equal(t, 10, len(reserved.Items))

			// Ensure the items reserved are marked as reserved in the database
			require.NoError(t, c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Limit: 10_000}))

			for i := range reserved.Items {
				assert.Equal(t, list.Items[i].Id, reserved.Items[i].Id)
				assert.Equal(t, true, list.Items[i].IsReserved)
				// TODO: Allow config of the reservation deadline on the queue and then
				//  assert the ReserveDeadline is correct.
				assert.True(t, list.Items[i].ReserveDeadline.AsTime().Before(expire))
			}
		})

		t.Run("AnotherTenItems", func(t *testing.T) {
			req := pb.QueueReserveRequest{
				ClientId:       clientID,
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			}

			require.NoError(t, c.QueueReserve(ctx, &req, &secondReserve))
			require.Equal(t, 10, len(reserved.Items))

			var combined []*pb.QueueReserveItem
			combined = append(combined, reserved.Items...)
			combined = append(combined, secondReserve.Items...)

			require.NoError(t, c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Limit: 10_000}))
			assert.NotEqual(t, reserved.Items[0].Id, secondReserve.Items[0].Id)
			assert.Equal(t, combined[0].Id, list.Items[0].Id)
			require.Equal(t, 20, len(combined))
			require.Equal(t, 10_000, len(list.Items))

			// Ensure all the items reserved are marked as reserved in the database
			for i := range combined {
				assert.Equal(t, list.Items[i].Id, combined[i].Id)
				assert.Equal(t, true, list.Items[i].IsReserved)
				assert.True(t, list.Items[i].ReserveDeadline.AsTime().Before(expire))
			}
		})

		t.Run("DistributeNumRequested", func(t *testing.T) {
			requests := []*pb.QueueReserveRequest{
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
			responses := pauseAndReserve(t, ctx, c, queueName, requests)

			assert.Equal(t, int32(5), requests[0].BatchSize)
			assert.Equal(t, 5, len(responses[0].Items))
			assert.Equal(t, int32(10), requests[1].BatchSize)
			assert.Equal(t, 10, len(responses[1].Items))
			assert.Equal(t, int32(20), requests[2].BatchSize)
			assert.Equal(t, 20, len(responses[2].Items))

			// Fetch items from storage, ensure items are reserved
			require.NoError(t, c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Limit: 10_000}))
			require.Equal(t, 10_000, len(list.Items))

			var found int
			for _, item := range list.Items {
				// Find the reserved item in the batch request
				if findInResponses(t, responses, item.Id) {
					found++
					// Ensure the item is reserved
					require.Equal(t, true, item.IsReserved)
				}
			}
			assert.Equal(t, 35, found, "expected to find 35 reserved items, got %d", found)
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
			require.NoError(t, c.StorageQueueList(ctx, queueName, &list, nil))
			require.Equal(t, 23, len(list.Items))

			requests := []*pb.QueueReserveRequest{
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
			responses := pauseAndReserve(t, ctx, c, queueName, requests)

			// Reserve() should fairly distribute items across all requests
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

			requests := []*pb.QueueReserveRequest{
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      20,
					RequestTimeout: "4s",
				},
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      6,
					RequestTimeout: "4s",
				},
				{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      1,
					RequestTimeout: "5s",
				},
			}
			responses := pauseAndReserve(t, ctx, c, queueName, requests)

			// Reserve() should return no items, and
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
		d, c, ctx := newDaemon(t, _store, 30*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueInfo{QueueName: queueName}))

		t.Run("Success", func(t *testing.T) {
			items := writeRandomItems(t, ctx, c, queueName, 10)
			require.Len(t, items, 10)

			var reserved pb.QueueReserveResponse
			var list pb.StorageQueueListResponse

			req := pb.QueueReserveRequest{
				ClientId:       clientID,
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			}

			require.NoError(t, c.QueueReserve(ctx, &req, &reserved))
			require.Equal(t, 10, len(reserved.Items))

			require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				Ids:            que.CollectIDs(reserved.Items),
				QueueName:      queueName,
				RequestTimeout: "1m",
			}))

			// Fetch items from storage, ensure items are no longer available
			require.NoError(t, c.StorageQueueList(ctx, queueName, &list, nil))
			require.Equal(t, 0, len(list.Items))
		})

		t.Run("NotReserved", func(t *testing.T) {
			// Attempt to complete an item that has not been reserved
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
			assert.Contains(t, e.Message(), " is not marked as reserved")
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
			assert.Equal(t, "invalid storage id; 'another-invalid-id': expected format "+
				"<queue_name>~<storage_id>", e.Message())
			assert.Equal(t, 400, e.Code())
		})
	})

	t.Run("Stats", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		clientID := random.String("client-", 10)
		d, c, ctx := newDaemon(t, _store, 30*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueInfo{QueueName: queueName}))

		items := writeRandomItems(t, ctx, c, queueName, 500)
		require.Len(t, items, 500)

		req := pb.QueueReserveRequest{
			ClientId:       clientID,
			QueueName:      queueName,
			BatchSize:      15,
			RequestTimeout: "1m",
		}

		var reserved pb.QueueReserveResponse
		require.NoError(t, c.QueueReserve(ctx, &req, &reserved))
		require.Equal(t, 15, len(reserved.Items))

		var stats pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))

		assert.Equal(t, int32(500), stats.Total)
		assert.Equal(t, int32(15), stats.TotalReserved)
		assert.NotEmpty(t, stats.AverageAge)
		assert.NotEmpty(t, stats.AverageReservedAge)
		t.Logf("total: %d average-age: %s reserved %d average-reserved: %s",
			stats.Total, stats.AverageAge, stats.TotalReserved, stats.AverageReservedAge)

		// TODO: After adding the ability to change the reserve duration, Use this Stats() to test the reserved age
	})

	t.Run("QueueProduceErrors", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 5*time.Second)
		defer d.Shutdown(t)
		maxItems := randomProduceItems(1_001)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueInfo{QueueName: queueName}))

		for _, test := range []struct {
			Name string
			Req  *pb.QueueProduceRequest
			Msg  string
			Code int
		}{
			{
				Name: "EmptyRequest",
				Req:  &pb.QueueProduceRequest{},
				Msg:  "invalid queue; queue name cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "InvalidQueue",
				Req: &pb.QueueProduceRequest{
					QueueName: "invalid~queue",
				},
				Msg:  "invalid queue_name; 'invalid~queue' cannot contain '~' character",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "NoItemsNilPointer",
				Req: &pb.QueueProduceRequest{
					QueueName: queueName,
					Items:     nil,
				},
				Msg:  "items cannot be empty; at least one item is required",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "NoItemsEmptyList",
				Req: &pb.QueueProduceRequest{
					QueueName: queueName,
					Items:     []*pb.QueueProduceItem{},
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
				Msg:  "request_timeout is required; '5m' is recommended, 15m is the maximum",
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
				Msg:  "request_timeout is invalid; maximum timeout is '15m' but '16m0s' was requested",
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
				Msg:  "request_timeout is invalid; time: invalid duration \"foo\" - expected format: 900ms, 5m or 15m",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "MaxItemsReached",
				Req: &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items:          maxItems,
				},
				Msg:  "too many items in request; max_produce_batch_size is 1000 but received 1001",
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

	t.Run("QueueReserveErrors", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		var clientID = random.String("client-", 10)
		d, c, ctx := newDaemon(t, _store, 5*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueInfo{QueueName: queueName}))

		for _, tc := range []struct {
			Name string
			Req  *pb.QueueReserveRequest
			Msg  string
			Code int
		}{
			{
				Name: "EmptyRequest",
				Req:  &pb.QueueReserveRequest{},
				Msg:  "invalid queue; queue name cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "ClientIdMissing",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
				},
				Msg:  "invalid client_id; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "BatchSizeCannotBeEmpty",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
					ClientId:  clientID,
				},
				Msg:  "invalid batch_size; must be greater than zero",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "BatchSizeMaximum",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
					ClientId:  clientID,
					BatchSize: 1_001,
				},
				Msg:  "invalid batch_size; exceeds maximum limit max_reserve_batch_size is 1000, but 1001 was requested",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "RequestTimeoutRequired",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
					ClientId:  clientID,
					BatchSize: 111,
				},
				Msg:  "request_timeout is required; '5m' is recommended, 15m is the maximum",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "RequestTimeoutTooLong",
				Req: &pb.QueueReserveRequest{
					QueueName:      queueName,
					ClientId:       clientID,
					BatchSize:      1_000,
					RequestTimeout: "16m",
				},
				Msg:  "invalid request_timeout; maximum timeout is '15m' but '16m0s' requested",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "RequestTimeoutInvalid",
				Req: &pb.QueueReserveRequest{
					QueueName:      queueName,
					ClientId:       clientID,
					BatchSize:      1_000,
					RequestTimeout: "foo",
				},
				Msg:  "request_timeout is invalid; time: invalid duration \"foo\" - expected format: 900ms, 5m or 15m",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "MinimumRequestTimeoutIsAllowed",
				Req: &pb.QueueReserveRequest{
					QueueName:      queueName,
					ClientId:       clientID,
					BatchSize:      1_000,
					RequestTimeout: "10ms",
				},
				Code: duh.CodeOK,
			},
			{
				Name: "RequestTimeoutIsTooShort",
				Req: &pb.QueueReserveRequest{
					QueueName:      queueName,
					ClientId:       clientID,
					BatchSize:      1_000,
					RequestTimeout: "1ms",
				},
				Msg:  "request_timeout is invalid; minimum timeout is '10ms' but '1ms' was requested",
				Code: duh.CodeBadRequest,
			},
		} {
			t.Run(tc.Name, func(t *testing.T) {
				var res pb.QueueReserveResponse
				err := c.QueueReserve(ctx, tc.Req, &res)
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
	})

	// TODO: Finish error testing
	t.Run("QueueCompleteErrors", func(t *testing.T) {})

	// TODO: Test duplicate client id on reserve
	// TODO: Test pause and unpause, ensure can produce and consume after un-paused and Ensure can shutdown
	// TODO: Test pause false without pause true
}

func findInResponses(t *testing.T, responses []*pb.QueueReserveResponse, id string) bool {
	t.Helper()

	for _, item := range responses {
		for _, idItem := range item.Items {
			if idItem.Id == id {
				return true
			}
		}
	}
	return false
}

func compareStorageItem(t *testing.T, l *pb.StorageQueueItem, r *pb.StorageQueueItem) {
	t.Helper()
	require.Equal(t, l.Id, r.Id)
	require.Equal(t, l.IsReserved, r.IsReserved)
	require.Equal(t, l.DeadDeadline.AsTime(), r.DeadDeadline.AsTime())
	require.Equal(t, l.ReserveDeadline.AsTime(), r.ReserveDeadline.AsTime())
	require.Equal(t, l.Attempts, r.Attempts)
	require.Equal(t, l.Reference, r.Reference)
	require.Equal(t, l.Encoding, r.Encoding)
	require.Equal(t, l.Kind, r.Kind)
	require.Equal(t, l.Payload, r.Payload)
}

func writeRandomItems(t *testing.T, ctx context.Context, c *que.Client,
	name string, count int) []*pb.StorageQueueItem {

	t.Helper()
	expire := time.Now().UTC().Add(random.Duration(time.Minute))

	var items []*pb.StorageQueueItem
	for i := 0; i < count; i++ {
		items = append(items, &pb.StorageQueueItem{
			DeadDeadline: timestamppb.New(expire),
			Attempts:     int32(rand.Intn(10)),
			Reference:    random.String("ref-", 10),
			Encoding:     random.String("enc-", 10),
			Kind:         random.String("kind-", 10),
			Payload:      []byte(fmt.Sprintf("message-%d", i)),
		})
	}

	var resp pb.StorageQueueAddResponse
	err := c.StorageQueueAdd(ctx, &pb.StorageQueueAddRequest{Items: items, QueueName: name}, &resp)
	require.NoError(t, err)
	return resp.Items
}

func pauseAndReserve(t *testing.T, ctx context.Context, c *que.Client, name string,
	requests []*pb.QueueReserveRequest) []*pb.QueueReserveResponse {
	t.Helper()

	// Pause processing of the queue
	require.NoError(t, c.QueuePause(ctx, &pb.QueuePauseRequest{
		QueueName:     name,
		PauseDuration: "2m",
		Pause:         true,
	}))

	responses := []*pb.QueueReserveResponse{{}, {}, {}}
	var wg sync.WaitGroup
	wg.Add(len(requests))

	for i := range requests {
		go func() {
			defer wg.Done()
			if err := c.QueueReserve(ctx, requests[i], responses[i]); err != nil {
				var d duh.Error
				if errors.As(err, &d) {
					if d.Code() == duh.CodeRetryRequest {
						return
					}
				}
				panic(err)
			}
		}()
	}

	_ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// Wait until every request is waiting
	err := retry.On(_ctx, RetryTenTimes, func(ctx context.Context, i int) error {
		var resp pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: name}, &resp))
		// There should eventually be 3 waiting reserve requests
		if int(resp.ReserveWaiting) != len(requests) {
			return fmt.Errorf("ReserveWaiting never reached expected %d", len(requests))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("while waiting on 3 reserved requests: %v", err)
	}

	// Unpause processing of the queue to allow the reservations to be filled.
	require.NoError(t, c.QueuePause(ctx, &pb.QueuePauseRequest{QueueName: name, Pause: false}))
	// Wait for each request to complete
	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for distribution of requests")
	}
	return responses
}
