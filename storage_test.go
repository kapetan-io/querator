package querator_test

import (
	"errors"
	"github.com/duh-rpc/duh-go"
	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
)

// TestQueueStorage tests the /storage/queue.* endpoints
func TestQueueStorage(t *testing.T) {
	badger := badgerTestSetup{Dir: t.TempDir()}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "InMemory",
			Setup: func(cp *clock.Provider) store.StorageConfig {
				return setupMemoryStorage(store.StorageConfig{Clock: cp})
			},
			TearDown: func() {},
		},
		{
			Name: "BadgerDB",
			Setup: func(cp *clock.Provider) store.StorageConfig {
				return badger.Setup(store.BadgerConfig{Clock: cp})
			},
			TearDown: func() {
				badger.Teardown()
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
			testQueueStorage(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueueStorage(t *testing.T, newStore NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t)

	_store := newStore(clock.NewProvider())
	defer tearDown()

	t.Run("CRUDCompare", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: _store})
		defer d.Shutdown(t)

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			ReserveTimeout:      ReserveTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		}))

		now := clock.Now().UTC()
		var items []*pb.StorageItem
		items = append(items, &pb.StorageItem{
			IsReserved:      true,
			ExpireDeadline:  timestamppb.New(now.Add(100_000 * clock.Minute)),
			ReserveDeadline: timestamppb.New(now.Add(3_000 * clock.Minute)),
			Attempts:        5,
			Reference:       "rainbow@dash.com",
			Encoding:        "rainbows",
			Kind:            "20% cooler",
			Payload: []byte("I mean... have I changed? Same sleek body. Same " +
				"flowing mane. Same spectacular hooves. Nope, I'm still awesome"),
		})
		items = append(items, &pb.StorageItem{
			IsReserved:      false,
			ExpireDeadline:  timestamppb.New(now.Add(1_000_000 * clock.Minute)),
			ReserveDeadline: timestamppb.New(now.Add(3_000 * clock.Minute)),
			Attempts:        10_000,
			Reference:       "rarity@dash.com",
			Encoding:        "beauty",
			Kind:            "sparkles",
			Payload:         []byte("Whining? I am not whining, I am complaining"),
		})

		var resp pb.StorageItemsImportResponse
		err := c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{Items: items, QueueName: queueName}, &resp)
		require.NoError(t, err)

		require.Equal(t, len(items), len(resp.Items))

		// Ensure all the fields are indeed the same
		assert.NotEmpty(t, resp.Items[0].Id)
		assert.NotEmpty(t, resp.Items[0].CreatedAt.AsTime())
		assert.Equal(t, items[0].IsReserved, resp.Items[0].IsReserved)
		assert.Equal(t, 0, resp.Items[0].ExpireDeadline.AsTime().Compare(items[0].ExpireDeadline.AsTime()))
		assert.Equal(t, 0, resp.Items[0].ReserveDeadline.AsTime().Compare(items[0].ReserveDeadline.AsTime()))
		assert.Equal(t, items[0].Attempts, resp.Items[0].Attempts)
		assert.Equal(t, items[0].Reference, resp.Items[0].Reference)
		assert.Equal(t, items[0].Encoding, resp.Items[0].Encoding)
		assert.Equal(t, items[0].Kind, resp.Items[0].Kind)
		assert.Equal(t, items[0].Payload, resp.Items[0].Payload)

		assert.NotEmpty(t, resp.Items[1].Id)
		assert.NotEmpty(t, resp.Items[1].CreatedAt.AsTime())
		assert.Equal(t, items[1].IsReserved, resp.Items[1].IsReserved)
		assert.Equal(t, 0, resp.Items[1].ExpireDeadline.AsTime().Compare(items[1].ExpireDeadline.AsTime()))
		assert.Equal(t, 0, resp.Items[1].ReserveDeadline.AsTime().Compare(items[1].ReserveDeadline.AsTime()))
		assert.Equal(t, items[1].Attempts, resp.Items[1].Attempts)
		assert.Equal(t, items[1].Reference, resp.Items[1].Reference)
		assert.Equal(t, items[1].Encoding, resp.Items[1].Encoding)
		assert.Equal(t, items[1].Kind, resp.Items[1].Kind)
		assert.Equal(t, items[1].Payload, resp.Items[1].Payload)
	})

	t.Run("CRUD", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, 15*clock.Second, que.ServiceConfig{StorageConfig: _store})
		defer d.Shutdown(t)

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			ReserveTimeout:      ReserveTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		}))
		items := writeRandomItems(t, ctx, c, queueName, 10_000)

		t.Run("List", func(t *testing.T) {
			var resp pb.StorageItemsListResponse
			err := c.StorageItemsList(ctx, queueName, 0, &resp, &que.ListOptions{Limit: 10_000})
			require.NoError(t, err)

			assert.Equal(t, len(items), len(resp.Items))
			for i := range items {
				assert.NotEmpty(t, resp.Items[i].CreatedAt.AsTime())
				assert.Equal(t, items[i].Id, resp.Items[i].Id)
				assert.Equal(t, items[i].IsReserved, resp.Items[i].IsReserved)
				assert.Equal(t, items[i].Attempts, resp.Items[i].Attempts)
				assert.Equal(t, items[i].Reference, resp.Items[i].Reference)
				assert.Equal(t, items[i].Encoding, resp.Items[i].Encoding)
				assert.Equal(t, items[i].Kind, resp.Items[i].Kind)
				assert.Equal(t, items[i].Payload, resp.Items[i].Payload)
			}

			t.Run("MoreThanAvailable", func(t *testing.T) {
				var more pb.StorageItemsListResponse
				err = c.StorageItemsList(ctx, queueName, 0, &more, &que.ListOptions{Limit: 20_000})
				require.NoError(t, err)
				assert.Equal(t, 10_000, len(more.Items))

				compareStorageItem(t, items[0], more.Items[0])
				compareStorageItem(t, items[10_000-1], more.Items[len(more.Items)-1])
			})

			t.Run("LessThanAvailable", func(t *testing.T) {
				var limit pb.StorageItemsListResponse
				err = c.StorageItemsList(ctx, queueName, 0, &limit, &que.ListOptions{Limit: 1_000})
				require.NoError(t, err)
				assert.Equal(t, 1_000, len(limit.Items))
				compareStorageItem(t, items[0], limit.Items[0])
				compareStorageItem(t, items[1_000-1], limit.Items[len(limit.Items)-1])
			})

			t.Run("GetOne", func(t *testing.T) {
				var limit pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &limit,
					&que.ListOptions{Pivot: items[10].Id, Limit: 1}))

				assert.Equal(t, 1, len(limit.Items))
				assert.Equal(t, items[10].Id, limit.Items[0].Id)
			})

			t.Run("FirstOne", func(t *testing.T) {
				var limit pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &limit, &que.ListOptions{Limit: 1}))

				assert.Equal(t, 1, len(limit.Items))
				assert.Equal(t, items[0].Id, limit.Items[0].Id)
			})

			t.Run("PivotNotFound", func(t *testing.T) {
				var limit pb.StorageItemsListResponse
				// Take the first KSUID in the list and get the previous id in the sequence
				// which is not an ID that exists in the data store.
				uid, err := ksuid.Parse(items[0].Id)
				require.NoError(t, err)
				require.NotEqual(t, items[0].Id, uid.Prev().String())

				// Attempt to use that id as a pivot
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &limit,
					&que.ListOptions{Pivot: uid.Prev().String(), Limit: 1}))

				// Should return the first item in the queue
				assert.Equal(t, 1, len(limit.Items))

				assert.Equal(t, items[0].Id, limit.Items[0].Id)
			})

			t.Run("WithPivot", func(t *testing.T) {
				id := items[1000].Id
				var list pb.StorageItemsListResponse
				err := c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Pivot: id, Limit: 10})
				require.NoError(t, err)

				assert.Equal(t, 10, len(list.Items))
				compareStorageItem(t, items[1000], list.Items[0])
				for i := range list.Items {
					compareStorageItem(t, items[i+1000], list.Items[i])
				}

				// TODO: Replace this test with a test of the list iterator for client.StorageItemsList()
				t.Run("PageThroughItems", func(t *testing.T) {
					pivot := list.Items[9]
					var page pb.StorageItemsListResponse
					err = c.StorageItemsList(ctx, queueName, 0, &page, &que.ListOptions{Pivot: pivot.Id, Limit: 10})
					require.NoError(t, err)
					// First item in the returned page is the pivot we requested
					compareStorageItem(t, pivot, page.Items[0])
					// And 9 other items after the pivot
					compareStorageItem(t, items[1009], page.Items[0])
					compareStorageItem(t, items[1018], page.Items[9])

					pivot = page.Items[9]
					err = c.StorageItemsList(ctx, queueName, 0, &page, &que.ListOptions{Pivot: pivot.Id, Limit: 10})
					require.NoError(t, err)
					// Includes the pivot
					compareStorageItem(t, pivot, page.Items[0])
					// And 9 other items
					compareStorageItem(t, items[1018], page.Items[0])
					compareStorageItem(t, items[1027], page.Items[9])
				})

				t.Run("PageIncludesPivot", func(t *testing.T) {
					item := list.Items[9]
					err = c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Pivot: item.Id, Limit: 1})
					require.NoError(t, err)

					require.Equal(t, 1, len(list.Items))
					assert.Equal(t, item.Id, list.Items[0].Id)
					compareStorageItem(t, item, list.Items[0])
					compareStorageItem(t, items[1009], list.Items[0])
				})
			})
		})

		t.Run("Delete", func(t *testing.T) {
			require.NoError(t, c.StorageItemsDelete(ctx, &pb.StorageItemsDeleteRequest{
				QueueName: queueName,
				Ids:       que.CollectIDs(items[0:1_000]),
			}))

			var deleted pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &deleted, &que.ListOptions{Limit: 10_000}))

			// Assert the items deleted do not exist
			for _, d := range items[0:1_000] {
				for _, item := range deleted.Items {
					if item.Id == d.Id {
						t.Fatalf("Found deleted message %s", d.Id)
					}
				}
			}

			t.Run("AlreadyDeletedIsOk", func(t *testing.T) {
				require.NoError(t, c.StorageItemsDelete(ctx, &pb.StorageItemsDeleteRequest{
					QueueName: queueName,
					Ids:       que.CollectIDs(items[0:1_000]),
				}))
			})
		})
	})

	t.Run("StorageItemsListErrors", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, 5*clock.Second, que.ServiceConfig{StorageConfig: _store})
		defer d.Shutdown(t)

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			ReserveTimeout:      ReserveTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		}))

		for _, test := range []struct {
			Name string
			Req  *pb.StorageItemsListRequest
			Msg  string
			Code int
		}{
			{
				Name: "InvalidQueue",
				Req: &pb.StorageItemsListRequest{
					QueueName: "invalid~queue",
				},
				Msg:  "queue name is invalid; 'invalid~queue' cannot contain '~' character",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "InvalidPartition",
				Req: &pb.StorageItemsListRequest{
					QueueName: queueName,
					Partition: 1,
				},
				Msg:  "partition is invalid; '1' is not a valid partition",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "OutOfBoundsPartition",
				Req: &pb.StorageItemsListRequest{
					QueueName: queueName,
					Partition: 500,
				},
				Msg:  "partition is invalid; '500' is not a valid partition",
				Code: duh.CodeBadRequest,
			},
		} {
			t.Run(test.Name, func(t *testing.T) {
				var resp pb.StorageItemsListResponse
				err := c.StorageItemsList(ctx, test.Req.QueueName, int(test.Req.Partition), &resp, &que.ListOptions{
					Limit: int(test.Req.Limit),
					Pivot: test.Req.Pivot,
				})
				if test.Code != duh.CodeOK {
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Contains(t, e.Message(), test.Msg)
					assert.Equal(t, test.Code, e.Code())
				}
			})
		}
	})
	t.Run("StorageItemsImportErrors", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, 5*clock.Second, que.ServiceConfig{StorageConfig: _store})
		defer d.Shutdown(t)

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			ReserveTimeout:      ReserveTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		}))

		for _, test := range []struct {
			Name string
			Req  *pb.StorageItemsImportRequest
			Msg  string
			Code int
		}{
			{
				Name: "EmptyRequest",
				Req:  &pb.StorageItemsImportRequest{},
				Msg:  "queue name is invalid; queue name cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "InvalidQueue",
				Req: &pb.StorageItemsImportRequest{
					QueueName: "invalid~queue",
				},
				Msg:  "queue name is invalid; 'invalid~queue' cannot contain '~' character",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "NoItems",
				Req: &pb.StorageItemsImportRequest{
					QueueName: queueName,
					Items:     nil,
				},
				Msg:  "items is invalid; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "ItemsEmptyList",
				Req: &pb.StorageItemsImportRequest{
					Items:     []*pb.StorageItem{},
					QueueName: queueName,
				},
				Msg:  "items is invalid; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "InvalidPartition",
				Req: &pb.StorageItemsImportRequest{
					Items:     []*pb.StorageItem{},
					QueueName: queueName,
					Partition: 1,
				},
				Msg:  "partition is invalid; '1' is not a valid partition",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "OutOfBoundsPartition",
				Req: &pb.StorageItemsImportRequest{
					Items:     []*pb.StorageItem{},
					QueueName: queueName,
					Partition: 500,
				},
				Msg:  "partition is invalid; '500' is not a valid partition",
				Code: duh.CodeBadRequest,
			},
		} {
			t.Run(test.Name, func(t *testing.T) {
				var resp pb.StorageItemsImportResponse
				err := c.StorageItemsImport(ctx, test.Req, &resp)
				if test.Code != duh.CodeOK {
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Contains(t, e.Message(), test.Msg)
					assert.Equal(t, test.Code, e.Code())
				}
			})
		}
	})

	t.Run("StorageItemsDeleteErrors", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, 5*clock.Second, que.ServiceConfig{StorageConfig: _store})
		defer d.Shutdown(t)

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			ReserveTimeout:      ReserveTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		}))

		for _, test := range []struct {
			Name string
			Req  *pb.StorageItemsDeleteRequest
			Msg  string
			Code int
		}{
			{
				Name: "EmptyRequest",
				Req:  &pb.StorageItemsDeleteRequest{},
				Msg:  "queue name is invalid; queue name cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "InvalidQueue",
				Req: &pb.StorageItemsDeleteRequest{
					QueueName: "invalid~queue",
				},
				Msg:  "queue name is invalid; 'invalid~queue' cannot contain '~' character",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "NoIds",
				Req: &pb.StorageItemsDeleteRequest{
					QueueName: queueName,
					Ids:       nil,
				},
				Msg:  "ids is invalid; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "IdsEmptyList",
				Req: &pb.StorageItemsDeleteRequest{
					QueueName: queueName,
					Ids:       []string{},
				},
				Msg:  "ids is invalid; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "InvalidIds",
				Req: &pb.StorageItemsDeleteRequest{
					QueueName: queueName,
					Ids:       []string{"invalid-id", "another-invalid-id"},
				},
				Msg:  "invalid storage id; 'invalid-id'",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "InvalidPartition",
				Req: &pb.StorageItemsDeleteRequest{
					QueueName: queueName,
					Ids:       []string{"id1", "id2"},
					Partition: 1,
				},
				Msg:  "partition is invalid; '1' is not a valid partition",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "OutOfBoundsPartition",
				Req: &pb.StorageItemsDeleteRequest{
					QueueName: queueName,
					Ids:       []string{"id1", "id2"},
					Partition: 500,
				},
				Msg:  "partition is invalid; '500' is not a valid partition",
				Code: duh.CodeBadRequest,
			},
		} {
			t.Run(test.Name, func(t *testing.T) {
				err := c.StorageItemsDelete(ctx, test.Req)
				if test.Code != duh.CodeOK {
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Contains(t, e.Message(), test.Msg)
					assert.Equal(t, test.Code, e.Code())
				}
			})
		}
	})
}
