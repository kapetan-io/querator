package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
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

// TestQueueStorage tests the /storage/queue.* endpoints
func TestQueueStorage(t *testing.T) {
	badger := badgerTestSetup{Dir: t.TempDir()}
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
				return badger.Setup(store.BadgerConfig{})
			},
			TearDown: func() {
				badger.Teardown()
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
			testQueueStorage(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueueStorage(t *testing.T, setUp NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	storage := setUp()
	defer tearDown()

	t.Run("CRUDCompare", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		}))

		now := clock.Now().UTC()
		var items []*pb.StorageItem
		items = append(items, &pb.StorageItem{
			IsLeased:       true,
			ExpireDeadline: timestamppb.New(now.Add(100_000 * clock.Minute)),
			LeaseDeadline:  timestamppb.New(now.Add(3_000 * clock.Minute)),
			Attempts:       5,
			Reference:      "rainbow@dash.com",
			Encoding:       "rainbows",
			Kind:           "20% cooler",
			Payload: []byte("I mean... have I changed? Same sleek body. Same " +
				"flowing mane. Same spectacular hooves. Nope, I'm still awesome"),
		})
		items = append(items, &pb.StorageItem{
			IsLeased:       false,
			ExpireDeadline: timestamppb.New(now.Add(1_000_000 * clock.Minute)),
			LeaseDeadline:  timestamppb.New(now.Add(3_000 * clock.Minute)),
			Attempts:       10_000,
			Reference:      "rarity@dash.com",
			Encoding:       "beauty",
			Kind:           "sparkles",
			Payload:        []byte("Whining? I am not whining, I am complaining"),
		})

		var resp pb.StorageItemsImportResponse
		err := c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{Items: items, QueueName: queueName}, &resp)
		require.NoError(t, err)

		require.Equal(t, len(items), len(resp.Items))

		// Ensure all the fields are indeed the same
		assert.NotEmpty(t, resp.Items[0].Id)
		assert.NotEmpty(t, resp.Items[0].CreatedAt.AsTime())
		assert.Equal(t, items[0].IsLeased, resp.Items[0].IsLeased)
		// Compare timestamps with microsecond precision (PostgreSQL TIMESTAMPTZ precision)
		assert.Equal(t, 0, resp.Items[0].ExpireDeadline.AsTime().Truncate(clock.Microsecond).Compare(
			items[0].ExpireDeadline.AsTime().Truncate(clock.Microsecond)))
		assert.Equal(t, 0, resp.Items[0].LeaseDeadline.AsTime().Truncate(clock.Microsecond).Compare(
			items[0].LeaseDeadline.AsTime().Truncate(clock.Microsecond)))
		assert.Equal(t, items[0].Attempts, resp.Items[0].Attempts)
		assert.Equal(t, items[0].Reference, resp.Items[0].Reference)
		assert.Equal(t, items[0].Encoding, resp.Items[0].Encoding)
		assert.Equal(t, items[0].Kind, resp.Items[0].Kind)
		assert.Equal(t, items[0].Payload, resp.Items[0].Payload)

		assert.NotEmpty(t, resp.Items[1].Id)
		assert.NotEmpty(t, resp.Items[1].CreatedAt.AsTime())
		assert.Equal(t, items[1].IsLeased, resp.Items[1].IsLeased)
		// Compare timestamps with microsecond precision (PostgreSQL TIMESTAMPTZ precision)
		assert.Equal(t, 0, resp.Items[1].ExpireDeadline.AsTime().Truncate(clock.Microsecond).Compare(
			items[1].ExpireDeadline.AsTime().Truncate(clock.Microsecond)))
		assert.Equal(t, 0, resp.Items[1].LeaseDeadline.AsTime().Truncate(clock.Microsecond).Compare(
			items[1].LeaseDeadline.AsTime().Truncate(clock.Microsecond)))
		assert.Equal(t, items[1].Attempts, resp.Items[1].Attempts)
		assert.Equal(t, items[1].Reference, resp.Items[1].Reference)
		assert.Equal(t, items[1].Encoding, resp.Items[1].Encoding)
		assert.Equal(t, items[1].Kind, resp.Items[1].Kind)
		assert.Equal(t, items[1].Payload, resp.Items[1].Payload)
	})

	t.Run("CRUD", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 15*clock.Second, svc.Config{StorageConfig: storage})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		}))
		items := writeRandomItems(t, ctx, c, queueName, 10_000)

		t.Run("List", func(t *testing.T) {
			var resp pb.StorageItemsListResponse
			err := c.StorageItemsList(ctx, queueName, 0, &resp, &querator.ListOptions{Limit: 10_000})
			require.NoError(t, err)

			assert.Equal(t, len(items), len(resp.Items))
			for i := range items {
				assert.NotEmpty(t, resp.Items[i].CreatedAt.AsTime())
				assert.Equal(t, items[i].Id, resp.Items[i].Id)
				assert.Equal(t, items[i].IsLeased, resp.Items[i].IsLeased)
				assert.Equal(t, items[i].Attempts, resp.Items[i].Attempts)
				assert.Equal(t, items[i].Reference, resp.Items[i].Reference)
				assert.Equal(t, items[i].Encoding, resp.Items[i].Encoding)
				assert.Equal(t, items[i].Kind, resp.Items[i].Kind)
				assert.Equal(t, items[i].Payload, resp.Items[i].Payload)
			}

			t.Run("MoreThanAvailable", func(t *testing.T) {
				var more pb.StorageItemsListResponse
				err = c.StorageItemsList(ctx, queueName, 0, &more, &querator.ListOptions{Limit: 20_000})
				require.NoError(t, err)
				assert.Equal(t, 10_000, len(more.Items))

				require.Equal(t, storageItemFieldsOf(items[0]), storageItemFieldsOf(more.Items[0]))
				require.Equal(t, storageItemFieldsOf(items[10_000-1]), storageItemFieldsOf(more.Items[len(more.Items)-1]))
			})

			t.Run("LessThanAvailable", func(t *testing.T) {
				var limit pb.StorageItemsListResponse
				err = c.StorageItemsList(ctx, queueName, 0, &limit, &querator.ListOptions{Limit: 1_000})
				require.NoError(t, err)
				assert.Equal(t, 1_000, len(limit.Items))
				require.Equal(t, storageItemFieldsOf(items[0]), storageItemFieldsOf(limit.Items[0]))
				require.Equal(t, storageItemFieldsOf(items[1_000-1]), storageItemFieldsOf(limit.Items[len(limit.Items)-1]))
			})

			t.Run("GetOne", func(t *testing.T) {
				var limit pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &limit,
					&querator.ListOptions{Pivot: items[10].Id, Limit: 1}))

				assert.Equal(t, 1, len(limit.Items))
				assert.Equal(t, items[10].Id, limit.Items[0].Id)
			})

			t.Run("FirstOne", func(t *testing.T) {
				var limit pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &limit, &querator.ListOptions{Limit: 1}))

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
					&querator.ListOptions{Pivot: uid.Prev().String(), Limit: 1}))

				// Should return the first item in the queue
				assert.Equal(t, 1, len(limit.Items))

				assert.Equal(t, items[0].Id, limit.Items[0].Id)
			})

			t.Run("WithPivot", func(t *testing.T) {
				id := items[1000].Id
				var list pb.StorageItemsListResponse
				err := c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Pivot: id, Limit: 10})
				require.NoError(t, err)

				assert.Equal(t, 10, len(list.Items))
				require.Equal(t, storageItemFieldsOf(items[1000]), storageItemFieldsOf(list.Items[0]))
				for i := range list.Items {
					require.Equal(t, storageItemFieldsOf(items[i+1000]), storageItemFieldsOf(list.Items[i]))
				}

				// TODO: Replace this test with a test of the list iterator for client.StorageItemsList()
				t.Run("PageThroughItems", func(t *testing.T) {
					pivot := list.Items[9]
					var page pb.StorageItemsListResponse
					err = c.StorageItemsList(ctx, queueName, 0, &page, &querator.ListOptions{Pivot: pivot.Id, Limit: 10})
					require.NoError(t, err)
					// First item in the returned page is the pivot we requested
					require.Equal(t, storageItemFieldsOf(pivot), storageItemFieldsOf(page.Items[0]))
					// And 9 other items after the pivot
					require.Equal(t, storageItemFieldsOf(items[1009]), storageItemFieldsOf(page.Items[0]))
					require.Equal(t, storageItemFieldsOf(items[1018]), storageItemFieldsOf(page.Items[9]))

					pivot = page.Items[9]
					err = c.StorageItemsList(ctx, queueName, 0, &page, &querator.ListOptions{Pivot: pivot.Id, Limit: 10})
					require.NoError(t, err)
					// Includes the pivot
					require.Equal(t, storageItemFieldsOf(pivot), storageItemFieldsOf(page.Items[0]))
					// And 9 other items
					require.Equal(t, storageItemFieldsOf(items[1018]), storageItemFieldsOf(page.Items[0]))
					require.Equal(t, storageItemFieldsOf(items[1027]), storageItemFieldsOf(page.Items[9]))
				})

				t.Run("PageIncludesPivot", func(t *testing.T) {
					item := list.Items[9]
					err = c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Pivot: item.Id, Limit: 1})
					require.NoError(t, err)

					require.Equal(t, 1, len(list.Items))
					assert.Equal(t, item.Id, list.Items[0].Id)
					require.Equal(t, storageItemFieldsOf(item), storageItemFieldsOf(list.Items[0]))
					require.Equal(t, storageItemFieldsOf(items[1009]), storageItemFieldsOf(list.Items[0]))
				})
			})
		})

		t.Run("Delete", func(t *testing.T) {
			require.NoError(t, c.StorageItemsDelete(ctx, &pb.StorageItemsDeleteRequest{
				QueueName: queueName,
				Ids:       querator.CollectIDs(items[0:1_000]),
			}))

			var deleted pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &deleted, &querator.ListOptions{Limit: 10_000}))

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
					Ids:       querator.CollectIDs(items[0:1_000]),
				}))
			})
		})
	})

	t.Run("StorageItemsListErrors", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 5*clock.Second, svc.Config{StorageConfig: storage})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
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
				err := c.StorageItemsList(ctx, test.Req.QueueName, int(test.Req.Partition), &resp, &querator.ListOptions{
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
		d := newDaemon(t, 5*clock.Second, svc.Config{StorageConfig: storage})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
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
		d := newDaemon(t, 5*clock.Second, svc.Config{StorageConfig: storage})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
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

	t.Run("QueuesKeepPlacement", func(t *testing.T) {
		name := random.String("queue-", 10)
		backend := storage.PartitionStorage[0].Name
		ctx := context.Background()
		require.NoError(t, storage.Queues.Add(ctx, types.QueueInfo{
			Name:                name,
			LeaseTimeout:        clock.Minute,
			ExpireTimeout:       clock.Hour,
			RequestedPartitions: 1,
			NextPartition:       5,
			PartitionInfo: []types.PartitionInfo{
				{PartitionNum: 3, StorageName: backend, Draining: true},
				{PartitionNum: 4, StorageName: backend},
			},
		}))

		var saved types.QueueInfo
		require.NoError(t, storage.Queues.Get(ctx, name, &saved))
		assert.Equal(t, 5, saved.NextPartition)
		require.Len(t, saved.PartitionInfo, 2)
		assert.Equal(t, 3, saved.PartitionInfo[0].PartitionNum)
		assert.True(t, saved.PartitionInfo[0].Draining)
		assert.Equal(t, 4, saved.PartitionInfo[1].PartitionNum)
		assert.False(t, saved.PartitionInfo[1].Draining)

		require.NoError(t, storage.Queues.Update(ctx, types.QueueInfo{
			Name:      name,
			Reference: "updated",
			UpdatedAt: clock.Now().UTC(),
		}))
		var updated types.QueueInfo
		require.NoError(t, storage.Queues.Get(ctx, name, &updated))
		assert.Equal(t, "updated", updated.Reference)
		assert.Equal(t, 5, updated.NextPartition)
		require.Len(t, updated.PartitionInfo, 2)
		assert.True(t, updated.PartitionInfo[0].Draining)
	})

	t.Run("PartitionInfoSafeForConcurrentUse", func(t *testing.T) {
		name := random.String("queue-", 10)
		partition := storage.PartitionStorage[0].PartitionStore.Get(types.PartitionInfo{
			PartitionNum: 0,
			StorageName:  storage.PartitionStorage[0].Name,
			Queue:        types.QueueInfo{Name: name, MaxAttempts: 1},
		})
		defer func() { require.NoError(t, partition.Close(context.Background())) }()

		concurrently(8, func(i int) {
			for range 100 {
				if i%2 == 0 {
					partition.UpdateQueueInfo(types.QueueInfo{Name: name, MaxAttempts: i + 2})
					continue
				}
				assert.Equal(t, name, partition.Info().Queue.Name)
			}
		})
		assert.Greater(t, partition.Info().Queue.MaxAttempts, 1)
	})

	t.Run("QueuesAddDuplicateName", func(t *testing.T) {
		info := types.QueueInfo{
			Name:                random.String("queue-", 10),
			LeaseTimeout:        clock.Minute,
			ExpireTimeout:       clock.Hour,
			RequestedPartitions: 1,
		}
		ctx := context.Background()
		defer func() { require.NoError(t, storage.Queues.Close(ctx)) }()
		require.NoError(t, storage.Queues.Add(ctx, info))

		err := storage.Queues.Add(ctx, info)
		require.ErrorIs(t, err, types.ErrQueueAlreadyExists)
		require.ErrorContains(t, err, info.Name)
		assert.NotErrorIs(t, err, types.ErrNamespaceAlreadyExists)
	})
}
