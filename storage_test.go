package querator_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"log/slog"
	"testing"
	"time"
)

var log = slog.New(slog.NewTextHandler(io.Discard, nil))

// TestQueueStorage tests the /storage/queue.* endpoints
func TestQueueStorage(t *testing.T) {
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
			testQueueStorage(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueueStorage(t *testing.T, newStore NewStorageFunc, tearDown func()) {
	_store := newStore()
	defer tearDown()

	t.Run("CRUDCompare", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{QueueName: queueName}))

		now := time.Now().UTC()
		var items []*pb.StorageQueueItem
		items = append(items, &pb.StorageQueueItem{
			IsReserved:      true,
			DeadDeadline:    timestamppb.New(now.Add(100_000 * time.Minute)),
			ReserveDeadline: timestamppb.New(now.Add(3_000 * time.Minute)),
			Attempts:        5,
			Reference:       "rainbow@dash.com",
			Encoding:        "rainbows",
			Kind:            "20% cooler",
			Payload: []byte("I mean... have I changed? Same sleek body. Same " +
				"flowing mane. Same spectacular hooves. Nope, I'm still awesome"),
		})
		items = append(items, &pb.StorageQueueItem{
			IsReserved:      false,
			DeadDeadline:    timestamppb.New(now.Add(1_000_000 * time.Minute)),
			ReserveDeadline: timestamppb.New(now.Add(3_000 * time.Minute)),
			Attempts:        10_000,
			Reference:       "rarity@dash.com",
			Encoding:        "beauty",
			Kind:            "sparkles",
			Payload:         []byte("Whining? I am not whining, I am complaining"),
		})

		var resp pb.StorageQueueAddResponse
		err := c.StorageQueueAdd(ctx, &pb.StorageQueueAddRequest{Items: items, QueueName: queueName}, &resp)
		require.NoError(t, err)

		require.Equal(t, len(items), len(resp.Items))

		// Ensure all the fields are indeed the same
		assert.NotEmpty(t, resp.Items[0].Id)
		assert.NotEmpty(t, resp.Items[0].CreatedAt.AsTime())
		assert.Equal(t, items[0].IsReserved, resp.Items[0].IsReserved)
		assert.Equal(t, 0, resp.Items[0].DeadDeadline.AsTime().Compare(items[0].DeadDeadline.AsTime()))
		assert.Equal(t, 0, resp.Items[0].ReserveDeadline.AsTime().Compare(items[0].ReserveDeadline.AsTime()))
		assert.Equal(t, items[0].Attempts, resp.Items[0].Attempts)
		assert.Equal(t, items[0].Reference, resp.Items[0].Reference)
		assert.Equal(t, items[0].Encoding, resp.Items[0].Encoding)
		assert.Equal(t, items[0].Kind, resp.Items[0].Kind)
		assert.Equal(t, items[0].Payload, resp.Items[0].Payload)

		assert.NotEmpty(t, resp.Items[1].Id)
		assert.NotEmpty(t, resp.Items[1].CreatedAt.AsTime())
		assert.Equal(t, items[1].IsReserved, resp.Items[1].IsReserved)
		assert.Equal(t, 0, resp.Items[1].DeadDeadline.AsTime().Compare(items[1].DeadDeadline.AsTime()))
		assert.Equal(t, 0, resp.Items[1].ReserveDeadline.AsTime().Compare(items[1].ReserveDeadline.AsTime()))
		assert.Equal(t, items[1].Attempts, resp.Items[1].Attempts)
		assert.Equal(t, items[1].Reference, resp.Items[1].Reference)
		assert.Equal(t, items[1].Encoding, resp.Items[1].Encoding)
		assert.Equal(t, items[1].Kind, resp.Items[1].Kind)
		assert.Equal(t, items[1].Payload, resp.Items[1].Payload)
	})

	t.Run("CRUD", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{QueueName: queueName}))
		items := writeRandomItems(t, ctx, c, queueName, 10_000)

		var resp pb.StorageQueueListResponse
		err := c.StorageQueueList(ctx, queueName, &resp, &que.ListOptions{Limit: 10_000})
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

		t.Run("ListMoreThanAvailable", func(t *testing.T) {
			var more pb.StorageQueueListResponse
			err = c.StorageQueueList(ctx, queueName, &more, &que.ListOptions{Limit: 20_000})
			require.NoError(t, err)
			assert.Equal(t, 10_000, len(more.Items))

			compareStorageItem(t, items[0], more.Items[0])
			compareStorageItem(t, items[10_000-1], more.Items[len(more.Items)-1])
		})

		t.Run("ListLessThanAvailable", func(t *testing.T) {
			var limit pb.StorageQueueListResponse
			err = c.StorageQueueList(ctx, queueName, &limit, &que.ListOptions{Limit: 1_000})
			require.NoError(t, err)
			assert.Equal(t, 1_000, len(limit.Items))
			compareStorageItem(t, items[0], limit.Items[0])
			compareStorageItem(t, items[1_000-1], limit.Items[len(limit.Items)-1])
		})

		t.Run("GetFirstOne", func(t *testing.T) {
			var limit pb.StorageQueueListResponse
			require.NoError(t, c.StorageQueueList(ctx, queueName, &limit, &que.ListOptions{Limit: 1}))

			assert.Equal(t, 1, len(limit.Items))
			assert.Equal(t, items[0].Id, limit.Items[0].Id)
		})

		t.Run("Get", func(t *testing.T) {
			var limit pb.StorageQueueListResponse
			require.NoError(t, c.StorageQueueList(ctx, queueName, &limit,
				&que.ListOptions{Pivot: items[10].Id, Limit: 1}))

			assert.Equal(t, 1, len(limit.Items))
			assert.Equal(t, items[10].Id, limit.Items[0].Id)
		})

		t.Run("Delete", func(t *testing.T) {
			require.NoError(t, c.StorageQueueDelete(ctx, &pb.StorageQueueDeleteRequest{
				QueueName: queueName,
				Ids:       que.CollectIDs(items[0:1_000]),
			}))

			var deleted pb.StorageQueueListResponse
			require.NoError(t, c.StorageQueueList(ctx, queueName, &deleted, &que.ListOptions{Limit: 10_000}))

			// Assert the items deleted do not exist
			for _, d := range items[0:1_000] {
				for _, item := range deleted.Items {
					if item.Id == d.Id {
						t.Fatalf("Found deleted message %s", d.Id)
					}
				}
			}

			t.Run("DeleteAlreadyDeletedIsOk", func(t *testing.T) {
				require.NoError(t, c.StorageQueueDelete(ctx, &pb.StorageQueueDeleteRequest{
					QueueName: queueName,
					Ids:       que.CollectIDs(items[0:1_000]),
				}))
			})
		})
	})

	t.Run("ListWithPivot", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{QueueName: queueName}))
		items := writeRandomItems(t, ctx, c, queueName, 10_000)

		id := items[1000].Id
		var list pb.StorageQueueListResponse
		err := c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Pivot: id, Limit: 10})
		require.NoError(t, err)

		assert.Equal(t, 10, len(list.Items))
		compareStorageItem(t, items[1000], list.Items[0])
		for i := range list.Items {
			compareStorageItem(t, items[i+1000], list.Items[i])
		}

		t.Run("ListIncludePivot", func(t *testing.T) {
			item := list.Items[9]
			err = c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Pivot: item.Id, Limit: 1})
			require.NoError(t, err)

			require.Equal(t, 1, len(list.Items))
			assert.Equal(t, item.Id, list.Items[0].Id)
			compareStorageItem(t, item, list.Items[0])
			compareStorageItem(t, items[1009], list.Items[0])
		})

		// TODO: Replace this test with a test of the list iterator for client.StorageQueueList()
		t.Run("PivotMorePages", func(t *testing.T) {
			item := list.Items[0]
			err = c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Pivot: item.Id, Limit: 10})
			require.NoError(t, err)
			compareStorageItem(t, items[1018], list.Items[len(list.Items)-1])

			item = list.Items[9]
			err = c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Pivot: item.Id, Limit: 10})
			require.NoError(t, err)
			compareStorageItem(t, items[1027], list.Items[len(list.Items)-1])
		})

	})

	// TODO: Finish these tests
	t.Run("StorageQueueListErrors", func(t *testing.T) {})
	t.Run("StorageQueueAddErrors", func(t *testing.T) {})

	t.Run("StorageQueueDeleteErrors", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 5*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{QueueName: queueName}))

		for _, test := range []struct {
			Name string
			Req  *pb.StorageQueueDeleteRequest
			Msg  string
			Code int
		}{
			{
				Name: "EmptyRequest",
				Req:  &pb.StorageQueueDeleteRequest{},
				Msg:  "invalid queue; queue name cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "InvalidQueue",
				Req: &pb.StorageQueueDeleteRequest{
					QueueName: "invalid~queue",
				},
				Msg:  "invalid queue_name; 'invalid~queue' cannot contain '~' character",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "NoIds",
				Req: &pb.StorageQueueDeleteRequest{
					QueueName: queueName,
					Ids:       nil,
				},
				Msg:  "ids is invalid; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "IdsEmptyList",
				Req: &pb.StorageQueueDeleteRequest{
					QueueName: queueName,
					Ids:       []string{},
				},
				Msg:  "ids is invalid; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "InvalidIds",
				Req: &pb.StorageQueueDeleteRequest{
					QueueName: queueName,
					Ids:       []string{"invalid-id", "another-invalid-id"},
				},
				Msg:  "invalid storage id; 'invalid-id': expected format <queue_name>~<storage_id>",
				Code: duh.CodeBadRequest,
			},
		} {
			t.Run(test.Name, func(t *testing.T) {
				err := c.StorageQueueDelete(ctx, test.Req)
				if test.Code != duh.CodeOK {
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Equal(t, test.Msg, e.Message())
					assert.Equal(t, test.Code, e.Code())
				}
			})
		}
	})

}

type testDaemon struct {
	cancel context.CancelFunc
	ctx    context.Context
	d      *daemon.Daemon
}

func (td *testDaemon) Shutdown(t *testing.T) {
	require.NoError(t, td.d.Shutdown(td.ctx))
	td.cancel()
}

func (td *testDaemon) MustClient() *que.Client {
	return td.d.MustClient()
}

func (td *testDaemon) Context() context.Context {
	return td.ctx
}

func newDaemon(t *testing.T, s store.Storage, duration time.Duration) (*testDaemon, *que.Client, context.Context) {
	var err error
	td := &testDaemon{}
	td.ctx, td.cancel = context.WithTimeout(context.Background(), duration)
	td.d, err = daemon.NewDaemon(td.ctx, daemon.Config{
		Logger: log,
		Store:  s,
	})
	require.NoError(t, err)
	return td, td.d.MustClient(), td.ctx
}

func randomProduceItems(count int) []*pb.QueueProduceItem {
	var items []*pb.QueueProduceItem
	for i := 0; i < count; i++ {
		items = append(items, &pb.QueueProduceItem{
			Reference: random.String("ref-", 10),
			Encoding:  random.String("enc-", 10),
			Kind:      random.String("kind-", 10),
			Bytes:     []byte(fmt.Sprintf("message-%d", i)),
		})
	}
	return items
}

// TODO: Defer
// TODO: Implement clock style thingy so we can freeze time and advance time in order to test Deadlines and such.
// TODO: Test /queue.produce and all the possible incorrect way it could be called
// TODO: Test /queue.reserve and all the possible incorrect way it could be called
// TODO: Test /queue.complete and all the possible incorrect way it could be called
