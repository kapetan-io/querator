package querator_test

import (
	"context"
	"fmt"
	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math/rand"
	"os"
	"path/filepath"
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
//	// TODO: QueueCreate should create a queue in storage
//	require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))
//
//}

type NewStorageFunc func() store.Storage

func TestStorage(t *testing.T) {
	var dir string

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		{
			Name: "BoltDB",
			Setup: func() store.Storage {
				dir = random.String("test-data-", 10)
				dir = filepath.Join("test-data", dir)
				if err := os.Mkdir(dir, 0777); err != nil {
					panic(err)
				}
				return store.NewBoltStorage(store.BoltOptions{
					StorageDir: dir,
				})
			},
			TearDown: func() {
				if err := os.RemoveAll(dir); err != nil {
					panic(err)
				}
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
			testStorage(t, tc.Setup, tc.TearDown)
		})
	}
}

func testStorage(t *testing.T, setup NewStorageFunc, tearDown func()) {
	_store := setup()
	defer tearDown()

	t.Run("AddAndListCompare", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))

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

	t.Run("AddAndList", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))
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

		t.Run("AskForMoreThanIsAvailable", func(t *testing.T) {
			var more pb.StorageQueueListResponse
			err = c.StorageQueueList(ctx, queueName, &more, &que.ListOptions{Limit: 20_000})
			require.NoError(t, err)
			assert.Equal(t, 10_000, len(more.Items))

			compareStorageItem(t, items[0], more.Items[0])
			compareStorageItem(t, items[10_000-1], more.Items[len(more.Items)-1])
		})

		t.Run("AskForLessThanIsAvailable", func(t *testing.T) {
			var limit pb.StorageQueueListResponse
			err = c.StorageQueueList(ctx, queueName, &limit, &que.ListOptions{Limit: 1_000})
			require.NoError(t, err)
			assert.Equal(t, 1_000, len(limit.Items))
			compareStorageItem(t, items[0], limit.Items[0])
			compareStorageItem(t, items[1_000-1], limit.Items[len(limit.Items)-1])
		})
	})

	t.Run("ReadPivot", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))
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

		t.Run("ListResultsIncludePivot", func(t *testing.T) {
			item := list.Items[9]
			err = c.StorageQueueList(ctx, queueName, &list, &que.ListOptions{Pivot: item.Id, Limit: 1})
			require.NoError(t, err)

			require.Equal(t, 1, len(list.Items))
			assert.Equal(t, item.Id, list.Items[0].Id)
			compareStorageItem(t, item, list.Items[0])
			compareStorageItem(t, items[1009], list.Items[0])
		})

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
		// TODO: Test the list iterator on the StorageQueueList
	})

	t.Run("Produce", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)
		now := time.Now().UTC()

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))
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
