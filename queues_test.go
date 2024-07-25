package querator_test

import (
	"context"
	"math/rand"

	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestQueuesStorage(t *testing.T) {
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
			testQueuesStorage(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueuesStorage(t *testing.T, newStore NewStorageFunc, tearDown func()) {
	_store := newStore()
	defer tearDown()
	t.Run("CRUDCompare", func(t *testing.T) {})

	t.Run("CRUD", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, _store, 10*time.Second)
		defer d.Shutdown(t)

		t.Run("Create", func(t *testing.T) {
			now := time.Now().UTC()
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:      queueName,
				DeadQueue:      queueName + "-dead",
				Reference:      "CreateTestRef",
				ReserveTimeout: "1m",
				DeadTimeout:    "10m",
				MaxAttempts:    10,
			}))

			var list pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &list, nil))
			require.Equal(t, 1, len(list.Items))
			assert.Equal(t, queueName, list.Items[0].QueueName)
			assert.NotEmpty(t, list.Items[0].CreatedAt)
			assert.True(t, now.Before(list.Items[0].CreatedAt.AsTime()))
			assert.NotEmpty(t, list.Items[0].UpdatedAt)
			assert.True(t, now.Before(list.Items[0].UpdatedAt.AsTime()))
			assert.Equal(t, int32(10), list.Items[0].MaxAttempts)
			assert.Equal(t, "1m0s", list.Items[0].ReserveTimeout)
			assert.Equal(t, "10m0s", list.Items[0].DeadTimeout)
			assert.Equal(t, queueName+"-dead", list.Items[0].DeadQueue)
			assert.Equal(t, "CreateTestRef", list.Items[0].Reference)
		})

		now := time.Now().UTC()
		queues := createRandomQueues(t, ctx, c, 200)

		t.Run("Get", func(t *testing.T) {
			var list pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &list, &que.ListOptions{
				Pivot: queues[100].QueueName,
				Limit: 1,
			}))
			l := queues[100]
			r := list.Items[0]
			require.Equal(t, 1, len(list.Items))
			assert.Equal(t, l.QueueName, r.QueueName)
			assert.NotEmpty(t, r.CreatedAt)
			assert.True(t, now.Before(r.CreatedAt.AsTime()))
			assert.NotEmpty(t, r.UpdatedAt)
			assert.True(t, now.Before(r.UpdatedAt.AsTime()))
			assert.Equal(t, l.MaxAttempts, r.MaxAttempts)
			assert.Equal(t, l.ReserveTimeout, r.ReserveTimeout)
			assert.Equal(t, l.DeadTimeout, r.DeadTimeout)
			assert.Equal(t, l.DeadQueue, r.DeadQueue)
			assert.Equal(t, l.Reference, r.Reference)
		})
		t.Run("Update", func(t *testing.T) {

			t.Run("MaxAttempts", func(t *testing.T) {
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:   queues[50].QueueName,
					MaxAttempts: queues[50].MaxAttempts + 1,
				}))

				var list pb.QueuesListResponse
				require.NoError(t, c.QueuesList(ctx, &list, &que.ListOptions{
					Pivot: queues[50].QueueName,
					Limit: 1,
				}))
				l := list.Items[0]
				assert.Equal(t, queues[50].QueueName, l.QueueName)
				assert.NotEmpty(t, l.CreatedAt)
				assert.True(t, now.Before(l.CreatedAt.AsTime()))
				assert.NotEmpty(t, l.UpdatedAt)
				assert.True(t, time.Now().After(l.UpdatedAt.AsTime()))
				assert.True(t, now.Before(l.UpdatedAt.AsTime()))
				assert.True(t, l.CreatedAt.AsTime().Before(l.UpdatedAt.AsTime()))
				assert.Equal(t, queues[50].MaxAttempts+1, l.MaxAttempts)
			})
			t.Run("ReserveTimeout", func(t *testing.T) {})
			t.Run("DeadTimeout", func(t *testing.T) {})
			t.Run("Reference", func(t *testing.T) {})

			//t.Run("UpdateDeadQueue", func(t *testing.T) {
			//	// TODO: Produce an item, and ensure it goes to the dead queue once that functionality is complete
			//})

		})
		t.Run("Delete", func(t *testing.T) {})
	})

	t.Run("List", func(t *testing.T) {})
	t.Run("ListWithPivot", func(t *testing.T) {})
	t.Run("ListMoreThanAvailable", func(t *testing.T) {})
	t.Run("ListLessThanAvailable", func(t *testing.T) {})
	t.Run("ListWithPivot", func(t *testing.T) {})
	t.Run("ListIncludePivot", func(t *testing.T) {})
	t.Run("ListIterator", func(t *testing.T) {})
	t.Run("DeleteAlreadyDeletedIsOk", func(t *testing.T) {})

	t.Run("QueuesCreateErrors", func(t *testing.T) {
		// TODO: Already exists
	})
	t.Run("QueuesGetErrors", func(t *testing.T) {})
	t.Run("QueuesListErrors", func(t *testing.T) {})
	t.Run("QueuesUpdateErrors", func(t *testing.T) {
		// TODO: Update the queue name is not allowed
		// TODO: No Such Queue
	})
	t.Run("QueuesDeleteErrors", func(t *testing.T) {})

}

type Pair struct {
	Reserve string
	Dead    string
}

var validTimeouts = []Pair{
	{
		Reserve: "15s",
		Dead:    "1m0s",
	},
	{
		Reserve: "1m0s",
		Dead:    "10m0s",
	},
	{
		Reserve: "10m0s",
		Dead:    "24h0m0s",
	},
	{
		Reserve: "30m0s",
		Dead:    "1h0m0s",
	},
}

func createRandomQueues(t *testing.T, ctx context.Context, c *que.Client, count int) []*pb.QueueInfo {
	t.Helper()

	var items []*pb.QueueInfo
	for i := 0; i < count; i++ {
		p := random.Slice(validTimeouts)
		info := pb.QueueInfo{
			QueueName:      random.String("queue-", 10),
			DeadQueue:      random.String("dead-", 10),
			Reference:      random.String("ref-", 10),
			MaxAttempts:    int32(rand.Intn(100)),
			ReserveTimeout: p.Reserve,
			DeadTimeout:    p.Dead,
		}
		items = append(items, &info)
		require.NoError(t, c.QueuesCreate(ctx, &info))
	}
	return items
}
