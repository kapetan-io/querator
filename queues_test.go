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
			l := queues[100]
			require.NoError(t, c.QueuesList(ctx, &list, &que.ListOptions{
				Pivot: l.QueueName,
				Limit: 1,
			}))
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
				l := queues[51]
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:   l.QueueName,
					MaxAttempts: l.MaxAttempts + 1,
				}))

				var list pb.QueuesListResponse
				require.NoError(t, c.QueuesList(ctx, &list, &que.ListOptions{
					Pivot: l.QueueName,
					Limit: 1,
				}))
				r := list.Items[0]
				assert.Equal(t, l.QueueName, r.QueueName)
				assert.NotEmpty(t, r.CreatedAt)
				assert.True(t, now.Before(r.CreatedAt.AsTime()))
				assert.NotEmpty(t, r.UpdatedAt)
				assert.True(t, time.Now().After(r.UpdatedAt.AsTime()))
				assert.True(t, now.Before(r.UpdatedAt.AsTime()))
				assert.True(t, r.CreatedAt.AsTime().Before(r.UpdatedAt.AsTime()))
				assert.Equal(t, l.MaxAttempts+1, r.MaxAttempts)
			})

			t.Run("ReserveTimeout", func(t *testing.T) {
				l := queues[51]

				rt, err := time.ParseDuration(l.ReserveTimeout)
				require.NoError(t, err)
				rt += 10 * time.Second

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:      l.QueueName,
					ReserveTimeout: rt.String(),
				}))

				var list pb.QueuesListResponse
				require.NoError(t, c.QueuesList(ctx, &list, &que.ListOptions{
					Pivot: l.QueueName,
					Limit: 1,
				}))
				r := list.Items[0]
				assert.Equal(t, l.QueueName, r.QueueName)
				assert.NotEmpty(t, r.CreatedAt)
				assert.True(t, now.Before(r.CreatedAt.AsTime()))
				assert.NotEmpty(t, r.UpdatedAt)
				assert.True(t, time.Now().After(r.UpdatedAt.AsTime()))
				assert.True(t, now.Before(r.UpdatedAt.AsTime()))
				assert.True(t, r.CreatedAt.AsTime().Before(r.UpdatedAt.AsTime()))
				assert.Equal(t, rt.String(), r.ReserveTimeout)
			})
			t.Run("DeadTimeout", func(t *testing.T) {
				l := queues[52]

				dt, err := time.ParseDuration(l.DeadTimeout)
				require.NoError(t, err)
				dt += 20 * time.Second

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:   l.QueueName,
					DeadTimeout: dt.String(),
				}))

				var list pb.QueuesListResponse
				require.NoError(t, c.QueuesList(ctx, &list, &que.ListOptions{
					Pivot: l.QueueName,
					Limit: 1,
				}))
				r := list.Items[0]
				assert.Equal(t, l.QueueName, r.QueueName)
				assert.NotEmpty(t, r.CreatedAt)
				assert.True(t, now.Before(r.CreatedAt.AsTime()))
				assert.NotEmpty(t, r.UpdatedAt)
				assert.True(t, time.Now().After(r.UpdatedAt.AsTime()))
				assert.True(t, now.Before(r.UpdatedAt.AsTime()))
				assert.True(t, r.CreatedAt.AsTime().Before(r.UpdatedAt.AsTime()))
				assert.Equal(t, dt.String(), r.DeadTimeout)
			})
			t.Run("Reference", func(t *testing.T) {
				l := queues[53]

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName: l.QueueName,
					Reference: "SomethingElse",
				}))

				var list pb.QueuesListResponse
				require.NoError(t, c.QueuesList(ctx, &list, &que.ListOptions{
					Pivot: l.QueueName,
					Limit: 1,
				}))
				r := list.Items[0]
				assert.Equal(t, l.QueueName, r.QueueName)
				assert.NotEmpty(t, r.CreatedAt)
				assert.True(t, now.Before(r.CreatedAt.AsTime()))
				assert.NotEmpty(t, r.UpdatedAt)
				assert.True(t, time.Now().After(r.UpdatedAt.AsTime()))
				assert.True(t, now.Before(r.UpdatedAt.AsTime()))
				assert.True(t, r.CreatedAt.AsTime().Before(r.UpdatedAt.AsTime()))
				assert.Equal(t, "SomethingElse", r.Reference)
			})

			t.Run("Everything", func(t *testing.T) {
				l := queues[54]

				dt, err := time.ParseDuration(l.DeadTimeout)
				require.NoError(t, err)
				dt += 35 * time.Second

				rt, err := time.ParseDuration(l.ReserveTimeout)
				require.NoError(t, err)
				rt += 5 * time.Second

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:      l.QueueName,
					Reference:      "FriendshipIsMagic",
					MaxAttempts:    l.MaxAttempts + 1,
					ReserveTimeout: rt.String(),
					DeadTimeout:    dt.String(),
				}))

				var list pb.QueuesListResponse
				require.NoError(t, c.QueuesList(ctx, &list, &que.ListOptions{
					Pivot: l.QueueName,
					Limit: 1,
				}))

				r := list.Items[0]
				assert.Equal(t, l.QueueName, r.QueueName)
				assert.NotEmpty(t, r.CreatedAt)
				assert.True(t, now.Before(r.CreatedAt.AsTime()))
				assert.NotEmpty(t, r.UpdatedAt)
				assert.True(t, time.Now().After(r.UpdatedAt.AsTime()))
				assert.True(t, now.Before(r.UpdatedAt.AsTime()))
				assert.True(t, r.CreatedAt.AsTime().Before(r.UpdatedAt.AsTime()))
				assert.Equal(t, "FriendshipIsMagic", r.Reference)
				assert.Equal(t, l.MaxAttempts+1, r.MaxAttempts)
				assert.Equal(t, dt.String(), r.DeadTimeout)
				assert.Equal(t, rt.String(), r.ReserveTimeout)
			})
		})
		t.Run("Delete", func(t *testing.T) {
			// TODO: <---- DO THIS NEXT
			// TODO: Delete the first queue in the list
			// TODO: Delete the last queue in the list
		})
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
		// TODO: ReserveTimeout larger than DeadTimeout
		// TODO: Missing ReserveTimeout and DeadTimeout
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
