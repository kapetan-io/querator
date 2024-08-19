package querator_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"testing"
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
			Setup: func(cp *clock.Provider) store.Storage {
				return store.NewMemoryStorage(store.MemoryStorageConfig{Clock: cp})
			},
			TearDown: func() {},
		},
		{
			Name: "BoltDB",
			Setup: func(cp *clock.Provider) store.Storage {
				return bdb.Setup(store.BoltConfig{Clock: cp})
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

func testQueuesStorage(t *testing.T, setup NewStorageFunc, tearDown func()) {
	t.Run("CRUD", func(t *testing.T) {
		_store := setup(clock.NewProvider())
		defer tearDown()
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{Storage: _store})
		defer d.Shutdown(t)

		t.Run("Create", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			now := clock.Now().UTC()
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

		// TODO: Test Create with Named DeadLetter queue

		now := clock.Now().UTC()
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
				assert.True(t, clock.Now().After(r.UpdatedAt.AsTime()))
				assert.True(t, now.Before(r.UpdatedAt.AsTime()))
				assert.True(t, r.CreatedAt.AsTime().Before(r.UpdatedAt.AsTime()))
				assert.Equal(t, l.MaxAttempts+1, r.MaxAttempts)

				t.Run("Respected", func(t *testing.T) {
					// TODO: Ensure producing and consuming on this queue respects the updated MaxAttempts
				})
			})

			t.Run("ReserveTimeout", func(t *testing.T) {
				l := queues[51]

				rt, err := clock.ParseDuration(l.ReserveTimeout)
				require.NoError(t, err)
				rt += 10 * clock.Second

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
				assert.True(t, clock.Now().After(r.UpdatedAt.AsTime()))
				assert.True(t, now.Before(r.UpdatedAt.AsTime()))
				assert.True(t, r.CreatedAt.AsTime().Before(r.UpdatedAt.AsTime()))
				assert.Equal(t, rt.String(), r.ReserveTimeout)
				t.Run("Respected", func(t *testing.T) {
					// TODO: Ensure producing and consuming on this queue respects the updated value
				})
			})
			t.Run("DeadTimeout", func(t *testing.T) {
				l := queues[52]

				dt, err := clock.ParseDuration(l.DeadTimeout)
				require.NoError(t, err)
				dt += 20 * clock.Second

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
				assert.True(t, clock.Now().After(r.UpdatedAt.AsTime()))
				assert.True(t, now.Before(r.UpdatedAt.AsTime()))
				assert.True(t, r.CreatedAt.AsTime().Before(r.UpdatedAt.AsTime()))
				assert.Equal(t, dt.String(), r.DeadTimeout)
				t.Run("Respected", func(t *testing.T) {
					// TODO: Ensure producing and consuming on this queue respects the updated value
				})
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
				assert.True(t, clock.Now().After(r.UpdatedAt.AsTime()))
				assert.True(t, now.Before(r.UpdatedAt.AsTime()))
				assert.True(t, r.CreatedAt.AsTime().Before(r.UpdatedAt.AsTime()))
				assert.Equal(t, "SomethingElse", r.Reference)
			})

			t.Run("Everything", func(t *testing.T) {
				l := queues[54]

				dt, err := clock.ParseDuration(l.DeadTimeout)
				require.NoError(t, err)
				dt += 35 * clock.Second

				rt, err := clock.ParseDuration(l.ReserveTimeout)
				require.NoError(t, err)
				rt += 5 * clock.Second

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
				assert.True(t, clock.Now().After(r.UpdatedAt.AsTime()))
				assert.True(t, now.Before(r.UpdatedAt.AsTime()))
				assert.True(t, r.CreatedAt.AsTime().Before(r.UpdatedAt.AsTime()))
				assert.Equal(t, "FriendshipIsMagic", r.Reference)
				assert.Equal(t, l.MaxAttempts+1, r.MaxAttempts)
				assert.Equal(t, dt.String(), r.DeadTimeout)
				assert.Equal(t, rt.String(), r.ReserveTimeout)
			})
		})
		t.Run("Delete", func(t *testing.T) {
			l := queues[0]

			// Ensure the queue still exists
			var list pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &list, &que.ListOptions{
				Pivot: l.QueueName,
				Limit: 1,
			}))
			r := list.Items[0]
			assert.Equal(t, l.QueueName, r.QueueName)

			// Attempt to delete
			require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: l.QueueName}))

			// Queue should no longer exist
			require.NoError(t, c.QueuesList(ctx, &list, &que.ListOptions{
				Pivot: l.QueueName,
				Limit: 1,
			}))
			// List should return the next item in the list, not the one we requested
			r = list.Items[0]
			assert.NotEqual(t, l.QueueName, r.QueueName)

			// TODO
			t.Run("DeleteAlreadyDeletedIsOk", func(t *testing.T) {})
		})
	})

	t.Run("List", func(t *testing.T) {
		_store := setup(clock.NewProvider())
		defer tearDown()
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{Storage: _store})
		defer d.Shutdown(t)

		queues := createRandomQueues(t, ctx, c, 100)

		var list pb.QueuesListResponse
		require.NoError(t, c.QueuesList(ctx, &list, nil))
		assert.Equal(t, 100, len(list.Items))

		t.Run("MoreThanAvailable", func(t *testing.T) {
			var more pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &more, &que.ListOptions{Limit: 20_000}))
			assert.Equal(t, 100, len(more.Items))

			compareQueueInfo(t, queues[0], more.Items[0])
			compareQueueInfo(t, queues[100-1], more.Items[len(more.Items)-1])
		})
		t.Run("LessThanAvailable", func(t *testing.T) {
			var less pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &less, &que.ListOptions{Limit: 50}))
			assert.Equal(t, 50, len(less.Items))

			compareQueueInfo(t, queues[0], less.Items[0])
			compareQueueInfo(t, queues[50-1], less.Items[len(less.Items)-1])
		})
		t.Run("GetOne", func(t *testing.T) {
			var one pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &one, &que.ListOptions{Pivot: queues[20].QueueName, Limit: 1}))
			assert.Equal(t, 1, len(one.Items))

			compareQueueInfo(t, queues[20], one.Items[len(one.Items)-1])
		})
		t.Run("First", func(t *testing.T) {
			var first pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &first, &que.ListOptions{Limit: 1}))
			assert.Equal(t, 1, len(first.Items))

			compareQueueInfo(t, queues[0], first.Items[len(first.Items)-1])
		})
		t.Run("WithPivot", func(t *testing.T) {
			name := queues[60].QueueName
			var pivot pb.QueuesListResponse
			err := c.QueuesList(ctx, &pivot, &que.ListOptions{Pivot: name, Limit: 10})
			require.NoError(t, err)

			assert.Equal(t, 10, len(pivot.Items))
			compareQueueInfo(t, queues[60], pivot.Items[0])
			for i := range pivot.Items {
				compareQueueInfo(t, queues[i+60], pivot.Items[i])
			}
			t.Run("PageThroughItems", func(t *testing.T) {
				name := queues[0].QueueName
				var page pb.QueuesListResponse
				err := c.QueuesList(ctx, &page, &que.ListOptions{Pivot: name, Limit: 10})
				require.NoError(t, err)
				compareQueueInfo(t, queues[0], page.Items[0])
				compareQueueInfo(t, queues[9], page.Items[9])

				t.Run("PageIncludesPivot", func(t *testing.T) {
					name = queues[9].QueueName
					err = c.QueuesList(ctx, &page, &que.ListOptions{Pivot: name, Limit: 10})
					require.NoError(t, err)
					compareQueueInfo(t, queues[9], page.Items[0])
					compareQueueInfo(t, queues[18], page.Items[9])
				})
			})
		})
	})

	t.Run("Errors", func(t *testing.T) {
		_store := setup(clock.NewProvider())
		defer tearDown()
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{Storage: _store})
		defer d.Shutdown(t)

		var queueName = random.String("queue-", 10)
		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:      queueName,
			DeadQueue:      queueName + "-dead",
			Reference:      "CreateTestRef",
			ReserveTimeout: "1m",
			DeadTimeout:    "10m",
			MaxAttempts:    10,
		}))

		t.Run("QueuesCreate", func(t *testing.T) {
			for _, test := range []struct {
				Name string
				Req  *pb.QueueInfo
				Msg  string
				Code int
			}{
				{
					Name: "EmptyRequest",
					Req:  &pb.QueueInfo{},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidQueue",
					Req: &pb.QueueInfo{
						QueueName: "invalid~queue",
					},
					Msg:  "queue name is invalid; 'invalid~queue' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "QueueNameMaxLength",
					Req: &pb.QueueInfo{
						QueueName: random.String("", 2_001),
					},
					Msg:  "queue name is invalid; cannot be greater than '512' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "AlreadyExists",
					Req: &pb.QueueInfo{
						ReserveTimeout: ReserveTimeout,
						DeadTimeout:    DeadTimeout,
						QueueName:      queueName,
					},
					Msg:  "invalid queue; '" + queueName + "' already exists",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "MissingReservationTimeout",
					Req: &pb.QueueInfo{
						QueueName:   random.String("queue-", 10),
						DeadTimeout: "24h0m0s",
					},
					Msg:  "reserve timeout is invalid; cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "MissingDeadTimeout",
					Req: &pb.QueueInfo{
						QueueName:      random.String("queue-", 10),
						ReserveTimeout: "24h0m0s",
					},
					Msg:  "dead timeout is invalid; cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ReferenceMaxLength",
					Req: &pb.QueueInfo{
						QueueName: "ReferenceMaxLength",
						Reference: random.String("", 2_001),
					},
					Msg:  "reference field is invalid; cannot be greater than '2000' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ReserveTimeoutMaxLength",
					Req: &pb.QueueInfo{
						QueueName:      "ReserveTimeoutMaxLength",
						ReserveTimeout: random.String("", 2_001),
					},
					Msg:  "reserve timeout is invalid; cannot be greater than '15' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ReserveTimeoutTooLong",
					Req: &pb.QueueInfo{
						QueueName:      "ReserveTimeoutTooLong",
						ReserveTimeout: "1h0m0s",
						DeadTimeout:    "30m0s",
					},
					Msg:  "reserve timeout is too long; 1h0m0s cannot be greater than the dead timeout 30m0s",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidReserveTimeout",
					Req: &pb.QueueInfo{
						QueueName:      "InvalidReserveTimeout",
						ReserveTimeout: "foo",
					},
					Msg:  "reserve timeout is invalid; time: invalid duration \"foo\" -  expected format: 8m, 15m or 1h",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidDeadTimeout",
					Req: &pb.QueueInfo{
						QueueName:   "InvalidDeadTimeout",
						DeadTimeout: "foo",
					},
					Msg:  "dead timeout is invalid; time: invalid duration \"foo\" - expected format: 60m, 2h or 24h",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "DeadTimeoutMaxLength",
					Req: &pb.QueueInfo{
						QueueName:   "DeadTimeoutMaxLength",
						DeadTimeout: random.String("", 2_001),
					},
					Msg:  "dead timeout is invalid; cannot be greater than '15' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidDeadLetterQueue",
					Req: &pb.QueueInfo{
						QueueName: "InvalidDeadLetterQueue",
						DeadQueue: "invalid~deadLetter",
					},
					Msg:  "dead queue is invalid; 'invalid~deadLetter' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "DeadQueueMaxLength",
					Req: &pb.QueueInfo{
						QueueName:   "DeadTimeoutMaxLength",
						DeadTimeout: random.String("", 2_001),
					},
					Msg:  "dead timeout is invalid; cannot be greater than '15' characters",
					Code: duh.CodeBadRequest,
				},
				{
					// TODO: We may want to allow -1 to indicate infinite retries, or just use 0
					Name: "InvalidNegativeMaxAttempts",
					Req: &pb.QueueInfo{
						QueueName:   "InvalidMaxAttempts",
						MaxAttempts: math.MinInt32,
					},
					Msg:  "max attempts is invalid; cannot be negative number",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidMaxAttempts",
					Req: &pb.QueueInfo{
						QueueName:   "InvalidMaxAttempts",
						MaxAttempts: math.MaxInt32,
					},
					Msg:  "max attempts is invalid; cannot be greater than 65536",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					err := c.QueuesCreate(ctx, test.Req)
					if test.Code != duh.CodeOK {
						var e duh.Error
						require.True(t, errors.As(err, &e))
						assert.Equal(t, test.Msg, e.Message())
						assert.Equal(t, test.Code, e.Code())
						assert.Contains(t, e.Error(), test.Msg)
					}
				})
			}
		})
		t.Run("QueuesList", func(t *testing.T) {
			for _, test := range []struct {
				Name string
				Opts *que.ListOptions
				Msg  string
				Code int
			}{
				{
					Name: "InvalidLimit",
					Opts: &que.ListOptions{
						Limit: -1,
					},
					Msg:  "limit is invalid; limit cannot be negative",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidLimit",
					Opts: &que.ListOptions{
						Limit: math.MaxInt32,
					},
					Msg:  "limit is invalid; cannot be greater than 65536",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidPivot",
					Opts: &que.ListOptions{
						Pivot: "invalid~queue",
					},
					Msg:  "pivot is invalid; 'invalid~queue' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					var resp pb.QueuesListResponse
					err := c.QueuesList(ctx, &resp, test.Opts)
					if test.Code != duh.CodeOK {
						var e duh.Error
						require.True(t, errors.As(err, &e))
						assert.Equal(t, test.Msg, e.Message())
						assert.Equal(t, test.Code, e.Code())
						assert.Contains(t, e.Error(), test.Msg)
					}
				})
			}
		})
		t.Run("QueuesUpdate", func(t *testing.T) {
			for _, test := range []struct {
				Name string
				Req  *pb.QueueInfo
				Msg  string
				Code int
			}{
				{
					Name: "EmptyRequest",
					Req:  &pb.QueueInfo{},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidQueue",
					Req: &pb.QueueInfo{
						QueueName: "invalid~queue",
					},
					Msg:  "queue name is invalid; 'invalid~queue' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "NoSuchQueue",
					Req: &pb.QueueInfo{
						QueueName: "noSuchQueue",
					},
					Msg:  "queue does not exist",
					Code: duh.CodeRequestFailed,
				},
				{
					Name: "QueueNameMaxLength",
					Req: &pb.QueueInfo{
						QueueName: random.String("", 2_001),
					},
					Msg:  "queue name is invalid; cannot be greater than '512' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ReferenceMaxLength",
					Req: &pb.QueueInfo{
						QueueName: queueName,
						Reference: random.String("", 2_001),
					},
					Msg:  "reference field is invalid; cannot be greater than '2000' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ReserveTimeoutTooLong",
					Req: &pb.QueueInfo{
						QueueName:      queueName,
						ReserveTimeout: "1h0m0s",
						DeadTimeout:    "30m0s",
					},
					Msg:  "reserve timeout is too long; 1h0m0s cannot be greater than the dead timeout 30m0s",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidReserveTimeout",
					Req: &pb.QueueInfo{
						QueueName:      queueName,
						ReserveTimeout: "foo",
					},
					Msg:  "reserve timeout is invalid; time: invalid duration \"foo\" -  expected format: 8m, 15m or 1h",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidDeadTimeout",
					Req: &pb.QueueInfo{
						QueueName:   queueName,
						DeadTimeout: "foo",
					},
					Msg:  "dead timeout is invalid; time: invalid duration \"foo\" - expected format: 60m, 2h or 24h",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "DeadTimeoutMaxLength",
					Req: &pb.QueueInfo{
						QueueName:   "DeadTimeoutMaxLength",
						DeadTimeout: random.String("", 2_001),
					},
					Msg:  "dead timeout is invalid; cannot be greater than '15' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "DeadQueueMaxLength",
					Req: &pb.QueueInfo{
						QueueName:   "DeadTimeoutMaxLength",
						DeadTimeout: random.String("", 2_001),
					},
					Msg:  "dead timeout is invalid; cannot be greater than '15' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidDeadLetterQueue",
					Req: &pb.QueueInfo{
						QueueName: queueName,
						DeadQueue: "invalid~deadLetter",
					},
					Msg:  "dead queue is invalid; 'invalid~deadLetter' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
				{
					// TODO: This might be allowed later to indicate infinite retries
					Name: "InvalidMaxAttempts",
					Req: &pb.QueueInfo{
						QueueName:   queueName,
						MaxAttempts: -1,
					},
					Msg:  "max attempts is invalid; cannot be negative number",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidMaxAttempts",
					Req: &pb.QueueInfo{
						QueueName:   "InvalidMaxAttempts",
						MaxAttempts: math.MaxInt32,
					},
					Msg:  "max attempts is invalid; cannot be greater than 65536",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					err := c.QueuesUpdate(ctx, test.Req)
					if test.Code != duh.CodeOK {
						var e duh.Error
						require.True(t, errors.As(err, &e))
						assert.Equal(t, test.Msg, e.Message())
						assert.Equal(t, test.Code, e.Code())
					}
				})
			}
		})
		t.Run("QueuesDelete", func(t *testing.T) {
			for _, test := range []struct {
				Name string
				Req  *pb.QueuesDeleteRequest
				Msg  string
				Code int
			}{
				{
					Name: "EmptyRequest",
					Req:  &pb.QueuesDeleteRequest{},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidQueue",
					Req: &pb.QueuesDeleteRequest{
						QueueName: "invalid~queue",
					},
					Msg:  "queue name is invalid; 'invalid~queue' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "QueueNameMaxLength",
					Req: &pb.QueuesDeleteRequest{
						QueueName: random.String("", 2_001),
					},
					Msg:  "queue name is invalid; cannot be greater than '512' characters",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					err := c.QueuesDelete(ctx, test.Req)
					if test.Code != duh.CodeOK {
						var e duh.Error
						require.True(t, errors.As(err, &e))
						assert.Equal(t, test.Msg, e.Message())
						assert.Equal(t, test.Code, e.Code())
					}
				})
			}
		})
	})
}

func compareQueueInfo(t *testing.T, expected *pb.QueueInfo, actual *pb.QueueInfo) {
	t.Helper()
	require.Equal(t, expected.QueueName, actual.QueueName)
	require.Equal(t, expected.DeadTimeout, actual.DeadTimeout)
	require.Equal(t, expected.ReserveTimeout, actual.ReserveTimeout)
	require.Equal(t, expected.MaxAttempts, actual.MaxAttempts)
	require.Equal(t, expected.DeadQueue, actual.DeadQueue)
	require.Equal(t, expected.Reference, actual.Reference)
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

	var idx int
	var items []*pb.QueueInfo
	for i := 0; i < count; i++ {
		timeOuts := random.Slice(validTimeouts)
		info := pb.QueueInfo{
			QueueName:      fmt.Sprintf("queue-%05d", idx),
			DeadQueue:      random.String("dead-", 10),
			Reference:      random.String("ref-", 10),
			MaxAttempts:    int32(rand.Intn(100)),
			ReserveTimeout: timeOuts.Reserve,
			DeadTimeout:    timeOuts.Dead,
		}
		idx++
		items = append(items, &info)
		require.NoError(t, c.QueuesCreate(ctx, &info))
	}
	return items
}
