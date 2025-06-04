package querator_test

import (
	"errors"
	"github.com/duh-rpc/duh-go"
	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"math"
	"testing"
)

func TestQueuesStorage(t *testing.T) {
	badger := badgerTestSetup{Dir: t.TempDir()}

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
		//{
		//	Name: "SurrealDB",
		//},
		//{
		//	Name: "PostgresSQL",
		//},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testQueues(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueues(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t)

	t.Run("CRUD", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		t.Run("Create", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			now := clock.Now().UTC()
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				DeadQueue:           queueName + "-dead",
				Reference:           "CreateTestRef",
				LeaseTimeout:        "1m",
				ExpireTimeout:       "10m",
				MaxAttempts:         10,
				RequestedPartitions: 1,
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
			assert.Equal(t, "1m0s", list.Items[0].LeaseTimeout)
			assert.Equal(t, "10m0s", list.Items[0].ExpireTimeout)
			assert.Equal(t, queueName+"-dead", list.Items[0].DeadQueue)
			assert.Equal(t, "CreateTestRef", list.Items[0].Reference)
		})

		now := clock.Now().UTC()
		queues := createRandomQueues(t, ctx, c, 50)

		t.Run("GetByPartition", func(t *testing.T) {
			var list pb.QueuesListResponse
			l := queues[10]
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
			assert.Equal(t, l.LeaseTimeout, r.LeaseTimeout)
			assert.Equal(t, l.ExpireTimeout, r.ExpireTimeout)
			assert.Equal(t, l.DeadQueue, r.DeadQueue)
			assert.Equal(t, l.Reference, r.Reference)
		})
		t.Run("Update", func(t *testing.T) {

			t.Run("MaxAttempts", func(t *testing.T) {
				l := queues[31]
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

			t.Run("LeaseTimeout", func(t *testing.T) {
				l := queues[31]

				rt, err := clock.ParseDuration(l.LeaseTimeout)
				require.NoError(t, err)
				rt += 10 * clock.Second

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:    l.QueueName,
					LeaseTimeout: rt.String(),
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
				assert.Equal(t, rt.String(), r.LeaseTimeout)
				t.Run("Respected", func(t *testing.T) {
					// TODO: Ensure producing and consuming on this queue respects the updated value
				})
			})
			t.Run("ExpireTimeout", func(t *testing.T) {
				l := queues[32]

				dt, err := clock.ParseDuration(l.ExpireTimeout)
				require.NoError(t, err)
				dt += 20 * clock.Second

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:     l.QueueName,
					ExpireTimeout: dt.String(),
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
				assert.Equal(t, dt.String(), r.ExpireTimeout)
				t.Run("Respected", func(t *testing.T) {
					// TODO: Ensure producing and consuming on this queue respects the updated value
				})
			})
			t.Run("Reference", func(t *testing.T) {
				l := queues[33]

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
				l := queues[34]

				dt, err := clock.ParseDuration(l.ExpireTimeout)
				require.NoError(t, err)
				dt += 35 * clock.Second

				rt, err := clock.ParseDuration(l.LeaseTimeout)
				require.NoError(t, err)
				rt += 5 * clock.Second

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:     l.QueueName,
					Reference:     "FriendshipIsMagic",
					MaxAttempts:   l.MaxAttempts + 1,
					LeaseTimeout:  rt.String(),
					ExpireTimeout: dt.String(),
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
				assert.Equal(t, dt.String(), r.ExpireTimeout)
				assert.Equal(t, rt.String(), r.LeaseTimeout)
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

			// Partition should no longer exist
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

		t.Run("DeadLetterQueue", func(t *testing.T) {
			// TODO: Test Create with Named DeadLetter queue
			// Ensure the dead letter queue already exists when specified
			// dead letter queues MUST be created manually (I think?)
			t.Run("AvoidCircularQueue", func(t *testing.T) {
				// TODO: A dead letter queue cannot point to an other queue which itself has a dead letter queue
				//  a dead letter queue cannot point to it's self.
			})
		})

	})

	t.Run("List", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		queues := createRandomQueues(t, ctx, c, 50)

		var list pb.QueuesListResponse
		require.NoError(t, c.QueuesList(ctx, &list, nil))
		assert.Equal(t, 50, len(list.Items))

		t.Run("MoreThanAvailable", func(t *testing.T) {
			var more pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &more, &que.ListOptions{Limit: 20_000}))
			assert.Equal(t, 50, len(more.Items))

			compareQueueInfo(t, queues[0], more.Items[0])
			compareQueueInfo(t, queues[50-1], more.Items[len(more.Items)-1])
		})
		t.Run("LessThanAvailable", func(t *testing.T) {
			var less pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &less, &que.ListOptions{Limit: 30}))
			assert.Equal(t, 30, len(less.Items))

			compareQueueInfo(t, queues[0], less.Items[0])
			compareQueueInfo(t, queues[30-1], less.Items[len(less.Items)-1])
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
			name := queues[20].QueueName
			var pivot pb.QueuesListResponse
			err := c.QueuesList(ctx, &pivot, &que.ListOptions{Pivot: name, Limit: 10})
			require.NoError(t, err)

			assert.Equal(t, 10, len(pivot.Items))
			compareQueueInfo(t, queues[20], pivot.Items[0])
			for i := range pivot.Items {
				compareQueueInfo(t, queues[i+20], pivot.Items[i])
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

		t.Run("PivotNotFound", func(t *testing.T) {
			var page pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &page,
				&que.ListOptions{Pivot: "pueue-00000", Limit: 1}))

			// Should return the first queue in the list
			assert.Equal(t, 1, len(page.Items))

			assert.Equal(t, queues[0].QueueName, page.Items[0].QueueName)
		})
	})

	t.Run("Errors", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		var queueName = random.String("queue-", 10)
		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:           queueName,
			DeadQueue:           queueName + "-dead",
			Reference:           "CreateTestRef",
			LeaseTimeout:        "1m",
			ExpireTimeout:       "10m",
			MaxAttempts:         10,
			RequestedPartitions: 1,
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
					Name: "QueueWhiteSpace",
					Req: &pb.QueueInfo{
						LeaseTimeout:        LeaseTimeout,
						ExpireTimeout:       ExpireTimeout,
						QueueName:           "Friendship is Magic",
						RequestedPartitions: 1,
					},
					Msg:  "queue name is invalid; 'Friendship is Magic' cannot contain whitespace",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "AlreadyExists",
					Req: &pb.QueueInfo{
						LeaseTimeout:        LeaseTimeout,
						ExpireTimeout:       ExpireTimeout,
						QueueName:           queueName,
						RequestedPartitions: 1,
					},
					Msg:  "invalid queue; '" + queueName + "' already exists",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "MissingLeaseTimeout",
					Req: &pb.QueueInfo{
						QueueName:           random.String("queue-", 10),
						ExpireTimeout:       "24h0m0s",
						RequestedPartitions: 1,
					},
					Msg:  "lease timeout is invalid; cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "MissingExpireTimeout",
					Req: &pb.QueueInfo{
						QueueName:           random.String("queue-", 10),
						LeaseTimeout:        "24h0m0s",
						RequestedPartitions: 1,
					},
					Msg:  "expire timeout is invalid; cannot be empty",
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
					Name: "LeaseTimeoutMaxLength",
					Req: &pb.QueueInfo{
						QueueName:    "LeaseTimeoutMaxLength",
						LeaseTimeout: random.String("", 2_001),
					},
					Msg:  "lease timeout is invalid; cannot be greater than '15' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "LeaseTimeoutTooLong",
					Req: &pb.QueueInfo{
						QueueName:           "LeaseTimeoutTooLong",
						LeaseTimeout:        "1h0m0s",
						ExpireTimeout:       "30m0s",
						RequestedPartitions: 1,
					},
					Msg:  "lease timeout is too long; 1h0m0s cannot be greater than the expire timeout 30m0s",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidLeaseTimeout",
					Req: &pb.QueueInfo{
						QueueName:    "InvalidLeaseTimeout",
						LeaseTimeout: "foo",
					},
					Msg:  "lease timeout is invalid; time: invalid duration \"foo\" -  expected format: 8m, 15m or 1h",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidExpireTimeout",
					Req: &pb.QueueInfo{
						QueueName:     "InvalidExpireTimeout",
						ExpireTimeout: "foo",
					},
					Msg:  "expire timeout is invalid; time: invalid duration \"foo\" - expected format: 60m, 2h or 24h",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ExpireTimeoutMaxLength",
					Req: &pb.QueueInfo{
						QueueName:     "ExpireTimeoutMaxLength",
						ExpireTimeout: random.String("", 2_001),
					},
					Msg:  "expire timeout is invalid; cannot be greater than '15' characters",
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
						QueueName:     "ExpireTimeoutMaxLength",
						ExpireTimeout: random.String("", 2_001),
					},
					Msg:  "expire timeout is invalid; cannot be greater than '15' characters",
					Code: duh.CodeBadRequest,
				},
				{
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
					Name: "QueueWhiteSpace",
					Req: &pb.QueueInfo{
						LeaseTimeout:        LeaseTimeout,
						ExpireTimeout:       ExpireTimeout,
						QueueName:           "Friendship is Magic",
						RequestedPartitions: 1,
					},
					Msg:  "queue name is invalid; 'Friendship is Magic' cannot contain whitespace",
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
					Name: "LeaseTimeoutTooLong",
					Req: &pb.QueueInfo{
						QueueName:     queueName,
						LeaseTimeout:  "1h0m0s",
						ExpireTimeout: "30m0s",
					},
					Msg:  "lease timeout is too long; 1h0m0s cannot be greater than the expire timeout 30m0s",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidLeaseTimeout",
					Req: &pb.QueueInfo{
						QueueName:    queueName,
						LeaseTimeout: "foo",
					},
					Msg:  "lease timeout is invalid; time: invalid duration \"foo\" -  expected format: 8m, 15m or 1h",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidExpireTimeout",
					Req: &pb.QueueInfo{
						QueueName:     queueName,
						ExpireTimeout: "foo",
					},
					Msg:  "expire timeout is invalid; time: invalid duration \"foo\" - expected format: 60m, 2h or 24h",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ExpireTimeoutMaxLength",
					Req: &pb.QueueInfo{
						QueueName:     "ExpireTimeoutMaxLength",
						ExpireTimeout: random.String("", 2_001),
					},
					Msg:  "expire timeout is invalid; cannot be greater than '15' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "DeadQueueMaxLength",
					Req: &pb.QueueInfo{
						QueueName:     "ExpireTimeoutMaxLength",
						ExpireTimeout: random.String("", 2_001),
					},
					Msg:  "expire timeout is invalid; cannot be greater than '15' characters",
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
					Name: "InvalidMinAttempts",
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
						QueueName:   queueName,
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
				{
					Name: "QueueWhiteSpace",
					Req: &pb.QueuesDeleteRequest{
						QueueName: "Friendship is Magic",
					},
					Msg:  "queue name is invalid; 'Friendship is Magic' cannot contain whitespace",
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
