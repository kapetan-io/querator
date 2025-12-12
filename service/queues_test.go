package service_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/duh-rpc/duh-go/retry"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestQueuesStorage(t *testing.T) {
	badger := badgerTestSetup{Dir: t.TempDir()}
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
		// {
		// 	Name: "SurrealDB",
		// },
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testQueues(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueues(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("CRUD", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		t.Run("Create", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			var deadQueueName = queueName + "-dead"
			now := clock.Now().UTC()

			// Create DLQ first
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           deadQueueName,
				LeaseTimeout:        "1m",
				ExpireTimeout:       "10m",
				RequestedPartitions: 1,
			}))

			// Create queue with DLQ reference
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				DeadQueue:           deadQueueName,
				Reference:           "CreateTestRef",
				LeaseTimeout:        "1m",
				ExpireTimeout:       "10m",
				MaxAttempts:         10,
				RequestedPartitions: 1,
			}))

			var list pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
				Pivot: queueName,
				Limit: 1,
			}))
			require.Equal(t, 1, len(list.Items))
			assert.Equal(t, queueName, list.Items[0].QueueName)
			assert.NotEmpty(t, list.Items[0].CreatedAt)
			assert.True(t, now.Before(list.Items[0].CreatedAt.AsTime()))
			assert.NotEmpty(t, list.Items[0].UpdatedAt)
			assert.True(t, now.Before(list.Items[0].UpdatedAt.AsTime()))
			assert.Equal(t, int32(10), list.Items[0].MaxAttempts)
			assert.Equal(t, "1m0s", list.Items[0].LeaseTimeout)
			assert.Equal(t, "10m0s", list.Items[0].ExpireTimeout)
			assert.Equal(t, deadQueueName, list.Items[0].DeadQueue)
			assert.Equal(t, "CreateTestRef", list.Items[0].Reference)
		})

		now := clock.Now().UTC()
		queues := createRandomQueues(t, ctx, c, 50)

		t.Run("GetByPartition", func(t *testing.T) {
			var list pb.QueuesListResponse
			l := queues[10]
			require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
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
				require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
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
				require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
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
				require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
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
			})
			t.Run("Reference", func(t *testing.T) {
				l := queues[33]

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName: l.QueueName,
					Reference: "SomethingElse",
				}))

				var list pb.QueuesListResponse
				require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
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
				require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
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
			require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
				Pivot: l.QueueName,
				Limit: 1,
			}))
			r := list.Items[0]
			assert.Equal(t, l.QueueName, r.QueueName)

			// Attempt to delete
			require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: l.QueueName}))

			// Partition should no longer exist
			require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
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
			t.Run("Validation", func(t *testing.T) {
				t.Run("NonExistentDLQ", func(t *testing.T) {
					queueName := random.String("queue-", 10)
					err := c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						DeadQueue:           "nonexistent-dlq",
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "does not exist")
				})

				t.Run("SelfReference", func(t *testing.T) {
					queueName := random.String("queue-", 10)
					err := c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						DeadQueue:           queueName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "cannot reference itself")
				})

				t.Run("DLQHasDLQ", func(t *testing.T) {
					queueC := random.String("queue-c-", 10)
					queueB := random.String("queue-b-", 10)
					queueA := random.String("queue-a-", 10)

					// Create queue C (no DLQ)
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueC,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					// Create queue B with DeadQueue = C
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueB,
						DeadQueue:           queueC,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					// Attempt to create queue A with DeadQueue = B (should fail)
					err := c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueA,
						DeadQueue:           queueB,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "cannot have its own dead_queue")
				})

				t.Run("ValidDLQ", func(t *testing.T) {
					dlqName := random.String("dlq-", 10)
					queueName := random.String("queue-", 10)

					// Create DLQ queue (no DLQ of its own)
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					// Create source queue with valid DeadQueue reference
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						DeadQueue:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					// Verify queue was created with correct DLQ
					var list pb.QueuesListResponse
					require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
						Pivot: queueName,
						Limit: 1,
					}))
					require.Equal(t, 1, len(list.Items))
					assert.Equal(t, dlqName, list.Items[0].DeadQueue)
				})
			})
		})

	})

	t.Run("List", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
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
			require.NoError(t, c.QueuesList(ctx, &more, &querator.ListOptions{Limit: 20_000}))
			assert.Equal(t, 50, len(more.Items))

			compareQueueInfo(t, queues[0], more.Items[0])
			compareQueueInfo(t, queues[50-1], more.Items[len(more.Items)-1])
		})
		t.Run("LessThanAvailable", func(t *testing.T) {
			var less pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &less, &querator.ListOptions{Limit: 30}))
			assert.Equal(t, 30, len(less.Items))

			compareQueueInfo(t, queues[0], less.Items[0])
			compareQueueInfo(t, queues[30-1], less.Items[len(less.Items)-1])
		})
		t.Run("GetOne", func(t *testing.T) {
			var one pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &one, &querator.ListOptions{Pivot: queues[20].QueueName, Limit: 1}))
			assert.Equal(t, 1, len(one.Items))

			compareQueueInfo(t, queues[20], one.Items[len(one.Items)-1])
		})
		t.Run("First", func(t *testing.T) {
			var first pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &first, &querator.ListOptions{Limit: 1}))
			assert.Equal(t, 1, len(first.Items))

			compareQueueInfo(t, queues[0], first.Items[len(first.Items)-1])
		})
		t.Run("WithPivot", func(t *testing.T) {
			name := queues[20].QueueName
			var pivot pb.QueuesListResponse
			err := c.QueuesList(ctx, &pivot, &querator.ListOptions{Pivot: name, Limit: 10})
			require.NoError(t, err)

			assert.Equal(t, 10, len(pivot.Items))
			compareQueueInfo(t, queues[20], pivot.Items[0])
			for i := range pivot.Items {
				compareQueueInfo(t, queues[i+20], pivot.Items[i])
			}
			t.Run("PageThroughItems", func(t *testing.T) {
				name := queues[0].QueueName
				var page pb.QueuesListResponse
				err := c.QueuesList(ctx, &page, &querator.ListOptions{Pivot: name, Limit: 10})
				require.NoError(t, err)
				compareQueueInfo(t, queues[0], page.Items[0])
				compareQueueInfo(t, queues[9], page.Items[9])

				t.Run("PageIncludesPivot", func(t *testing.T) {
					name = queues[9].QueueName
					err = c.QueuesList(ctx, &page, &querator.ListOptions{Pivot: name, Limit: 10})
					require.NoError(t, err)
					compareQueueInfo(t, queues[9], page.Items[0])
					compareQueueInfo(t, queues[18], page.Items[9])
				})
			})
		})

		t.Run("PivotNotFound", func(t *testing.T) {
			var page pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &page,
				&querator.ListOptions{Pivot: "pueue-00000", Limit: 1}))

			// Should return the first queue in the list
			assert.Equal(t, 1, len(page.Items))

			assert.Equal(t, queues[0].QueueName, page.Items[0].QueueName)
		})
	})

	t.Run("Errors", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		var queueName = random.String("queue-", 10)
		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:           queueName,
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
				Opts *querator.ListOptions
				Msg  string
				Code int
			}{
				{
					Name: "InvalidLimit",
					Opts: &querator.ListOptions{
						Limit: -1,
					},
					Msg:  "limit is invalid; limit cannot be negative",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidLimit",
					Opts: &querator.ListOptions{
						Limit: math.MaxInt32,
					},
					Msg:  "limit is invalid; cannot be greater than 65536",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidPivot",
					Opts: &querator.ListOptions{
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
		t.Run("QueuesInfo", func(t *testing.T) {
			// Test successful info retrieval first
			t.Run("Success", func(t *testing.T) {
				var resp pb.QueueInfo
				err := c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &resp)
				require.NoError(t, err)

				// Verify that we got valid queue info back
				assert.Equal(t, queueName, resp.QueueName)
				assert.Equal(t, "", resp.DeadQueue)
				assert.Equal(t, "CreateTestRef", resp.Reference)
				assert.Equal(t, "1m0s", resp.LeaseTimeout)
				assert.Equal(t, "10m0s", resp.ExpireTimeout)
				assert.Equal(t, int32(10), resp.MaxAttempts)
				assert.Equal(t, int32(0), resp.RequestedPartitions) // partitions start at 0
				assert.NotNil(t, resp.CreatedAt)
				assert.NotNil(t, resp.UpdatedAt)
			})

			// Test validation errors
			for _, test := range []struct {
				Name string
				Req  *pb.QueuesInfoRequest
				Msg  string
				Code int
			}{
				{
					Name: "EmptyQueueName",
					Req:  &pb.QueuesInfoRequest{QueueName: ""},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "WhitespaceQueueName",
					Req:  &pb.QueuesInfoRequest{QueueName: "   "},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "QueueNotFound",
					Req:  &pb.QueuesInfoRequest{QueueName: "non-existent-queue"},
					Msg:  "queue does not exist; no such queue named 'non-existent-queue'",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					var resp pb.QueueInfo
					err := c.QueuesInfo(ctx, test.Req, &resp)
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

	t.Run("UpdateBehavior", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d, c, ctx := newDaemon(t, 60*clock.Second, svc.Config{
			StorageConfig: setup(),
			Clock:         now,
		})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		t.Run("MaxAttempts", func(t *testing.T) {
			// Baseline test to verify lifecycle works (mirrors MaxAttemptsWithDLQ)
			t.Run("BaselineWithDLQ", func(t *testing.T) {
				dlqName := random.String("dlq-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqName,
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})

				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
					MaxAttempts:         2,
					DeadQueue:           dlqName,
				})

				ref := random.String("ref-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// First lease (Attempts=1)
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))
				require.Len(t, lease.Items, 1)
				assert.Equal(t, int32(1), lease.Items[0].Attempts)

				// Advance time, wait for re-queue
				now.Advance(2 * clock.Minute)
				err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					item := findInStorageList(ref, &resp)
					if item == nil {
						return fmt.Errorf("item not found")
					}
					if item.IsLeased {
						return fmt.Errorf("expected re-queue")
					}
					return nil
				})
				require.NoError(t, err)

				// Second lease (Attempts=2)
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))
				require.Len(t, lease.Items, 1)
				assert.Equal(t, int32(2), lease.Items[0].Attempts)

				// Advance time, wait for DLQ routing
				now.Advance(2 * clock.Minute)
				err = retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					if findInStorageList(ref, &resp) != nil {
						return fmt.Errorf("expected item removed from main queue")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify in DLQ
				var dlqResp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
				require.NotNil(t, findInStorageList(ref, &dlqResp))
			})

			t.Run("DecreaseAffectsExistingItems", func(t *testing.T) {
				// Create DLQ first
				dlqName := random.String("dlq-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqName,
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})

				// Create main queue with MaxAttempts=5
				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
					MaxAttempts:         5,
					DeadQueue:           dlqName,
				})

				// Produce an item
				ref := random.String("ref-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// First lease: Attempts becomes 1
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))

				require.Len(t, lease.Items, 1)
				leased := lease.Items[0]
				assert.Equal(t, ref, leased.Reference)
				assert.Equal(t, int32(1), leased.Attempts)

				// Update queue to MaxAttempts=2 (decrease from 5)
				// NOTE: Must include DeadQueue to preserve DLQ config since Update replaces fields
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:   queueName,
					MaxAttempts: 2,
					DeadQueue:   dlqName,
				}))

				// Advance time past LeaseTimeout, wait for re-queue
				now.Advance(2 * clock.Minute)

				err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					item := findInStorageList(ref, &resp)
					if item == nil {
						return fmt.Errorf("item not found in storage")
					}
					if item.IsLeased {
						return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify item is still in main queue with Attempts = 1
				var resp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				item := findInStorageList(ref, &resp)
				require.NotNil(t, item)
				assert.False(t, item.IsLeased)
				assert.Equal(t, int32(1), item.Attempts)

				// Second lease: Attempts becomes 2
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))

				require.Len(t, lease.Items, 1)
				leased = lease.Items[0]
				assert.Equal(t, ref, leased.Reference)
				assert.Equal(t, int32(2), leased.Attempts)

				// Advance time past LeaseTimeout again
				now.Advance(2 * clock.Minute)

				// Wait for item to be removed from main queue (routed to DLQ)
				err = retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					if findInStorageList(ref, &resp) != nil {
						return fmt.Errorf("expected item to be removed from main queue")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify item is NOT in main queue
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				require.Nil(t, findInStorageList(ref, &resp))

				// Verify item IS in DLQ (2 >= MaxAttempts of 2)
				var dlqResp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
				dlqItem := findInStorageList(ref, &dlqResp)
				require.NotNil(t, dlqItem)
				assert.Equal(t, ref, dlqItem.Reference)
			})

			t.Run("IncreaseAllowsMoreAttempts", func(t *testing.T) {
				// Create DLQ first
				dlqName := random.String("dlq-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqName,
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})

				// Create main queue with MaxAttempts=2
				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
					MaxAttempts:         2,
					DeadQueue:           dlqName,
				})

				// Produce an item
				ref := random.String("ref-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// First lease: Attempts becomes 1
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))

				require.Len(t, lease.Items, 1)
				leased := lease.Items[0]
				assert.Equal(t, ref, leased.Reference)
				assert.Equal(t, int32(1), leased.Attempts)

				// Advance time, let lease timeout, wait for re-queue
				now.Advance(2 * clock.Minute)

				err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					item := findInStorageList(ref, &resp)
					if item == nil {
						return fmt.Errorf("item not found in storage")
					}
					if item.IsLeased {
						return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
					}
					return nil
				})
				require.NoError(t, err)

				// Second lease: Attempts becomes 2
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))

				require.Len(t, lease.Items, 1)
				leased = lease.Items[0]
				assert.Equal(t, ref, leased.Reference)
				assert.Equal(t, int32(2), leased.Attempts)

				// Update queue to MaxAttempts=5 (increase from 2)
				// NOTE: Must include DeadQueue to preserve DLQ config since Update replaces fields
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:   queueName,
					MaxAttempts: 5,
					DeadQueue:   dlqName,
				}))

				// Advance time, let lease timeout
				now.Advance(2 * clock.Minute)

				// Wait for item to be re-queued (NOT routed to DLQ)
				err = retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					item := findInStorageList(ref, &resp)
					if item == nil {
						return fmt.Errorf("item not found in storage")
					}
					if item.IsLeased {
						return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify item is still in main queue (not in DLQ)
				var resp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				item := findInStorageList(ref, &resp)
				require.NotNil(t, item)
				assert.False(t, item.IsLeased)
				assert.Equal(t, int32(2), item.Attempts)

				// Verify item can be leased again (Attempts becomes 3)
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))

				require.Len(t, lease.Items, 1)
				leased = lease.Items[0]
				assert.Equal(t, ref, leased.Reference)
				assert.Equal(t, int32(3), leased.Attempts)

				// Verify item is NOT in DLQ
				var dlqResp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
				assert.Nil(t, findInStorageList(ref, &dlqResp))
			})

			t.Run("SetToUnlimited", func(t *testing.T) {
				// Create DLQ first
				dlqName := random.String("dlq-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqName,
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})

				// Create main queue with MaxAttempts=2
				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
					MaxAttempts:         2,
					DeadQueue:           dlqName,
				})

				// Produce an item
				ref := random.String("ref-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// First lease: Attempts becomes 1
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))

				require.Len(t, lease.Items, 1)
				leased := lease.Items[0]
				assert.Equal(t, ref, leased.Reference)
				assert.Equal(t, int32(1), leased.Attempts)

				// Update queue to MaxAttempts=0 (unlimited)
				// NOTE: Must include DeadQueue to preserve DLQ config since Update replaces fields
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:   queueName,
					MaxAttempts: 0,
					DeadQueue:   dlqName,
				}))

				// Loop 5+ times: lease, advance time, verify item re-queues
				const numCycles = 5
				for cycle := 2; cycle <= numCycles+1; cycle++ {
					// Advance time past LeaseTimeout
					now.Advance(2 * clock.Minute)

					// Wait for item to be re-queued
					err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
						var resp pb.StorageItemsListResponse
						if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
							return err
						}
						item := findInStorageList(ref, &resp)
						if item == nil {
							return fmt.Errorf("item not found in storage")
						}
						if item.IsLeased {
							return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
						}
						return nil
					})
					require.NoError(t, err)

					// Lease the item again
					require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
						ClientId:       random.String("client-", 10),
						RequestTimeout: "5s",
						QueueName:      queueName,
						BatchSize:      1,
					}, &lease))

					require.Len(t, lease.Items, 1)
					leased = lease.Items[0]
					assert.Equal(t, ref, leased.Reference)
					assert.Equal(t, int32(cycle), leased.Attempts)
				}

				// Verify item never routes to DLQ
				var dlqResp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
				assert.Nil(t, findInStorageList(ref, &dlqResp))

				// Verify item is still in main queue
				var resp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				item := findInStorageList(ref, &resp)
				require.NotNil(t, item)
			})
		})

		t.Run("LeaseTimeout", func(t *testing.T) {
			t.Run("NewLeasesUseUpdatedTimeout", func(t *testing.T) {
				// Create queue with LeaseTimeout="5m"
				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "5m0s",
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})

				// Produce an item
				ref := random.String("ref-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// Update queue to LeaseTimeout="1m"
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:    queueName,
					LeaseTimeout: "1m0s",
				}))

				// Lease item
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))

				require.Len(t, lease.Items, 1)
				leased := lease.Items[0]
				assert.Equal(t, ref, leased.Reference)

				// Verify LeaseDeadline is ~1 minute from now (not 5)
				expectedDeadline := now.Now().Add(clock.Minute)
				actualDeadline := leased.LeaseDeadline.AsTime()
				assert.True(t, actualDeadline.Before(expectedDeadline.Add(5*clock.Second)))
				assert.True(t, actualDeadline.After(expectedDeadline.Add(-5*clock.Second)))

				// Advance time 2 minutes - past 1m LeaseTimeout
				now.Advance(2 * clock.Minute)

				// Wait for item to be re-queued (lease expired at ~1m mark)
				err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					item := findInStorageList(ref, &resp)
					if item == nil {
						return fmt.Errorf("item not found in storage")
					}
					if item.IsLeased {
						return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify item is no longer leased
				var resp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				item := findInStorageList(ref, &resp)
				require.NotNil(t, item)
				assert.False(t, item.IsLeased)
			})

			t.Run("ExistingLeasesKeepOriginalDeadline", func(t *testing.T) {
				// Create queue with LeaseTimeout="5m"
				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "5m0s",
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})

				// Produce an item
				ref := random.String("ref-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// Lease item (LeaseDeadline set to ~5m from now)
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))

				require.Len(t, lease.Items, 1)
				leased := lease.Items[0]
				assert.Equal(t, ref, leased.Reference)

				// Verify LeaseDeadline is ~5 minutes from now
				expectedDeadline := now.Now().Add(5 * clock.Minute)
				actualDeadline := leased.LeaseDeadline.AsTime()
				assert.True(t, actualDeadline.Before(expectedDeadline.Add(5*clock.Second)))
				assert.True(t, actualDeadline.After(expectedDeadline.Add(-5*clock.Second)))

				// Update queue to LeaseTimeout="1m"
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:    queueName,
					LeaseTimeout: "1m0s",
				}))

				// Advance time 2 minutes - past new 1m timeout, but before original 5m deadline
				now.Advance(2 * clock.Minute)

				// Verify item is STILL leased (original 5m deadline not yet passed)
				var resp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				item := findInStorageList(ref, &resp)
				require.NotNil(t, item)
				assert.True(t, item.IsLeased)

				// Advance time 4 more minutes (total 6m, past original 5m deadline)
				now.Advance(4 * clock.Minute)

				// Wait for item to be re-queued
				err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					item := findInStorageList(ref, &resp)
					if item == nil {
						return fmt.Errorf("item not found in storage")
					}
					if item.IsLeased {
						return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify item is no longer leased
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				item = findInStorageList(ref, &resp)
				require.NotNil(t, item)
				assert.False(t, item.IsLeased)
			})
		})

		t.Run("ExpireTimeout", func(t *testing.T) {
			t.Run("NewItemsUseUpdatedTimeout", func(t *testing.T) {
				// Create queue with ExpireTimeout="10m"
				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       "10m0s",
					RequestedPartitions: 1,
				})

				// Update queue to ExpireTimeout="2m"
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:     queueName,
					ExpireTimeout: "2m0s",
				}))

				// Produce item AFTER the update
				ref := random.String("ref-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// Verify item's ExpireDeadline is ~2 minutes from now (not 10)
				var resp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				item := findInStorageList(ref, &resp)
				require.NotNil(t, item)

				expectedDeadline := now.Now().Add(2 * clock.Minute)
				actualDeadline := item.ExpireDeadline.AsTime()
				assert.True(t, actualDeadline.Before(expectedDeadline.Add(5*clock.Second)))
				assert.True(t, actualDeadline.After(expectedDeadline.Add(-5*clock.Second)))
			})

			t.Run("ExistingItemsUnaffected", func(t *testing.T) {
				// Create queue with ExpireTimeout="10m"
				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       "10m0s",
					RequestedPartitions: 1,
				})

				// Produce item (ExpireDeadline set to ~10m)
				ref := random.String("ref-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// Record the original ExpireDeadline
				var resp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				item := findInStorageList(ref, &resp)
				require.NotNil(t, item)
				originalDeadline := item.ExpireDeadline.AsTime()

				// Update queue to ExpireTimeout="2m"
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:     queueName,
					ExpireTimeout: "2m0s",
				}))

				// Verify item's ExpireDeadline is UNCHANGED (still ~10m from creation)
				require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
				item = findInStorageList(ref, &resp)
				require.NotNil(t, item)
				assert.Equal(t, originalDeadline, item.ExpireDeadline.AsTime())
			})
		})

		t.Run("DeadQueue", func(t *testing.T) {
			t.Run("AddDLQAfterCreation", func(t *testing.T) {
				// Create DLQ queue
				dlqName := random.String("dlq-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqName,
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})

				// Create main queue WITHOUT DeadQueue, MaxAttempts=2
				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
					MaxAttempts:         2,
				})

				// Produce item
				ref := random.String("ref-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// Update main queue to set DeadQueue
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:   queueName,
					MaxAttempts: 2,
					DeadQueue:   dlqName,
				}))

				// First lease: Attempts becomes 1
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))
				require.Len(t, lease.Items, 1)
				assert.Equal(t, int32(1), lease.Items[0].Attempts)

				// Advance time past LeaseTimeout
				now.Advance(2 * clock.Minute)

				// Wait for item to be re-queued
				err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					item := findInStorageList(ref, &resp)
					if item == nil {
						return fmt.Errorf("item not found in storage")
					}
					if item.IsLeased {
						return fmt.Errorf("expected item to be re-queued (IsLeased=false)")
					}
					return nil
				})
				require.NoError(t, err)

				// Second lease: Attempts becomes 2
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))
				require.Len(t, lease.Items, 1)
				assert.Equal(t, int32(2), lease.Items[0].Attempts)

				// Advance time past LeaseTimeout
				now.Advance(2 * clock.Minute)

				// Wait for item to be removed from main queue (routed to DLQ)
				err = retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					if findInStorageList(ref, &resp) != nil {
						return fmt.Errorf("expected item to be removed from main queue")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify item IS in DLQ
				var dlqResp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
				dlqItem := findInStorageList(ref, &dlqResp)
				require.NotNil(t, dlqItem)
				assert.Equal(t, ref, dlqItem.Reference)
			})

			t.Run("ChangeDLQRoutesNewFailures", func(t *testing.T) {
				// Create DLQ-A and DLQ-B queues
				dlqAName := random.String("dlq-a-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqAName,
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})

				dlqBName := random.String("dlq-b-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqBName,
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})

				// Create main queue with DeadQueue=DLQ-A, MaxAttempts=2
				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
					MaxAttempts:         2,
					DeadQueue:           dlqAName,
				})

				// Produce item-1
				ref1 := random.String("ref1-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref1,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// Exhaust item-1 attempts
				for i := 0; i < 2; i++ {
					var lease pb.QueueLeaseResponse
					require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
						ClientId:       random.String("client-", 10),
						RequestTimeout: "5s",
						QueueName:      queueName,
						BatchSize:      1,
					}, &lease))
					require.Len(t, lease.Items, 1)

					now.Advance(2 * clock.Minute)

					if i < 1 {
						// Wait for re-queue
						err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, _ int) error {
							var resp pb.StorageItemsListResponse
							if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
								return err
							}
							item := findInStorageList(ref1, &resp)
							if item == nil {
								return fmt.Errorf("item not found")
							}
							if item.IsLeased {
								return fmt.Errorf("expected item to be re-queued")
							}
							return nil
						})
						require.NoError(t, err)
					}
				}

				// Wait for item-1 to be removed from main queue
				err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					if findInStorageList(ref1, &resp) != nil {
						return fmt.Errorf("expected item to be removed")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify item-1 is in DLQ-A
				var dlqAResp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqAName, 0, &dlqAResp, nil))
				require.NotNil(t, findInStorageList(ref1, &dlqAResp))

				// Update main queue to set DeadQueue=DLQ-B
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:   queueName,
					MaxAttempts: 2,
					DeadQueue:   dlqBName,
				}))

				// Produce item-2
				ref2 := random.String("ref2-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref2,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// Exhaust item-2 attempts
				for i := 0; i < 2; i++ {
					var lease pb.QueueLeaseResponse
					require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
						ClientId:       random.String("client-", 10),
						RequestTimeout: "5s",
						QueueName:      queueName,
						BatchSize:      1,
					}, &lease))
					require.Len(t, lease.Items, 1)

					now.Advance(2 * clock.Minute)

					if i < 1 {
						// Wait for re-queue
						err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, _ int) error {
							var resp pb.StorageItemsListResponse
							if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
								return err
							}
							item := findInStorageList(ref2, &resp)
							if item == nil {
								return fmt.Errorf("item not found")
							}
							if item.IsLeased {
								return fmt.Errorf("expected item to be re-queued")
							}
							return nil
						})
						require.NoError(t, err)
					}
				}

				// Wait for item-2 to be removed from main queue
				err = retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					if findInStorageList(ref2, &resp) != nil {
						return fmt.Errorf("expected item to be removed")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify item-2 is in DLQ-B (not DLQ-A)
				var dlqBResp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqBName, 0, &dlqBResp, nil))
				require.NotNil(t, findInStorageList(ref2, &dlqBResp))

				require.NoError(t, c.StorageItemsList(ctx, dlqAName, 0, &dlqAResp, nil))
				assert.Nil(t, findInStorageList(ref2, &dlqAResp))
			})

			t.Run("RemoveDLQDeletesFailedItems", func(t *testing.T) {
				// Create DLQ queue
				dlqName := random.String("dlq-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           dlqName,
					LeaseTimeout:        LeaseTimeout,
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
				})

				// Create main queue with DeadQueue, MaxAttempts=2
				queueName := random.String("queue-", 10)
				createQueueAndWait(t, ctx, c, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m0s",
					ExpireTimeout:       ExpireTimeout,
					RequestedPartitions: 1,
					MaxAttempts:         2,
					DeadQueue:           dlqName,
				})

				// Update main queue to remove DeadQueue
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueueInfo{
					QueueName:   queueName,
					MaxAttempts: 2,
					DeadQueue:   "",
				}))

				// Produce item
				ref := random.String("ref-", 10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items: []*pb.QueueProduceItem{
						{
							Reference: ref,
							Encoding:  "test-encoding",
							Kind:      "test-kind",
							Bytes:     []byte("test payload"),
						},
					},
				}))

				// Exhaust attempts
				for i := 0; i < 2; i++ {
					var lease pb.QueueLeaseResponse
					require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
						ClientId:       random.String("client-", 10),
						RequestTimeout: "5s",
						QueueName:      queueName,
						BatchSize:      1,
					}, &lease))
					require.Len(t, lease.Items, 1)

					now.Advance(2 * clock.Minute)

					if i < 1 {
						// Wait for re-queue
						err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, _ int) error {
							var resp pb.StorageItemsListResponse
							if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
								return err
							}
							item := findInStorageList(ref, &resp)
							if item == nil {
								return fmt.Errorf("item not found")
							}
							if item.IsLeased {
								return fmt.Errorf("expected item to be re-queued")
							}
							return nil
						})
						require.NoError(t, err)
					}
				}

				// Wait for item to be removed from main queue
				err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
					var resp pb.StorageItemsListResponse
					if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
						return err
					}
					if findInStorageList(ref, &resp) != nil {
						return fmt.Errorf("expected item to be removed")
					}
					return nil
				})
				require.NoError(t, err)

				// Verify item is NOT in DLQ (was deleted)
				var dlqResp pb.StorageItemsListResponse
				require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &dlqResp, nil))
				assert.Nil(t, findInStorageList(ref, &dlqResp))
			})
		})
	})
}
