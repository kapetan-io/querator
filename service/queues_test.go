package service_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	"github.com/duh-rpc/duh-go/retry"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/placement"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/encoding/protojson"
)

type countedQueues struct {
	store.Queues
	lists atomic.Int32
}

func (c *countedQueues) List(ctx context.Context, items *[]types.QueueInfo, opts types.ListOptions) error {
	c.lists.Add(1)
	return c.Queues.List(ctx, items, opts)
}

func TestQueuesStorage(t *testing.T) {
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
			testQueues(t, tc.Setup, tc.TearDown)
		})
	}
}

func testQueues(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("CRUD", func(t *testing.T) {
		conf := setup()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

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
			// /queues.list reads the queue back from storage, proving the storage name is saved
			require.Len(t, list.Items[0].Partitions, 1)
			assert.Equal(t, int32(0), list.Items[0].Partitions[0].Partition)
			assert.Equal(t, conf.PartitionStorage[0].Name, list.Items[0].Partitions[0].StorageName)
			assert.Equal(t, "CreateTestRef", list.Items[0].Reference)
		})

		t.Run("PartitionState", func(t *testing.T) {
			queueName := random.String("queue-", 10)
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m",
				ExpireTimeout:       "10m",
				RequestedPartitions: 3,
			}))

			var info pb.QueueInfo
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			require.Len(t, info.Partitions, 3)
			for _, p := range info.Partitions {
				assert.Equal(t, "available", p.State)
			}

			var list pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{Pivot: queueName, Limit: 1}))
			require.Len(t, list.Items, 1)
			require.Len(t, list.Items[0].Partitions, 3)
			for _, p := range list.Items[0].Partitions {
				assert.Equal(t, "available", p.State)
			}
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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:   l.QueueName,
					MaxAttempts: new(int32(l.MaxAttempts + 1)),
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

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
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

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
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

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName: l.QueueName,
					Reference: new("SomethingElse"),
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

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:     l.QueueName,
					Reference:     new("FriendshipIsMagic"),
					MaxAttempts:   new(int32(l.MaxAttempts + 1)),
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

			t.Run("InfoShowsUpdatedSettings", func(t *testing.T) {
				queueName := random.String("queue-", 10)
				deadName := queueName + "-dead"

				require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
					QueueName:           deadName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "10m",
					RequestedPartitions: 1,
				}))
				require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "10m",
					RequestedPartitions: 1,
				}))

				var before pb.QueueInfo
				require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &before))

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:           queueName,
					LeaseTimeout:        "2m",
					ExpireTimeout:       "20m",
					MaxAttempts:         new(int32(5)),
					DeadQueue:           new(deadName),
					Reference:           new("updated"),
					RequestedPartitions: 2,
				}))

				var info pb.QueueInfo
				require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
				assert.Equal(t, "2m0s", info.LeaseTimeout)
				assert.Equal(t, "20m0s", info.ExpireTimeout)
				assert.Equal(t, int32(5), info.MaxAttempts)
				assert.Equal(t, deadName, info.DeadQueue)
				assert.Equal(t, "updated", info.Reference)
				assert.Equal(t, int32(2), info.RequestedPartitions)
				assert.True(t, before.UpdatedAt.AsTime().Before(info.UpdatedAt.AsTime()))

				// Fields an update never carries keep the values the queue was created with
				assert.Equal(t, before.Namespace, info.Namespace)
				assert.Equal(t, before.CreatedAt.AsTime(), info.CreatedAt.AsTime())
				assert.Len(t, info.Partitions, len(before.Partitions))

				var list pb.QueuesListResponse
				require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{Pivot: queueName, Limit: 1}))
				require.Len(t, list.Items, 1)
				stored := list.Items[0]
				assert.Equal(t, info.LeaseTimeout, stored.LeaseTimeout)
				assert.Equal(t, info.ExpireTimeout, stored.ExpireTimeout)
				assert.Equal(t, info.MaxAttempts, stored.MaxAttempts)
				assert.Equal(t, info.DeadQueue, stored.DeadQueue)
				assert.Equal(t, info.Reference, stored.Reference)
				assert.Equal(t, info.RequestedPartitions, stored.RequestedPartitions)
				assert.Equal(t, info.UpdatedAt.AsTime(), stored.UpdatedAt.AsTime())
			})

			t.Run("InfoDuringUpdate", func(t *testing.T) {
				const updates = 20
				queueName := random.String("queue-", 10)
				require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "10m",
					RequestedPartitions: 1,
				}))

				updateWhileReadingInfo(t, ctx, c, queueName, updates)

				var info pb.QueueInfo
				require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
				assert.Equal(t, fmt.Sprintf("reference-%d", updates-1), info.Reference)
			})

			t.Run("PartialUpdateLeasesWithKeptLeaseTimeout", func(t *testing.T) {
				queueName := random.String("queue-", 10)
				require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "10m",
					RequestedPartitions: 1,
				}))

				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName: queueName,
					Reference: new("only-reference"),
				}))

				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items:          produceRandomItems(1),
				}))
				// The update carries no lease_timeout, so a lease must still last the 1m the queue was created with
				start := clock.Now().UTC()
				var lease pb.QueueLeaseResponse
				require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "5s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease))
				require.Len(t, lease.Items, 1)
				assert.True(t, lease.Items[0].LeaseDeadline.AsTime().After(start.Add(30*clock.Second)))
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

		t.Run("NegativeTimeouts", func(t *testing.T) {
			refused := func(t *testing.T, err error, contains string) {
				require.Error(t, err)
				var duhErr duh.Error
				require.True(t, errors.As(err, &duhErr))
				assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
				assert.Contains(t, duhErr.Message(), contains)
			}

			for _, tc := range []struct {
				Name, Lease, Expire, Message string
			}{
				{"NegativeLease", "-5m", "10m", "lease timeout is invalid; cannot be negative"},
				{"NegativeExpire", "1m", "-5m", "expire timeout is invalid; cannot be negative"},
				{"BothNegative", "-10m", "-5m", "lease timeout is invalid; cannot be negative"},
			} {
				t.Run("Create"+tc.Name, func(t *testing.T) {
					queueName := random.String("queue-", 10)
					refused(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						LeaseTimeout:        tc.Lease,
						ExpireTimeout:       tc.Expire,
						RequestedPartitions: 1,
					}), tc.Message)

					var info pb.QueueInfo
					require.Error(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
				})

				t.Run("Update"+tc.Name, func(t *testing.T) {
					queueName := random.String("queue-", 10)
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						LeaseTimeout:        "1m",
						ExpireTimeout:       "10m",
						RequestedPartitions: 1,
					}))

					refused(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName:     queueName,
						LeaseTimeout:  tc.Lease,
						ExpireTimeout: tc.Expire,
					}), tc.Message)

					var info pb.QueueInfo
					require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
					assert.Equal(t, "1m0s", info.LeaseTimeout)
					assert.Equal(t, "10m0s", info.ExpireTimeout)
				})
			}

			t.Run("UpdateNegativeLeaseOnly", func(t *testing.T) {
				queueName := random.String("queue-", 10)
				require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
					QueueName:           queueName,
					LeaseTimeout:        "1m",
					ExpireTimeout:       "10m",
					RequestedPartitions: 1,
				}))
				refused(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:    queueName,
					LeaseTimeout: "-5m",
				}), "lease timeout is invalid; cannot be negative")

				var info pb.QueueInfo
				require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
				assert.Equal(t, "1m0s", info.LeaseTimeout)
				assert.Equal(t, "10m0s", info.ExpireTimeout)
			})
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
					assert.Contains(t, duhErr.Message(), "already has its own dead_queue configured")
				})

				t.Run("MultiPartitionDLQ", func(t *testing.T) {
					dlqName := random.String("dlq-", 10)
					queueName := random.String("queue-", 10)

					// Create a DLQ with multiple partitions. SourceID dedup is partition-scoped,
					// so a multi-partition DLQ cannot guarantee no duplicates on a retried move (ENG-57).
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 2,
					}))

					// Creating a source queue that references the multi-partition DLQ must be rejected
					err := c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						DeadQueue:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "must have a single partition")
				})

				t.Run("MultiPartitionDLQOnUpdate", func(t *testing.T) {
					dlqName := random.String("dlq-", 10)
					queueName := random.String("queue-", 10)

					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 2,
					}))

					// Create a source queue with no DLQ, then attempt to add the multi-partition DLQ via update
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName: queueName,
						DeadQueue: new(dlqName),
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "must have a single partition")
				})

				t.Run("DLQGainsDLQOnUpdate", func(t *testing.T) {
					queueC := random.String("queue-c-", 10)
					queueB := random.String("queue-b-", 10)
					queueA := random.String("queue-a-", 10)

					for _, name := range []string{queueC, queueB} {
						require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
							QueueName:           name,
							ExpireTimeout:       "10m",
							LeaseTimeout:        "1m",
							RequestedPartitions: 1,
						}))
					}

					// B gains a dead-letter queue of its own after it was loaded, so it can no longer be named as one
					require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName: queueB,
						DeadQueue: new(queueC),
					}))

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
					assert.Contains(t, duhErr.Message(), "already has its own dead_queue configured")
				})

				t.Run("DLQGainsPartitionsOnUpdate", func(t *testing.T) {
					dlqName := random.String("dlq-", 10)
					queueName := random.String("queue-", 10)

					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					// The dead-letter queue gains a second requested partition after it was loaded, so it can no longer be named as one
					require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName:           dlqName,
						RequestedPartitions: 2,
					}))

					err := c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						DeadQueue:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "must have a single partition")
				})

				t.Run("CycleByUpdates", func(t *testing.T) {
					queueA := random.String("queue-a-", 10)
					queueB := random.String("queue-b-", 10)

					for _, name := range []string{queueA, queueB} {
						require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
							QueueName:           name,
							ExpireTimeout:       "10m",
							LeaseTimeout:        "1m",
							RequestedPartitions: 1,
						}))
					}

					require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName: queueA,
						DeadQueue: new(queueB),
					}))

					err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName: queueB,
						DeadQueue: new(queueA),
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "'"+queueA+"' already has its own dead_queue configured")

					var info pb.QueueInfo
					require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueB}, &info))
					assert.Equal(t, "", info.DeadQueue)
				})

				t.Run("CycleByCreateThenUpdate", func(t *testing.T) {
					queueA := random.String("queue-a-", 10)
					queueB := random.String("queue-b-", 10)

					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueA,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueB,
						DeadQueue:           queueA,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName: queueA,
						DeadQueue: new(queueB),
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "'"+queueB+"' already has its own dead_queue configured")

					var info pb.QueueInfo
					require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueA}, &info))
					assert.Equal(t, "", info.DeadQueue)
				})

				t.Run("UpdateGivesDeadQueueItsOwnDeadQueue", func(t *testing.T) {
					queueA := random.String("queue-a-", 10)
					queueB := random.String("queue-b-", 10)
					queueC := random.String("queue-c-", 10)

					for _, name := range []string{queueB, queueC} {
						require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
							QueueName:           name,
							ExpireTimeout:       "10m",
							LeaseTimeout:        "1m",
							RequestedPartitions: 1,
						}))
					}
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueA,
						DeadQueue:           queueB,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					// B is A's dead queue, so giving B a dead queue of its own would form the chain A→B→C
					err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName: queueB,
						DeadQueue: new(queueC),
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "is the dead_queue of '"+queueA+"'")

					var info pb.QueueInfo
					require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueB}, &info))
					assert.Equal(t, "", info.DeadQueue)
				})

				t.Run("UpdateGivesDeadQueueTwoPartitions", func(t *testing.T) {
					dlqName := random.String("dlq-", 10)
					queueName := random.String("queue-", 10)

					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						DeadQueue:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName:           dlqName,
						RequestedPartitions: 2,
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "is the dead_queue of '"+queueName+"'")

					// Re-sending the same dead_queue is allowed, so the source queue stays updatable
					require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName: queueName,
						DeadQueue: new(dlqName),
						Reference: new("still-updatable"),
					}))

					var info pb.QueueInfo
					require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: dlqName}, &info))
					assert.Equal(t, int32(1), info.RequestedPartitions)
				})

				t.Run("DeleteNamedDLQ", func(t *testing.T) {
					dlqName := random.String("dlq-", 10)
					queueName := random.String("queue-", 10)

					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						DeadQueue:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					err := c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: dlqName})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "is the dead_queue of '"+queueName+"'")

					var info pb.QueueInfo
					require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: dlqName}, &info))
					assert.Equal(t, dlqName, info.QueueName)

					require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName: queueName,
						DeadQueue: new(""),
					}))
					require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: dlqName}))
				})

				t.Run("ChainByUpdates", func(t *testing.T) {
					queueA := random.String("queue-a-", 10)
					queueB := random.String("queue-b-", 10)
					queueC := random.String("queue-c-", 10)

					for _, name := range []string{queueA, queueB, queueC} {
						require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
							QueueName:           name,
							ExpireTimeout:       "10m",
							LeaseTimeout:        "1m",
							RequestedPartitions: 1,
						}))
					}

					require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName: queueB,
						DeadQueue: new(queueC),
					}))

					// The tail link is formed first, so pointing A at B would complete the chain A→B→C
					err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
						QueueName: queueA,
						DeadQueue: new(queueB),
					})
					require.Error(t, err)
					var duhErr duh.Error
					require.True(t, errors.As(err, &duhErr))
					assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
					assert.Contains(t, duhErr.Message(), "already has its own dead_queue configured")

					var info pb.QueueInfo
					require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueA}, &info))
					assert.Equal(t, "", info.DeadQueue)
				})

				t.Run("DeleteSourceOfDLQ", func(t *testing.T) {
					dlqName := random.String("dlq-", 10)
					queueName := random.String("queue-", 10)

					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						DeadQueue:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					// Only the named queue is guarded; the queue doing the naming deletes freely, which then frees the dead queue
					require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: queueName}))
					require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: dlqName}))

					var info pb.QueueInfo
					assert.Error(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: dlqName}, &info))
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

				t.Run("MultiPartitionSourceQueue", func(t *testing.T) {
					dlqName := random.String("dlq-", 10)
					queueName := random.String("queue-", 10)

					// Single-partition DLQ
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 1,
					}))

					// The single-partition constraint applies only to the DLQ. A source queue with
					// multiple partitions referencing a single-partition DLQ is valid (ENG-57).
					require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
						QueueName:           queueName,
						DeadQueue:           dlqName,
						ExpireTimeout:       "10m",
						LeaseTimeout:        "1m",
						RequestedPartitions: 8,
					}))

					var list pb.QueuesListResponse
					require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{
						Pivot: queueName,
						Limit: 1,
					}))
					require.Equal(t, 1, len(list.Items))
					assert.Equal(t, dlqName, list.Items[0].DeadQueue)
					assert.Equal(t, int32(8), list.Items[0].RequestedPartitions)
				})
			})
		})

	})

	t.Run("StoredDeadLetterChain", func(t *testing.T) {
		conf := setup()
		queueA := random.String("queue-a-", 10)
		queueB := random.String("queue-b-", 10)
		queueC := random.String("queue-c-", 10)

		// The API no longer lets a chain form, so the chain A→B→C is written to storage
		// before the daemon starts, as a version without the chain checks would have left it
		for _, link := range []struct{ name, dead string }{{queueC, ""}, {queueB, queueC}, {queueA, queueB}} {
			info := types.QueueInfo{
				Name:                link.name,
				DeadQueue:           link.dead,
				LeaseTimeout:        clock.Minute,
				ExpireTimeout:       10 * clock.Minute,
				RequestedPartitions: 1,
			}
			placed, err := placement.Create(info, conf.Backends())
			require.NoError(t, err)
			require.NoError(t, conf.Queues.Add(context.Background(), placed))
		}

		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		// The last queue in the chain has no dead_queue and one partition, so it updates
		require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: queueC,
			Reference: new("tail"),
		}))

		err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: queueB,
			DeadQueue: new(queueC),
			Reference: new("middle"),
		})
		require.Error(t, err)
		var duhErr duh.Error
		require.True(t, errors.As(err, &duhErr))
		assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "'"+queueB+"' is the dead_queue of '"+queueA+"'")
		assert.Contains(t, duhErr.Message(), "clear or change dead_queue on '"+queueA+"' first")

		err = c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: queueA,
			DeadQueue: new(queueB),
			Reference: new("head"),
		})
		require.Error(t, err)
		require.True(t, errors.As(err, &duhErr))
		assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "'"+queueB+"' already has its own dead_queue configured")
		assert.Contains(t, duhErr.Message(), "clear or change dead_queue on '"+queueB+"' first")

		// Leaving dead_queue out keeps the link, so the update is checked against the saved chain
		err = c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: queueB,
			Reference: new("middle"),
		})
		require.Error(t, err)
		require.True(t, errors.As(err, &duhErr))
		assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "'"+queueB+"' is the dead_queue of '"+queueA+"'")

		err = c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: queueA,
			Reference: new("head"),
		})
		require.Error(t, err)
		require.True(t, errors.As(err, &duhErr))
		assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "'"+queueB+"' already has its own dead_queue configured")

		// Clearing the middle queue's dead_queue breaks the chain, after which both queues update
		require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: queueB,
			DeadQueue: new(""),
		}))
		require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: queueB,
			Reference: new("middle"),
		}))
		require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: queueA,
			DeadQueue: new(queueB),
			Reference: new("head"),
		}))

		var info pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueB}, &info))
		assert.Equal(t, "", info.DeadQueue)
		assert.Equal(t, "middle", info.Reference)
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueA}, &info))
		assert.Equal(t, queueB, info.DeadQueue)
		assert.Equal(t, "head", info.Reference)
	})

	t.Run("StoredQueueNamesMissingDeadQueue", func(t *testing.T) {
		conf := setup()
		stored := random.String("stored-", 10)
		missing := random.String("missing-", 10)
		other := random.String("other-", 10)

		// The API refuses a dead_queue that does not exist, so this queue is written
		// to storage before the daemon starts, as an older server would have left it
		for _, link := range []struct{ name, dead string }{{other, ""}, {stored, missing}} {
			info := types.QueueInfo{
				Name:                link.name,
				DeadQueue:           link.dead,
				LeaseTimeout:        clock.Minute,
				ExpireTimeout:       10 * clock.Minute,
				RequestedPartitions: 1,
			}
			placed, err := placement.Create(info, conf.Backends())
			require.NoError(t, err)
			require.NoError(t, conf.Queues.Add(context.Background(), placed))
		}

		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		// Leaving dead_queue out keeps the link, so the saved dead_queue must still be valid
		err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: stored,
			Reference: new("owner"),
		})
		require.Error(t, err)
		var duhErr duh.Error
		require.True(t, errors.As(err, &duhErr))
		assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "dead_queue does not exist; '"+missing+"' was not found")

		err = c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:           missing,
			ExpireTimeout:       "10m",
			LeaseTimeout:        "1m",
			RequestedPartitions: 2,
		})
		require.Error(t, err)
		require.True(t, errors.As(err, &duhErr))
		assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "'"+missing+"' is the dead_queue of '"+stored+"' and must have a single partition")

		// other exists and nothing names it, so only the stored queue can cause the refusal
		err = c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:           missing,
			DeadQueue:           other,
			ExpireTimeout:       "10m",
			LeaseTimeout:        "1m",
			RequestedPartitions: 1,
		})
		require.Error(t, err)
		require.True(t, errors.As(err, &duhErr))
		assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "'"+missing+"' is the dead_queue of '"+stored+"' and cannot have its own dead_queue")

		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:           missing,
			ExpireTimeout:       "10m",
			LeaseTimeout:        "1m",
			RequestedPartitions: 1,
		}))

		require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: stored,
			DeadQueue: new(""),
			Reference: new("owner"),
		}))
		var info pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: stored}, &info))
		assert.Equal(t, "", info.DeadQueue)
		assert.Equal(t, "owner", info.Reference)
	})

	t.Run("StoredDeadQueueWithPartitions", func(t *testing.T) {
		conf := setup()
		dead := random.String("dead-", 10)
		source1 := random.String("source1-", 10)
		source2 := random.String("source2-", 10)
		other := random.String("other-", 10)

		// The API refuses a dead_queue with more than one partition, so these queues are
		// written to storage before the daemon starts, as the config file would leave them
		for _, queue := range []struct {
			name, dead string
			partitions int
		}{{dead, "", 2}, {other, "", 1}, {source1, dead, 1}, {source2, dead, 1}} {
			info := types.QueueInfo{
				Name:                queue.name,
				DeadQueue:           queue.dead,
				LeaseTimeout:        clock.Minute,
				ExpireTimeout:       10 * clock.Minute,
				RequestedPartitions: queue.partitions,
			}
			placed, err := placement.Create(info, conf.Backends())
			require.NoError(t, err)
			require.NoError(t, conf.Queues.Add(context.Background(), placed))
		}

		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		for _, name := range []string{source1, dead} {
			err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
				QueueName: name,
				Reference: new("owner"),
			})
			require.Error(t, err)
			var duhErr duh.Error
			require.True(t, errors.As(err, &duhErr))
			assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
			assert.Contains(t, duhErr.Message(), "must have a single partition")
		}

		require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: source1,
			DeadQueue: new(""),
		}))
		require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: source2,
			DeadQueue: new(other),
		}))
		require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
			QueueName: dead,
			Reference: new("owner"),
		}))

		var info pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: source1}, &info))
		assert.Equal(t, "", info.DeadQueue)
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: source2}, &info))
		assert.Equal(t, other, info.DeadQueue)
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: dead}, &info))
		assert.Equal(t, "owner", info.Reference)
	})

	t.Run("DeleteDeadQueueNamedAfterFirstPage", func(t *testing.T) {
		conf := setup()
		dead := "a-dead-queue"
		source := "z-source-queue"

		const filler = 1500
		infos := []types.QueueInfo{{Name: dead}}
		for i := 0; i < filler; i++ {
			infos = append(infos, types.QueueInfo{Name: fmt.Sprintf("filler-%05d", i)})
		}
		infos = append(infos, types.QueueInfo{Name: source, DeadQueue: dead})
		for _, info := range infos {
			info.LeaseTimeout = clock.Minute
			info.ExpireTimeout = 10 * clock.Minute
			info.RequestedPartitions = 1
			placed, err := placement.Create(info, conf.Backends())
			require.NoError(t, err)
			require.NoError(t, conf.Queues.Add(context.Background(), placed))
		}

		counted := &countedQueues{Queues: conf.Queues}
		conf.Queues = counted
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		counted.lists.Store(0)
		err := c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: dead})
		require.Error(t, err)
		var duhErr duh.Error
		require.True(t, errors.As(err, &duhErr))
		assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
		assert.Contains(t, duhErr.Message(), "it is the dead_queue of '"+source+"'")
		assert.GreaterOrEqual(t, counted.lists.Load(), int32(2))

		var info pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: dead}, &info))
	})

	t.Run("List", func(t *testing.T) {
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queues := createRandomQueues(t, ctx, c, 50)

		var list pb.QueuesListResponse
		require.NoError(t, c.QueuesList(ctx, &list, nil))
		assert.Equal(t, 50, len(list.Items))

		t.Run("MoreThanAvailable", func(t *testing.T) {
			var more pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &more, &querator.ListOptions{Limit: 20_000}))
			assert.Equal(t, 50, len(more.Items))

			require.Equal(t, queueInfoFieldsOf(queues[0]), queueInfoFieldsOf(more.Items[0]))
			require.Equal(t, queueInfoFieldsOf(queues[50-1]), queueInfoFieldsOf(more.Items[len(more.Items)-1]))
		})
		t.Run("LessThanAvailable", func(t *testing.T) {
			var less pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &less, &querator.ListOptions{Limit: 30}))
			assert.Equal(t, 30, len(less.Items))

			require.Equal(t, queueInfoFieldsOf(queues[0]), queueInfoFieldsOf(less.Items[0]))
			require.Equal(t, queueInfoFieldsOf(queues[30-1]), queueInfoFieldsOf(less.Items[len(less.Items)-1]))
		})
		t.Run("GetOne", func(t *testing.T) {
			var one pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &one, &querator.ListOptions{Pivot: queues[20].QueueName, Limit: 1}))
			assert.Equal(t, 1, len(one.Items))

			require.Equal(t, queueInfoFieldsOf(queues[20]), queueInfoFieldsOf(one.Items[len(one.Items)-1]))
		})
		t.Run("First", func(t *testing.T) {
			var first pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &first, &querator.ListOptions{Limit: 1}))
			assert.Equal(t, 1, len(first.Items))

			require.Equal(t, queueInfoFieldsOf(queues[0]), queueInfoFieldsOf(first.Items[len(first.Items)-1]))
		})
		t.Run("WithPivot", func(t *testing.T) {
			name := queues[20].QueueName
			var pivot pb.QueuesListResponse
			err := c.QueuesList(ctx, &pivot, &querator.ListOptions{Pivot: name, Limit: 10})
			require.NoError(t, err)

			assert.Equal(t, 10, len(pivot.Items))
			require.Equal(t, queueInfoFieldsOf(queues[20]), queueInfoFieldsOf(pivot.Items[0]))
			for i := range pivot.Items {
				require.Equal(t, queueInfoFieldsOf(queues[i+20]), queueInfoFieldsOf(pivot.Items[i]))
			}
			t.Run("PageThroughItems", func(t *testing.T) {
				name := queues[0].QueueName
				var page pb.QueuesListResponse
				err := c.QueuesList(ctx, &page, &querator.ListOptions{Pivot: name, Limit: 10})
				require.NoError(t, err)
				require.Equal(t, queueInfoFieldsOf(queues[0]), queueInfoFieldsOf(page.Items[0]))
				require.Equal(t, queueInfoFieldsOf(queues[9]), queueInfoFieldsOf(page.Items[9]))

				t.Run("PageIncludesPivot", func(t *testing.T) {
					name = queues[9].QueueName
					err = c.QueuesList(ctx, &page, &querator.ListOptions{Pivot: name, Limit: 10})
					require.NoError(t, err)
					require.Equal(t, queueInfoFieldsOf(queues[9]), queueInfoFieldsOf(page.Items[0]))
					require.Equal(t, queueInfoFieldsOf(queues[18]), queueInfoFieldsOf(page.Items[9]))
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

		// Issue #9: When the pivot sorts after all existing items, the result
		// should be empty — not wrap around to the beginning of the list.
		t.Run("PivotPastEnd", func(t *testing.T) {
			var page pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &page,
				&querator.ListOptions{Pivot: "zzzzz-99999", Limit: 10}))
			assert.Empty(t, page.Items)
		})
	})

	t.Run("Errors", func(t *testing.T) {
		conf := setup()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

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
					Name: "InvalidQueueName",
					Req: &pb.QueueInfo{
						QueueName: "invalid queue~name",
					},
					Msg:  "queue name is invalid; 'invalid queue~name' cannot contain whitespace",
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
					Msg:  "lease timeout is invalid; time: invalid duration \"foo\" - expected format: 8m, 15m or 1h",
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
				{
					Name: "PartitionsGreaterThanMax",
					Req: &pb.QueueInfo{
						QueueName:           random.String("queue-", 10),
						LeaseTimeout:        "1m",
						ExpireTimeout:       "10m",
						RequestedPartitions: 1_025,
					},
					Msg:  "partitions is invalid; cannot be greater than 1024",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "PartitionsZero",
					Req: &pb.QueueInfo{
						QueueName:           random.String("queue-", 10),
						LeaseTimeout:        "1m",
						ExpireTimeout:       "10m",
						RequestedPartitions: 0,
					},
					Msg:  "partitions is invalid; cannot be less than 1",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "PartitionsNegative",
					Req: &pb.QueueInfo{
						QueueName:           random.String("queue-", 10),
						LeaseTimeout:        "1m",
						ExpireTimeout:       "10m",
						RequestedPartitions: -1,
					},
					Msg:  "partitions is invalid; cannot be less than 1",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "NamespaceContainsTilde",
					Req: &pb.QueueInfo{
						QueueName: random.String("queue-", 10),
						Namespace: "invalid~namespace",
					},
					Msg:  "namespace name is invalid; 'invalid~namespace' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "NamespaceContainsColon",
					Req: &pb.QueueInfo{
						QueueName: random.String("queue-", 10),
						Namespace: "invalid:namespace",
					},
					Msg:  "namespace name is invalid; 'invalid:namespace' cannot contain ':' character",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "NamespaceContainsWhitespace",
					Req: &pb.QueueInfo{
						QueueName: random.String("queue-", 10),
						Namespace: "invalid namespace",
					},
					Msg:  "namespace name is invalid; 'invalid namespace' cannot contain whitespace",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "NamespaceExceedsMaxLength",
					Req: &pb.QueueInfo{
						QueueName: random.String("queue-", 10),
						Namespace: random.String("ns-", 300),
					},
					Msg:  "namespace name is invalid; cannot be greater than '256' characters",
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
				Req  *pb.QueuesUpdateRequest
				Msg  string
				Code int
			}{
				{
					Name: "EmptyRequest",
					Req:  &pb.QueuesUpdateRequest{},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidQueue",
					Req: &pb.QueuesUpdateRequest{
						QueueName: "invalid~queue",
					},
					Msg:  "queue name is invalid; 'invalid~queue' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "NoSuchQueue",
					Req: &pb.QueuesUpdateRequest{
						QueueName: "noSuchQueue",
					},
					Msg:  "queue does not exist; no such queue named 'noSuchQueue'",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "QueueNameMaxLength",
					Req: &pb.QueuesUpdateRequest{
						QueueName: random.String("", 2_001),
					},
					Msg:  "queue name is invalid; cannot be greater than '512' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "QueueWhiteSpace",
					Req: &pb.QueuesUpdateRequest{
						LeaseTimeout:        LeaseTimeout,
						ExpireTimeout:       ExpireTimeout,
						QueueName:           "Friendship is Magic",
						RequestedPartitions: 1,
					},
					Msg:  "queue name is invalid; 'Friendship is Magic' cannot contain whitespace",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidNamespace",
					Req: &pb.QueuesUpdateRequest{
						QueueName: queueName,
						Namespace: "invalid~namespace",
					},
					Msg:  "namespace name is invalid; 'invalid~namespace' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ReferenceMaxLength",
					Req: &pb.QueuesUpdateRequest{
						QueueName: queueName,
						Reference: new(random.String("", 2_001)),
					},
					Msg:  "reference field is invalid; cannot be greater than '2000' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "LeaseTimeoutTooLong",
					Req: &pb.QueuesUpdateRequest{
						QueueName:     queueName,
						LeaseTimeout:  "1h0m0s",
						ExpireTimeout: "30m0s",
					},
					Msg:  "lease timeout is too long; 1h0m0s cannot be greater than the expire timeout 30m0s",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidLeaseTimeout",
					Req: &pb.QueuesUpdateRequest{
						QueueName:    queueName,
						LeaseTimeout: "foo",
					},
					Msg:  "lease timeout is invalid; time: invalid duration \"foo\" - expected format: 8m, 15m or 1h",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidExpireTimeout",
					Req: &pb.QueuesUpdateRequest{
						QueueName:     queueName,
						ExpireTimeout: "foo",
					},
					Msg:  "expire timeout is invalid; time: invalid duration \"foo\" - expected format: 60m, 2h or 24h",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "ExpireTimeoutMaxLength",
					Req: &pb.QueuesUpdateRequest{
						QueueName:     "ExpireTimeoutMaxLength",
						ExpireTimeout: random.String("", 2_001),
					},
					Msg:  "expire timeout is invalid; cannot be greater than '15' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "DeadQueueMaxLength",
					Req: &pb.QueuesUpdateRequest{
						QueueName:     "ExpireTimeoutMaxLength",
						ExpireTimeout: random.String("", 2_001),
					},
					Msg:  "expire timeout is invalid; cannot be greater than '15' characters",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidDeadLetterQueue",
					Req: &pb.QueuesUpdateRequest{
						QueueName: queueName,
						DeadQueue: new("invalid~deadLetter"),
					},
					Msg:  "dead queue is invalid; 'invalid~deadLetter' cannot contain '~' character",
					Code: duh.CodeBadRequest,
				},
				{
					// TODO: This might be allowed later to indicate infinite retries
					Name: "InvalidMinAttempts",
					Req: &pb.QueuesUpdateRequest{
						QueueName:   queueName,
						MaxAttempts: new(int32(-1)),
					},
					Msg:  "max attempts is invalid; cannot be negative number",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidMaxAttempts",
					Req: &pb.QueuesUpdateRequest{
						QueueName:   queueName,
						MaxAttempts: new(int32(math.MaxInt32)),
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

			t.Run("PartitionsGreaterThanMax", func(t *testing.T) {
				err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:           queueName,
					RequestedPartitions: 1_025,
					Reference:           new("not-saved"),
				})
				var e duh.Error
				require.True(t, errors.As(err, &e))
				assert.Equal(t, "partitions is invalid; cannot be greater than 1024", e.Message())
				assert.Equal(t, duh.CodeBadRequest, e.Code())

				var info pb.QueueInfo
				require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
				assert.Equal(t, int32(1), info.RequestedPartitions)
				assert.Equal(t, "CreateTestRef", info.Reference)
			})
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
				assert.Equal(t, int32(1), resp.RequestedPartitions)
				assert.NotNil(t, resp.CreatedAt)
				assert.NotNil(t, resp.UpdatedAt)
				require.Len(t, resp.Partitions, 1)
				assert.Equal(t, int32(0), resp.Partitions[0].Partition)
				assert.Equal(t, conf.PartitionStorage[0].Name, resp.Partitions[0].StorageName)
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

		d := newDaemon(t, 60*clock.Second, svc.Config{
			StorageConfig: setup(),
			Clock:         now,
		})

		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		t.Run("OmittedTimeoutsAreKept", func(t *testing.T) {
			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})
			require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
				QueueName:   queueName,
				MaxAttempts: new(int32(5)),
			}))

			ref := random.String("ref-", 10)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items:          []*pb.QueueProduceItem{{Reference: ref, Bytes: []byte("test payload")}},
			}))
			var lease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &lease))
			require.Len(t, lease.Items, 1)

			var resp pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &resp, nil))
			item := findInStorageList(ref, &resp)
			require.NotNil(t, item)
			start := now.Now().UTC()
			assert.WithinDuration(t, start.Add(24*clock.Hour), item.ExpireDeadline.AsTime(), clock.Microsecond)
			assert.WithinDuration(t, start.Add(clock.Minute), item.LeaseDeadline.AsTime(), clock.Microsecond)
		})

		t.Run("OmittedSettingsAreKept", func(t *testing.T) {
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
				DeadQueue:           dlqName,
				MaxAttempts:         7,
				Reference:           "owner-1",
				RequestedPartitions: 1,
			})

			for _, update := range []*pb.QueuesUpdateRequest{
				{QueueName: queueName, LeaseTimeout: "2m0s"},
				{QueueName: queueName, RequestedPartitions: 1},
			} {
				require.NoError(t, c.QueuesUpdate(ctx, update))

				var info pb.QueueInfo
				require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
				assert.Equal(t, "2m0s", info.LeaseTimeout)
				assert.Equal(t, dlqName, info.DeadQueue)
				assert.Equal(t, int32(7), info.MaxAttempts)
				assert.Equal(t, "owner-1", info.Reference)
			}
		})

		t.Run("RaiseLeaseAndExpireTogether", func(t *testing.T) {
			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       "10m0s",
				RequestedPartitions: 1,
			})

			require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
				QueueName:     queueName,
				LeaseTimeout:  "20m0s",
				ExpireTimeout: "30m0s",
			}))

			var info pb.QueueInfo
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			assert.Equal(t, "20m0s", info.LeaseTimeout)
			assert.Equal(t, "30m0s", info.ExpireTimeout)
		})

		t.Run("LeaseLongerThanResultingExpireIsRefused", func(t *testing.T) {
			queueName := random.String("queue-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       "10m0s",
				RequestedPartitions: 1,
			})

			for _, update := range []*pb.QueuesUpdateRequest{
				{QueueName: queueName, LeaseTimeout: "20m0s"},
				{QueueName: queueName, LeaseTimeout: "20m0s", ExpireTimeout: "15m0s"},
			} {
				err := c.QueuesUpdate(ctx, update)
				var duhErr duh.Error
				require.True(t, errors.As(err, &duhErr))
				assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
				assert.Contains(t, duhErr.Message(), "lease timeout is too long")
			}

			var info pb.QueueInfo
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			assert.Equal(t, "1m0s", info.LeaseTimeout)
			assert.Equal(t, "10m0s", info.ExpireTimeout)
		})

		t.Run("ExplicitClear", func(t *testing.T) {
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
				DeadQueue:           dlqName,
				MaxAttempts:         7,
				Reference:           "owner-1",
				RequestedPartitions: 1,
			})

			require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
				QueueName:   queueName,
				DeadQueue:   new(""),
				MaxAttempts: new(int32(0)),
				Reference:   new(""),
			}))

			var info pb.QueueInfo
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			assert.Equal(t, "", info.DeadQueue)
			assert.Equal(t, int32(0), info.MaxAttempts)
			assert.Equal(t, "", info.Reference)
			assert.Equal(t, "1m0s", info.LeaseTimeout)

			var list pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{Pivot: queueName, Limit: 1}))
			require.Len(t, list.Items, 1)
			assert.Equal(t, "", list.Items[0].DeadQueue)
			assert.Equal(t, int32(0), list.Items[0].MaxAttempts)
			assert.Equal(t, "", list.Items[0].Reference)
		})

		t.Run("JSONRequests", func(t *testing.T) {
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
				DeadQueue:           dlqName,
				MaxAttempts:         7,
				Reference:           "owner-1",
				RequestedPartitions: 1,
			})

			endpoint := fmt.Sprintf("http://%s%s", d.d.Listener.Addr().String(), transport.RPCQueuesUpdate)
			post := func(body string) {
				r, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
				require.NoError(t, err)
				r.Header.Set("Content-Type", duh.ContentTypeJSON)
				require.NoError(t, duh.HTTP1Client.Do(r, &v1.Reply{}))
			}

			post(`{"queue_name":"` + queueName + `","lease_timeout":"2m0s"}`)
			var info pb.QueueInfo
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			assert.Equal(t, "2m0s", info.LeaseTimeout)
			assert.Equal(t, dlqName, info.DeadQueue)
			assert.Equal(t, int32(7), info.MaxAttempts)
			assert.Equal(t, "owner-1", info.Reference)

			// A body copied from '/queues.info' is accepted, and its read-only fields are ignored
			echo, err := protojson.Marshal(&info)
			require.NoError(t, err)
			post(string(echo))
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			assert.Equal(t, dlqName, info.DeadQueue)
			assert.Len(t, info.Partitions, 1)

			post(`{"queue_name":"` + queueName + `","dead_queue":null,"max_attempts":null,"reference":null,` +
				`"lease_timeout":"0s","expire_timeout":"","requested_partitions":0}`)
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			assert.Equal(t, "2m0s", info.LeaseTimeout)
			assert.Equal(t, ExpireTimeout, info.ExpireTimeout)
			assert.Equal(t, dlqName, info.DeadQueue)
			assert.Equal(t, int32(7), info.MaxAttempts)
			assert.Equal(t, "owner-1", info.Reference)
			assert.Equal(t, int32(1), info.RequestedPartitions)

			post(`{"queue_name":"` + queueName + `","dead_queue":"","max_attempts":0,"reference":""}`)
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			assert.Equal(t, "2m0s", info.LeaseTimeout)
			assert.Equal(t, "", info.DeadQueue)
			assert.Equal(t, int32(0), info.MaxAttempts)
			assert.Equal(t, "", info.Reference)
		})

		t.Run("EachSentSettingChangesOnlyItself", func(t *testing.T) {
			otherDLQ := random.String("dlq-", 10)
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           otherDLQ,
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				RequestedPartitions: 1,
			})

			for _, test := range []struct {
				Name   string
				Update func(name string) *pb.QueuesUpdateRequest
				Want   func(dlq string) *pb.QueueInfo
			}{
				{
					Name: "LeaseTimeout",
					Update: func(name string) *pb.QueuesUpdateRequest {
						return &pb.QueuesUpdateRequest{QueueName: name, LeaseTimeout: "2m0s"}
					},
					Want: func(dlq string) *pb.QueueInfo {
						return &pb.QueueInfo{LeaseTimeout: "2m0s", ExpireTimeout: "24h0m0s", DeadQueue: dlq, MaxAttempts: 7, Reference: "owner-1", RequestedPartitions: 1}
					},
				},
				{
					Name: "ExpireTimeout",
					Update: func(name string) *pb.QueuesUpdateRequest {
						return &pb.QueuesUpdateRequest{QueueName: name, ExpireTimeout: "12h0m0s"}
					},
					Want: func(dlq string) *pb.QueueInfo {
						return &pb.QueueInfo{LeaseTimeout: "1m0s", ExpireTimeout: "12h0m0s", DeadQueue: dlq, MaxAttempts: 7, Reference: "owner-1", RequestedPartitions: 1}
					},
				},
				{
					Name: "MaxAttempts",
					Update: func(name string) *pb.QueuesUpdateRequest {
						return &pb.QueuesUpdateRequest{QueueName: name, MaxAttempts: new(int32(3))}
					},
					Want: func(dlq string) *pb.QueueInfo {
						return &pb.QueueInfo{LeaseTimeout: "1m0s", ExpireTimeout: "24h0m0s", DeadQueue: dlq, MaxAttempts: 3, Reference: "owner-1", RequestedPartitions: 1}
					},
				},
				{
					Name: "DeadQueue",
					Update: func(name string) *pb.QueuesUpdateRequest {
						return &pb.QueuesUpdateRequest{QueueName: name, DeadQueue: new(otherDLQ)}
					},
					Want: func(string) *pb.QueueInfo {
						return &pb.QueueInfo{LeaseTimeout: "1m0s", ExpireTimeout: "24h0m0s", DeadQueue: otherDLQ, MaxAttempts: 7, Reference: "owner-1", RequestedPartitions: 1}
					},
				},
				{
					Name: "Reference",
					Update: func(name string) *pb.QueuesUpdateRequest {
						return &pb.QueuesUpdateRequest{QueueName: name, Reference: new("owner-2")}
					},
					Want: func(dlq string) *pb.QueueInfo {
						return &pb.QueueInfo{LeaseTimeout: "1m0s", ExpireTimeout: "24h0m0s", DeadQueue: dlq, MaxAttempts: 7, Reference: "owner-2", RequestedPartitions: 1}
					},
				},
				{
					Name: "RequestedPartitions",
					Update: func(name string) *pb.QueuesUpdateRequest {
						return &pb.QueuesUpdateRequest{QueueName: name, RequestedPartitions: 2}
					},
					Want: func(dlq string) *pb.QueueInfo {
						return &pb.QueueInfo{LeaseTimeout: "1m0s", ExpireTimeout: "24h0m0s", DeadQueue: dlq, MaxAttempts: 7, Reference: "owner-1", RequestedPartitions: 2}
					},
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
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
						ExpireTimeout:       "24h0m0s",
						DeadQueue:           dlqName,
						MaxAttempts:         7,
						Reference:           "owner-1",
						RequestedPartitions: 1,
					})

					require.NoError(t, c.QueuesUpdate(ctx, test.Update(queueName)))
					want := test.Want(dlqName)

					var info pb.QueueInfo
					require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
					assert.Equal(t, want.LeaseTimeout, info.LeaseTimeout)
					assert.Equal(t, want.ExpireTimeout, info.ExpireTimeout)
					assert.Equal(t, want.DeadQueue, info.DeadQueue)
					assert.Equal(t, want.MaxAttempts, info.MaxAttempts)
					assert.Equal(t, want.Reference, info.Reference)
					assert.Equal(t, want.RequestedPartitions, info.RequestedPartitions)

					var list pb.QueuesListResponse
					require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{Pivot: queueName, Limit: 1}))
					require.Len(t, list.Items, 1)
					assert.Equal(t, queueName, list.Items[0].QueueName)
					assert.Equal(t, want.LeaseTimeout, list.Items[0].LeaseTimeout)
					assert.Equal(t, want.ExpireTimeout, list.Items[0].ExpireTimeout)
					assert.Equal(t, want.DeadQueue, list.Items[0].DeadQueue)
					assert.Equal(t, want.MaxAttempts, list.Items[0].MaxAttempts)
					assert.Equal(t, want.Reference, list.Items[0].Reference)
					assert.Equal(t, want.RequestedPartitions, list.Items[0].RequestedPartitions)
				})
			}
		})

		t.Run("EmptyDeadQueueStillChecksPartitions", func(t *testing.T) {
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
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				DeadQueue:           dlqName,
				RequestedPartitions: 1,
			})

			err := c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
				QueueName:           dlqName,
				DeadQueue:           new(""),
				RequestedPartitions: 2,
			})
			require.Error(t, err)
			var duhErr duh.Error
			require.True(t, errors.As(err, &duhErr))
			assert.Equal(t, duh.CodeBadRequest, duhErr.Code())
			assert.Contains(t, duhErr.Message(), "'"+dlqName+"' is the dead_queue of '"+queueName+"' and must have a single partition")

			var info pb.QueueInfo
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: dlqName}, &info))
			assert.Equal(t, int32(1), info.RequestedPartitions)
		})

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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:   queueName,
					MaxAttempts: new(int32(2)),
					DeadQueue:   new(dlqName),
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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:   queueName,
					MaxAttempts: new(int32(5)),
					DeadQueue:   new(dlqName),
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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:   queueName,
					MaxAttempts: new(int32(0)),
					DeadQueue:   new(dlqName),
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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:   queueName,
					MaxAttempts: new(int32(2)),
					DeadQueue:   new(dlqName),
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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:   queueName,
					MaxAttempts: new(int32(2)),
					DeadQueue:   new(dlqBName),
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
				require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
					QueueName:   queueName,
					MaxAttempts: new(int32(2)),
					DeadQueue:   new(""),
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

// updateWhileReadingInfo updates the queue's reference updates times while another goroutine reads
// the queue's info as often. The goroutines start in a named function because Tiger's TS-C02 does
// not see a Wait that follows Go inside a function literal.
func updateWhileReadingInfo(t *testing.T, ctx context.Context, c *querator.Client, queueName string, updates int) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range updates {
			assert.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
				QueueName: queueName,
				Reference: new(fmt.Sprintf("reference-%d", i)),
			}))
		}
	})
	wg.Go(func() {
		for range updates {
			var info pb.QueueInfo
			assert.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			assert.Equal(t, queueName, info.QueueName)
		}
	})
	wg.Wait()
}
