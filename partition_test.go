package querator_test

import (
	"context"
	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPartitions(t *testing.T) {
	//bdb := boltTestSetup{Dir: t.TempDir()}
	//badgerdb := badgerTestSetup{Dir: t.TempDir()}

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
		//{
		//	Name: "BoltDB",
		//	Setup: func(cp *clock.Provider) store.StorageConfig {
		//		return bdb.Setup(store.BoltConfig{Clock: cp})
		//	},
		//	TearDown: func() {
		//		bdb.Teardown()
		//	},
		//},
		//{
		//	Name: "BadgerDB",
		//	Setup: func(cp *clock.Provider) store.StorageConfig {
		//		return badgerdb.Setup(store.BadgerConfig{Clock: cp})
		//	},
		//	TearDown: func() {
		//		badgerdb.Teardown()
		//	},
		//},
		//{
		//	Name: "SurrealDB",
		//},
		//{
		//	Name: "PostgresSQL",
		//},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testPartitions(t, tc.Setup, tc.TearDown)
		})
	}
}

func testPartitions(t *testing.T, setup NewStorageFunc, tearDown func()) {
	t.Run("ProduceAndReserve", func(t *testing.T) {
		_store := setup(clock.NewProvider())
		defer tearDown()
		d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: _store})
		defer d.Shutdown(t)

		t.Run("TwoPartitions", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			ClientID := random.String("client-", 10)

			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				DeadQueue:           queueName + "-dead",
				Reference:           "CreateTestRef",
				ReserveTimeout:      "1m",
				DeadTimeout:         "10m",
				MaxAttempts:         10,
				RequestedPartitions: 2,
			}))

			// Should be queued into partition 0
			partitionZeroItems := randomProduceItems(10)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				Items:          partitionZeroItems,
				QueueName:      queueName,
				RequestTimeout: "1m",
			}))
			var list pb.StorageItemsListResponse
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
			assert.Equal(t, len(list.Items), 10)
			assert.Equal(t, partitionZeroItems[0].Bytes, list.Items[0].Payload)

			// Should be queued into partition 1
			partitionOneItems := randomProduceItems(11)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				Items:          partitionOneItems,
				QueueName:      queueName,
				RequestTimeout: "1m",
			}))

			require.NoError(t, c.StorageItemsList(ctx, queueName, 1, &list, nil))
			assert.Equal(t, len(list.Items), 11)
			assert.Equal(t, partitionOneItems[0].Bytes, list.Items[0].Payload)

			var reserve pb.QueueReserveResponse
			require.NoError(t, c.QueueReserve(ctx, &pb.QueueReserveRequest{
				QueueName:      queueName,
				ClientId:       ClientID,
				RequestTimeout: "5s",
				BatchSize:      10,
			}, &reserve))

			assert.Equal(t, queueName, reserve.QueueName)
			assert.Equal(t, 10, len(reserve.Items))

			require.NoError(t, c.QueueReserve(ctx, &pb.QueueReserveRequest{
				QueueName:      queueName,
				ClientId:       ClientID,
				RequestTimeout: "5s",
				BatchSize:      10,
			}, &reserve))

			assert.Equal(t, queueName, reserve.QueueName)
			assert.Equal(t, 10, len(reserve.Items))

			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
			var reserved, notReserved int
			for _, item := range list.Items {
				if item.IsReserved {
					reserved++
				} else {
					notReserved++
				}
			}
			assert.Equal(t, 10, reserved)

			require.NoError(t, c.StorageItemsList(ctx, queueName, 1, &list, nil))
			for _, item := range list.Items {
				if item.IsReserved {
					reserved++
				} else {
					notReserved++
				}
			}
			// All the items in both partitions should be in reserved status except 1
			assert.Equal(t, 20, reserved)
			assert.Equal(t, 1, notReserved)
		})

		t.Run("OpportunisticReservation", func(t *testing.T) {
			var queueName = random.String("queue-", 10)

			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				DeadQueue:           queueName + "-dead",
				Reference:           "CreateTestRef",
				ReserveTimeout:      "1m",
				DeadTimeout:         "10m",
				MaxAttempts:         10,
				RequestedPartitions: 2,
			}))

			// Produce 50 items, 10 items at a time.
			for i := 0; i < 5; i++ {
				partitionZeroItems := randomProduceItems(10)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					Items:          partitionZeroItems,
					QueueName:      queueName,
					RequestTimeout: "1m",
				}))
			}

			// Distribution of items to partitions should look like this
			// Partition 0 - 10 10 10  (30 items)
			// Partition 1 - 10 10     (20 items)

			var list pb.StorageItemsListResponse
			var reserved, notReserved int
			require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
			for _, item := range list.Items {
				if item.IsReserved {
					reserved++
				} else {
					notReserved++
				}
			}
			assert.Equal(t, 0, reserved)
			assert.Equal(t, 30, notReserved)

			reserved, notReserved = 0, 0
			require.NoError(t, c.StorageItemsList(ctx, queueName, 1, &list, nil))
			for _, item := range list.Items {
				if item.IsReserved {
					reserved++
				} else {
					notReserved++
				}
			}
			assert.Equal(t, 0, reserved)
			assert.Equal(t, 20, notReserved)

			for _, tc := range []struct {
				req      *pb.QueueReserveRequest
				expected int
			}{
				{
					req: &pb.QueueReserveRequest{
						ClientId:       random.String("client-", 10),
						QueueName:      queueName,
						BatchSize:      5,
						RequestTimeout: "1m",
					},
					expected: 5,
				},
				{
					req: &pb.QueueReserveRequest{
						ClientId:       random.String("client-", 10),
						QueueName:      queueName,
						BatchSize:      10,
						RequestTimeout: "1m",
					},
					expected: 10,
				},
				{
					req: &pb.QueueReserveRequest{
						ClientId:       random.String("client-", 10),
						QueueName:      queueName,
						BatchSize:      20,
						RequestTimeout: "1m",
					},
					expected: 20,
				},
			} {
				var resp pb.QueueReserveResponse
				require.NoError(t, c.QueueReserve(ctx, tc.req, &resp))
				assert.Equal(t, tc.expected, len(resp.Items))
			}

			// Reservation of items should look like this
			// Partition 0 - 5 10 (15 remain)
			// Partition 1 - 20   (0 remain)

			assertPartition(t, ctx, c, queueName, Partition{Partition: 0, Reserved: 15, NotReserved: 15})
			assertPartition(t, ctx, c, queueName, Partition{Partition: 1, Reserved: 20, NotReserved: 0})
		})

		t.Run("OpportunisticReservationSingle", func(t *testing.T) {
			var queueName = random.String("queue-", 10)

			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				DeadQueue:           queueName + "-dead",
				Reference:           "CreateTestRef",
				ReserveTimeout:      "1m",
				DeadTimeout:         "10m",
				MaxAttempts:         10,
				RequestedPartitions: 3,
			}))

			// Produce 25 items, 5 items at a time.
			for i := 0; i < 5; i++ {
				partitionZeroItems := randomProduceItems(5)
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					Items:          partitionZeroItems,
					QueueName:      queueName,
					RequestTimeout: "1m",
				}))
			}

			// Distribution of items to partitions should look like this
			// Partition 0 - 5
			// Partition 1 - 5 5
			// Partition 2 - 5 5

			// NOTE: This test depends upon the sort algorithm which is used by `slices.SortFunc()` for
			// deterministic choice of items produced to a partition. If that algorithm implementation
			// ever changes, then this test will likely fail. I'm being lazy and not making this test sort
			// implementation agnostic. My apologies to whomever has to fix this one day.
			assertPartition(t, ctx, c, queueName, Partition{Partition: 0, Reserved: 0, NotReserved: 5})
			assertPartition(t, ctx, c, queueName, Partition{Partition: 1, Reserved: 0, NotReserved: 10})
			assertPartition(t, ctx, c, queueName, Partition{Partition: 2, Reserved: 0, NotReserved: 10})
			clientId := random.String("client-", 10)

			for _, tc := range []struct {
				req *pb.QueueReserveRequest
				exp int
			}{
				{
					req: &pb.QueueReserveRequest{BatchSize: 1},
					exp: 1,
				},
				{
					req: &pb.QueueReserveRequest{BatchSize: 1},
					exp: 1,
				},
				{
					req: &pb.QueueReserveRequest{BatchSize: 1},
					exp: 1,
				},
				{
					req: &pb.QueueReserveRequest{BatchSize: 1},
					exp: 1,
				},
				{
					req: &pb.QueueReserveRequest{BatchSize: 1},
					exp: 1,
				},
				{
					req: &pb.QueueReserveRequest{BatchSize: 1},
					exp: 1,
				},
				{
					req: &pb.QueueReserveRequest{BatchSize: 1},
					exp: 1,
				},
				{
					req: &pb.QueueReserveRequest{BatchSize: 1},
					exp: 1,
				},
				{
					req: &pb.QueueReserveRequest{BatchSize: 1},
					exp: 1,
				},
				{
					req: &pb.QueueReserveRequest{BatchSize: 1},
					exp: 1,
				},
			} {
				var resp pb.QueueReserveResponse
				tc.req.QueueName = queueName
				tc.req.RequestTimeout = "1m"
				tc.req.ClientId = clientId
				require.NoError(t, c.QueueReserve(ctx, tc.req, &resp))
				assert.Equal(t, tc.exp, len(resp.Items))
			}

			// Since reservation is opportunistic, none of the reserve requests will come from partition 0
			// until the other partitions have an equal or lesser number of items in them. In this way
			// partition consumption should remain even.
			assertPartition(t, ctx, c, queueName, Partition{Partition: 0, Reserved: 0, NotReserved: 5})
			assertPartition(t, ctx, c, queueName, Partition{Partition: 1, Reserved: 5, NotReserved: 5})
			assertPartition(t, ctx, c, queueName, Partition{Partition: 2, Reserved: 5, NotReserved: 5})

		})
	})
	// TODO: Errors
	// TODO: Attempt to complete ids for a partition which does not exist
}

type Partition struct {
	Name        string
	Partition   int
	Reserved    int
	NotReserved int
}

func assertPartition(t *testing.T, ctx context.Context, c *que.Client, name string, expected Partition) {
	t.Helper()
	var list pb.StorageItemsListResponse
	var reserved, notReserved int
	require.NoError(t, c.StorageItemsList(ctx, name, expected.Partition, &list, nil))
	for _, item := range list.Items {
		if item.IsReserved {
			reserved++
		} else {
			notReserved++
		}
	}
	assert.Equal(t, expected.Reserved, reserved)
	assert.Equal(t, expected.NotReserved, notReserved)
}
