package querator_test

import (
	"fmt"
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

			fmt.Println("Create queue:", queueName)

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
	})
	// TODO: Even Distribution with 2 consumers and 2 Producers
	// TODO: Even Distribution with 3 consumers and small batch request sizes

	// TODO: Errors
	// TODO: Attempt to complete ids for a partition which does not exist
}
