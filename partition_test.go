package querator_test

import (
	"context"
	"fmt"
	"github.com/duh-rpc/duh-go/retry"
	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"testing"
)

func TestPartitions(t *testing.T) {
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
		// {
		// 	Name: "SurrealDB",
		// },
		// {
		// 	Name: "PostgresSQL",
		// },
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testPartitions(t, tc.Setup, tc.TearDown)
		})
	}
}

func testPartitions(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t)
	d, c, ctx := newDaemon(t, 10*clock.Second, que.ServiceConfig{StorageConfig: setup()})
	defer func() {
		d.Shutdown(t)
		tearDown()
	}()

	t.Run("TwoPartitions", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		ClientID := random.String("client-", 10)

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			Reference:           "CreateTestRef",
			LeaseTimeout:        "1m",
			ExpireTimeout:       "10m",
			MaxAttempts:         10,
			RequestedPartitions: 2,
		})

		// Should be queued into partition 0
		partitionZeroItems := produceRandomItems(10)
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
		partitionOneItems := produceRandomItems(11)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			Items:          partitionOneItems,
			QueueName:      queueName,
			RequestTimeout: "1m",
		}))

		require.NoError(t, c.StorageItemsList(ctx, queueName, 1, &list, nil))
		assert.Equal(t, len(list.Items), 11)
		assert.Equal(t, partitionOneItems[0].Bytes, list.Items[0].Payload)

		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			QueueName:      queueName,
			ClientId:       ClientID,
			RequestTimeout: "5s",
			BatchSize:      10,
		}, &lease))

		assert.Equal(t, queueName, lease.QueueName)
		assert.Equal(t, 10, len(lease.Items))

		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			QueueName:      queueName,
			ClientId:       ClientID,
			RequestTimeout: "5s",
			BatchSize:      10,
		}, &lease))

		assert.Equal(t, queueName, lease.QueueName)
		assert.Equal(t, 10, len(lease.Items))

		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		var leased, notLeased int
		for _, item := range list.Items {
			if item.IsLeased {
				leased++
			} else {
				notLeased++
			}
		}
		assert.Equal(t, 10, leased)

		require.NoError(t, c.StorageItemsList(ctx, queueName, 1, &list, nil))
		for _, item := range list.Items {
			if item.IsLeased {
				leased++
			} else {
				notLeased++
			}
		}
		// All the items in both partitions should be in leased status except 1
		assert.Equal(t, 20, leased)
		assert.Equal(t, 1, notLeased)
	})

	t.Run("OpportunisticLease", func(t *testing.T) {
		var queueName = random.String("queue-", 10)

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			Reference:           "CreateTestRef",
			LeaseTimeout:        "1m",
			ExpireTimeout:       "10m",
			MaxAttempts:         10,
			RequestedPartitions: 2,
		})

		// Produce 50 items
		for i := 0; i < 5; i++ {
			items := produceRandomItems(10)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				Items:          items,
				QueueName:      queueName,
				RequestTimeout: "1m",
			}))
		}

		// Distribution of items to partitions should look like this
		// Partition 0 - 10 10 10  (30 items)
		// Partition 1 - 10 10     (20 items)

		var list pb.StorageItemsListResponse
		var leased, notLeased int
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		for _, item := range list.Items {
			if item.IsLeased {
				leased++
			} else {
				notLeased++
			}
		}
		assert.Equal(t, 0, leased)
		assert.Equal(t, 30, notLeased)

		leased, notLeased = 0, 0
		require.NoError(t, c.StorageItemsList(ctx, queueName, 1, &list, nil))
		for _, item := range list.Items {
			if item.IsLeased {
				leased++
			} else {
				notLeased++
			}
		}
		assert.Equal(t, 0, leased)
		assert.Equal(t, 20, notLeased)

		for _, tc := range []struct {
			req      *pb.QueueLeaseRequest
			expected int
		}{
			{
				req: &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      5,
					RequestTimeout: "1m",
				},
				expected: 5,
			},
			{
				req: &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      10,
					RequestTimeout: "1m",
				},
				expected: 10,
			},
			{
				req: &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					QueueName:      queueName,
					BatchSize:      20,
					RequestTimeout: "1m",
				},
				expected: 20,
			},
		} {
			var resp pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, tc.req, &resp))
			assert.Equal(t, tc.expected, len(resp.Items))
		}

		// Lease of items should look like this
		// Partition 0 - 5 10 (15 remain)
		// Partition 1 - 20   (0 remain)

		assertPartition(t, ctx, c, queueName, Partition{Partition: 0, Leased: 15, NotLeased: 15})
		assertPartition(t, ctx, c, queueName, Partition{Partition: 1, Leased: 20, NotLeased: 0})
	})

	t.Run("OnePartitionManyConsumers", func(t *testing.T) {
		var queueName = random.String("queue-", 10)

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			Reference:           "CreateTestMany",
			LeaseTimeout:        "1m",
			ExpireTimeout:       "10m",
			MaxAttempts:         10,
			RequestedPartitions: 1,
		})

		// Produce 50 items
		for i := 0; i < 5; i++ {
			items := produceRandomItems(10)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				Items:          items,
				QueueName:      queueName,
				RequestTimeout: "1m",
			}))
		}

		requests := []*pb.QueueLeaseRequest{
			{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      2,
				RequestTimeout: "1m",
			},
			{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      2,
				RequestTimeout: "1m",
			},
			{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      2,
				RequestTimeout: "1m",
			},
			{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      2,
				RequestTimeout: "1m",
			},
			{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      2,
				RequestTimeout: "1m",
			},
		}

		// 10 concurrent requests, made 5 times to consume all 50 items
		for i := 0; i < 5; i++ {
			responses := pauseAndLease(t, ctx, d.Service(), c, queueName, requests)
			for _, resp := range responses {
				assert.Equal(t, 2, len(resp.Items))
			}
		}
		assertPartition(t, ctx, c, queueName, Partition{Partition: 0, Leased: 50, NotLeased: 0})
	})

	t.Run("ManyPartitionsOneConsumer", func(t *testing.T) {
		var queueName = random.String("queue-", 10)

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			Reference:           "CreateTestMany",
			LeaseTimeout:        "10m",
			ExpireTimeout:       "10m",
			MaxAttempts:         10,
			RequestedPartitions: 10,
		})

		// Produce 50 items
		for i := 0; i < 10; i++ {
			items := produceRandomItems(5)
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				Items:          items,
				QueueName:      queueName,
				RequestTimeout: "1m",
			}))
		}

		// A single consumer reserving all items from all partitions
		var count int
		for count < 50 {
			var resp pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      5,
				RequestTimeout: "1m",
			}, &resp))
			count += len(resp.Items)
		}
		assertPartition(t, ctx, c, queueName, Partition{Partition: 0, Leased: 5, NotLeased: 0})
		assertPartition(t, ctx, c, queueName, Partition{Partition: 1, Leased: 5, NotLeased: 0})
		assertPartition(t, ctx, c, queueName, Partition{Partition: 2, Leased: 5, NotLeased: 0})
		assertPartition(t, ctx, c, queueName, Partition{Partition: 3, Leased: 5, NotLeased: 0})
		assertPartition(t, ctx, c, queueName, Partition{Partition: 4, Leased: 5, NotLeased: 0})
		assertPartition(t, ctx, c, queueName, Partition{Partition: 5, Leased: 5, NotLeased: 0})
		assertPartition(t, ctx, c, queueName, Partition{Partition: 6, Leased: 5, NotLeased: 0})
		assertPartition(t, ctx, c, queueName, Partition{Partition: 7, Leased: 5, NotLeased: 0})
		assertPartition(t, ctx, c, queueName, Partition{Partition: 8, Leased: 5, NotLeased: 0})
		assertPartition(t, ctx, c, queueName, Partition{Partition: 9, Leased: 5, NotLeased: 0})
	})

	t.Run("WaitingConsumers", func(t *testing.T) {
		var queueName = random.String("queue-", 10)

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			Reference:           "WaitingConsumers",
			LeaseTimeout:        "1m",
			ExpireTimeout:       "10m",
			MaxAttempts:         10,
			RequestedPartitions: 5,
		})

		requests := []*pb.QueueLeaseRequest{
			{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			},
			{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			},
			{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			},
			{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			},
			{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      10,
				RequestTimeout: "1m",
			},
		}

		leaseCh := make(chan []*pb.QueueLeaseResponse)
		go func() {
			leaseCh <- pauseAndLease(t, ctx, d.Service(), c, queueName, requests)
		}()

		// Wait until the lease requests are all waiting
		err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
			var resp pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &resp))
			if int(resp.LogicalQueues[0].LeaseWaiting) != len(requests) {
				return fmt.Errorf("LeaseWaiting never reached expected %d", len(requests))
			}
			return nil
		})
		require.NoError(t, err)

		// Now produce 10 items
		items := produceRandomItems(10)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			Items:          items,
			QueueName:      queueName,
			RequestTimeout: "1m",
		}))

		// The 10 items produced are distributed amongst all the waiting lease requests.
		resp := <-leaseCh
		for _, r := range resp {
			assert.Equal(t, 2, len(r.Items))
		}
	})
	// TODO: Errors
	// TODO: Attempt to complete ids for a partition which does not exist
}

type Partition struct {
	Name      string
	Partition int
	Leased    int
	NotLeased int
}

func assertPartition(t *testing.T, ctx context.Context, c *que.Client, name string, expected Partition) {
	t.Helper()
	var list pb.StorageItemsListResponse
	var leased, notLeased int
	require.NoError(t, c.StorageItemsList(ctx, name, expected.Partition, &list, nil))
	for _, item := range list.Items {
		if item.IsLeased {
			leased++
		} else {
			notLeased++
		}
	}
	assert.Equal(t, expected.Leased, leased)
	assert.Equal(t, expected.NotLeased, notLeased)
}

