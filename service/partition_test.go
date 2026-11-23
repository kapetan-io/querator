package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/duh-rpc/duh-go/retry"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestPartitions(t *testing.T) {
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
			testPartitions(t, tc.Setup, tc.TearDown)
		})
	}
}

func testPartitions(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)
	d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
	defer func() {
		d.Shutdown(t)
		tearDown()
	}()
	c, ctx := d.MustClient(), d.Context()

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

	// Regression for ENG-56: QueueStats must report each partition's own
	// number and in-memory state when the partitions hold different loads.
	t.Run("StatsPerPartitionInMemory", func(t *testing.T) {
		var queueName = random.String("queue-", 10)

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m",
			ExpireTimeout:       "10m",
			MaxAttempts:         10,
			RequestedPartitions: 2,
		})

		// First batch lands in partition 0, second batch in partition 1, so the
		// two partitions hold different loads.
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			Items:          produceRandomItems(11),
			QueueName:      queueName,
			RequestTimeout: "1m",
		}))
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			Items:          produceRandomItems(10),
			QueueName:      queueName,
			RequestTimeout: "1m",
		}))

		var stats pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
		require.Len(t, stats.LogicalQueues, 1)
		require.Len(t, stats.LogicalQueues[0].Partitions, 2)

		// The Partitions slice is ordered by partition number. No items are
		// leased, so each partition's in-memory UnLeased must equal its
		// storage-derived Total, and each entry must carry its own number.
		partition0 := stats.LogicalQueues[0].Partitions[0]
		assert.Equal(t, int32(0), partition0.Partition)
		assert.Equal(t, int32(11), partition0.Total)
		assert.Equal(t, int32(11), partition0.UnLeased)

		partition1 := stats.LogicalQueues[0].Partitions[1]
		assert.Equal(t, int32(1), partition1.Partition)
		assert.Equal(t, int32(10), partition1.Total)
		assert.Equal(t, int32(10), partition1.UnLeased)
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

		// A single consumer reserving all items from all partitions. Each lease returns at
		// least one item, so 50 leases are always enough.
		var count int
		for range 50 {
			if count >= 50 {
				break
			}
			var resp pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				QueueName:      queueName,
				BatchSize:      5,
				RequestTimeout: "1m",
			}, &resp))
			count += len(resp.Items)
		}
		require.Equal(t, 50, count)
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

		var resp []*pb.QueueLeaseResponse
		backgroundCall{
			Call: func() {
				resp = pauseAndLease(t, ctx, d.Service(), c, queueName, requests)
			},
			While: func() {
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
			},
		}.run()

		// The 10 items produced are distributed amongst all the waiting lease requests.
		for _, r := range resp {
			assert.Equal(t, 2, len(r.Items))
		}
	})
}

type Partition struct {
	Name      string
	Partition int
	Leased    int
	NotLeased int
}

func assertPartition(t *testing.T, ctx context.Context, c *querator.Client, name string, expected Partition) {
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

// TestPartitionPlacement and TestPartitionSpread sit outside TestPartitions because each needs
// several partition storage backends in one daemon. Each TestPartitions setup configures one
// backend and its teardown cleans up only the last one set up, so it can't build a second.
// QueuesManager picks a partition's backend before writing to it, so in-memory backends are
// enough here. TestQueuesStorage/<Backend>/CRUD/Create proves each backend type saves the name.
func TestPartitionPlacement(t *testing.T) {
	defer goleak.VerifyNone(t, goleakOptions...)
	conf := setupMemoryStorage(store.Config{})
	conf.PartitionStorage = []store.PartitionStorage{
		{
			PartitionStore: store.NewMemoryPartitionStore(conf, log),
			Name:           "memory-0",
			Affinity:       1,
		},
		{
			PartitionStore: store.NewMemoryPartitionStore(conf, log),
			Name:           "memory-1",
			Affinity:       1,
		},
	}
	d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
	defer d.Shutdown(t)
	c, ctx := d.MustClient(), d.Context()

	queueName := random.String("queue-", 10)
	createQueueAndWait(t, ctx, c, &pb.QueueInfo{
		QueueName:           queueName,
		LeaseTimeout:        "1m",
		ExpireTimeout:       "10m",
		RequestedPartitions: 4,
	})

	// Each produce request lands in a single partition (ADR-0019), so one request per
	// partition puts 3 items in every partition.
	for range 4 {
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			Items:          produceRandomItems(3),
			QueueName:      queueName,
			RequestTimeout: "1m",
		}))
	}

	// No public API can tell two backends' contents apart, so this reads each backend the test
	// configured through the PartitionStore interface Querator itself uses. It catches a
	// partition opened on a different backend than the one /queues.info names for it.
	var info pb.QueueInfo
	require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
	require.Len(t, info.Partitions, 4)
	for _, p := range info.Partitions {
		var items []*types.Item
		require.NoError(t, store.Find(p.StorageName, conf.PartitionStorage).PartitionStore.Get(types.PartitionInfo{
			Queue:        types.QueueInfo{Name: queueName},
			PartitionNum: int(p.Partition),
			StorageName:  p.StorageName,
		}).List(ctx, &items, types.ListOptions{Limit: 100}))
		assert.Len(t, items, 3)
	}
	assert.Equal(t, map[string]int{"memory-0": 2, "memory-1": 2}, partitionsPerBackend(t, ctx, c, queueName))

	// Completing removes items, so this runs after the per backend count above.
	clientID := random.String("client-", 10)
	var done int
	for range 12 {
		if done == 12 {
			break
		}
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			QueueName:      queueName,
			ClientId:       clientID,
			RequestTimeout: "5s",
			BatchSize:      12,
		}, &lease))

		var ids []string
		for _, item := range lease.Items {
			ids = append(ids, item.Id)
		}
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Partition:      lease.Partition,
			Ids:            ids,
		}))
		done += len(lease.Items)
	}
	assert.Equal(t, 12, done)

	for partition := range 4 {
		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, partition, &list, nil))
		assert.Empty(t, list.Items)
	}
}

func TestPartitionSpread(t *testing.T) {
	defer goleak.VerifyNone(t, goleakOptions...)

	for _, test := range []struct {
		Name       string
		Affinities []float64
		Partitions int32
		Expected   map[string]int
	}{
		{
			Name:       "EqualAffinity",
			Affinities: []float64{1, 1},
			Partitions: 4,
			Expected:   map[string]int{"memory-0": 2, "memory-1": 2},
		},
		{
			Name:       "UnevenPartitionCount",
			Affinities: []float64{1, 1},
			Partitions: 3,
			Expected:   map[string]int{"memory-0": 2, "memory-1": 1},
		},
		{
			Name:       "FivePartitionsOnThreeEqualBackends",
			Affinities: []float64{1, 1, 1},
			Partitions: 5,
			Expected:   map[string]int{"memory-0": 2, "memory-1": 2, "memory-2": 1},
		},
		{
			Name:       "FractionalAffinity",
			Affinities: []float64{1, 0.5, 0.3, 0.1},
			Partitions: 10,
			Expected:   map[string]int{"memory-0": 5, "memory-1": 3, "memory-2": 2},
		},
		{
			Name:       "ZeroAffinity",
			Affinities: []float64{1, 0},
			Partitions: 4,
			Expected:   map[string]int{"memory-0": 4},
		},
		{
			Name:       "WeightedAffinity",
			Affinities: []float64{3, 1},
			Partitions: 4,
			Expected:   map[string]int{"memory-0": 3, "memory-1": 1},
		},
		{
			Name:       "FractionalShareGoesByQueueName",
			Affinities: []float64{1, 2},
			Partitions: 4,
			Expected:   map[string]int{"memory-0": 1, "memory-1": 3},
		},
		{
			Name:       "FractionalSharesSkipZeroAffinity",
			Affinities: []float64{0, 1, 1, 1},
			Partitions: 4,
			Expected:   map[string]int{"memory-1": 2, "memory-2": 1, "memory-3": 1},
		},
		{
			Name:       "NegativeAffinity",
			Affinities: []float64{2, -1},
			Partitions: 4,
			Expected:   map[string]int{"memory-0": 4},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			conf := setupMemoryStorage(store.Config{})
			conf.PartitionStorage = nil
			for i, affinity := range test.Affinities {
				conf.PartitionStorage = append(conf.PartitionStorage, store.PartitionStorage{
					PartitionStore: store.NewMemoryPartitionStore(conf, log),
					Name:           fmt.Sprintf("memory-%d", i),
					Affinity:       affinity,
				})
			}
			d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
			defer d.Shutdown(t)
			c, ctx := d.MustClient(), d.Context()

			// The queue name is part of the placement hash, so a fixed name gives a fixed spread
			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				QueueName:           "queue-spread",
				LeaseTimeout:        "1m",
				ExpireTimeout:       "10m",
				RequestedPartitions: test.Partitions,
			})
			assert.Equal(t, test.Expected, partitionsPerBackend(t, ctx, c, "queue-spread"))
		})
	}
}

// partitionsPerBackend counts the partitions of a queue held by each storage backend, as reported by /queues.info
func partitionsPerBackend(t *testing.T, ctx context.Context, c *querator.Client, queueName string) map[string]int {
	t.Helper()
	var info pb.QueueInfo
	require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
	counts := make(map[string]int)
	for _, p := range info.Partitions {
		counts[p.StorageName]++
	}
	return counts
}
