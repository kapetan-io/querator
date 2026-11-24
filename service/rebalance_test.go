package service_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/duh-rpc/duh-go/retry"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRebalance(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}
	mongo := mongoTestSetup{}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
		Lifetime storageLifetime
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
				return badgerdb.Setup(store.BadgerConfig{})
			},
			TearDown: func() {
				badgerdb.Teardown()
			},
			Lifetime: outlivesDaemon,
		},
		{
			Name: "PostgreSQL",
			Setup: func() store.Config {
				return postgres.Setup(store.PostgresConfig{})
			},
			TearDown: func() {
				postgres.Teardown()
			},
			Lifetime: outlivesDaemon,
		},
		{
			Name: "MongoDB",
			Setup: func() store.Config {
				return mongo.Setup(store.MongoConfig{})
			},
			TearDown: func() {
				mongo.Teardown()
			},
			Lifetime: outlivesDaemon,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testRebalance(t, tc.Setup, tc.TearDown, tc.Lifetime)
		})
	}
}

const extraBackend = "memory-extra"

// rebalanceCounts is what one queues.rebalance call replied.
type rebalanceCounts struct {
	ChangedQueues      int32
	CreatedPartitions  int32
	DrainingPartitions int32
}

func rebalance(t *testing.T, ctx context.Context, c *querator.Client) rebalanceCounts {
	t.Helper()
	var resp pb.QueuesRebalanceResponse
	require.NoError(t, c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &resp))
	require.NotNil(t, resp.QueuesChanged)
	require.NotNil(t, resp.PartitionsCreated)
	require.NotNil(t, resp.PartitionsDraining)
	return rebalanceCounts{
		ChangedQueues:      resp.GetQueuesChanged(),
		CreatedPartitions:  resp.GetPartitionsCreated(),
		DrainingPartitions: resp.GetPartitionsDraining(),
	}
}

// listedPartition is one partition as queues.info lists it.
type listedPartition struct {
	Partition   int32
	StorageName string
	State       string
}

func listedPartitions(t *testing.T, ctx context.Context, c *querator.Client, queueName string) []listedPartition {
	t.Helper()
	var info pb.QueueInfo
	require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
	listed := make([]listedPartition, 0, len(info.Partitions))
	for _, p := range info.Partitions {
		listed = append(listed, listedPartition{Partition: p.Partition, StorageName: p.StorageName, State: p.State})
	}
	return listed
}

func availableOn(listed []listedPartition, storageName string) []int32 {
	var numbers []int32
	for _, p := range listed {
		if p.StorageName == storageName && p.State == "available" {
			numbers = append(numbers, p.Partition)
		}
	}
	return numbers
}

func drainingOn(listed []listedPartition, storageName string) []int32 {
	var numbers []int32
	for _, p := range listed {
		if p.StorageName == storageName && p.State == "draining" {
			numbers = append(numbers, p.Partition)
		}
	}
	return numbers
}

func setAffinity(t *testing.T, ctx context.Context, c *querator.Client, backend string, affinity float64) {
	t.Helper()
	require.NoError(t, c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
		Backend:  backend,
		Affinity: new(affinity),
	}))
}

func setPartitionCount(t *testing.T, ctx context.Context, c *querator.Client, queueName string, count int32) {
	t.Helper()
	require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
		QueueName:           queueName,
		RequestedPartitions: count,
	}))
}

func createQueueWithPartitions(t *testing.T, ctx context.Context, c *querator.Client, queueName string, count int32) {
	t.Helper()
	createQueueAndWait(t, ctx, c, &pb.QueueInfo{
		QueueName:           queueName,
		LeaseTimeout:        "1m0s",
		ExpireTimeout:       "24h0m0s",
		RequestedPartitions: count,
	})
}

// queueTotal is the number of items in every partition of the queue.
func queueTotal(t *testing.T, ctx context.Context, c *querator.Client, queueName string) int32 {
	t.Helper()
	var stats pb.QueueStatsResponse
	require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
	var total int32
	for _, logical := range stats.LogicalQueues {
		for _, p := range logical.Partitions {
			total += p.Total
		}
	}
	return total
}

func leaseBatch(t *testing.T, ctx context.Context, c *querator.Client, queueName string, size int32) *pb.QueueLeaseResponse {
	t.Helper()
	var lease pb.QueueLeaseResponse
	require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
		ClientId:       random.String("client-", 10),
		RequestTimeout: "3s",
		QueueName:      queueName,
		BatchSize:      size,
	}, &lease))
	return &lease
}

func leaseAndCompleteOne(t *testing.T, ctx context.Context, c *querator.Client, queueName string) string {
	t.Helper()
	lease := leaseBatch(t, ctx, c, queueName, 1)
	require.Len(t, lease.Items, 1)
	require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
		QueueName:      queueName,
		Partition:      lease.Partition,
		RequestTimeout: "5s",
		Ids:            []string{lease.Items[0].Id},
	}))
	return lease.Items[0].Reference
}

// moveOnlyPartition gives the first backend affinity 0 and the extra backend affinity 1, then
// rebalances, so a queue whose one partition is on the first backend gets a draining partition
// there and an available one on the extra backend.
func moveOnlyPartition(t *testing.T, ctx context.Context, c *querator.Client, first string) rebalanceCounts {
	t.Helper()
	setAffinity(t, ctx, c, extraBackend, 1)
	setAffinity(t, ctx, c, first, 0)
	return rebalance(t, ctx, c)
}

var errSaveFailed = errors.New("queue record save failed by test")

// queueStoreFault fails the save of one queue's partition list, as a queue store does when it
// goes down part way through a rebalance.
type queueStoreFault struct {
	failing atomic.Pointer[string]
}

func (f *queueStoreFault) Wrap(conf store.Config) store.Config {
	conf.Queues = faultyQueues{Queues: conf.Queues, fault: f}
	return conf
}

func (f *queueStoreFault) FailSavesOf(queueName string) {
	f.failing.Store(&queueName)
}

func (f *queueStoreFault) AllowSaves() {
	f.failing.Store(nil)
}

type faultyQueues struct {
	store.Queues
	fault *queueStoreFault
}

func (f faultyQueues) SavePartitions(ctx context.Context, list types.PartitionList) error {
	if failing := f.fault.failing.Load(); failing != nil && *failing == list.QueueName {
		return errSaveFailed
	}
	return f.Queues.SavePartitions(ctx, list)
}

func testRebalance(t *testing.T, setup NewStorageFunc, tearDown func(), lifetime storageLifetime) {
	t.Run("ManyQueues", func(t *testing.T) {
		// 1200 queues cross the 1,000 queue list page but take minutes to create, so CI runs 5
		queueCount := 5
		if env := os.Getenv("QUERATOR_REBALANCE_QUEUES"); env != "" {
			var err error
			queueCount, err = strconv.Atoi(env)
			require.NoError(t, err)
		}
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 10*clock.Minute, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		prefix := random.String("many-", 6)
		queueNames := make([]string, 0, queueCount)
		for i := range queueCount {
			queueName := fmt.Sprintf("%s-%04d", prefix, i)
			require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           queueName,
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       "24h0m0s",
				RequestedPartitions: 1,
			}))
			setPartitionCount(t, ctx, c, queueName, 2)
			queueNames = append(queueNames, queueName)
		}

		start := clock.Now()
		counts := rebalance(t, ctx, c)
		t.Logf("rebalance of %d queues took %s", queueCount, clock.Since(start))

		assert.Equal(t, rebalanceCounts{ChangedQueues: int32(queueCount), CreatedPartitions: int32(queueCount)}, counts)
		for _, queueName := range queueNames {
			listed := listedPartitions(t, ctx, c, queueName)
			require.Equal(t, []int32{0, 1}, availableOn(listed, name), queueName)
			require.Empty(t, drainingOn(listed, name), queueName)
		}
		assert.Equal(t, rebalanceCounts{}, rebalance(t, ctx, c))
	})

	t.Run("ItemCountDoesNotSlowRebalance", func(t *testing.T) {
		const (
			smallItems = 1_000
			largeItems = 100_000
			rounds     = 4
		)
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 10*clock.Minute, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		fill := func(items int) string {
			queueName := random.String("items-", 10)
			createQueueWithPartitions(t, ctx, c, queueName, 1)
			for range items / 1_000 {
				require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
					Items:          produceRandomItems(1_000),
					QueueName:      queueName,
					RequestTimeout: "1m",
				}))
			}
			return queueName
		}
		small, large := fill(smallItems), fill(largeItems)

		// Each round grows one queue by a partition, so the call changes exactly that queue
		fastest := map[string]clock.Duration{}
		for round := range rounds {
			for _, queueName := range []string{small, large} {
				setPartitionCount(t, ctx, c, queueName, int32(round+2))
				start := clock.Now()
				assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 1}, rebalance(t, ctx, c))
				elapsed := clock.Since(start)
				if best, ok := fastest[queueName]; !ok || elapsed < best {
					fastest[queueName] = elapsed
				}
			}
		}
		t.Logf("rebalance with %d items took %s, with %d items took %s",
			smallItems, fastest[small], largeItems, fastest[large])

		// A rebalance that read the items would take as long as a stats call scanning them
		assert.Less(t, fastest[large], fastest[small]+250*clock.Millisecond)
		assert.Equal(t, int32(smallItems), queueTotal(t, ctx, c, small))
		assert.Equal(t, int32(largeItems), queueTotal(t, ctx, c, large))
	})

	t.Run("CountChangeWaitsForRebalance", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 2)

		// A saved count changes nothing until the rebalance
		setPartitionCount(t, ctx, c, queueName, 4)
		assert.Equal(t, []int32{0, 1}, availableOn(listedPartitions(t, ctx, c, queueName), name))

		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 2}, rebalance(t, ctx, c))
		listed := listedPartitions(t, ctx, c, queueName)
		assert.Equal(t, []int32{0, 1, 2, 3}, availableOn(listed, name))
		assert.Empty(t, drainingOn(listed, name))

		// Lowering the count marks the surplus draining and keeps it listed
		setPartitionCount(t, ctx, c, queueName, 1)
		assert.Equal(t, []int32{0, 1, 2, 3}, availableOn(listedPartitions(t, ctx, c, queueName), name))
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, DrainingPartitions: 3}, rebalance(t, ctx, c))
		listed = listedPartitions(t, ctx, c, queueName)
		assert.Equal(t, []int32{0}, availableOn(listed, name))
		assert.Equal(t, []int32{1, 2, 3}, drainingOn(listed, name))

		// Growing again never reuses the number of a draining partition
		setPartitionCount(t, ctx, c, queueName, 3)
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 2}, rebalance(t, ctx, c))
		listed = listedPartitions(t, ctx, c, queueName)
		assert.Equal(t, []int32{0, 4, 5}, availableOn(listed, name))
		assert.Equal(t, []int32{1, 2, 3}, drainingOn(listed, name))

		// A second rebalance has nothing left to do
		assert.Equal(t, rebalanceCounts{}, rebalance(t, ctx, c))
		assert.Equal(t, listed, listedPartitions(t, ctx, c, queueName))

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(6),
		}))
		assert.Equal(t, int32(6), queueTotal(t, ctx, c, queueName))
	})

	t.Run("ZeroAffinityDrainsAndReplaces", func(t *testing.T) {
		conf := withExtraBackend(setup(), extraBackend, 1, nil)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 2)
		before := listedPartitions(t, ctx, c, queueName)
		onExtra := availableOn(before, extraBackend)
		onMain := availableOn(before, name)
		require.Len(t, onExtra, 1)
		require.Len(t, onMain, 1)

		setAffinity(t, ctx, c, extraBackend, 0)
		assert.Equal(t, before, listedPartitions(t, ctx, c, queueName))

		// What storage/backends.list says a rebalance would do is what the rebalance does
		pending := listBackends(t, d)
		drain := findBackend(t, pending, extraBackend).Pending.Drain
		require.Len(t, drain, 1)
		assert.Equal(t, onExtra, drain[0].Partitions)
		create := findBackend(t, pending, name).Pending.Create
		require.Len(t, create, 1)
		assert.Equal(t, []int32{2}, create[0].Partitions)

		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 1, DrainingPartitions: 1},
			rebalance(t, ctx, c))

		// The first queues.info after the rebalance shows the old partition draining and its
		// replacement available
		listed := listedPartitions(t, ctx, c, queueName)
		assert.Equal(t, onExtra, drainingOn(listed, extraBackend))
		assert.Empty(t, availableOn(listed, extraBackend))
		assert.Equal(t, []int32{onMain[0], 2}, availableOn(listed, name))

		for _, b := range listBackends(t, d).Backends {
			assert.Empty(t, b.Pending.Create)
			assert.Empty(t, b.Pending.Drain)
		}

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(4),
		}))
		assert.Equal(t, int32(4), queueTotal(t, ctx, c, queueName))

		// Raising the affinity again creates a new partition, and the draining one stays draining
		setAffinity(t, ctx, c, extraBackend, 1)
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 1, DrainingPartitions: 1},
			rebalance(t, ctx, c))
		listed = listedPartitions(t, ctx, c, queueName)
		assert.Equal(t, onExtra, drainingOn(listed, extraBackend))
		assert.Equal(t, []int32{3}, availableOn(listed, extraBackend))
		assert.Len(t, availableOn(listed, name), 1)
		assert.Len(t, drainingOn(listed, name), 1)
	})

	t.Run("LowerCountAndZeroAffinityCreatesNothing", func(t *testing.T) {
		conf := withExtraBackend(setup(), extraBackend, 1, nil)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 4)
		before := listedPartitions(t, ctx, c, queueName)
		onMain := availableOn(before, name)
		require.Len(t, onMain, 2)

		// The remaining backend already holds the two partitions the lower count calls for
		setPartitionCount(t, ctx, c, queueName, 2)
		setAffinity(t, ctx, c, extraBackend, 0)
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, DrainingPartitions: 2}, rebalance(t, ctx, c))

		listed := listedPartitions(t, ctx, c, queueName)
		require.Len(t, listed, 4)
		assert.Equal(t, onMain, availableOn(listed, name))
		assert.Len(t, drainingOn(listed, extraBackend), 2)
	})

	t.Run("UnequalAffinityShrinkAndGrow", func(t *testing.T) {
		conf := withExtraBackend(setup(), extraBackend, 3, nil)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 8)
		listed := listedPartitions(t, ctx, c, queueName)
		require.Len(t, availableOn(listed, name), 2)
		require.Len(t, availableOn(listed, extraBackend), 6)

		setPartitionCount(t, ctx, c, queueName, 4)
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, DrainingPartitions: 4}, rebalance(t, ctx, c))
		listed = listedPartitions(t, ctx, c, queueName)
		assert.Len(t, availableOn(listed, name), 1)
		assert.Len(t, availableOn(listed, extraBackend), 3)

		setPartitionCount(t, ctx, c, queueName, 8)
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 4}, rebalance(t, ctx, c))
		listed = listedPartitions(t, ctx, c, queueName)
		assert.Len(t, listed, 12)
		assert.Len(t, availableOn(listed, name), 2)
		assert.Len(t, availableOn(listed, extraBackend), 6)
	})

	t.Run("CompleteAndRetryFromDrainingPartition", func(t *testing.T) {
		conf := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 1)
		produceInOrder(t, ctx, c, queueName, 2)
		lease := leaseBatch(t, ctx, c, queueName, 2)
		require.Len(t, lease.Items, 2)
		require.Equal(t, int32(0), lease.Partition)

		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 1, DrainingPartitions: 1},
			moveOnlyPartition(t, ctx, c, conf.PartitionStorage[0].Name))

		// The loaded queue still counts both leased items in the partition it held before the rebalance
		var stats pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
		require.Len(t, stats.LogicalQueues, 1)
		require.Len(t, stats.LogicalQueues[0].Partitions, 2)
		assert.Equal(t, int32(2), stats.LogicalQueues[0].Partitions[0].Total)
		assert.Equal(t, int32(2), stats.LogicalQueues[0].Partitions[0].TotalLeased)
		assert.Equal(t, int32(0), stats.LogicalQueues[0].Partitions[1].Total)
		assert.Equal(t, int32(0), stats.LogicalQueues[0].Partitions[1].TotalLeased)

		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      lease.Partition,
			RequestTimeout: "5s",
			Ids:            []string{lease.Items[0].Id},
		}))
		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: lease.Partition,
			Items:     []*pb.QueueRetryItem{{Id: lease.Items[1].Id}},
		}))

		// The retried item is still in the draining partition and is leased from there
		again := leaseBatch(t, ctx, c, queueName, 1)
		require.Len(t, again.Items, 1)
		assert.Equal(t, int32(0), again.Partition)
		assert.Equal(t, lease.Items[1].Reference, again.Items[0].Reference)
	})

	t.Run("OnePartitionQueueWaitsForBackendGoneFromConfig", func(t *testing.T) {
		// The InMemory queue store keeps no record of the queue across a restart
		if lifetime == endsWithDaemon {
			return
		}
		const gone = "memory-gone"
		conf := withExtraBackend(setup(), gone, 0, nil)
		defer tearDown()
		main := conf.PartitionStorage[0].Name
		first := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		c, ctx := first.MustClient(), first.Context()

		queueName := random.String("queue-", 10)
		setAffinity(t, ctx, c, gone, 1)
		setAffinity(t, ctx, c, main, 0)
		createQueueWithPartitions(t, ctx, c, queueName, 1)
		require.Equal(t, []int32{0}, availableOn(listedPartitions(t, ctx, c, queueName), gone))
		setAffinity(t, ctx, c, main, 1)
		setAffinity(t, ctx, c, gone, 0)
		first.Shutdown(t)

		// The backend that holds the queue's only partition is taken out of the config
		conf.PartitionStorage = conf.PartitionStorage[:1]
		second := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer second.Shutdown(t)
		c, ctx = second.MustClient(), second.Context()

		// Before the rebalance the queue's only partition cannot be read, so produce is refused
		err := c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "2s",
			Items:          []*pb.QueueProduceItem{{Reference: "before-move", Bytes: []byte("x")}},
		})
		var refused duh.Error
		require.ErrorAs(t, err, &refused)
		assert.Equal(t, duh.CodeRetryRequest, refused.Code())

		drain := findBackend(t, listBackends(t, second), gone).Pending.Drain
		require.Len(t, drain, 1)
		assert.Equal(t, queueName, drain[0].QueueName)
		assert.Equal(t, []int32{0}, drain[0].Partitions)

		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 1, DrainingPartitions: 1},
			rebalance(t, ctx, c))
		listed := listedPartitions(t, ctx, c, queueName)
		assert.Equal(t, []int32{0}, drainingOn(listed, gone))
		assert.Equal(t, []int32{1}, availableOn(listed, main))

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          []*pb.QueueProduceItem{{Reference: "after-move", Bytes: []byte("x")}},
		}))

		// The item waits until the backend is restored, since the partition it left may hold older items
		var lease pb.QueueLeaseResponse
		err = c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "1s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &lease)
		var e duh.Error
		require.ErrorAs(t, err, &e)
		assert.Equal(t, querator.MsgRequestTimeout, e.Message())
		assert.Equal(t, duh.CodeRetryRequest, e.Code())
		assert.Empty(t, lease.Items)
	})

	t.Run("DrainingPartitionStaysDrainingAfterRestart", func(t *testing.T) {
		// The InMemory queue store keeps no record of the queue across a restart
		if lifetime == endsWithDaemon {
			return
		}
		conf := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		main := conf.PartitionStorage[0].Name
		first := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		c, ctx := first.MustClient(), first.Context()

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 1)
		produceInOrder(t, ctx, c, queueName, 2)
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 1, DrainingPartitions: 1},
			moveOnlyPartition(t, ctx, c, main))
		first.Shutdown(t)

		second := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer second.Shutdown(t)
		c, ctx = second.MustClient(), second.Context()

		listed := listedPartitions(t, ctx, c, queueName)
		assert.Equal(t, []int32{0}, drainingOn(listed, main))
		assert.Equal(t, []int32{1}, availableOn(listed, extraBackend))
		assert.Equal(t, rebalanceCounts{}, rebalance(t, ctx, c))

		// A produce after the restart still stays out of the draining partition
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          []*pb.QueueProduceItem{{Reference: "after-restart", Bytes: []byte("x")}},
		}))
		drained := leaseBatch(t, ctx, c, queueName, 3)
		require.Len(t, drained.Items, 2)
		assert.Equal(t, int32(0), drained.Partition)
		assert.Equal(t, "item-00000", drained.Items[0].Reference)
		assert.Equal(t, "item-00001", drained.Items[1].Reference)
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      drained.Partition,
			RequestTimeout: "5s",
			Ids:            []string{drained.Items[0].Id, drained.Items[1].Id},
		}))

		moved := leaseBatch(t, ctx, c, queueName, 3)
		require.Len(t, moved.Items, 1)
		assert.Equal(t, int32(1), moved.Partition)
		assert.Equal(t, "after-restart", moved.Items[0].Reference)
	})

	t.Run("OnePartitionQueueKeepsOrderAfterMove", func(t *testing.T) {
		conf := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 1)
		produceInOrder(t, ctx, c, queueName, 2)

		moveOnlyPartition(t, ctx, c, conf.PartitionStorage[0].Name)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          []*pb.QueueProduceItem{{Reference: "after-move", Bytes: []byte("x")}},
		}))

		assert.Equal(t, "item-00000", leaseAndCompleteOne(t, ctx, c, queueName))
		assert.Equal(t, "item-00001", leaseAndCompleteOne(t, ctx, c, queueName))
		assert.Equal(t, "after-move", leaseAndCompleteOne(t, ctx, c, queueName))
	})

	t.Run("OnePartitionQueueWaitsForDownBackend", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		fault := &partitionFault{}
		conf.PartitionStorage = fault.Wrap(store.Config{PartitionStorage: conf.PartitionStorage}).PartitionStorage
		conf = withExtraBackend(conf, extraBackend, 0, nil)
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 1)
		produceInOrder(t, ctx, c, queueName, 1)
		moveOnlyPartition(t, ctx, c, conf.PartitionStorage[0].Name)

		// The backend holding the draining partition goes down
		fault.statsFailing.Store(true)
		require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{QueueName: queueName}))

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          []*pb.QueueProduceItem{{Reference: "after-move", Bytes: []byte("x")}},
		}))

		// The newer item waits behind the item the down backend holds
		var lease pb.QueueLeaseResponse
		err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "1s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &lease)
		var e duh.Error
		require.ErrorAs(t, err, &e)
		assert.Equal(t, querator.MsgRequestTimeout, e.Message())
		assert.Equal(t, duh.CodeRetryRequest, e.Code())
		assert.Empty(t, lease.Items)

		fault.statsFailing.Store(false)
		require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{QueueName: queueName}))
		assert.Equal(t, "item-00000", leaseAndCompleteOne(t, ctx, c, queueName))
		assert.Equal(t, "after-move", leaseAndCompleteOne(t, ctx, c, queueName))
	})

	t.Run("DeadLetterItemStoredOnceWithDrainingPartition", func(t *testing.T) {
		// A source sends a dead item a second time only when a restart made it forget the dead
		// letter queue already stored the item, and an InMemory store holds nothing after a restart.
		if lifetime == endsWithDaemon {
			return
		}
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		stores := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		first := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(stores), Clock: now})
		c, ctx := first.MustClient(), first.Context()

		queueName := random.String("queue-", 10)
		dlqName := random.String("dlq-", 10)
		createOnePartitionQueue(t, ctx, c, dlqName)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			MaxAttempts:         1,
			DeadQueue:           dlqName,
			RequestedPartitions: 1,
		})
		produceInOrder(t, ctx, c, queueName, 1)
		require.Len(t, leaseUpTo(t, ctx, c, queueName, 1), 1)

		// The dead letter queue stores the item and the source fails to delete it
		spy.FailNextItemDelete()
		now.Advance(clock.Minute + clock.Second)
		require.Eventually(t, func() bool {
			return queueTotal(t, ctx, c, dlqName) == 1
		}, 5*clock.Second, 20*clock.Millisecond)

		// The partition holding the stored item becomes draining, and the daemon stops before the
		// source deletes its item
		moveOnlyPartition(t, ctx, c, stores.PartitionStorage[0].Name)
		assert.Len(t, listedPartitions(t, ctx, c, dlqName), 2)
		require.Equal(t, int32(1), queueTotal(t, ctx, c, queueName))
		first.Shutdown(t)

		second := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: stores, Clock: now})
		defer second.Shutdown(t)
		c, ctx = second.MustClient(), second.Context()

		// The restarted source sends the item again, and the dead letter queue would store it in
		// its available partition if it did not look in the draining one
		require.Equal(t, int32(1), queueTotal(t, ctx, c, dlqName))
		require.Equal(t, int32(1), queueTotal(t, ctx, c, queueName))
		now.Advance(10 * clock.Second)
		require.Eventually(t, func() bool {
			return queueTotal(t, ctx, c, queueName) == 0
		}, 5*clock.Second, 20*clock.Millisecond)
		clock.Sleep(requestSettle)
		assert.Equal(t, int32(1), queueTotal(t, ctx, c, dlqName))
	})

	t.Run("TwoRebalancesAtOnce", func(t *testing.T) {
		conf := withExtraBackend(setup(), extraBackend, 1, nil)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 4)
		setAffinity(t, ctx, c, extraBackend, 0)

		var replies [2]pb.QueuesRebalanceResponse
		var errs [2]error
		concurrently(2, func(i int) {
			errs[i] = c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &replies[i])
		})
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])

		// Together the two calls did what one call does
		assert.Equal(t, int32(1), replies[0].GetQueuesChanged()+replies[1].GetQueuesChanged())
		assert.Equal(t, int32(2), replies[0].GetPartitionsCreated()+replies[1].GetPartitionsCreated())
		assert.Equal(t, int32(2), replies[0].GetPartitionsDraining()+replies[1].GetPartitionsDraining())

		listed := listedPartitions(t, ctx, c, queueName)
		require.Len(t, listed, 6)
		assert.Len(t, availableOn(listed, name), 4)
		assert.Len(t, drainingOn(listed, extraBackend), 2)
		assert.Equal(t, rebalanceCounts{}, rebalance(t, ctx, c))
	})

	t.Run("UpdateDuringRebalanceShowsInQueuesInfo", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 1)

		// Each round saves a rebalance and an update of the same queue at once, and the queue must
		// then show the later of the two saves
		const rounds = 15
		for round := 1; round <= rounds; round++ {
			reference := fmt.Sprintf("reference-%d", round)
			setPartitionCount(t, ctx, c, queueName, int32(round+1))

			var errs [2]error
			concurrently(2, func(i int) {
				if i == 0 {
					errs[i] = c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &pb.QueuesRebalanceResponse{})
					return
				}
				errs[i] = retry.On(ctx, retry.OnRetryable, func(ctx context.Context, _ int) error {
					return c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{QueueName: queueName, Reference: new(reference)})
				})
			})
			require.NoError(t, errs[0])
			require.NoError(t, errs[1])

			var info pb.QueueInfo
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
			require.Equal(t, reference, info.Reference)
			require.Len(t, info.Partitions, round+1)
		}
	})

	t.Run("RebalanceWhileQueuesAreCreated", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		// A rebalance opens new partitions while new queues open theirs on the same backend
		const existing, created = 5, 10
		prefix := random.String("queue-", 10)
		for i := range existing {
			queueName := fmt.Sprintf("%s-existing-%d", prefix, i)
			createQueueWithPartitions(t, ctx, c, queueName, 1)
			setPartitionCount(t, ctx, c, queueName, 3)
		}

		var errs [created + 1]error
		concurrently(created+1, func(i int) {
			if i == created {
				errs[i] = c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &pb.QueuesRebalanceResponse{})
				return
			}
			errs[i] = c.QueuesCreate(ctx, &pb.QueueInfo{
				QueueName:           fmt.Sprintf("%s-created-%d", prefix, i),
				LeaseTimeout:        "1m0s",
				ExpireTimeout:       "24h0m0s",
				RequestedPartitions: 2,
			})
		})
		for _, err := range errs {
			require.NoError(t, err)
		}

		for i := range existing {
			queueName := fmt.Sprintf("%s-existing-%d", prefix, i)
			assert.Equal(t, []int32{0, 1, 2}, availableOn(listedPartitions(t, ctx, c, queueName), name))
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          produceRandomItems(3),
			}))
			assert.Equal(t, int32(3), queueTotal(t, ctx, c, queueName))
		}
	})

	t.Run("QueueThatMissedARebalanceIsToldByTheNext", func(t *testing.T) {
		var fault partitionFault
		conf := withExtraBackend(fault.Wrap(setup()), extraBackend, 0, nil)
		defer tearDown()
		d := newDaemon(t, 20*clock.Second, svc.Config{
			StorageConfig:       conf,
			WriteTimeout:        300 * clock.Millisecond,
			MaxRequestsPerQueue: 1,
		})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 1)
		produceInOrder(t, ctx, c, queueName, 2)
		setAffinity(t, ctx, c, extraBackend, 1)
		setAffinity(t, ctx, c, conf.PartitionStorage[0].Name, 0)

		// A held clear keeps the queue busy, and one waiting produce fills its inbox, so the
		// rebalance saves the record and cannot tell the loaded queue
		hold := &clearHold{Reached: make(chan struct{}), Release: make(chan struct{})}
		fault.holdClear.Store(hold)
		var cleared, rebalanced error
		backgroundCall{
			Call: func() {
				cleared = c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true})
			},
			While: func() {
				hold.WaitReached(t, ctx)
				short, cancel := context.WithTimeout(ctx, 200*clock.Millisecond)
				defer cancel()
				require.Error(t, c.QueueProduce(short, &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5s",
					Items:          []*pb.QueueProduceItem{{Reference: "fills-the-inbox", Bytes: []byte("x")}},
				}))
				rebalanced = c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &pb.QueuesRebalanceResponse{})
				close(hold.Release)
			},
		}.run()
		require.NoError(t, cleared)
		require.Error(t, rebalanced)
		fault.holdClear.Store(nil)

		// Nothing is left to change, and the queue is told what the first call saved
		assert.Equal(t, rebalanceCounts{}, rebalance(t, ctx, c))
		listed := listedPartitions(t, ctx, c, queueName)
		assert.Equal(t, []int32{0}, drainingOn(listed, conf.PartitionStorage[0].Name))
		assert.Equal(t, []int32{1}, availableOn(listed, extraBackend))

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          []*pb.QueueProduceItem{{Reference: "after-the-tell", Bytes: []byte("x")}},
		}))
		var moved pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 1, &moved, &querator.ListOptions{Limit: 10}))
		require.Len(t, moved.Items, 1)
		assert.Equal(t, "after-the-tell", moved.Items[0].Reference)
	})

	t.Run("StoppedRebalanceIsFinishedByCallingAgain", func(t *testing.T) {
		fault := &queueStoreFault{}
		conf := fault.Wrap(setup())
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		// Rebalance visits queues in name order, so it changes the first and stops at the second
		suffix := random.String("", 10)
		firstQueue, secondQueue := "queue-a-"+suffix, "queue-b-"+suffix
		createQueueWithPartitions(t, ctx, c, firstQueue, 2)
		createQueueWithPartitions(t, ctx, c, secondQueue, 2)
		setPartitionCount(t, ctx, c, firstQueue, 3)
		setPartitionCount(t, ctx, c, secondQueue, 3)

		fault.FailSavesOf(secondQueue)
		err := c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &pb.QueuesRebalanceResponse{})
		var e duh.Error
		require.ErrorAs(t, err, &e)
		assert.Equal(t, duh.CodeInternalError, e.Code())

		// The queue whose save failed lists what it listed before
		assert.Equal(t, []int32{0, 1, 2}, availableOn(listedPartitions(t, ctx, c, firstQueue), name))
		assert.Equal(t, []int32{0, 1}, availableOn(listedPartitions(t, ctx, c, secondQueue), name))

		fault.AllowSaves()
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 1}, rebalance(t, ctx, c))
		assert.Equal(t, []int32{0, 1, 2}, availableOn(listedPartitions(t, ctx, c, firstQueue), name))
		assert.Equal(t, []int32{0, 1, 2}, availableOn(listedPartitions(t, ctx, c, secondQueue), name))
	})

	t.Run("DrainingPartitionTakesNoProduceAndIsLeasedFirst", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 2)
		for range 6 {
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          produceRandomItems(1),
			}))
		}

		setPartitionCount(t, ctx, c, queueName, 1)
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, DrainingPartitions: 1}, rebalance(t, ctx, c))
		require.Equal(t, []int32{1}, drainingOn(listedPartitions(t, ctx, c, queueName), name))

		var before pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &before))
		require.Len(t, before.LogicalQueues, 1)
		require.Len(t, before.LogicalQueues[0].Partitions, 2)
		require.Equal(t, int32(1), before.LogicalQueues[0].Partitions[1].Partition)
		held := before.LogicalQueues[0].Partitions[1].Total
		require.Positive(t, held)
		require.Equal(t, int32(6), before.LogicalQueues[0].Partitions[0].Total+held)

		// Every item produced after the rebalance goes to the available partition
		for range 6 {
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          produceRandomItems(1),
			}))
		}
		var after pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &after))
		require.Len(t, after.LogicalQueues[0].Partitions, 2)
		assert.Equal(t, held, after.LogicalQueues[0].Partitions[1].Total)
		assert.Equal(t, 12-held, after.LogicalQueues[0].Partitions[0].Total)

		// Consumers lease from the draining partition until it is empty, though the available
		// partition holds more items
		for range held {
			lease := leaseBatch(t, ctx, c, queueName, 1)
			require.Len(t, lease.Items, 1)
			assert.Equal(t, int32(1), lease.Partition)
			require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				QueueName:      queueName,
				Partition:      lease.Partition,
				RequestTimeout: "5s",
				Ids:            []string{lease.Items[0].Id},
			}))
		}
		lease := leaseBatch(t, ctx, c, queueName, 1)
		require.Len(t, lease.Items, 1)
		assert.Equal(t, int32(0), lease.Partition)
	})

	t.Run("FailedSaveLeavesReplacedPartitionAsItWas", func(t *testing.T) {
		fault := &queueStoreFault{}
		conf := withExtraBackend(fault.Wrap(setup()), extraBackend, 1, nil)
		defer tearDown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 2)
		setAffinity(t, ctx, c, extraBackend, 0)
		before := listedPartitions(t, ctx, c, queueName)
		onExtra := availableOn(before, extraBackend)
		require.Len(t, onExtra, 1)

		fault.FailSavesOf(queueName)
		err := c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &pb.QueuesRebalanceResponse{})
		var e duh.Error
		require.ErrorAs(t, err, &e)
		assert.Equal(t, duh.CodeInternalError, e.Code())

		// Neither the draining mark nor the replacement partition shows
		assert.Equal(t, before, listedPartitions(t, ctx, c, queueName))
		pending := listBackends(t, d)
		drain := findBackend(t, pending, extraBackend).Pending.Drain
		require.Len(t, drain, 1)
		assert.Equal(t, onExtra, drain[0].Partitions)
		create := findBackend(t, pending, name).Pending.Create
		require.Len(t, create, 1)
		assert.Equal(t, []int32{2}, create[0].Partitions)

		// The partition the failed rebalance would have marked draining still takes items
		for range 4 {
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          produceRandomItems(1),
			}))
		}
		var stats pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
		require.Len(t, stats.LogicalQueues, 1)
		require.Len(t, stats.LogicalQueues[0].Partitions, 2)
		assert.Equal(t, int32(2), stats.LogicalQueues[0].Partitions[0].Total)
		assert.Equal(t, int32(2), stats.LogicalQueues[0].Partitions[1].Total)

		fault.AllowSaves()
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 1, DrainingPartitions: 1},
			rebalance(t, ctx, c))
		listed := listedPartitions(t, ctx, c, queueName)
		assert.Equal(t, onExtra, drainingOn(listed, extraBackend))
		assert.Len(t, availableOn(listed, name), 2)
	})

	t.Run("ScheduledItemInDrainingPartitionIsDelivered", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		conf := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: conf, Clock: now})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 1)
		produceScheduled(t, ctx, c, queueName, 1, now.Now().Add(clock.Minute))

		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 1, DrainingPartitions: 1},
			moveOnlyPartition(t, ctx, c, conf.PartitionStorage[0].Name))

		// The item is not due, so the draining partition still holds it as scheduled
		var stats pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
		require.Len(t, stats.LogicalQueues, 1)
		require.Len(t, stats.LogicalQueues[0].Partitions, 2)
		assert.Equal(t, int32(1), stats.LogicalQueues[0].Partitions[0].Scheduled)
		assert.Equal(t, int32(0), stats.LogicalQueues[0].Partitions[1].Scheduled)

		now.Advance(clock.Minute + clock.Second)
		require.Eventually(t, func() bool {
			var due pb.QueueStatsResponse
			if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &due); err != nil {
				return false
			}
			return due.LogicalQueues[0].Partitions[0].Scheduled == 0
		}, 10*clock.Second, 50*clock.Millisecond)

		// Once due it is leased from the draining partition, and nothing is left behind
		lease := leaseBatch(t, ctx, c, queueName, 1)
		require.Len(t, lease.Items, 1)
		assert.Equal(t, int32(0), lease.Partition)
		assert.Equal(t, "item-00000", lease.Items[0].Reference)
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      lease.Partition,
			RequestTimeout: "5s",
			Ids:            []string{lease.Items[0].Id},
		}))
		assert.Equal(t, int32(0), queueTotal(t, ctx, c, queueName))
	})

	t.Run("NoItemLostWhileProducedAndLeasedDuringRebalance", func(t *testing.T) {
		conf := withExtraBackend(setup(), extraBackend, 1, nil)
		defer tearDown()
		d := newDaemon(t, 90*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		first := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 4)

		const producers, consumers, batch = 2, 4, 10
		var produced, leased atomic.Int64
		var mu sync.Mutex
		seen := map[string]int{}
		var failures []error
		fail := func(err error) {
			mu.Lock()
			defer mu.Unlock()
			failures = append(failures, err)
		}

		stop := make(chan struct{})
		var producing sync.WaitGroup
		for p := range producers {
			producing.Go(func() {
				for n := 0; ; n++ {
					select {
					case <-stop:
						return
					default:
					}
					items := make([]*pb.QueueProduceItem, batch)
					for i := range items {
						items[i] = &pb.QueueProduceItem{
							Reference: fmt.Sprintf("producer-%d-%05d", p, n*batch+i),
							Bytes:     []byte("x"),
						}
					}
					if err := c.QueueProduce(ctx, &pb.QueueProduceRequest{
						QueueName:      queueName,
						RequestTimeout: "5s",
						Items:          items,
					}); err != nil {
						fail(fmt.Errorf("produce: %w", err))
						return
					}
					produced.Add(batch)
				}
			})
		}

		drained := make(chan struct{})
		var consuming sync.WaitGroup
		for range consumers {
			consuming.Go(func() {
				for {
					select {
					case <-drained:
						return
					default:
					}
					var lease pb.QueueLeaseResponse
					err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
						ClientId:       random.String("client-", 10),
						RequestTimeout: "1s",
						QueueName:      queueName,
						BatchSize:      batch,
					}, &lease)
					var e duh.Error
					if errors.As(err, &e) && e.Code() == duh.CodeRetryRequest {
						continue
					}
					if err != nil {
						fail(fmt.Errorf("lease: %w", err))
						return
					}
					ids := make([]string, 0, len(lease.Items))
					mu.Lock()
					for _, item := range lease.Items {
						seen[item.Reference]++
						ids = append(ids, item.Id)
					}
					mu.Unlock()
					if err := c.QueueComplete(ctx, &pb.QueueCompleteRequest{
						QueueName:      queueName,
						Partition:      lease.Partition,
						RequestTimeout: "5s",
						Ids:            ids,
					}); err != nil {
						fail(fmt.Errorf("complete: %w", err))
						return
					}
					leased.Add(int64(len(ids)))
				}
			})
		}

		moreProduced := func() {
			from := produced.Load()
			require.Eventually(t, func() bool { return produced.Load() >= from+10*batch },
				20*clock.Second, 10*clock.Millisecond)
		}

		// Two rebalances land while items are produced, leased and completed. The first drains the
		// extra backend's partitions, and the second drains the first backend's.
		moreProduced()
		setAffinity(t, ctx, c, extraBackend, 0)
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 2, DrainingPartitions: 2},
			rebalance(t, ctx, c))
		moreProduced()
		setAffinity(t, ctx, c, extraBackend, 1)
		setAffinity(t, ctx, c, first, 0)
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 4, DrainingPartitions: 4},
			rebalance(t, ctx, c))
		moreProduced()

		close(stop)
		producing.Wait()
		assert.Eventually(t, func() bool { return leased.Load() == produced.Load() },
			30*clock.Second, 20*clock.Millisecond)
		close(drained)
		consuming.Wait()

		// Every produce was accepted, and every item was leased once
		require.Empty(t, failures)
		assert.Len(t, seen, int(produced.Load()))
		for _, reference := range slices.Sorted(maps.Keys(seen)) {
			assert.Equal(t, 1, seen[reference], reference)
		}
		assert.Equal(t, int32(0), queueTotal(t, ctx, c, queueName))
	})

	t.Run("RebalanceWhileQueuesAreDeleted", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		name := conf.PartitionStorage[0].Name

		// Every queue is owed two more partitions, and half of them are deleted as the rebalance runs
		const kept, deleted = 5, 10
		prefix := random.String("queue-", 10)
		for i := range kept + deleted {
			queueName := fmt.Sprintf("%s-%02d", prefix, i)
			createQueueWithPartitions(t, ctx, c, queueName, 1)
			setPartitionCount(t, ctx, c, queueName, 3)
		}

		var errs [deleted + 1]error
		concurrently(deleted+1, func(i int) {
			if i == deleted {
				errs[i] = c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &pb.QueuesRebalanceResponse{})
				return
			}
			errs[i] = c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: fmt.Sprintf("%s-%02d", prefix, kept+i)})
		})
		for _, err := range errs {
			require.NoError(t, err)
		}

		for i := range kept {
			queueName := fmt.Sprintf("%s-%02d", prefix, i)
			assert.Equal(t, []int32{0, 1, 2}, availableOn(listedPartitions(t, ctx, c, queueName), name))
		}
		for i := range deleted {
			var info pb.QueueInfo
			err := c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: fmt.Sprintf("%s-%02d", prefix, kept+i)}, &info)
			var e duh.Error
			require.ErrorAs(t, err, &e)
			assert.Equal(t, duh.CodeBadRequest, e.Code())
		}
		assert.Equal(t, rebalanceCounts{}, rebalance(t, ctx, c))
	})

	t.Run("Errors", func(t *testing.T) {
		t.Run("QueuesRebalance", func(t *testing.T) {
			t.Run("NoBackendHasAffinity", func(t *testing.T) {
				conf := setup()
				defer tearDown()
				d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
				defer d.Shutdown(t)
				c, ctx := d.MustClient(), d.Context()
				name := conf.PartitionStorage[0].Name

				queueName := random.String("queue-", 10)
				createQueueWithPartitions(t, ctx, c, queueName, 2)
				setPartitionCount(t, ctx, c, queueName, 3)
				setAffinity(t, ctx, c, name, 0)
				before := listedPartitions(t, ctx, c, queueName)

				err := c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &pb.QueuesRebalanceResponse{})
				var e duh.Error
				require.ErrorAs(t, err, &e)
				assert.Equal(t, duh.CodeBadRequest, e.Code())
				assert.Equal(t, "rebalance is refused; no storage backend can take partitions, at least one "+
					"storage backend needs a saved affinity above 0, see '/v1/storage/backends.update'", e.Message())
				assert.Equal(t, before, listedPartitions(t, ctx, c, queueName))
			})

			t.Run("BackendWithAffinityMissingFromConfig", func(t *testing.T) {
				// An InMemory queue store forgets its backend records when the daemon stops, so it
				// never holds a record for a backend the config file no longer lists.
				if lifetime == endsWithDaemon {
					return
				}
				conf := withExtraBackend(setup(), "memory-removed", 1, nil)
				defer tearDown()
				first := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
				c, ctx := first.MustClient(), first.Context()
				queueName := random.String("queue-", 10)
				createQueueWithPartitions(t, ctx, c, queueName, 2)
				setPartitionCount(t, ctx, c, queueName, 4)
				before := listedPartitions(t, ctx, c, queueName)
				first.Shutdown(t)

				conf.PartitionStorage = conf.PartitionStorage[:1]
				second := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
				defer second.Shutdown(t)
				c, ctx = second.MustClient(), second.Context()

				err := c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &pb.QueuesRebalanceResponse{})
				var e duh.Error
				require.ErrorAs(t, err, &e)
				assert.Equal(t, duh.CodeBadRequest, e.Code())
				assert.Equal(t, "rebalance is refused; storage backend 'memory-removed' has affinity 1 and is "+
					"missing from this instance's config file; add it back or save affinity 0 for it", e.Message())
				assert.Equal(t, before, listedPartitions(t, ctx, c, queueName))
			})
		})
	})
}

func TestRebalancePermission(t *testing.T) {
	d := newDaemonWithAuth(t, func() store.Config { return setupMemoryStorage(store.Config{}) })
	defer d.Shutdown(t)
	ctx := d.Context()
	admin := newClientWithAPIKey(t, d, d.AdminKey)

	queueName := random.String("queue-", 10)
	createQueueWithPartitions(t, ctx, admin, queueName, 1)
	setPartitionCount(t, ctx, admin, queueName, 2)

	t.Run("RefusedWithoutQueueRebalance", func(t *testing.T) {
		limited := newClientWithAPIKey(t, d, CreateUserWithLimitedPermissions(t, ctx, admin,
			[]string{auth.QueueList, auth.QueueUpdate, auth.BackendUpdate}))
		err := limited.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &pb.QueuesRebalanceResponse{})
		var e duh.Error
		require.ErrorAs(t, err, &e)
		assert.Equal(t, duh.CodeForbidden, e.Code())
		assert.Len(t, listedPartitions(t, ctx, admin, queueName), 1)
	})

	t.Run("RefusedWithQueueRebalanceOutsideSystemNamespace", func(t *testing.T) {
		namespace := random.String("tenant-", 10)
		require.NoError(t, admin.NamespacesCreate(ctx, &pb.NamespaceInfo{Name: namespace}))
		scoped := newClientWithAPIKey(t, d, CreateUserWithNamespacePermissions(t, ctx, admin, namespace,
			[]string{auth.QueueRebalance}))
		err := scoped.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &pb.QueuesRebalanceResponse{})
		var e duh.Error
		require.ErrorAs(t, err, &e)
		assert.Equal(t, duh.CodeForbidden, e.Code())
		assert.Len(t, listedPartitions(t, ctx, admin, queueName), 1)
	})

	t.Run("GrantedQueueRebalance", func(t *testing.T) {
		granted := newClientWithAPIKey(t, d, CreateUserWithLimitedPermissions(t, ctx, admin,
			[]string{auth.QueueRebalance}))
		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 1}, rebalance(t, ctx, granted))
		assert.Len(t, listedPartitions(t, ctx, admin, queueName), 2)
	})
}
