package service_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDrainRemoval(t *testing.T) {
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
			testDrainRemoval(t, tc.Setup, tc.TearDown, tc.Lifetime)
		})
	}
}

const removalWait = 5 * clock.Second

// partitionsOn lists a queue's partition numbers by storage backend, read from queues.list, which
// reads the queue store and does not load the queue.
func partitionsOn(t *testing.T, ctx context.Context, c *querator.Client, queueName string) map[string][]int32 {
	t.Helper()
	var list pb.QueuesListResponse
	require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{Pivot: queueName, Limit: 1}))
	require.Len(t, list.Items, 1)
	require.Equal(t, queueName, list.Items[0].QueueName)
	numbers := make(map[string][]int32)
	for _, p := range list.Items[0].Partitions {
		numbers[p.StorageName] = append(numbers[p.StorageName], p.Partition)
	}
	return numbers
}

func produceOne(t *testing.T, ctx context.Context, c *querator.Client, queueName string, item *pb.QueueProduceItem) {
	t.Helper()
	require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
		QueueName:      queueName,
		RequestTimeout: "5s",
		Items:          []*pb.QueueProduceItem{item},
	}))
}

func completeLeased(t *testing.T, ctx context.Context, c *querator.Client, lease *pb.QueueLeaseResponse) {
	t.Helper()
	ids := make([]string, 0, len(lease.Items))
	for _, item := range lease.Items {
		ids = append(ids, item.Id)
	}
	require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
		QueueName:      lease.QueueName,
		Partition:      lease.Partition,
		RequestTimeout: "5s",
		Ids:            ids,
	}))
}

// removalSaveFault refuses every save of a named queue's partition list and counts the
// refusals, as a queue store does when it goes down between deleting a partition's storage and
// saving the record that no longer lists it.
type removalSaveFault struct {
	mu      sync.Mutex
	failing map[string]int
}

func (f *removalSaveFault) Wrap(conf store.Config) store.Config {
	conf.Queues = removalSaveQueues{Queues: conf.Queues, fault: f}
	return conf
}

func (f *removalSaveFault) FailSavesOf(names ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failing = make(map[string]int, len(names))
	for _, name := range names {
		f.failing[name] = 0
	}
}

func (f *removalSaveFault) Refused(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failing[name]
}

type removalSaveQueues struct {
	store.Queues
	fault *removalSaveFault
}

func (q removalSaveQueues) SavePartitions(ctx context.Context, list types.PartitionList) error {
	q.fault.mu.Lock()
	refused, failing := q.fault.failing[list.QueueName]
	if failing {
		q.fault.failing[list.QueueName] = refused + 1
	}
	q.fault.mu.Unlock()
	if failing {
		return errSaveFailed
	}
	return q.Queues.SavePartitions(ctx, list)
}

func testDrainRemoval(t *testing.T, setup NewStorageFunc, tearDown func(), lifetime storageLifetime) {
	t.Run("EmptiedDrainingPartitionsAreRemoved", func(t *testing.T) {
		conf := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		d := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		main := conf.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 2)
		for range 4 {
			produceOne(t, ctx, c, queueName, &pb.QueueProduceItem{Reference: "item", Bytes: []byte("x")})
		}

		assert.Equal(t, rebalanceCounts{ChangedQueues: 1, CreatedPartitions: 2, DrainingPartitions: 2},
			moveOnlyPartition(t, ctx, c, main))
		assert.Equal(t, []int32{0, 1}, drainingOn(listedPartitions(t, ctx, c, queueName), main))
		assert.False(t, findBackend(t, listBackends(t, d), main).GetSafeToStop())

		// Consumers empty the draining partitions, and Querator removes them with no call
		for range 2 {
			completeLeased(t, ctx, c, leaseBatch(t, ctx, c, queueName, 10))
		}
		require.Eventually(t, func() bool {
			return len(partitionsOn(t, ctx, c, queueName)[main]) == 0 &&
				len(drainingOn(listedPartitions(t, ctx, c, queueName), main)) == 0
		}, removalWait, 20*clock.Millisecond)

		listed := listedPartitions(t, ctx, c, queueName)
		assert.Empty(t, drainingOn(listed, main))
		assert.Empty(t, availableOn(listed, main))
		assert.Equal(t, []int32{2, 3}, availableOn(listed, extraBackend))
		assert.True(t, findBackend(t, listBackends(t, d), main).GetSafeToStop())
		assert.Equal(t, rebalanceCounts{}, rebalance(t, ctx, c))
	})

	t.Run("LeasedOrScheduledItemKeepsDrainingPartition", func(t *testing.T) {
		conf := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		d := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		main := conf.PartitionStorage[0].Name

		leasedName := random.String("leased-", 10)
		createQueueWithPartitions(t, ctx, c, leasedName, 1)
		produceOne(t, ctx, c, leasedName, &pb.QueueProduceItem{Reference: "leased", Bytes: []byte("x")})
		leased := leaseBatch(t, ctx, c, leasedName, 1)
		require.Len(t, leased.Items, 1)

		scheduledName := random.String("scheduled-", 10)
		createQueueWithPartitions(t, ctx, c, scheduledName, 1)
		produceOne(t, ctx, c, scheduledName, &pb.QueueProduceItem{
			EnqueueAt: timestamppb.New(clock.Now().UTC().Add(clock.Hour)),
			Reference: "scheduled",
			Bytes:     []byte("x"),
		})

		emptyName := random.String("empty-", 10)
		createQueueWithPartitions(t, ctx, c, emptyName, 1)

		moveOnlyPartition(t, ctx, c, main)

		// The empty queue's draining partition goes, and the other two stay listed as draining
		require.Eventually(t, func() bool {
			return len(partitionsOn(t, ctx, c, emptyName)[main]) == 0
		}, removalWait, 20*clock.Millisecond)
		clock.Sleep(requestSettle)
		assert.Equal(t, []int32{0}, drainingOn(listedPartitions(t, ctx, c, leasedName), main))
		assert.Equal(t, []int32{0}, drainingOn(listedPartitions(t, ctx, c, scheduledName), main))
		assert.False(t, findBackend(t, listBackends(t, d), main).GetSafeToStop())

		// Completing the leased item empties its partition
		completeLeased(t, ctx, c, leased)
		require.Eventually(t, func() bool {
			return len(partitionsOn(t, ctx, c, leasedName)[main]) == 0
		}, removalWait, 20*clock.Millisecond)
		assert.Equal(t, []int32{0}, drainingOn(listedPartitions(t, ctx, c, scheduledName), main))
	})

	t.Run("UnansweredDeadLetterMoveKeepsDrainingPartition", func(t *testing.T) {
		const writeTimeout = 500 * clock.Millisecond
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		spy := &lifecycleSpy{}
		stores := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: spy.Wrap(stores), Clock: now, WriteTimeout: writeTimeout})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		main := stores.PartitionStorage[0].Name

		// The dead letter queue lives on the extra backend, so the rebalance below leaves it alone
		queueName := random.String("queue-", 10)
		dlqName := random.String("dlq-", 10)
		setAffinity(t, ctx, c, extraBackend, 1)
		setAffinity(t, ctx, c, main, 0)
		createOnePartitionQueue(t, ctx, c, dlqName)
		setAffinity(t, ctx, c, main, 1)
		setAffinity(t, ctx, c, extraBackend, 0)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			MaxAttempts:         1,
			DeadQueue:           dlqName,
			RequestedPartitions: 1,
		})
		require.Equal(t, []int32{0}, partitionsOn(t, ctx, c, dlqName)[extraBackend])
		produceInOrder(t, ctx, c, queueName, 1)
		require.Len(t, leaseUpTo(t, ctx, c, queueName, 1), 1)

		// The dead letter queue's write of the move is held until the source stops waiting for it
		spy.HoldNextProducePastDeadline()
		now.Advance(clock.Minute + clock.Second)
		spy.WaitForProduceHeld(t, ctx)
		clock.Sleep(writeTimeout + requestSettle)

		// The source partition becomes draining while its move is unanswered, and stays listed
		moveOnlyPartition(t, ctx, c, main)
		clock.Sleep(requestSettle)
		assert.Equal(t, []int32{0}, drainingOn(listedPartitions(t, ctx, c, queueName), main))
		assert.Equal(t, []int32{0}, partitionsOn(t, ctx, c, queueName)[main])

		// Once the dead letter queue answers, the source deletes its item and the partition goes
		spy.ReleaseProduce()
		require.Eventually(t, func() bool {
			now.Advance(clock.Second)
			return len(partitionsOn(t, ctx, c, queueName)[main]) == 0
		}, removalWait, 20*clock.Millisecond)
		assert.Equal(t, int32(1), queueTotal(t, ctx, c, dlqName))
	})

	t.Run("ExpiredItemLeavesDrainingPartitionToBeRemoved", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		stores := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: stores, Clock: now})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		main := stores.PartitionStorage[0].Name

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "1h0m0s",
			RequestedPartitions: 1,
		})
		produceInOrder(t, ctx, c, queueName, 1)
		require.Len(t, leaseUpTo(t, ctx, c, queueName, 1), 1)
		moveOnlyPartition(t, ctx, c, main)
		require.Equal(t, []int32{0}, drainingOn(listedPartitions(t, ctx, c, queueName), main))

		// The lease expires and a lifecycle pass counts the item still in the partition
		now.Advance(clock.Minute + clock.Second)
		clock.Sleep(requestSettle)
		require.Equal(t, []int32{0}, drainingOn(listedPartitions(t, ctx, c, queueName), main))

		// The lifecycle deletes the expired item, which leaves the partition empty
		now.Advance(clock.Hour)
		require.Eventually(t, func() bool {
			now.Advance(clock.Minute)
			return len(partitionsOn(t, ctx, c, queueName)[main]) == 0
		}, removalWait, 20*clock.Millisecond)
	})

	t.Run("InterruptedRemovalFinishesAfterRestart", func(t *testing.T) {
		// The InMemory queue store keeps no record of the queue across a restart
		if lifetime == endsWithDaemon {
			return
		}
		fault := &removalSaveFault{}
		stores := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		main := stores.PartitionStorage[0].Name
		first := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: fault.Wrap(stores)})
		c, ctx := first.MustClient(), first.Context()

		usedName := random.String("used-", 10)
		idleName := random.String("idle-", 10)
		for _, name := range []string{usedName, idleName} {
			createQueueWithPartitions(t, ctx, c, name, 1)
			produceOne(t, ctx, c, name, &pb.QueueProduceItem{Reference: "item", Bytes: []byte("x")})
		}
		assert.Equal(t, rebalanceCounts{ChangedQueues: 2, CreatedPartitions: 2, DrainingPartitions: 2},
			moveOnlyPartition(t, ctx, c, main))

		// Each removal deletes the partition's storage and then fails to save the record
		fault.FailSavesOf(usedName, idleName)
		for _, name := range []string{usedName, idleName} {
			completeLeased(t, ctx, c, leaseBatch(t, ctx, c, name, 1))
		}
		require.Eventually(t, func() bool {
			return fault.Refused(usedName) != 0 && fault.Refused(idleName) != 0
		}, removalWait, 20*clock.Millisecond)
		assert.Equal(t, []int32{0}, partitionsOn(t, ctx, c, usedName)[main])
		assert.Equal(t, []int32{0}, partitionsOn(t, ctx, c, idleName)[main])
		first.Shutdown(t)

		second := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: stores})
		defer second.Shutdown(t)
		c, ctx = second.MustClient(), second.Context()

		// The restart finishes both removals, though no call names the idle queue
		require.Eventually(t, func() bool {
			return len(partitionsOn(t, ctx, c, idleName)[main]) == 0 &&
				len(partitionsOn(t, ctx, c, usedName)[main]) == 0 &&
				len(drainingOn(listedPartitions(t, ctx, c, usedName), main)) == 0
		}, removalWait, 20*clock.Millisecond)
		assert.Equal(t, []int32{1}, partitionsOn(t, ctx, c, idleName)[extraBackend])

		listed := listedPartitions(t, ctx, c, usedName)
		assert.Equal(t, []listedPartition{{Partition: 1, StorageName: extraBackend, State: "available"}}, listed)
		assert.True(t, findBackend(t, listBackends(t, second), main).GetSafeToStop())
	})

	t.Run("RecreatedQueueHoldsNoItemsOfTheDeletedQueue", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 2)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(10),
		}))
		produceOne(t, ctx, c, queueName, &pb.QueueProduceItem{
			EnqueueAt: timestamppb.New(clock.Now().UTC().Add(clock.Hour)),
			Reference: "scheduled",
			Bytes:     []byte("x"),
		})
		require.Equal(t, int32(10), queueTotal(t, ctx, c, queueName))

		require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: queueName}))
		createQueueWithPartitions(t, ctx, c, queueName, 2)

		assert.Equal(t, int32(0), queueTotal(t, ctx, c, queueName))
		var stats pb.QueueStatsResponse
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
		for _, logical := range stats.LogicalQueues {
			for _, p := range logical.Partitions {
				assert.Equal(t, int32(0), p.Scheduled)
			}
		}
		var scheduled pb.StorageItemsListResponse
		for _, partition := range []int32{0, 1} {
			require.NoError(t, c.StorageScheduledList(ctx, queueName, int(partition), &scheduled, nil))
			assert.Empty(t, scheduled.Items)
		}
	})

	t.Run("DeleteLeavesItemsOfAQueueWithASimilarName", func(t *testing.T) {
		conf := setup()
		defer tearDown()
		d := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		// Partition 0 of "<name>1" and partition 10 of "<name>" join to the same text.
		kept := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, kept+"1", 1)
		createQueueWithPartitions(t, ctx, c, kept, 11)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      kept + "1",
			RequestTimeout: "5s",
			Items:          produceRandomItems(10),
		}))
		require.Equal(t, int32(10), queueTotal(t, ctx, c, kept+"1"))

		require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: kept}))

		assert.Equal(t, int32(10), queueTotal(t, ctx, c, kept+"1"))
	})

	t.Run("DeleteRefusesQueueOnBackendMissingFromConfig", func(t *testing.T) {
		// The InMemory queue store keeps no record of the queue across a restart
		if lifetime == endsWithDaemon {
			return
		}
		stores := withExtraBackend(setup(), extraBackend, 1, nil)
		defer tearDown()
		main := stores.PartitionStorage[0].Name
		first := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: stores})
		c, ctx := first.MustClient(), first.Context()
		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 2)
		placed := partitionsOn(t, ctx, c, queueName)
		require.Len(t, placed[main], 1)
		require.Len(t, placed[extraBackend], 1)
		for range 4 {
			produceOne(t, ctx, c, queueName, &pb.QueueProduceItem{Reference: "held", Bytes: []byte("x")})
		}
		held := itemsIn(t, ctx, c, queueName, placed[main][0])
		require.NotZero(t, held)
		first.Shutdown(t)

		withoutExtra := stores
		withoutExtra.PartitionStorage = stores.PartitionStorage[:1]
		second := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: withoutExtra})
		c, ctx = second.MustClient(), second.Context()
		err := c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: queueName})
		var duhErr duh.Error
		require.ErrorAs(t, err, &duhErr)
		assert.Equal(t, duh.CodeRequestFailed, duhErr.Code())
		assert.Equal(t, fmt.Sprintf("cannot delete queue '%s'; its partition %d is on storage backend '%s', "+
			"which is not available; please retry once '%s' is available", queueName, placed[extraBackend][0], extraBackend, extraBackend),
			duhErr.Message())

		// The partition on the configured backend still holds its items
		assert.Equal(t, held, itemsIn(t, ctx, c, queueName, placed[main][0]))
		second.Shutdown(t)

		// With the backend back the delete succeeds
		third := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: stores})
		defer third.Shutdown(t)
		c, ctx = third.MustClient(), third.Context()
		require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: queueName}))
		var info pb.QueueInfo
		require.Error(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
	})

	t.Run("DeletePartlyDoneWhenPartitionStorageFails", func(t *testing.T) {
		fault := &partitionFault{}
		defer tearDown()
		d := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: fault.Wrap(setup())})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 2)
		for range 4 {
			produceOne(t, ctx, c, queueName, &pb.QueueProduceItem{Reference: "held", Bytes: []byte("x")})
		}
		require.NotZero(t, itemsIn(t, ctx, c, queueName, 0))
		held := itemsIn(t, ctx, c, queueName, 1)
		require.NotZero(t, held)

		fault.failingDeleteStorage.Store(1 + 1)
		err := c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: queueName})
		var duhErr duh.Error
		require.ErrorAs(t, err, &duhErr)
		assert.Equal(t, duh.CodeRequestFailed, duhErr.Code())
		assert.Equal(t, fmt.Sprintf("queue '%s' is only partly deleted; deleting partition 1 on '%s' failed: %s; "+
			"please retry to finish deleting the queue", queueName, setup().PartitionStorage[0].Name, errDeleteStorageFailed),
			duhErr.Message())

		// The queue is kept, partition 0 is already empty and partition 1 still holds its items.
		// The InMemory store drops a partition's items once delete closes the queue.
		assert.Zero(t, itemsIn(t, ctx, c, queueName, 0))
		if lifetime != endsWithDaemon {
			assert.Equal(t, held, itemsIn(t, ctx, c, queueName, 1))
		}

		fault.failingDeleteStorage.Store(0)
		require.NoError(t, c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: queueName}))
		var info pb.QueueInfo
		require.Error(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: queueName}, &info))
	})

	t.Run("LifecycleRunKeepsDrainingPartitionWithLeasedOrScheduledItem", func(t *testing.T) {
		conf := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		d := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: conf})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()
		main := conf.PartitionStorage[0].Name

		leasedName := random.String("leased-", 10)
		createQueueWithPartitions(t, ctx, c, leasedName, 1)
		produceOne(t, ctx, c, leasedName, &pb.QueueProduceItem{Reference: "leased", Bytes: []byte("x")})
		require.Len(t, leaseBatch(t, ctx, c, leasedName, 1).Items, 1)

		scheduledName := random.String("scheduled-", 10)
		createQueueWithPartitions(t, ctx, c, scheduledName, 1)
		produceOne(t, ctx, c, scheduledName, &pb.QueueProduceItem{
			EnqueueAt: timestamppb.New(clock.Now().UTC().Add(clock.Hour)),
			Reference: "scheduled",
			Bytes:     []byte("x"),
		})

		moveOnlyPartition(t, ctx, c, main)

		// queue.reload makes the lifecycle run on every partition now, and each run ends by
		// checking whether its draining partition can go
		for _, name := range []string{leasedName, scheduledName} {
			require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{QueueName: name}))
			require.Eventually(t, func() bool {
				var stats pb.QueueStatsResponse
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: name}, &stats); err != nil {
					return false
				}
				for _, logical := range stats.LogicalQueues {
					for _, p := range logical.Partitions {
						if p.Partition == 0 {
							next, err := clock.ParseDuration(p.NextLifecycleRun)
							return err == nil && next > 0
						}
					}
				}
				return true
			}, removalWait, 20*clock.Millisecond)

			assert.Equal(t, []int32{0}, drainingOn(listedPartitions(t, ctx, c, name), main))
			assert.Equal(t, []int32{0}, partitionsOn(t, ctx, c, name)[main])
		}
		assert.False(t, findBackend(t, listBackends(t, d), main).GetSafeToStop())
	})

	t.Run("EmptiedDrainingPartitionIsRemovedAfterRestart", func(t *testing.T) {
		// The InMemory queue store keeps no record of the queue across a restart
		if lifetime == endsWithDaemon {
			return
		}
		fault := &partitionFault{}
		stores := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		main := stores.PartitionStorage[0].Name
		faulty := stores
		faulty.PartitionStorage = slices.Concat(
			fault.Wrap(store.Config{PartitionStorage: stores.PartitionStorage[:1]}).PartitionStorage,
			stores.PartitionStorage[1:])
		first := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: faulty})
		c, ctx := first.MustClient(), first.Context()

		idleName := random.String("idle-", 10)
		createQueueWithPartitions(t, ctx, c, idleName, 1)
		produceOne(t, ctx, c, idleName, &pb.QueueProduceItem{Reference: "item", Bytes: []byte("x")})
		moveOnlyPartition(t, ctx, c, main)

		// The removal cannot read the emptied partition's counts, so it keeps its storage and
		// is still listed when the daemon stops
		fault.statsFailing.Store(true)
		completeLeased(t, ctx, c, leaseBatch(t, ctx, c, idleName, 1))
		clock.Sleep(requestSettle)
		assert.Equal(t, []int32{0}, partitionsOn(t, ctx, c, idleName)[main])
		first.Shutdown(t)

		second := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: stores})
		defer second.Shutdown(t)
		c, ctx = second.MustClient(), second.Context()

		// No call names the queue, and the restart removes its draining partition
		require.Eventually(t, func() bool {
			return len(partitionsOn(t, ctx, c, idleName)[main]) == 0
		}, removalWait, 20*clock.Millisecond)
		assert.Equal(t, []int32{1}, partitionsOn(t, ctx, c, idleName)[extraBackend])
		assert.True(t, findBackend(t, listBackends(t, second), main).GetSafeToStop())
	})
	t.Run("RemovalFinishesOnceTheRecordCanBeSaved", func(t *testing.T) {
		fault := &removalSaveFault{}
		stores := withExtraBackend(setup(), extraBackend, 0, nil)
		defer tearDown()
		main := stores.PartitionStorage[0].Name
		d := newDaemon(t, 20*clock.Second, svc.Config{StorageConfig: fault.Wrap(stores)})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueWithPartitions(t, ctx, c, queueName, 1)
		produceOne(t, ctx, c, queueName, &pb.QueueProduceItem{Reference: "item", Bytes: []byte("x")})
		moveOnlyPartition(t, ctx, c, main)

		// The removal deletes the emptied partition's storage, then cannot save the record
		fault.FailSavesOf(queueName)
		completeLeased(t, ctx, c, leaseBatch(t, ctx, c, queueName, 1))
		require.Eventually(t, func() bool {
			return fault.Refused(queueName) != 0
		}, removalWait, 20*clock.Millisecond)
		assert.Equal(t, []int32{0}, drainingOn(listedPartitions(t, ctx, c, queueName), main))

		// Once saves work again, the same daemon finishes the removal
		fault.FailSavesOf()
		require.Eventually(t, func() bool {
			return len(partitionsOn(t, ctx, c, queueName)[main]) == 0 &&
				len(drainingOn(listedPartitions(t, ctx, c, queueName), main)) == 0
		}, 3*removalWait, 20*clock.Millisecond)
		assert.True(t, findBackend(t, listBackends(t, d), main).GetSafeToStop())
	})
}

// itemsIn reads one partition's item count from queue.stats.
func itemsIn(t *testing.T, ctx context.Context, c *querator.Client, queueName string, partition int32) int32 {
	t.Helper()
	var stats pb.QueueStatsResponse
	require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
	for _, logical := range stats.LogicalQueues {
		for _, p := range logical.Partitions {
			if p.Partition == partition {
				return p.Total
			}
		}
	}
	require.FailNow(t, "partition not in queue.stats", "partition %d", partition)
	return 0
}
