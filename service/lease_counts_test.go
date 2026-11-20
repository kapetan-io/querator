package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLeaseCounts(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
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
				return badgerdb.Setup(store.BadgerConfig{})
			},
			TearDown: func() {
				badgerdb.Teardown()
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
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testLeaseCounts(t, tc.Setup, tc.TearDown)
		})
	}
}

func testLeaseCounts(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("Complete", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: LeaseTimeout})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 3)
		lease := leaseItems(t, ctx, c, queueName, 3)
		assert.Equal(t, leaseCounts{UnLeased: 0, TotalLeased: 3}, partitionLeaseCounts(t, ctx, c, queueName))

		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      lease.Partition,
			RequestTimeout: "5s",
			Ids:            querator.CollectIDs(lease.Items),
		}))
		assert.Equal(t, leaseCounts{UnLeased: 0, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("CompleteSome", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: LeaseTimeout})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 5)
		lease := leaseItems(t, ctx, c, queueName, 3)

		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      lease.Partition,
			RequestTimeout: "5s",
			Ids:            querator.CollectIDs(lease.Items[:2]),
		}))
		assert.Equal(t, leaseCounts{UnLeased: 2, TotalLeased: 1}, partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("CompleteRefusedPartway", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: LeaseTimeout})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 3)
		lease := leaseItems(t, ctx, c, queueName, 2)

		// The second ID is an item that was never leased, so storage refuses the request
		// there. Whatever storage applied before the refusal, the counts follow storage.
		err := c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      lease.Partition,
			RequestTimeout: "5s",
			Ids:            []string{lease.Items[0].Id, unleasedItemID(t, ctx, c, queueName)},
		})
		var e duh.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, duh.CodeBadRequest, e.Code())
		assert.Equal(t, storagePartitionLeaseCounts(t, ctx, c, queueName, 0), partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("Retry", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: LeaseTimeout})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 3)
		lease := leaseItems(t, ctx, c, queueName, 3)

		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: lease.Partition,
			Items: []*pb.QueueRetryItem{
				{Id: lease.Items[0].Id},
				{Id: lease.Items[1].Id},
				{Id: lease.Items[2].Id},
			},
		}))
		assert.Equal(t, leaseCounts{UnLeased: 3, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("RetryDead", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: LeaseTimeout})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 2)
		lease := leaseItems(t, ctx, c, queueName, 2)

		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: lease.Partition,
			Items: []*pb.QueueRetryItem{
				{Id: lease.Items[0].Id, Dead: true},
				{Id: lease.Items[1].Id, Dead: true},
			},
		}))
		assert.Equal(t, leaseCounts{UnLeased: 0, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("RetryScheduled", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: LeaseTimeout})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 2)
		lease := leaseItems(t, ctx, c, queueName, 2)

		// A scheduled item is not waiting to be leased until it is due, so it leaves the
		// leased count without joining the unleased count.
		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: lease.Partition,
			Items: []*pb.QueueRetryItem{
				{Id: lease.Items[0].Id, RetryAt: timestamppb.New(clock.Now().UTC().Add(clock.Hour))},
				{Id: lease.Items[1].Id},
			},
		}))
		assert.Equal(t, leaseCounts{UnLeased: 1, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("ScheduledRetryLeasedAndCompleted", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{Clock: now}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: LeaseTimeout})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 1)
		lease := leaseItems(t, ctx, c, queueName, 1)
		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: lease.Partition,
			Items: []*pb.QueueRetryItem{
				{Id: lease.Items[0].Id, RetryAt: timestamppb.New(now.Now().UTC().Add(clock.Minute))},
			},
		}))

		now.Advance(2 * clock.Minute)
		require.NoError(t, retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
			if waiting := storagePartitionLeaseCounts(t, ctx, c, queueName, 0).UnLeased; waiting != 1 {
				return fmt.Errorf("storage has %d items waiting, want the retried item", waiting)
			}
			return nil
		}))

		// PostgreSQL and MongoDB do not add a scheduled item to the unleased count when it comes
		// due (ENG-774), so the queue expects to lease nothing while storage leases the due item.
		// The leased count must follow storage, or the complete takes it below zero.
		lease = leaseItems(t, ctx, c, queueName, 1)
		assert.Equal(t, int32(1), partitionLeaseCounts(t, ctx, c, queueName).TotalLeased)
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      lease.Partition,
			RequestTimeout: "5s",
			Ids:            querator.CollectIDs(lease.Items),
		}))
		assert.Equal(t, leaseCounts{UnLeased: 0, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("RetryRefusedPartway", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: LeaseTimeout})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 3)
		lease := leaseItems(t, ctx, c, queueName, 2)

		err := c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: lease.Partition,
			Items: []*pb.QueueRetryItem{
				{Id: lease.Items[0].Id},
				{Id: unleasedItemID(t, ctx, c, queueName)},
			},
		})
		var e duh.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, duh.CodeBadRequest, e.Code())
		assert.Equal(t, storagePartitionLeaseCounts(t, ctx, c, queueName, 0), partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("LeaseExpires", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{Clock: now}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: "300ms"})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 3)
		leaseItems(t, ctx, c, queueName, 3)
		assert.Equal(t, leaseCounts{UnLeased: 0, TotalLeased: 3}, partitionLeaseCounts(t, ctx, c, queueName))

		now.Advance(2 * clock.Second)
		require.NoError(t, retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
			if leased := storagePartitionLeaseCounts(t, ctx, c, queueName, 0).TotalLeased; leased != 0 {
				return fmt.Errorf("storage still has %d leased items", leased)
			}
			return nil
		}))
		assert.Equal(t, leaseCounts{UnLeased: 3, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("RetryDeadScheduledAndImmediate", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: LeaseTimeout})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 4)
		lease := leaseItems(t, ctx, c, queueName, 3)

		// Only the immediate retry goes back to waiting; the dead item leaves the partition
		// and the scheduled item is not waiting until it is due.
		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: lease.Partition,
			Items: []*pb.QueueRetryItem{
				{Id: lease.Items[0].Id, Dead: true},
				{Id: lease.Items[1].Id, RetryAt: timestamppb.New(clock.Now().UTC().Add(clock.Hour))},
				{Id: lease.Items[2].Id},
			},
		}))
		assert.Equal(t, leaseCounts{UnLeased: 2, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("CompleteConcurrentWithOneRefused", func(t *testing.T) {
		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: LeaseTimeout})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 6)
		lease := leaseItems(t, ctx, c, queueName, 5)
		unleased := unleasedItemID(t, ctx, c, queueName)

		// Concurrent requests open extra connections, some of which never carry a request.
		// The server's shutdown waits 5s on such a connection unless the client closes it.
		conf := querator.WithNoTLS(d.d.Listener.Addr().String())
		defer conf.Client.CloseIdleConnections()
		concurrent, err := querator.NewClient(conf)
		require.NoError(t, err)

		// Requests sent together may land in one complete batch; the refused request must not
		// change what the others count.
		requests := [][]string{
			{lease.Items[0].Id},
			{lease.Items[1].Id},
			{lease.Items[2].Id},
			{lease.Items[3].Id},
			{lease.Items[4].Id, unleased},
		}
		errs := make([]error, len(requests))
		var wg sync.WaitGroup
		for i, ids := range requests {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = concurrent.QueueComplete(ctx, &pb.QueueCompleteRequest{
					QueueName:      queueName,
					Partition:      lease.Partition,
					RequestTimeout: "5s",
					Ids:            ids,
				})
			}()
		}
		wg.Wait()
		for _, err := range errs[:4] {
			require.NoError(t, err)
		}
		var e duh.Error
		require.True(t, errors.As(errs[4], &e))
		assert.Equal(t, duh.CodeBadRequest, e.Code())
		assert.Equal(t, storagePartitionLeaseCounts(t, ctx, c, queueName, 0), partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("LeaseExpiresAfterSomeCompleted", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		queueName := random.String("queue-", 10)
		d := startLeaseCountsQueue(t, setup, svc.Config{Clock: now}, &pb.QueueInfo{QueueName: queueName, LeaseTimeout: "300ms"})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		produceItems(t, ctx, c, queueName, 4)
		lease := leaseItems(t, ctx, c, queueName, 3)
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      lease.Partition,
			RequestTimeout: "5s",
			Ids:            []string{lease.Items[0].Id},
		}))

		now.Advance(2 * clock.Second)
		require.NoError(t, retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
			if leased := storagePartitionLeaseCounts(t, ctx, c, queueName, 0).TotalLeased; leased != 0 {
				return fmt.Errorf("storage still has %d leased items", leased)
			}
			return nil
		}))
		assert.Equal(t, leaseCounts{UnLeased: 3, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))

		lease = leaseItems(t, ctx, c, queueName, 3)
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			Partition:      lease.Partition,
			RequestTimeout: "5s",
			Ids:            querator.CollectIDs(lease.Items),
		}))
		assert.Equal(t, leaseCounts{UnLeased: 0, TotalLeased: 0}, partitionLeaseCounts(t, ctx, c, queueName))
	})

	t.Run("CompleteAndRetryAcrossPartitions", func(t *testing.T) {
		const partitions = 2
		queueName := random.String("queue-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup()})
		c, ctx := d.MustClient(), d.Context()
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: partitions,
		})
		for range 4 {
			produceItems(t, ctx, c, queueName, 2)
		}
		stats := queueLeaseCounts(t, ctx, c, queueName)
		require.Len(t, stats, partitions)
		for _, counts := range stats {
			require.NotZero(t, counts.UnLeased)
		}

		var leases []*pb.QueueLeaseResponse
		var leased int
		for range 8 {
			if leased == 8 {
				break
			}
			var lease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      3,
			}, &lease))
			require.NotEmpty(t, lease.Items)
			leases = append(leases, &lease)
			leased += len(lease.Items)
		}
		for partition, counts := range queueLeaseCounts(t, ctx, c, queueName) {
			assert.Equal(t, storagePartitionLeaseCounts(t, ctx, c, queueName, partition), counts)
		}

		for i, lease := range leases {
			if i%2 == 0 {
				require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
					QueueName:      queueName,
					Partition:      lease.Partition,
					RequestTimeout: "5s",
					Ids:            querator.CollectIDs(lease.Items),
				}))
				continue
			}
			var items []*pb.QueueRetryItem
			for _, item := range lease.Items {
				items = append(items, &pb.QueueRetryItem{Id: item.Id})
			}
			require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
				QueueName: queueName,
				Partition: lease.Partition,
				Items:     items,
			}))
		}
		for partition, counts := range queueLeaseCounts(t, ctx, c, queueName) {
			storage := storagePartitionLeaseCounts(t, ctx, c, queueName, partition)
			assert.Equal(t, int32(0), counts.TotalLeased)
			assert.Equal(t, storage.UnLeased, counts.UnLeased)
		}
	})
}

// startLeaseCountsQueue starts a daemon with a one-partition queue, so the lease counts
// read from /queue.stats all belong to that partition.
func startLeaseCountsQueue(t *testing.T, setup NewStorageFunc, conf svc.Config, info *pb.QueueInfo) *testDaemon {
	t.Helper()
	conf.StorageConfig = setup()
	d := newDaemon(t, 30*clock.Second, conf)
	info.ExpireTimeout = ExpireTimeout
	info.RequestedPartitions = 1
	createQueueAndWait(t, d.Context(), d.MustClient(), info)
	return d
}

func leaseItems(t *testing.T, ctx context.Context, c *querator.Client, queueName string,
	count int) *pb.QueueLeaseResponse {
	t.Helper()
	var lease pb.QueueLeaseResponse
	require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
		ClientId:       random.String("client-", 10),
		RequestTimeout: "5s",
		QueueName:      queueName,
		BatchSize:      int32(count),
	}, &lease))
	require.Len(t, lease.Items, count)
	return &lease
}

func unleasedItemID(t *testing.T, ctx context.Context, c *querator.Client, queueName string) string {
	t.Helper()
	var list pb.StorageItemsListResponse
	require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 100}))
	for _, item := range list.Items {
		if !item.IsLeased {
			return item.Id
		}
	}
	require.Fail(t, "no unleased item in storage")
	return ""
}

func queueLeaseCounts(t *testing.T, ctx context.Context, c *querator.Client, queueName string) []leaseCounts {
	t.Helper()
	var stats pb.QueueStatsResponse
	require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
	require.Len(t, stats.LogicalQueues, 1)
	counts := make([]leaseCounts, len(stats.LogicalQueues[0].Partitions))
	for _, p := range stats.LogicalQueues[0].Partitions {
		require.Less(t, int(p.Partition), len(counts))
		counts[p.Partition] = leaseCounts{UnLeased: p.UnLeased, TotalLeased: p.TotalLeased}
	}
	return counts
}

// storagePartitionLeaseCounts recounts a partition's items from storage, the numbers the
// in-memory lease counts in /queue.stats must match.
func storagePartitionLeaseCounts(t *testing.T, ctx context.Context, c *querator.Client, queueName string,
	partition int) leaseCounts {
	t.Helper()
	var list pb.StorageItemsListResponse
	require.NoError(t, c.StorageItemsList(ctx, queueName, partition, &list, &querator.ListOptions{Limit: 100}))
	var counts leaseCounts
	for _, item := range list.Items {
		if item.IsLeased {
			counts.TotalLeased++
		} else {
			counts.UnLeased++
		}
	}
	return counts
}
