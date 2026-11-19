package service_test

import (
	"context"
	"testing"

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

func TestScheduled(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}
	mongo := mongoTestSetup{}

	for _, tc := range []struct {
		Setup      NewStorageFunc
		TearDown   func()
		Name       string
		Persistent bool
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
			Persistent: true,
		},
		{
			Name: "PostgreSQL",
			Setup: func() store.Config {
				return postgres.Setup(store.PostgresConfig{})
			},
			TearDown: func() {
				postgres.Teardown()
			},
			Persistent: true,
		},
		{
			Name: "MongoDB",
			Setup: func() store.Config {
				return mongo.Setup(store.MongoConfig{})
			},
			TearDown: func() {
				mongo.Teardown()
			},
			Persistent: true,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testScheduled(t, tc.Setup, tc.TearDown, tc.Persistent)
		})
	}
}

func testScheduled(t *testing.T, setup NewStorageFunc, tearDown func(), persistent bool) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("EveryScheduledItemIsEnqueued", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items: []*pb.QueueProduceItem{
				{Reference: "first", Bytes: []byte("1"), EnqueueAt: timestamppb.New(now.Now().Add(clock.Minute))},
				{Reference: "second", Bytes: []byte("2"), EnqueueAt: timestamppb.New(now.Now().Add(2 * clock.Minute))},
				{Reference: "third", Bytes: []byte("3"), EnqueueAt: timestamppb.New(now.Now().Add(3 * clock.Minute))},
			},
		}))

		for _, ref := range []string{"first", "second", "third"} {
			now.Advance(clock.Minute)
			untilEnqueued(t, ctx, c, queueName, ref)
			assert.Equal(t, ref, leaseAndComplete(t, ctx, c, queueName))
		}
	})

	t.Run("ScheduledItemAtExactEnqueueAtIsEnqueued", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items: []*pb.QueueProduceItem{
				{Reference: "exact", Bytes: []byte("x"), EnqueueAt: timestamppb.New(now.Now().Add(clock.Minute))},
			},
		}))

		// The frozen clock now reads exactly the item's EnqueueAt, and never moves past it.
		now.Advance(clock.Minute)
		untilEnqueued(t, ctx, c, queueName, "exact")
	})

	t.Run("ScheduledItemIsHandedToWaitingConsumer", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items: []*pb.QueueProduceItem{
				{Reference: "wake", Bytes: []byte("x"), EnqueueAt: timestamppb.New(now.Now().Add(clock.Minute))},
			},
		}))

		lease := waitingLease(t, ctx, c, queueName, func() {
			now.Advance(clock.Minute + clock.Second)
		})
		require.Len(t, lease.Items, 1)
		assert.Equal(t, "wake", lease.Items[0].Reference)
	})

	t.Run("ExpiredLeaseIsHandedToWaitingConsumer", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          []*pb.QueueProduceItem{{Reference: "expires", Bytes: []byte("x")}},
		}))
		require.Equal(t, "expires", leaseOne(t, ctx, c, queueName).Reference)

		lease := waitingLease(t, ctx, c, queueName, func() {
			now.Advance(clock.Minute + clock.Second)
		})
		require.Len(t, lease.Items, 1)
		assert.Equal(t, "expires", lease.Items[0].Reference)
	})

	t.Run("EveryScheduledRetryIsEnqueued", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup(), Clock: now})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items: []*pb.QueueProduceItem{
				{Reference: "first", Bytes: []byte("1")},
				{Reference: "second", Bytes: []byte("2")},
			},
		}))

		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      2,
		}, &lease))
		require.Len(t, lease.Items, 2)
		first, second := findInLeaseResp("first", &lease), findInLeaseResp("second", &lease)
		require.NotNil(t, first)
		require.NotNil(t, second)

		require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
			QueueName: queueName,
			Partition: lease.Partition,
			Items: []*pb.QueueRetryItem{
				{Id: first.Id, RetryAt: timestamppb.New(now.Now().Add(clock.Minute))},
				{Id: second.Id, RetryAt: timestamppb.New(now.Now().Add(2 * clock.Minute))},
			},
		}))

		for _, ref := range []string{"first", "second"} {
			now.Advance(clock.Minute)
			untilEnqueued(t, ctx, c, queueName, ref)
			assert.Equal(t, ref, leaseAndComplete(t, ctx, c, queueName))
		}
	})

	t.Run("ScheduledItemsInStorageAreEnqueuedAfterRestart", func(t *testing.T) {
		if !persistent {
			t.Skip("storage does not outlive the daemon")
		}
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		conf := setup()
		defer tearDown()
		queueName := random.String("queue-", 10)

		first := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: conf, Clock: now})
		stopped := false
		defer func() {
			if !stopped {
				first.Shutdown(t)
			}
		}()
		c, ctx := first.MustClient(), first.Context()
		createOnePartitionQueue(t, ctx, c, queueName)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items: []*pb.QueueProduceItem{
				{Reference: "overdue", Bytes: []byte("1"), EnqueueAt: timestamppb.New(now.Now().Add(clock.Minute))},
				{Reference: "future", Bytes: []byte("2"), EnqueueAt: timestamppb.New(now.Now().Add(3 * clock.Minute))},
			},
		}))
		first.Shutdown(t)
		stopped = true

		// The first item is already overdue when the queue loads again.
		now.Advance(2 * clock.Minute)
		second := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: conf, Clock: now})
		defer second.Shutdown(t)
		c, ctx = second.MustClient(), second.Context()

		untilEnqueued(t, ctx, c, queueName, "overdue")
		assert.Equal(t, "overdue", leaseAndComplete(t, ctx, c, queueName))

		now.Advance(clock.Minute)
		untilEnqueued(t, ctx, c, queueName, "future")
		assert.Equal(t, "future", leaseAndComplete(t, ctx, c, queueName))
	})
}

func createOnePartitionQueue(t *testing.T, ctx context.Context, c *querator.Client, queueName string) {
	t.Helper()
	createQueueAndWait(t, ctx, c, &pb.QueueInfo{
		QueueName:           queueName,
		LeaseTimeout:        "1m0s",
		ExpireTimeout:       "24h0m0s",
		RequestedPartitions: 1,
	})
}

// waitingLease starts a lease request, waits until the queue reports it waiting, calls
// advance, and returns what the lease request received. The request times out after
// 15 minutes of the frozen clock, so it returns only when an item is handed to it, or
// fails the test after 5 seconds of wall time.
func waitingLease(t *testing.T, ctx context.Context, c *querator.Client, queueName string,
	advance func()) *pb.QueueLeaseResponse {
	t.Helper()
	leaseCtx, cancel := context.WithTimeout(ctx, 5*clock.Second)
	defer cancel()

	var lease pb.QueueLeaseResponse
	var err error
	backgroundCall{
		Call: func() {
			err = c.QueueLease(leaseCtx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "15m",
				QueueName:      queueName,
				BatchSize:      1,
			}, &lease)
		},
		While: func() {
			require.NoError(t, untilLeaseClientWaiting(t, c, queueName, 1))
			advance()
		},
	}.run()
	require.NoError(t, err)
	return &lease
}

// leaseAndComplete leases one item from a one-partition queue and completes it. The tests
// advance the clock by the queue's lease timeout between leases, which would otherwise expire
// the lease and offer the same item again.
func leaseAndComplete(t *testing.T, ctx context.Context, c *querator.Client, queueName string) string {
	t.Helper()
	item := leaseOne(t, ctx, c, queueName)
	require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
		QueueName:      queueName,
		RequestTimeout: "5s",
		Partition:      0,
		Ids:            []string{item.Id},
	}))
	return item.Reference
}

// untilEnqueued waits until the item is listed un-leased. StorageItemsList omits scheduled
// items, so a listed item has been enqueued. A lease request against a frozen clock blocks
// instead of failing, so this polls storage.
func untilEnqueued(t *testing.T, ctx context.Context, c *querator.Client, queueName, ref string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var resp pb.StorageItemsListResponse
		if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
			return false
		}
		item := findInStorageList(ref, &resp)
		return item != nil && !item.IsLeased
	}, 3*clock.Second, 50*clock.Millisecond)
}
