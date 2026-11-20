package service_test

import (
	"errors"
	"testing"

	"github.com/duh-rpc/duh-go"

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

// TestBadgerStats runs against BadgerDB only, because only BadgerDB's stats scan runs past the read
// timeout; the other backends stop at it. See docs/adr/0030-badger-stats-scan-past-read-timeout.md.
func TestBadgerStats(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	testBadgerStats(t, func() store.Config {
		return badgerdb.Setup(store.BadgerConfig{})
	}, badgerdb.Teardown)
}

func testBadgerStats(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("CountsAndAgesHoldAcrossReloadAndRestart", func(t *testing.T) {
		const (
			numOlder     = 10
			numNewer     = 5
			numLeased    = 3
			numScheduled = 4
		)
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		var queueName = random.String("queue-", 10)
		conf := svc.Config{StorageConfig: setup(), Clock: now}
		d := newDaemon(t, 30*clock.Second, conf)
		running := true
		defer func() {
			if running {
				d.Shutdown(t)
			}
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			QueueName:           queueName,
			RequestedPartitions: 1,
		})

		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			Items:          produceRandomItems(numOlder),
			QueueName:      queueName,
			RequestTimeout: "1m",
		}))
		now.Advance(10 * clock.Second)

		var scheduled []*pb.QueueProduceItem
		for range numScheduled {
			scheduled = append(scheduled, &pb.QueueProduceItem{
				Reference: random.String("ref-", 10),
				Utf8:      "scheduled",
				EnqueueAt: timestamppb.New(now.Now().Add(clock.Hour)),
			})
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			Items:          append(produceRandomItems(numNewer), scheduled...),
			QueueName:      queueName,
			RequestTimeout: "1m",
		}))

		var leased pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			QueueName:      queueName,
			BatchSize:      numLeased,
			RequestTimeout: "1m",
		}, &leased))
		require.Len(t, leased.Items, numLeased)
		now.Advance(20 * clock.Second)

		// Ten items are 30s old and five are 20s old, so the average is 400s/15. The three
		// leased items were leased 20s ago with a 1m lease, so 40s of each lease remains.
		partitionStats := func(t *testing.T) *pb.QueuePartitionStats {
			var stats pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
			require.Len(t, stats.LogicalQueues, 1)
			require.Len(t, stats.LogicalQueues[0].Partitions, 1)
			p := stats.LogicalQueues[0].Partitions[0]
			assert.Equal(t, int32(numOlder+numNewer), p.Total)
			assert.Equal(t, int32(numLeased), p.TotalLeased)
			assert.Equal(t, int32(numScheduled), p.Scheduled)
			assert.Equal(t, int32(0), p.Failures)
			assert.Equal(t, (400 * clock.Second / 15).String(), p.AverageAge)
			assert.Equal(t, (40 * clock.Second).String(), p.AverageLeasedAge)
			return p
		}

		t.Run("Stats", func(t *testing.T) {
			partitionStats(t)
		})

		// Reload and restart set the un-leased count from the storage scan.
		require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{QueueName: queueName}))
		t.Run("AfterReload", func(t *testing.T) {
			assert.Equal(t, int32(numOlder+numNewer-numLeased), partitionStats(t).UnLeased)
		})

		d.Shutdown(t)
		running = false
		d = newDaemon(t, 30*clock.Second, conf)
		running = true
		c, ctx = d.MustClient(), d.Context()
		t.Run("AfterRestart", func(t *testing.T) {
			assert.Equal(t, int32(numOlder+numNewer-numLeased), partitionStats(t).UnLeased)
		})
	})

	t.Run("ScanPastReadTimeoutStillLoads", func(t *testing.T) {
		const numItems = 10
		fault := &partitionFault{}
		var queueName = random.String("queue-", 10)
		conf := svc.Config{StorageConfig: fault.Wrap(setup()), ReadTimeout: 100 * clock.Millisecond}
		d := newDaemon(t, 30*clock.Second, conf)
		running := true
		defer func() {
			if running {
				d.Shutdown(t)
			}
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			Items:          produceRandomItems(numItems),
			QueueName:      queueName,
			RequestTimeout: "1m",
		}))

		fault.statsPastDeadline.Store(true)
		partitionStats := func(t *testing.T) *pb.QueuePartitionStats {
			var stats pb.QueueStatsResponse
			require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
			require.Len(t, stats.LogicalQueues, 1)
			require.Len(t, stats.LogicalQueues[0].Partitions, 1)
			return stats.LogicalQueues[0].Partitions[0]
		}

		require.NoError(t, c.QueueReload(ctx, &pb.QueueReloadRequest{QueueName: queueName}))
		t.Run("AfterReload", func(t *testing.T) {
			p := partitionStats(t)
			assert.Equal(t, int32(0), p.Failures)
			assert.Equal(t, int32(numItems), p.Total)
			assert.Equal(t, int32(numItems), p.UnLeased)
		})

		d.Shutdown(t)
		running = false
		d = newDaemon(t, 30*clock.Second, conf)
		running = true
		c, ctx = d.MustClient(), d.Context()
		t.Run("AfterRestart", func(t *testing.T) {
			p := partitionStats(t)
			assert.Equal(t, int32(0), p.Failures)
			assert.Equal(t, int32(numItems), p.Total)
			assert.Equal(t, int32(numItems), p.UnLeased)
		})
	})

	t.Run("CancelledContextFailsStats", func(t *testing.T) {
		fault := &partitionFault{}
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: fault.Wrap(setup())})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})
		fault.statsCancelled.Store(true)

		// An empty partition's scan ends before the cancel can reach it, so the cancel must still
		// fail the call. Repeat it, since how soon the cancel arrives varies from call to call.
		t.Run("EmptyPartition", func(t *testing.T) {
			for range 50 {
				var stats pb.QueueStatsResponse
				err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats)
				var e duh.Error
				require.True(t, errors.As(err, &e))
				require.Equal(t, duh.CodeInternalError, e.Code())
			}
		})

		fault.statsCancelled.Store(false)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			Items:          produceRandomItems(10),
			QueueName:      queueName,
			RequestTimeout: "1m",
		}))
		fault.statsCancelled.Store(true)

		t.Run("WithItems", func(t *testing.T) {
			var stats pb.QueueStatsResponse
			err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats)
			var e duh.Error
			require.True(t, errors.As(err, &e))
			assert.Equal(t, duh.CodeInternalError, e.Code())
		})
	})

	t.Run("CancelStopsScanPartway", func(t *testing.T) {
		const (
			numItems    = 5_000
			cancelAfter = 5 * clock.Millisecond
		)
		fault := &partitionFault{}
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: fault.Wrap(setup())})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})
		for range numItems / 1_000 {
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				Items:          produceRandomItems(1_000),
				QueueName:      queueName,
				RequestTimeout: "1m",
			}))
		}

		var stats pb.QueueStatsResponse
		start := clock.Now()
		require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
		full := clock.Since(start)
		require.Greater(t, full, 4*cancelAfter)

		// The cancel lands partway through the scan, so the call fails well before a full scan ends.
		fault.statsCancelAfter.Store(int64(cancelAfter))
		start = clock.Now()
		err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats)
		cancelled := clock.Since(start)
		var e duh.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, duh.CodeInternalError, e.Code())
		assert.Less(t, cancelled, full/2)
	})
}
