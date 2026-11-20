package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

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

// TestBadgerClear holds clear tests that depend on how BadgerDB clears a partition: with default
// options BadgerDB refuses a transaction of more than 104,855 writes, so a clear reads and deletes
// at most 1,000 keys per transaction. Some partitions here are larger than that limit, and the
// rest need several transactions. They run against BadgerDB only.
func TestBadgerClear(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	testBadgerClear(t, func() store.Config {
		return badgerdb.Setup(store.BadgerConfig{})
	}, badgerdb.Teardown)
}

func testBadgerClear(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("ClearLargePartition", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 2*clock.Minute, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()
		createOnePartitionQueue(t, ctx, c, queueName)

		produceItems(t, ctx, c, queueName, 105_000)
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      5,
		}, &lease))
		require.Len(t, lease.Items, 5)

		// The client gives up long before the clear of 105,000 items finishes; the clear goes on.
		short, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		err := c.QueueClear(short, &pb.QueueClearRequest{QueueName: queueName, Queue: true})
		cancel()
		require.ErrorContains(t, err, "context deadline exceeded")

		// The clear holds the queue's request loop, so this list waits until the clear finishes.
		list := listPartition(t, ctx, c, queueName)
		require.Len(t, list, 5)
		for i, item := range list {
			assert.Equal(t, lease.Items[i].Id, item.Id)
			assert.True(t, item.IsLeased)
		}
	})

	t.Run("ClearLargeDeadLetterPartition", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 2*clock.Minute, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()
		createOnePartitionQueue(t, ctx, c, queueName)

		// Each item carries a source id, as a dead letter item does, so clearing it deletes two
		// keys and 52,500 items need 105,000 deletes.
		importSourcedItems(t, ctx, c, queueName, 52_500)

		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true}))
		assert.Empty(t, listPartition(t, ctx, c, queueName))

		// An import skips an item whose source id is already in the partition, so these
		// come back only if the clear removed the source ids too.
		importSourcedItems(t, ctx, c, queueName, 10)
		assert.Len(t, listPartition(t, ctx, c, queueName), 10)
	})

	t.Run("ClearAgainAfterShutdownStopsIt", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		conf := svc.Config{StorageConfig: setup()}
		d := newDaemon(t, 2*clock.Minute, conf)
		running := true
		defer func() {
			if running {
				d.Shutdown(t)
			}
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()
		createOnePartitionQueue(t, ctx, c, queueName)
		produceItems(t, ctx, c, queueName, 30_000)

		var cleared, stopped error
		backgroundCall{
			Call: func() {
				cleared = c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true})
			},
			While: func() {
				time.Sleep(100 * time.Millisecond)
				stop, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				stopped = d.d.Shutdown(stop)
			},
		}.run()
		running = false
		require.NoError(t, stopped)
		require.ErrorContains(t, cleared, "queue is shutting down")

		d = newDaemon(t, 2*clock.Minute, conf)
		running = true
		c, ctx = d.MustClient(), d.Context()

		assert.NotEmpty(t, listPartition(t, ctx, c, queueName))
		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true}))
		assert.Empty(t, listPartition(t, ctx, c, queueName))
	})

	t.Run("ClearKeepsScheduledAndLeasedAcrossPages", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 2*clock.Minute, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()
		createOnePartitionQueue(t, ctx, c, queueName)
		leased := fillPagedPartition(t, ctx, c, queueName)

		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true}))

		p := partitionStats(t, ctx, c, queueName)
		assert.Equal(t, int32(6), p.Total)
		assert.Equal(t, int32(600), p.Scheduled)
		list := listPartition(t, ctx, c, queueName)
		require.Len(t, list, 6)
		for i, item := range list {
			assert.Equal(t, leased[i], item.Reference)
			assert.True(t, item.IsLeased)
		}
	})

	t.Run("ClearScheduledAcrossPages", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 2*clock.Minute, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()
		createOnePartitionQueue(t, ctx, c, queueName)
		fillPagedPartition(t, ctx, c, queueName)

		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Scheduled: true}))

		p := partitionStats(t, ctx, c, queueName)
		assert.Equal(t, int32(0), p.Scheduled)
		assert.Equal(t, int32(1_806), p.Total)
	})

	t.Run("ClearScheduledAndQueueAcrossPages", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 2*clock.Minute, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()
		createOnePartitionQueue(t, ctx, c, queueName)
		leased := fillPagedPartition(t, ctx, c, queueName)

		require.NoError(t, c.QueueClear(ctx, &pb.QueueClearRequest{
			QueueName: queueName,
			Scheduled: true,
			Queue:     true,
		}))

		p := partitionStats(t, ctx, c, queueName)
		assert.Equal(t, int32(0), p.Scheduled)
		assert.Equal(t, int32(6), p.Total)
		list := listPartition(t, ctx, c, queueName)
		require.Len(t, list, 6)
		for i, item := range list {
			assert.Equal(t, leased[i], item.Reference)
			assert.True(t, item.IsLeased)
		}
	})
}

// fillPagedPartition writes three rounds of 600 items, 200 scheduled items and 2 leased items,
// 2,406 keys in all, so the leased and scheduled items sit on each of the three pages a clear
// walks. It returns the references of the leased items in the order they were written.
func fillPagedPartition(t *testing.T, ctx context.Context, c *querator.Client, queueName string) []string {
	t.Helper()
	var leased []string
	enqueueAt := timestamppb.New(clock.Now().UTC().Add(clock.Hour))
	for round := range 3 {
		produceItems(t, ctx, c, queueName, 600)

		scheduled := make([]*pb.QueueProduceItem, 200)
		for i := range scheduled {
			scheduled[i] = &pb.QueueProduceItem{
				Reference: fmt.Sprintf("scheduled-%d-%d", round, i),
				EnqueueAt: enqueueAt,
			}
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items:          scheduled,
		}))

		deadline := timestamppb.New(clock.Now().UTC().Add(clock.Hour))
		req := &pb.StorageItemsImportRequest{QueueName: queueName, Partition: 0}
		for i := range 2 {
			reference := fmt.Sprintf("leased-%d-%d", round, i)
			leased = append(leased, reference)
			req.Items = append(req.Items, &pb.StorageItem{
				ExpireDeadline: deadline,
				LeaseDeadline:  deadline,
				Reference:      reference,
				IsLeased:       true,
			})
		}
		var resp pb.StorageItemsImportResponse
		require.NoError(t, c.StorageItemsImport(ctx, req, &resp))
	}
	return leased
}

func partitionStats(t *testing.T, ctx context.Context, c *querator.Client, queueName string) *pb.QueuePartitionStats {
	t.Helper()
	var stats pb.QueueStatsResponse
	require.NoError(t, c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: queueName}, &stats))
	require.Len(t, stats.LogicalQueues, 1)
	require.Len(t, stats.LogicalQueues[0].Partitions, 1)
	return stats.LogicalQueues[0].Partitions[0]
}

func produceItems(t *testing.T, ctx context.Context, c *querator.Client, queueName string, count int) {
	t.Helper()
	for done := 0; done < count; done += 1_000 {
		size := min(1_000, count-done)
		items := make([]*pb.QueueProduceItem, size)
		for i := range items {
			items[i] = &pb.QueueProduceItem{Reference: fmt.Sprintf("ref-%d", done+i)}
		}
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items:          items,
		}))
	}
}

func importSourcedItems(t *testing.T, ctx context.Context, c *querator.Client, queueName string, count int) {
	t.Helper()
	expire := timestamppb.New(clock.Now().UTC().Add(clock.Hour))
	for done := 0; done < count; done += 1_000 {
		size := min(1_000, count-done)
		req := &pb.StorageItemsImportRequest{QueueName: queueName, Partition: 0}
		for i := range size {
			req.Items = append(req.Items, &pb.StorageItem{
				SourceId:       fmt.Sprintf("source-%09d", done+i),
				ExpireDeadline: expire,
				Reference:      fmt.Sprintf("ref-%d", done+i),
			})
		}
		var resp pb.StorageItemsImportResponse
		require.NoError(t, c.StorageItemsImport(ctx, req, &resp))
	}
}

func listPartition(t *testing.T, ctx context.Context, c *querator.Client, queueName string) []*pb.StorageItem {
	t.Helper()
	var list pb.StorageItemsListResponse
	require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 10}))
	return list.Items
}
