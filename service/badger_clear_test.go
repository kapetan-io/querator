package service_test

import (
	"context"
	"fmt"
	"strings"
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

// TestBadgerClear holds clear tests that depend on how BadgerDB clears a partition: BadgerDB
// refuses a transaction that writes too many keys or too many bytes, so a clear deletes in many
// small transactions. They run against BadgerDB only.
func TestBadgerClear(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	testBadgerClear(t, func() store.Config {
		return badgerdb.Setup(store.BadgerConfig{})
	}, badgerdb.Teardown)
}

func testBadgerClear(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("ClearLongSourceIDs", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		var fault partitionFault
		d := newDaemon(t, 2*clock.Minute, svc.Config{StorageConfig: fault.Wrap(setup())})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()
		createOnePartitionQueue(t, ctx, c, queueName)

		// Each item carries a 50KB source id, as a dead letter item can, so deleting all of
		// them in one transaction is more bytes than BadgerDB accepts.
		importLongSourceIDs(t, ctx, c, queueName, sourceIDs{Count: 220})
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      5,
		}, &lease))
		require.Len(t, lease.Items, 5)

		// The clear is held partway through until the client has given up, then goes on.
		hold := &clearHold{After: 50, Reached: make(chan struct{}), Release: make(chan struct{})}
		fault.holdClear.Store(hold)
		var cleared error
		backgroundCall{
			Call: func() {
				short, cancel := context.WithTimeout(ctx, 200*clock.Millisecond)
				defer cancel()
				cleared = c.QueueClear(short, &pb.QueueClearRequest{QueueName: queueName, Queue: true})
			},
			While: func() {
				select {
				case <-hold.Reached:
				case <-ctx.Done():
				}
			},
		}.run()
		require.ErrorContains(t, cleared, "context deadline exceeded")
		close(hold.Release)

		// The clear holds the queue's request loop, so this list waits until the clear finishes.
		list := listPartition(t, ctx, c, queueName)
		require.Len(t, list, 5)
		for i, item := range list {
			assert.Equal(t, lease.Items[i].Id, item.Id)
			assert.True(t, item.IsLeased)
		}

		// An import skips an item whose source id is already in the partition, so these
		// come back only if the clear removed the source ids too.
		fault.holdClear.Store(nil)
		importLongSourceIDs(t, ctx, c, queueName, sourceIDs{First: 100, Count: 3})
		assert.Len(t, listPartition(t, ctx, c, queueName), 8)
	})

	t.Run("ClearAgainAfterShutdownStopsIt", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		var fault partitionFault
		conf := svc.Config{StorageConfig: fault.Wrap(setup())}
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
		produceItems(t, ctx, c, queueName, 2_500)

		// The clear is held past its first transaction, and only shutdown lets it go on.
		hold := &clearHold{After: 1_500, Reached: make(chan struct{}), Release: make(chan struct{})}
		fault.holdClear.Store(hold)
		var cleared, stopped error
		backgroundCall{
			Call: func() {
				cleared = c.QueueClear(ctx, &pb.QueueClearRequest{QueueName: queueName, Queue: true})
			},
			While: func() {
				select {
				case <-hold.Reached:
				case <-ctx.Done():
				}
				stop, cancel := context.WithTimeout(ctx, 2*clock.Second)
				defer cancel()
				stopped = d.d.Shutdown(stop)
				close(hold.Release)
			},
		}.run()
		running = false
		require.NoError(t, stopped)
		require.ErrorContains(t, cleared, "queue is shutting down")

		fault.holdClear.Store(nil)
		d = newDaemon(t, 2*clock.Minute, conf)
		running = true
		c, ctx = d.MustClient(), d.Context()

		// Part of the partition is gone and the rest is still there.
		list := listPartition(t, ctx, c, queueName)
		require.NotEmpty(t, list)
		assert.NotEqual(t, "ref-0", list[0].Reference)
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

// fillPagedPartition writes three rounds of plain, scheduled and leased items, enough that a
// clear needs several transactions and each holds all three kinds. It returns the references of
// the leased items in the order they were written.
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

// sourceIDs names the items importLongSourceIDs writes: Count items numbered from First.
type sourceIDs struct {
	First int
	Count int
}

func importLongSourceIDs(t *testing.T, ctx context.Context, c *querator.Client, queueName string, ids sourceIDs) {
	t.Helper()
	expire := timestamppb.New(clock.Now().UTC().Add(clock.Hour))
	padding := strings.Repeat("x", 50_000)
	end := ids.First + ids.Count
	for done := ids.First; done < end; done += 20 {
		req := &pb.StorageItemsImportRequest{QueueName: queueName, Partition: 0}
		for i := done; i < min(done+20, end); i++ {
			req.Items = append(req.Items, &pb.StorageItem{
				SourceId:       fmt.Sprintf("%s-%04d", padding, i),
				ExpireDeadline: expire,
				Reference:      fmt.Sprintf("ref-%d", i),
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
