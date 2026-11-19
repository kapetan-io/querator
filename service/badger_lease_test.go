package service_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/duh-rpc/duh-go"

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

// TestBadgerLease holds lease tests that depend on how the BadgerDB backend finds leasable
// items: how long reading leased items takes, the WriteTimeout and BadgerDB's transaction
// size limit. They run against BadgerDB only.
func TestBadgerLease(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	testBadgerLease(t, func() store.Config {
		return badgerdb.Setup(store.BadgerConfig{})
	}, badgerdb.Teardown)
}

func testBadgerLease(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("LeaseTooBigForOneTransaction", func(t *testing.T) {
		const (
			numRequests = 10
			numPerReq   = 100
			numItems    = numRequests * numPerReq
			payloadSize = 20_000
			maxAttempts = 20
		)
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup()})
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

		// 1,000 items of 20KB each is about 20MB, more than BadgerDB writes in one
		// transaction, so one lease of all of them can save only part of them.
		payload := []byte(strings.Repeat("x", payloadSize))
		var want []string
		for r := range numRequests {
			items := make([]*pb.QueueProduceItem, 0, numPerReq)
			for i := range numPerReq {
				ref := fmt.Sprintf("R%03d", r*numPerReq+i)
				want = append(want, ref)
				items = append(items, &pb.QueueProduceItem{Reference: ref, Bytes: payload})
			}
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "1m",
				Items:          items,
			}))
		}

		// Each lease gets the items that fit in one transaction, and the next lease starts
		// at the first item not saved, so every item arrives once, in order.
		var got []string
		var leases int
		for range maxAttempts {
			var lease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      numItems,
			}, &lease))
			leases++
			for _, item := range lease.Items {
				got = append(got, item.Reference)
			}
			if len(got) >= numItems {
				break
			}
		}
		assert.Greater(t, leases, 1)
		assert.Equal(t, want, got)

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list,
			&querator.ListOptions{Limit: numItems}))
		require.Len(t, list.Items, numItems)
		for _, item := range list.Items {
			assert.True(t, item.IsLeased)
		}
	})

	t.Run("LeaseReadsLeasedItemsOnce", func(t *testing.T) {
		const (
			numBatches  = 20
			batchSize   = 1_000
			numFree     = 9
			maxAttempts = 30
		)
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: setup()})
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

		// Partition order is item free-0, 20,000 leased items, then items free-1 to free-9
		now := clock.Now().UTC()
		expire := timestamppb.New(now.Add(24 * clock.Hour))
		var res pb.StorageItemsImportResponse
		require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
			QueueName: queueName,
			Items:     []*pb.StorageItem{{ExpireDeadline: expire, Reference: "free-0"}},
		}, &res))
		leaseDeadline := timestamppb.New(now.Add(clock.Hour))
		for range numBatches {
			items := make([]*pb.StorageItem, 0, batchSize)
			for range batchSize {
				items = append(items, &pb.StorageItem{
					IsLeased:       true,
					LeaseDeadline:  leaseDeadline,
					ExpireDeadline: expire,
					Attempts:       1,
				})
			}
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: queueName,
				Items:     items,
			}, &res))
		}
		free := make([]*pb.StorageItem, 0, numFree)
		for i := range numFree {
			free = append(free, &pb.StorageItem{ExpireDeadline: expire, Reference: fmt.Sprintf("free-%d", i+1)})
		}
		require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
			QueueName: queueName,
			Items:     free,
		}, &res))

		// Each lease takes the next free item, in the order they were queued. On a slow machine
		// reading past the leased items can outlast the WriteTimeout, so a lease that gets
		// nothing times out and is asked again, and its time counts toward that item.
		durations := make([]time.Duration, 0, numFree+1)
		for i := range numFree + 1 {
			start := time.Now()
			var lease pb.QueueLeaseResponse
			for range maxAttempts {
				err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
					ClientId:       random.String("client-", 10),
					RequestTimeout: "1s",
					QueueName:      queueName,
					BatchSize:      1,
				}, &lease)
				if err == nil {
					break
				}
				var e duh.Error
				require.True(t, errors.As(err, &e))
				require.Equal(t, querator.MsgRequestTimeout, e.Message())
			}
			durations = append(durations, time.Since(start))
			require.Len(t, lease.Items, 1)
			assert.Equal(t, fmt.Sprintf("free-%d", i), lease.Items[0].Reference)
		}

		// Getting free-1 reads past the 20,000 leased items. The eight leases after it must
		// not read them again, so together they take less time than getting free-1.
		var after time.Duration
		for _, duration := range durations[2:] {
			after += duration
		}
		assert.Less(t, after, durations[1])
	})

	t.Run("LeaseStopsAtWriteTimeout", func(t *testing.T) {
		const (
			numBatches  = 10
			batchSize   = 1_000
			maxAttempts = 500
		)
		var queueName = random.String("queue-", 10)
		d := newDaemon(t, 30*clock.Second, svc.Config{
			StorageConfig: setup(),
			WriteTimeout:  20 * clock.Millisecond,
		})
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

		// Partition order is item A, 10,000 leased items, then item B. Reading past the
		// leased items takes longer than the 20ms WriteTimeout.
		now := clock.Now().UTC()
		expire := timestamppb.New(now.Add(24 * clock.Hour))
		var res pb.StorageItemsImportResponse
		require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
			QueueName: queueName,
			Items:     []*pb.StorageItem{{ExpireDeadline: expire, Reference: "A"}},
		}, &res))
		leaseDeadline := timestamppb.New(now.Add(clock.Hour))
		for range numBatches {
			items := make([]*pb.StorageItem, 0, batchSize)
			for range batchSize {
				items = append(items, &pb.StorageItem{
					IsLeased:       true,
					LeaseDeadline:  leaseDeadline,
					ExpireDeadline: expire,
					Attempts:       1,
				})
			}
			require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
				QueueName: queueName,
				Items:     items,
			}, &res))
		}
		require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
			QueueName: queueName,
			Items:     []*pb.StorageItem{{ExpireDeadline: expire, Reference: "B"}},
		}, &res))

		// Every lease asks for both items. A lease stops looking at the WriteTimeout, so A and
		// B arrive in separate leases, A first. A lease that got nothing waits until its
		// request times out, and each lease resumes where the last one stopped.
		var leased []*pb.QueueLeaseItem
		for range maxAttempts {
			var lease pb.QueueLeaseResponse
			err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "20ms",
				QueueName:      queueName,
				BatchSize:      2,
			}, &lease)
			if err != nil {
				var e duh.Error
				require.True(t, errors.As(err, &e))
				require.Equal(t, querator.MsgRequestTimeout, e.Message())
				continue
			}
			require.Len(t, lease.Items, 1)
			leased = append(leased, lease.Items[0])
			if len(leased) == 2 {
				break
			}
		}
		require.Len(t, leased, 2)
		assert.Equal(t, "A", leased[0].Reference)
		assert.Equal(t, "B", leased[1].Reference)

		// Item A, cut off from B by the WriteTimeout, is leased in storage
		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 1}))
		require.Len(t, list.Items, 1)
		assert.Equal(t, leased[0].Id, list.Items[0].Id)
		assert.True(t, list.Items[0].IsLeased)
	})

	t.Run("LeaseQueuedItemAfterRestart", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		conf := svc.Config{StorageConfig: setup()}
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
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items:          []*pb.QueueProduceItem{{Reference: "A"}, {Reference: "B"}, {Reference: "C"}},
		}))
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &lease))
		require.Len(t, lease.Items, 1)
		assert.Equal(t, "A", lease.Items[0].Reference)
		d.Shutdown(t)
		running = false

		// Nothing is produced after the restart, so no write lowers the lease pivot. The lease
		// must still find B and C from where the daemon's startup read of storage puts it.
		d = newDaemon(t, 30*clock.Second, conf)
		running = true
		c, ctx = d.MustClient(), d.Context()

		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "1s",
			QueueName:      queueName,
			BatchSize:      3,
		}, &lease))
		require.Len(t, lease.Items, 2)
		assert.Equal(t, "B", lease.Items[0].Reference)
		assert.Equal(t, "C", lease.Items[1].Reference)
	})
}
