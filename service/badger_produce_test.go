package service_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/duh-rpc/duh-go"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestBadgerProduce holds produce tests that depend on BadgerDB's transaction size limit, which
// no other backend has. They run against BadgerDB only, with a small write buffer so small items
// reach the limit.
func TestBadgerProduce(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	testBadgerProduce(t, func() store.Config {
		return badgerdb.Setup(store.BadgerConfig{MemTableSize: badgerMemTableSize})
	}, badgerdb.Teardown)
}

const (
	badgerMemTableSize     = 1 << 20
	badgerTransactionLimit = 15 * badgerMemTableSize / 100
)

func testBadgerProduce(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("ProduceTooBigForOneTransactionStoresNothing", func(t *testing.T) {
		// A dead letter move may be stored across several BadgerDB transactions. A client's
		// produce never is, so one too big for a transaction fails whole.
		const count, payload = 12, 20_000
		ctx, cancel := context.WithTimeout(context.Background(), 30*clock.Second)
		d, err := daemon.NewDaemon(ctx, daemon.Config{
			Service:               svc.Config{StorageConfig: setup(), Log: log},
			ListenAddress:         "localhost:0",
			MaxProducePayloadSize: 2 * count * payload,
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, d.Shutdown(ctx))
			cancel()
			tearDown()
		}()
		c := d.MustClient()

		queueName := random.String("queue-", 10)
		createOnePartitionQueue(t, ctx, c, queueName)

		items := make([]*pb.QueueProduceItem, count)
		for i := range items {
			items[i] = &pb.QueueProduceItem{
				Reference: fmt.Sprintf("item-%05d", i),
				Bytes:     []byte(strings.Repeat("x", payload)),
			}
		}
		err = c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          items,
		})
		var e duh.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, duh.CodeRetryRequest, e.Code())
		assert.Equal(t, int32(0), partitionTotal(t, ctx, c, queueName))
	})

	t.Run("DeadLetterMoveFailingPartwayDeletesOnlyWhatItStored", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		dlqName := random.String("dlq-", 10)
		createOnePartitionQueue(t, ctx, c, dlqName)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "1m0s",
			DeadQueue:           dlqName,
			RequestedPartitions: 1,
		})

		// The source stores item-00002 in one transaction, but its dead copy also carries a SourceID
		// and a source: key, which put it over the limit even alone in a transaction. Sizes from
		// about 364 to 465 bytes under the limit behave this way; 410 is near the middle. When this
		// produce fails, item-00002 no longer fits the source: the size window moved.
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items: []*pb.QueueProduceItem{
				{Reference: "item-00000", Bytes: []byte("x")},
				{Reference: "item-00001", Bytes: []byte("x")},
			},
		}))
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items: []*pb.QueueProduceItem{
				{Reference: "item-00002", Bytes: []byte(strings.Repeat("x", badgerTransactionLimit-410))},
			},
		}))

		// The move commits item-00000 and item-00001 in one transaction, then fails on item-00002.
		// The source deletes as many dead items as BadgerDB reports stored. Fewer than two leaves a
		// stored item in the source to be moved again; more than two deletes item-00002, which the
		// dead letter queue never stored.
		now.Advance(clock.Minute + clock.Second)
		waitForPageWritten(t, ctx, c, queueName)
		// Three here means item-00002's dead copy fit one transaction: the size window moved, and
		// the move never failed partway.
		require.Equal(t, int32(2), partitionTotal(t, ctx, c, dlqName))
		assert.ElementsMatch(t, []string{"item-00000", "item-00001"}, storedReferences(t, ctx, c, dlqName))
		assert.Equal(t, []string{"item-00002"}, storedReferences(t, ctx, c, queueName))
	})

	t.Run("LargeDeadItemsMoveToDeadLetter", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

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

		// Four items fit one transaction, so each produce stores whole, but the 16 dead items do not.
		const count, perRequest, payload = 16, 4, 20_000
		for done := 0; done < count; done += perRequest {
			items := make([]*pb.QueueProduceItem, perRequest)
			for i := range items {
				items[i] = &pb.QueueProduceItem{
					Reference: fmt.Sprintf("item-%05d", done+i),
					Bytes:     []byte(strings.Repeat("x", payload)),
				}
			}
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          items,
			}))
		}
		require.Len(t, leaseUpTo(t, ctx, c, queueName, count), count)

		now.Advance(clock.Minute + clock.Second)
		require.Eventually(t, func() bool {
			return partitionTotal(t, ctx, c, dlqName) == count
		}, pageWriteTimeout, 50*clock.Millisecond)
		assert.Equal(t, int32(0), partitionTotal(t, ctx, c, queueName))
	})

	t.Run("LargeDeadItemsMoveWhileClientsProduceToDeadLetter", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: setup(), Clock: now, WriteTimeout: pageWriteTimeout})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()
		c, ctx := d.MustClient(), d.Context()

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

		const count, perRequest, payload = 16, 4, 20_000
		for done := 0; done < count; done += perRequest {
			items := make([]*pb.QueueProduceItem, perRequest)
			for i := range items {
				items[i] = &pb.QueueProduceItem{
					Reference: fmt.Sprintf("item-%05d", done+i),
					Bytes:     []byte(strings.Repeat("x", payload)),
				}
			}
			require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Items:          items,
			}))
		}
		require.Len(t, leaseUpTo(t, ctx, c, queueName, count), count)

		// A paused queue holds the produce requests it receives and stores them all in one pass when
		// it resumes, so the move and both client produces share that pass once ProduceWaiting is 3.
		// Stored in one write, the move would be too big for BadgerDB and would fail both client
		// produces with it.
		require.NoError(t, d.Service().PauseQueue(ctx, dlqName))
		callCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		produceOne := func(reference string) error {
			return c.QueueProduce(callCtx, &pb.QueueProduceRequest{
				QueueName:      dlqName,
				RequestTimeout: "30s",
				Items:          []*pb.QueueProduceItem{{Reference: reference, Bytes: []byte("x")}},
			})
		}
		var produceErrs [2]error
		duringCalls(cancel, func() {
			now.Advance(clock.Minute + clock.Second)
			require.Eventually(t, func() bool {
				var stats pb.QueueStatsResponse
				if err := c.QueueStats(ctx, &pb.QueueStatsRequest{QueueName: dlqName}, &stats); err != nil {
					return false
				}
				return len(stats.LogicalQueues) == 1 && stats.LogicalQueues[0].ProduceWaiting == 3
			}, pageWriteTimeout, 10*clock.Millisecond)
			require.NoError(t, d.Service().ResumeQueue(ctx, dlqName))
		}, func() {
			produceErrs[0] = produceOne("client-1")
		}, func() {
			produceErrs[1] = produceOne("client-2")
		})

		require.NoError(t, produceErrs[0])
		require.NoError(t, produceErrs[1])
		require.Eventually(t, func() bool {
			return partitionTotal(t, ctx, c, dlqName) == count+2
		}, pageWriteTimeout, 50*clock.Millisecond)
		assert.Equal(t, int32(0), partitionTotal(t, ctx, c, queueName))
	})
}

// storedReferences lists the references of the items stored in the queue's only partition.
func storedReferences(t *testing.T, ctx context.Context, c *querator.Client, queueName string) []string {
	t.Helper()
	var list pb.StorageItemsListResponse
	require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 10}))
	references := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		references = append(references, item.Reference)
	}
	return references
}
