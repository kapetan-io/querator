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
// no other backend has. They run against BadgerDB only, with a 1 MiB write buffer so one
// transaction holds 157,286 bytes and small items reach the limit.
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
		// about 364 to 465 bytes under the limit behave this way; 410 is near the middle.
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
		assert.ElementsMatch(t, []string{"item-00000", "item-00001"}, storedReferences(t, ctx, c, dlqName))
		assert.Equal(t, []string{"item-00002"}, storedReferences(t, ctx, c, queueName))
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
