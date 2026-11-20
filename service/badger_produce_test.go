package service_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/duh-rpc/duh-go"

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
// no other backend has. They run against BadgerDB only.
func TestBadgerProduce(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	testBadgerProduce(t, func() store.Config {
		return badgerdb.Setup(store.BadgerConfig{})
	}, badgerdb.Teardown)
}

func testBadgerProduce(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("ProduceTooBigForOneTransactionStoresNothing", func(t *testing.T) {
		// A dead letter move may be stored across several BadgerDB transactions. A client's
		// produce never is, so one too big for a transaction fails whole.
		const count, payload = 12, 1_000_000
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
}
