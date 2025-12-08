package querator_test

import (
	"context"
	"fmt"
	"testing"
	"time"
	"unicode/utf8"

	que "github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/random"
	"github.com/kapetan-io/tackle/set"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// FuzzQueueInvariant tests the core queue property: all produced items must be
// accounted for through their lifecycle (produced -> leased -> completed),
// and completing all items should result in an empty queue.
func FuzzQueueInvariant(f *testing.F) {
	// Seed corpus with realistic item data
	f.Add(10, 3, []byte("test data"), "json", "text/plain", "ref-12345")
	f.Add(1, 1, []byte("\x00\x01\x02\x03\x04"), "binary", "application/octet-stream", "binary-ref")
	f.Add(50, 10, []byte("larger payload with special chars: \n\t\x00"), "protobuf",
		"application/octet-stream", "ref-abc-xyz")
	f.Add(100, 25, []byte(""), "", "", "")

	f.Fuzz(func(t *testing.T, itemCount, batchSize int,
		payload []byte, encoding, kind, reference string) {
		// Ensure valid input for each field
		if !utf8.ValidString(encoding) || !utf8.ValidString(kind) || !utf8.ValidString(reference) ||
			itemCount < 1 || batchSize < 1 || len(payload) < 1 {
			t.Skip()
		}
		d, c, ctx := newFuzzDaemon(t, que.ServiceConfig{
			StorageConfig: setupMemoryStorage(store.Config{}),
		})
		defer d.Shutdown(t)

		queueName := fmt.Sprintf("fuzz-%d-%s", itemCount, random.Alpha("", 10))
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "30s",
			ExpireTimeout:       "5m",
			MaxAttempts:         3,
			RequestedPartitions: 1,
		})

		items := make([]*pb.QueueProduceItem, 0, itemCount)
		produced := make(map[string]struct{})

		// Generate items with variations based on fuzz inputs
		for i := 0; i < itemCount; i++ {
			ref := fmt.Sprintf("%s-%d", reference, i)
			produced[ref] = struct{}{}

			items = append(items, &pb.QueueProduceItem{
				Encoding:  encoding,
				Bytes:     payload,
				Kind:      kind,
				Reference: ref,
			})
		}

		// Ensure the total produce payload does not exceed querator limits
		payload, err := proto.Marshal(&pb.QueueProduceRequest{
			QueueName:      queueName,
			Items:          items,
			RequestTimeout: "5s",
		})
		require.NoError(t, err)
		if len(payload) >= 5_000_000 {
			t.Skip()
		}

		// Produce all items
		err = c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			Items:          items,
			RequestTimeout: "5s",
		})
		require.NoError(t, err)

		// Verify all items are in storage
		var list pb.StorageItemsListResponse
		err = c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Limit: itemCount + 10})
		require.NoError(t, err)
		assert.Equal(t, itemCount, len(list.Items))

		// Lease all items in batches
		leasedItems := make(map[string]*pb.QueueLeaseItem)
		totalLeased := 0

		for totalLeased < itemCount {
			var lease pb.QueueLeaseResponse
			err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       fmt.Sprintf("client-%d", totalLeased),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      int32(batchSize),
			}, &lease)
			require.NoError(t, err, "Failed to lease items")

			// Track leased items
			for _, item := range lease.Items {
				assert.NotContains(t, leasedItems, item.Reference, "Item %s was leased multiple times", item.Reference)
				leasedItems[item.Reference] = item
				totalLeased++

				// Verify fields were preserved
				assert.Contains(t, item.Encoding, encoding)
				assert.Equal(t, kind, item.Kind)
			}

			// Break if no more items available
			if len(lease.Items) == 0 {
				break
			}
		}

		// Verify we leased all produced items
		assert.Equal(t, itemCount, totalLeased, "Should lease all produced items")
		assert.Equal(t, len(produced), len(leasedItems), "All produced items should be leased")

		// Verify all produced references were leased
		for ref := range produced {
			_, exists := leasedItems[ref]
			assert.True(t, exists, "Produced item %s was not leased", ref)
		}

		// Complete all leased items
		completedCount := 0
		for _, item := range leasedItems {
			err := c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Ids:            []string{item.Id},
			})
			require.NoError(t, err)
			completedCount++
		}
		assert.Equal(t, itemCount, completedCount)

		// Verify queue is now empty
		err = c.StorageItemsList(ctx, queueName, 0, &list, &que.ListOptions{Limit: 10})
		require.NoError(t, err)
		assert.Equal(t, 0, len(list.Items),
			"Queue should be empty after completing all items")

		// Try to lease again - should get nothing
		var finalLease pb.QueueLeaseResponse
		err = c.QueueLease(ctx, &pb.QueueLeaseRequest{
			BatchSize:      int32(batchSize),
			ClientId:       "final-client",
			QueueName:      queueName,
			RequestTimeout: "100ms",
		}, &finalLease)
		require.ErrorContains(t, err, "no items are in the queue, try again")
		assert.Equal(t, 0, len(finalLease.Items))

		// Clean up
		err = c.QueuesDelete(ctx, &pb.QueuesDeleteRequest{QueueName: queueName})
		require.NoError(t, err)
	})
}

func newFuzzDaemon(t *testing.T, conf que.ServiceConfig) (*testDaemon, *que.Client, context.Context) {
	t.Helper()

	set.Default(&conf.Log, log)
	td := &testDaemon{}
	var err error

	td.ctx, td.cancel = context.WithTimeout(context.Background(), 10*time.Second)
	td.d, err = daemon.NewDaemon(td.ctx, daemon.Config{
		ListenAddress: "localhost:0",
		Service:       conf,
	})
	require.NoError(t, err)
	return td, td.d.MustClient(), td.ctx
}
