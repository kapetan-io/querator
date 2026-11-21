package service_test

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fuzz inputs are mapped into the range the server accepts, so every input runs: up to the
// default produce and lease batch limits, and a produce request under fuzzProduceMax bytes.
const (
	fuzzProduceMax = 5_000_000
	// fuzzItemOverhead covers the reference suffix and protobuf framing of one item.
	fuzzItemOverhead = 64
)

// Fails to compile if the produce payload limit drops below fuzzProduceMax, because a negative
// constant can't convert to uint.
const _ = uint(5*duh.MegaByte - fuzzProduceMax)

// fuzzLimit is the upper end of the range fuzzRange maps a fuzz input onto. It is its own type
// so fuzzRange's two arguments can't be swapped.
type fuzzLimit int

// fuzzRange maps any n onto 1..limit, leaving n unchanged when it is already in that range.
func fuzzRange(n int, limit fuzzLimit) int {
	return ((n-1)%int(limit)+int(limit))%int(limit) + 1
}

// fuzzField truncates s to at most limit bytes and replaces invalid UTF-8 with '?'.
func fuzzField(s string, limit int) string {
	return strings.ToValidUTF8(s[:min(len(s), limit)], "?")
}

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
		itemCount = fuzzRange(itemCount, internal.DefaultMaxProduceBatchSize)
		batchSize = fuzzRange(batchSize, internal.DefaultMaxLeaseBatchSize)
		// Each item gets an equal share of the produce request. Fields come out of that share
		// first, each up to a quarter of it, and the payload gets what is left.
		share := fuzzProduceMax/itemCount - fuzzItemOverhead
		encoding, kind, reference = fuzzField(encoding, share/4), fuzzField(kind, share/4), fuzzField(reference, share/4)
		payload = payload[:min(len(payload), share-len(encoding)-len(kind)-len(reference))]

		d := newDaemon(t, 10*clock.Second, svc.Config{
			StorageConfig: setupMemoryStorage(store.Config{}),
		})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

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

		// Produce all items
		err := c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			Items:          items,
			RequestTimeout: "5s",
		})
		require.NoError(t, err)

		// Verify all items are in storage
		var list pb.StorageItemsListResponse
		err = c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: itemCount + 10})
		require.NoError(t, err)
		assert.Equal(t, itemCount, len(list.Items))

		// Lease all items in batches
		leasedItems := make(map[string]*pb.QueueLeaseItem)
		totalLeased := 0

		// Each pass leases at least one item or breaks, so itemCount passes are enough.
		for range itemCount {
			if totalLeased == itemCount {
				break
			}
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
		for _, ref := range slices.Sorted(maps.Keys(produced)) {
			_, exists := leasedItems[ref]
			assert.True(t, exists, "Produced item %s was not leased", ref)
		}

		// Complete all leased items
		completedCount := 0
		for _, ref := range slices.Sorted(maps.Keys(leasedItems)) {
			err := c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				QueueName:      queueName,
				RequestTimeout: "5s",
				Ids:            []string{leasedItems[ref].Id},
			})
			require.NoError(t, err)
			completedCount++
		}
		assert.Equal(t, itemCount, completedCount)

		// Verify queue is now empty
		err = c.StorageItemsList(ctx, queueName, 0, &list, &querator.ListOptions{Limit: 10})
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
