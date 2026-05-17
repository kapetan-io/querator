package service_test

import (
	"testing"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestBugValidation contains surface tests that demonstrate confirmed bugs from the PR review.
// Each test should FAIL with current buggy behavior and PASS once fixed.
func TestBugValidation(t *testing.T) {
	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
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
				badger := &badgerTestSetup{Dir: t.TempDir()}
				t.Cleanup(func() { badger.Teardown() })
				return badger.Setup(store.BadgerConfig{})
			},
			TearDown: func() {},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testBugValidation(t, tc.Setup, tc.TearDown)
		})
	}
}

func testBugValidation(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t, goleakOptions...)

	// Bug #4: QueuesCreate stores queues with empty namespace instead of _system.
	// When a queue is created without specifying a namespace, resolveNamespace returns
	// "_system" for authorization but info.Namespace is never updated. The queue is
	// stored with Namespace: "" instead of Namespace: "_system".
	t.Run("QueuesCreateEmptyNamespace", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		queueName := random.String("queue-", 10)

		// Create a queue without specifying a namespace
		require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			RequestedPartitions: 1,
		}))

		// List queues explicitly filtering by _system namespace.
		// The queue should appear because it was created with no namespace,
		// which should resolve to _system.
		var listRes pb.QueuesListResponse
		require.NoError(t, c.QueuesList(ctx, &listRes, &querator.ListOptions{
			Namespace: auth.SystemNamespace,
		}))

		var found bool
		for _, q := range listRes.Items {
			if q.QueueName == queueName {
				found = true
				assert.Equal(t, auth.SystemNamespace, q.Namespace)
			}
		}
		assert.True(t, found)
	})

	// Bug #9: SourceID index is never cleaned up on Complete.
	// After completing an item with a SourceID, re-importing an item with the same
	// SourceID should succeed, but currently the old index entry remains and the
	// second import is silently dropped as a duplicate.
	t.Run("SourceIDCleanedUpOnComplete", func(t *testing.T) {
		d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			RequestedPartitions: 1,
		})

		sourceID := random.String("source-", 10)

		// Import an item with a SourceID
		var importResp pb.StorageItemsImportResponse
		require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
			QueueName: queueName,
			Partition: 0,
			Items: []*pb.StorageItem{
				{
					SourceId:       sourceID,
					Reference:      "first-ref",
					Encoding:       "test-enc",
					Kind:           "test-kind",
					Payload:        []byte("first payload"),
					ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(1 * clock.Hour)),
					MaxAttempts:    3,
				},
			},
		}, &importResp))
		require.Len(t, importResp.Items, 1)

		// Lease and complete the item
		var leaseRes pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			QueueName:      queueName,
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			BatchSize:      1,
		}, &leaseRes))
		require.Len(t, leaseRes.Items, 1)

		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Ids:            []string{leaseRes.Items[0].Id},
		}))

		// Now import a new item with the same SourceID — should succeed
		var importResp2 pb.StorageItemsImportResponse
		require.NoError(t, c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
			QueueName: queueName,
			Partition: 0,
			Items: []*pb.StorageItem{
				{
					SourceId:       sourceID,
					Reference:      "second-ref",
					Encoding:       "test-enc",
					Kind:           "test-kind",
					Payload:        []byte("second payload"),
					ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(1 * clock.Hour)),
					MaxAttempts:    3,
				},
			},
		}, &importResp2))
		require.Len(t, importResp2.Items, 1)

		// Verify the new item exists in the queue
		var listResp pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &listResp, nil))
		require.Len(t, listResp.Items, 1)
		assert.Equal(t, "second-ref", listResp.Items[0].Reference)
		assert.Equal(t, sourceID, listResp.Items[0].SourceId)
	})

}
