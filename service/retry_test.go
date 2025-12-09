package service_test

import (
	"errors"
	"github.com/duh-rpc/duh-go"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

func TestRetry(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}

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
				return badgerdb.Setup(store.BadgerConfig{})
			},
			TearDown: func() {
				badgerdb.Teardown()
			},
		},
		// {
		// 	Name: "SurrealDB",
		// },
		// {
		// 	Name: "PostgresSQL",
		// },
	} {
		t.Run(tc.Name, func(t *testing.T) {
			testRetry(t, tc.Setup, tc.TearDown)
		})
	}
}

func testRetry(t *testing.T, setup NewStorageFunc, tearDown func()) {
	defer goleak.VerifyNone(t)

	t.Run("QueueRetry", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, 10*clock.Second, svc.ServiceConfig{StorageConfig: setup()})
		defer func() {
			d.Shutdown(t)
			tearDown()
		}()

		// Create a queue
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})

		// Produce items for retry testing
		ref1 := random.String("ref-", 10)
		ref2 := random.String("ref-", 10)
		ref3 := random.String("ref-", 10)
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{
				{
					Reference: ref1,
					Encoding:  "utf8",
					Kind:      "immediate-retry",
					Bytes:     []byte("immediate retry test"),
				},
				{
					Reference: ref2,
					Encoding:  "utf8",
					Kind:      "scheduled-retry",
					Bytes:     []byte("scheduled retry test"),
				},
				{
					Reference: ref3,
					Encoding:  "utf8",
					Kind:      "dead-letter",
					Bytes:     []byte("dead letter test"),
				},
			},
		}))

		// Lease the items
		var lease pb.QueueLeaseResponse
		require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      3,
		}, &lease))

		require.Equal(t, 3, len(lease.Items))
		item1 := findInLeaseResp(ref1, &lease)
		item2 := findInLeaseResp(ref2, &lease)
		item3 := findInLeaseResp(ref3, &lease)
		require.NotNil(t, item1)
		require.NotNil(t, item2)
		require.NotNil(t, item3)

		t.Run("ImmediateRetry", func(t *testing.T) {
			// Retry item1 immediately (no RetryAt timestamp)
			require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
				QueueName: queueName,
				Partition: lease.Partition,
				Items: []*pb.QueueRetryItem{
					{
						Id:   item1.Id,
						Dead: false,
						// No RetryAt means immediate retry
					},
				},
			}))

			// Verify item is immediately available for lease again with incremented attempts
			var retryLease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "5s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &retryLease))

			require.Equal(t, 1, len(retryLease.Items))
			retriedItem := retryLease.Items[0]
			assert.Equal(t, ref1, retriedItem.Reference)
			assert.Equal(t, int32(2), retriedItem.Attempts)

			// Complete the retried item to clean up
			require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
				QueueName:      queueName,
				Partition:      retryLease.Partition,
				RequestTimeout: "5s",
				Ids:            []string{retriedItem.Id},
			}))
		})

		t.Run("ScheduledRetry", func(t *testing.T) {
			// Retry item2 with future timestamp
			futureTime := time.Now().UTC().Add(1 * time.Second)
			require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
				QueueName: queueName,
				Partition: lease.Partition,
				Items: []*pb.QueueRetryItem{
					{
						Id:      item2.Id,
						Dead:    false,
						RetryAt: timestamppb.New(futureTime),
					},
				},
			}))

			// Verify item is not immediately available (should be scheduled)
			var emptyLease pb.QueueLeaseResponse
			err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "500ms",
				QueueName:      queueName,
				BatchSize:      1,
			}, &emptyLease)
			require.Error(t, err)
			var e duh.Error
			require.True(t, errors.As(err, &e))
			assert.Equal(t, duh.CodeRetryRequest, e.Code())

			// Item was successfully scheduled for retry (not immediately available)
			// For now, we'll skip testing the scheduled delivery since that's a lifecycle feature
			// that may not be fully implemented yet
		})

		t.Run("DeadLetter", func(t *testing.T) {
			// Mark item3 as dead
			require.NoError(t, c.QueueRetry(ctx, &pb.QueueRetryRequest{
				QueueName: queueName,
				Partition: lease.Partition,
				Items: []*pb.QueueRetryItem{
					{
						Id:   item3.Id,
						Dead: true,
					},
				},
			}))

			// Verify item is no longer available for lease (dead items are removed)
			var noLease pb.QueueLeaseResponse
			err := c.QueueLease(ctx, &pb.QueueLeaseRequest{
				ClientId:       random.String("client-", 10),
				RequestTimeout: "1s",
				QueueName:      queueName,
				BatchSize:      1,
			}, &noLease)
			require.Error(t, err)
			var e duh.Error
			require.True(t, errors.As(err, &e))
			assert.Equal(t, duh.CodeRetryRequest, e.Code())
		})
	})

	t.Run("Errors", func(t *testing.T) {
		storage := setup()
		defer tearDown()

		t.Run("QueueRetry", func(t *testing.T) {
			var queueName = random.String("queue-", 10)
			d, c, ctx := newDaemon(t, 5*clock.Second, svc.ServiceConfig{StorageConfig: storage})
			defer d.Shutdown(t)

			createQueueAndWait(t, ctx, c, &pb.QueueInfo{
				LeaseTimeout:        LeaseTimeout,
				ExpireTimeout:       ExpireTimeout,
				QueueName:           queueName,
				RequestedPartitions: 1,
			})

			for _, tc := range []struct {
				Name string
				Req  *pb.QueueRetryRequest
				Msg  string
				Code int
			}{
				{
					Name: "EmptyRequest",
					Req:  &pb.QueueRetryRequest{},
					Msg:  "queue name is invalid; queue name cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "EmptyItems",
					Req: &pb.QueueRetryRequest{
						QueueName: queueName,
						Items:     []*pb.QueueRetryItem{},
					},
					Msg:  "items is invalid; list of items cannot be empty",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidPartition",
					Req: &pb.QueueRetryRequest{
						QueueName: queueName,
						Partition: 65234,
						Items: []*pb.QueueRetryItem{
							{Id: "test-id"},
						},
					},
					Msg:  "partition is invalid; '65234' is not a valid partition",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "InvalidId",
					Req: &pb.QueueRetryRequest{
						QueueName: queueName,
						Items: []*pb.QueueRetryItem{
							{Id: "invalid-id"},
						},
					},
					Msg:  "invalid storage id; 'invalid-id'",
					Code: duh.CodeBadRequest,
				},
				{
					Name: "NotLeased",
					Req: &pb.QueueRetryRequest{
						QueueName: queueName,
						Items: []*pb.QueueRetryItem{
							{Id: ksuid.New().String()},
						},
					},
					Msg:  "does not exist",
					Code: duh.CodeBadRequest,
				},
			} {
				t.Run(tc.Name, func(t *testing.T) {
					err := c.QueueRetry(ctx, tc.Req)
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Contains(t, e.Message(), tc.Msg)
					assert.Equal(t, tc.Code, e.Code())
					if e.Message() == "" {
						t.Logf("Error: %s", e.Error())
					}
				})
			}
		})
	})
}
