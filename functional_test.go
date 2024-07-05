package querator_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"testing"
	"time"
)

type NewFunc func() store.Storage

var log = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestFunctionalSuite(t *testing.T) {
	testCases := []struct {
		Name string
		New  NewFunc
	}{
		{
			Name: "BuntDB",
			New: func() store.Storage {
				return store.NewBuntStorage(store.BuntOptions{Logger: log})
			},
		},
		//{
		//	Name: "PostgresSQL",
		//},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			testSuite(t, tc.New)
		})
	}
}

func testSuite(t *testing.T, newStore NewFunc) {

	t.Run("ProduceAndConsume", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, newStore, 10*time.Second)
		defer d.Shutdown(t)

		// Create a queue
		require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))

		// Produce a single message
		ref := random.String("ref-", 10)
		enc := random.String("enc-", 10)
		kind := random.String("kind-", 10)
		payload := []byte("I didn't learn a thing. I was right all along")
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{
				{
					Reference: ref,
					Encoding:  enc,
					Kind:      kind,
					Bytes:     payload,
				},
			},
		}))

		// Reserve a single message
		var reserve pb.QueueReserveResponse
		require.NoError(t, c.QueueReserve(ctx, &pb.QueueReserveRequest{
			ClientId:       random.String("client-", 10),
			RequestTimeout: "5s",
			QueueName:      queueName,
			BatchSize:      1,
		}, &reserve))

		// Ensure we got the item we produced
		assert.Equal(t, 1, len(reserve.Items))
		item := reserve.Items[0]
		assert.Equal(t, ref, item.Reference)
		assert.Equal(t, enc, item.Encoding)
		assert.Equal(t, kind, item.Kind)
		assert.Equal(t, int32(0), item.Attempts)
		assert.Equal(t, payload, item.Bytes)

		// Ensure the item is in storage and marked as reserved
		var inspect pb.StorageItem
		require.NoError(t, c.StorageInspect(ctx, item.Id, &inspect))

		assert.Equal(t, ref, inspect.Reference)
		assert.Equal(t, kind, inspect.Kind)
		assert.Equal(t, int32(0), inspect.Attempts)
		assert.Equal(t, payload, inspect.Payload)
		assert.Equal(t, item.Id, inspect.Id)
		assert.Equal(t, true, inspect.IsReserved)

		// Queue storage should have only one item
		var list pb.StorageListResponse
		require.NoError(t, c.StorageList(ctx, &pb.StorageListRequest{
			QueueName: queueName,
			Pivot:     "",
			Limit:     10,
		}, &list))
		assert.Equal(t, 1, len(list.Items))

		// Mark the item as complete
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Ids: []string{
				item.Id,
			},
		}))

		// Queue storage should be empty
		require.NoError(t, c.StorageList(ctx, &pb.StorageListRequest{
			QueueName: queueName,
			Pivot:     "",
			Limit:     10,
		}, &list))

		assert.Equal(t, 0, len(list.Items))

		// TODO: Delete the queue
	})

	t.Run("QueueCompleteCantCompleteWithoutReservation", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, newStore, 10*time.Second)
		defer d.Shutdown(t)
		items := randomProduceItems(10)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          items,
		}))

		// Queue should have 10 items
		var list pb.StorageListResponse
		require.NoError(t, c.StorageList(ctx, &pb.StorageListRequest{
			QueueName: queueName,
			Pivot:     "",
			Limit:     10,
		}, &list))
		assert.Equal(t, 10, len(list.Items))
		item := list.Items[0]

		// Mark the item as complete
		err := c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Ids: []string{
				item.Id,
			},
		})
		require.Error(t, err)
		var e duh.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, "!", e.Message())
		assert.Equal(t, 400, e.Code())

	})

	t.Run("QueueProduceErrors", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, newStore, 5*time.Second)
		defer d.Shutdown(t)
		maxItems := randomProduceItems(1_001)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))

		for _, test := range []struct {
			Name string
			Req  *pb.QueueProduceRequest
			Msg  string
			Code int
		}{
			{
				Name: "empty_request",
				Req:  &pb.QueueProduceRequest{},
				Msg:  "invalid queue_name; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "invalid_queue",
				Req: &pb.QueueProduceRequest{
					QueueName: "invalid~queue",
				},
				Msg:  "invalid queue_name; 'invalid~queue' cannot contain '~' character",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "no_items_nil_pointer",
				Req: &pb.QueueProduceRequest{
					QueueName: queueName,
					Items:     nil,
				},
				Msg:  "items cannot be empty; at least one item is required",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "no_items_empty_list",
				Req: &pb.QueueProduceRequest{
					QueueName: queueName,
					Items:     []*pb.QueueProduceItem{},
				},
				Msg:  "items cannot be empty; at least one item is required",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "items_with_no_payload_are_okay",
				Req: &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "5m",
					Items: []*pb.QueueProduceItem{
						{},
						{},
					},
				},
				Code: duh.CodeOK,
			},
			{
				Name: "request_timeout_is_required",
				Req: &pb.QueueProduceRequest{
					QueueName: queueName,
					Items: []*pb.QueueProduceItem{
						{},
					},
				},
				Msg:  "request_timeout is required; '5m' is recommended, 15m is the maximum",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "request_timeout_too_long",
				Req: &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "16m",
					Items: []*pb.QueueProduceItem{
						{},
					},
				},
				Msg:  "request_timeout is invalid; maximum timeout is '15m' but '16m0s' was requested",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "invalid_request_timeout",
				Req: &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "foo",
					Items: []*pb.QueueProduceItem{
						{},
					},
				},
				Msg:  "request_timeout is invalid; time: invalid duration \"foo\" - expected format: 900ms, 5m or 15m",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "invalid_request_timeout",
				Req: &pb.QueueProduceRequest{
					QueueName:      queueName,
					RequestTimeout: "1m",
					Items:          maxItems,
				},
				Msg:  "too many items in request; max_produce_batch_size is 1000 but received 1001",
				Code: duh.CodeBadRequest,
			},
			// TODO: Max items reached
		} {
			t.Run(test.Name, func(t *testing.T) {
				err := c.QueueProduce(ctx, test.Req)
				if test.Code != duh.CodeOK {
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Equal(t, test.Msg, e.Message())
					assert.Equal(t, test.Code, e.Code())
				}
			})
		}
	})

	t.Run("QueueReserveErrors", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		var clientID = random.String("client-", 10)
		d, c, ctx := newDaemon(t, newStore, 5*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))

		for _, tc := range []struct {
			Name string
			Req  *pb.QueueReserveRequest
			Msg  string
			Code int
		}{
			{
				Name: "empty_request",
				Req:  &pb.QueueReserveRequest{},
				Msg:  "invalid queue_name; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "client_id_missing",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
				},
				Msg:  "invalid client_id; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "batch_size_cannot_be_empty",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
					ClientId:  clientID,
				},
				Msg:  "invalid batch_size; must be greater than zero",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "batch_size_maximum",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
					ClientId:  clientID,
					BatchSize: 1_001,
				},
				Msg:  "invalid batch_size; exceeds maximum limit max_reserve_batch_size is 1000, but 1001 was requested",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "request_timeout_required",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
					ClientId:  clientID,
					BatchSize: 111,
				},
				Msg:  "request_timeout is required; '5m' is recommended, 15m is the maximum",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "request_timeout_too_long",
				Req: &pb.QueueReserveRequest{
					QueueName:      queueName,
					ClientId:       clientID,
					BatchSize:      1_000,
					RequestTimeout: "16m",
				},
				Msg:  "invalid request_timeout; maximum timeout is '15m' but '16m0s' requested",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "request_timeout_invalid",
				Req: &pb.QueueReserveRequest{
					QueueName:      queueName,
					ClientId:       clientID,
					BatchSize:      1_000,
					RequestTimeout: "foo",
				},
				Msg:  "request_timeout is invalid; time: invalid duration \"foo\" - expected format: 900ms, 5m or 15m",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "minimum_request_timeout_is_allowed",
				Req: &pb.QueueReserveRequest{
					QueueName:      queueName,
					ClientId:       clientID,
					BatchSize:      1_000,
					RequestTimeout: "10ms",
				},
				Code: duh.CodeOK,
			},
			{
				Name: "request_timeout_is_too_short",
				Req: &pb.QueueReserveRequest{
					QueueName:      queueName,
					ClientId:       clientID,
					BatchSize:      1_000,
					RequestTimeout: "1ms",
				},
				Msg:  "request_timeout is invalid; minimum timeout is '10ms' but '1ms' was requested",
				Code: duh.CodeBadRequest,
			},
		} {
			t.Run(tc.Name, func(t *testing.T) {
				var res pb.QueueReserveResponse
				err := c.QueueReserve(ctx, tc.Req, &res)
				if tc.Code != duh.CodeOK {
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Equal(t, tc.Msg, e.Message())
					assert.Equal(t, tc.Code, e.Code())
					if e.Message() == "" {
						t.Logf("Error: %s", e.Error())
					}
				}
			})
		}
	})
}

// TODO: Ensure the completed item is actually in reserved status before marking complete.

type testDaemon struct {
	cancel context.CancelFunc
	ctx    context.Context
	d      *daemon.Daemon
}

func (td *testDaemon) Shutdown(t *testing.T) {
	require.NoError(t, td.d.Shutdown(td.ctx))
	td.cancel()
}

func (td *testDaemon) MustClient() *querator.Client {
	return td.d.MustClient()
}

func (td *testDaemon) Context() context.Context {
	return td.ctx
}

func newDaemon(t *testing.T, newStore NewFunc, duration time.Duration) (*testDaemon, *querator.Client, context.Context) {
	var err error
	td := &testDaemon{}
	td.ctx, td.cancel = context.WithTimeout(context.Background(), duration)
	td.d, err = daemon.NewDaemon(td.ctx, daemon.Config{
		Store:  newStore(),
		Logger: log,
	})
	require.NoError(t, err)
	return td, td.d.MustClient(), td.ctx
}

func randomProduceItems(count int) []*pb.QueueProduceItem {
	var items []*pb.QueueProduceItem
	for i := 0; i < count; i++ {
		items = append(items, &pb.QueueProduceItem{
			Reference: random.String("ref-", 10),
			Encoding:  random.String("enc-", 10),
			Kind:      random.String("kind-", 10),
			Bytes:     []byte(fmt.Sprintf("message-%d", i)),
		})
	}
	return items
}

// TODO: Defer
// TODO: Implement clock style thingy so we can freeze time and advance time in order to test Deadlines and such.
// TODO: Test /queue.produce and all the possible incorrect way it could be called
// TODO: Test /queue.reserve and all the possible incorrect way it could be called
// TODO: Test /queue.complete and all the possible incorrect way it could be called
