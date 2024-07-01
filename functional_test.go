package querator_test

import (
	"context"
	"errors"
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
		d, c, ctx := newDaemon(t, newStore, 5*time.Second)
		defer d.Shutdown(t)

		// Create a queue
		err := c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName})
		require.NoError(t, err)

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
			RequestTimeout: "5m",
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
			RequestTimeout: "5m",
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

	t.Run("QueueProduceErrors", func(t *testing.T) {
		var queueName = random.String("queue-", 10)
		d, c, ctx := newDaemon(t, newStore, 5*time.Second)
		defer d.Shutdown(t)

		require.NoError(t, c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName}))

		for _, tc := range []struct {
			Name string
			Req  *pb.QueueProduceRequest
			Msg  string
			Code int
		}{
			{
				Name: "empty request",
				Req:  &pb.QueueProduceRequest{},
				Msg:  "invalid queue_name; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "invalid queue",
				Req: &pb.QueueProduceRequest{
					QueueName: "invalid~queue",
				},
				Msg:  "invalid queue_name; 'invalid~queue' cannot contain '~' character",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "no items, nil pointer",
				Req: &pb.QueueProduceRequest{
					QueueName: queueName,
					Items:     nil,
				},
				Msg:  "items cannot be empty; at least one item is required",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "no items, empty list",
				Req: &pb.QueueProduceRequest{
					QueueName: queueName,
					Items:     []*pb.QueueProduceItem{},
				},
				Msg:  "items cannot be empty; at least one item is required",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "items with no payload are okay",
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
				Name: "request_timeout is required",
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
				Name: "request_timeout too long",
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
				Name: "invalid request_timeout",
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
			// TODO: Max items reached
		} {
			t.Run(tc.Name, func(t *testing.T) {
				err := c.QueueProduce(ctx, tc.Req)
				if tc.Code != duh.CodeOK {
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Equal(t, e.Message(), tc.Msg)
					assert.Equal(t, e.Code(), tc.Code)
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
				Name: "empty request",
				Req:  &pb.QueueReserveRequest{},
				Msg:  "invalid queue_name; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "client id missing",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
				},
				Msg:  "invalid client_id; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "batch size cannot be empty",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
					ClientId:  clientID,
				},
				Msg:  "invalid batch_size; must be greater than zero",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "batch size maximum",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
					ClientId:  clientID,
					BatchSize: 1_001,
				},
				Msg:  "invalid batch_size; exceeds maximum limit max_reserve_batch_size is 1000, but 1001 was requested",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "request_timeout required",
				Req: &pb.QueueReserveRequest{
					QueueName: queueName,
					ClientId:  clientID,
					BatchSize: 111,
				},
				Msg:  "request_timeout is required; '5m' is recommended, 15m is the maximum",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "request_timeout too long",
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
				Name: "request_timeout invalid",
				Req: &pb.QueueReserveRequest{
					QueueName:      queueName,
					ClientId:       clientID,
					BatchSize:      1_000,
					RequestTimeout: "foo",
				},
				Msg:  "request_timeout is invalid; time: invalid duration \"foo\" - expected format: 900ms, 5m or 15m",
				Code: duh.CodeBadRequest,
			},
			// TODO: Consider setting a minimum request_timeout of 10ms, anything less would almost guarantee a cancelled request before fulfillment.
			//   ^^^^^ THIS NEXT
		} {
			t.Run(tc.Name, func(t *testing.T) {
				var res pb.QueueReserveResponse
				err := c.QueueReserve(ctx, tc.Req, &res)
				if tc.Code != duh.CodeOK {
					var e duh.Error
					require.True(t, errors.As(err, &e))
					assert.Equal(t, e.Message(), tc.Msg)
					assert.Equal(t, e.Code(), tc.Code)
				}
			})
		}
	})
}

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

// TODO: Defer
// TODO: Implement clock style thingy so we can freeze time and advance time in order to test Deadlines and such.
// TODO: Test /queue.produce and all the possible incorrect way it could be called
// TODO: Test /queue.reserve and all the possible incorrect way it could be called
// TODO: Test /queue.complete and all the possible incorrect way it could be called
