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
	var queueName = random.String("queue-", 10)

	t.Run("ProduceAndConsume", func(t *testing.T) {
		d, c, ctx := newDaemon(t, newStore, 5*time.Second)
		defer d.Shutdown(t)

		// Create a queue
		err := c.QueueCreate(ctx, &pb.QueueOptions{Name: queueName})
		require.NoError(t, err)

		// Produce a single message
		ref := random.String("ref-", 10)
		enc := random.String("enc-", 10)
		kind := random.String("kind-", 10)
		body := []byte("I didn't learn a thing. I was right all along")
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "1m",
			Items: []*pb.QueueProduceItem{
				{
					Reference: ref,
					Encoding:  enc,
					Kind:      kind,
					Body:      body,
				},
			},
		}))

		// Reserve a single message
		var reserve pb.QueueReserveResponse
		require.NoError(t, c.QueueReserve(ctx, &pb.QueueReserveRequest{
			ClientId:  random.String("client-", 10),
			QueueName: queueName,
			BatchSize: 1,
		}, &reserve))

		// Ensure we got the item we produced
		assert.Equal(t, 1, len(reserve.Items))
		item := reserve.Items[0]
		assert.Equal(t, ref, item.Reference)
		assert.Equal(t, enc, item.Encoding)
		assert.Equal(t, kind, item.Kind)
		assert.Equal(t, int32(0), item.Attempts)
		assert.Equal(t, body, item.Body)

		// Ensure the item is in storage and marked as reserved
		var inspect pb.StorageItem
		require.NoError(t, c.StorageInspect(ctx, item.Id, &inspect))

		assert.Equal(t, ref, inspect.Reference)
		assert.Equal(t, kind, inspect.Kind)
		assert.Equal(t, int32(0), inspect.Attempts)
		assert.Equal(t, body, inspect.Body)
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
			QueueName: queueName,
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
		d, c, ctx := newDaemon(t, newStore, 5*time.Second)
		defer d.Shutdown(t)

		for _, tc := range []struct {
			Name string
			Req  *pb.QueueProduceRequest
			Msg  string
			Code int
		}{
			{
				Name: "empty request",
				Req:  &pb.QueueProduceRequest{},
				Msg:  "invalid queue name; cannot be empty",
				Code: duh.CodeBadRequest,
			},
			{
				Name: "invalid queue",
				Req: &pb.QueueProduceRequest{
					QueueName: "invalid~queue",
				},
				Msg:  "invalid queue name; 'invalid~queue' cannot be contain '~' character",
				Code: duh.CodeBadRequest,
			},
			// TODO: Keep going <--- NEXT
		} {
			t.Run(tc.Name, func(t *testing.T) {
				err := c.QueueProduce(ctx, tc.Req)
				var e duh.Error
				require.True(t, errors.As(err, &e))
				assert.Equal(t, e.Message(), tc.Msg)
				assert.Equal(t, e.Code(), tc.Code)
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
