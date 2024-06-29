package querator_test

import (
	"context"
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

func TestProduceAndConsume(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d, err := daemon.NewDaemon(ctx, daemon.Config{
		Store:  store.NewBuntStorage(store.BuntOptions{Logger: log}),
		Logger: log,
	})
	require.NoError(t, err)
	defer func() { _ = d.Shutdown(context.Background()) }()
	c := d.MustClient()

	err = c.QueueCreate(ctx, &pb.QueueOptions{
		Name: "test-queue",
	})
	require.NoError(t, err)

	ref := random.String("ref-", 10)
	enc := random.String("enc-", 10)
	kind := random.String("kind-", 10)
	body := []byte("I didn't learn a thing. I was right all along")
	// Produce a single message
	require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
		QueueName:      "test-queue",
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
		QueueName: "test-queue",
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

	// Ensure the item is in storage
	var inspect pb.StorageItem
	require.NoError(t, c.StorageInspect(ctx, item.Id, &inspect))

	assert.Equal(t, ref, inspect.Reference)
	assert.Equal(t, kind, inspect.Kind)
	assert.Equal(t, int32(0), inspect.Attempts)
	assert.Equal(t, body, inspect.Body)
	assert.Equal(t, item.Id, inspect.Id)
	assert.Equal(t, true, inspect.IsReserved)

	// Mark the item as complete
	require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
		Ids: []string{
			item.Id,
		},
	}))

	// Storage should be empty
	var list pb.StorageListResponse
	require.NoError(t, c.StorageList(ctx, &pb.StorageListRequest{
		Pivot: "",
		Limit: 10,
	}, &list))

	assert.Equal(t, 0, len(list.Items))
}

// TODO: Defer
// TODO: Implement clock style thingy so we can freeze time and advance time in order to test Deadlines and such.
// TODO: Test /queue.produce and all the possible incorrect way it could be called
// TODO: Test /queue.reserve and all the possible incorrect way it could be called
// TODO: Test /queue.complete and all the possible incorrect way it could be called
