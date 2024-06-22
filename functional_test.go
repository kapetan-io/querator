package querator_test

import (
	"context"
	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestProduceAndConsume(t *testing.T) {
	// TODO: Simple produce and consume

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	d, err := daemon.NewDaemon(ctx, daemon.Config{})
	require.NoError(t, err)
	defer func() { _ = d.Shutdown(context.Background()) }()
	c := d.MustClient()

	ref := random.String("ref-", 10)
	enc := random.String("enc-", 10)
	kind := random.String("kind-", 10)
	body := []byte("I didn't learn a thing. I was right all along")
	// Produce a single message
	var produce pb.QueueProduceResponse
	require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
		QueueName:      "test-queue",
		Reference:      ref,
		Encoding:       enc,
		Kind:           kind,
		DeadTimeout:    "24h",
		RequestTimeout: "1m",
		MaxAttempts:    10,
		Body:           body,
	}, &produce))
	assert.Equal(t, "queue-p-12048123098", produce.ItemId)

	// Reserve a single message
	var reserve pb.QueueReserveResponse
	require.NoError(t, c.QueueReserve(ctx, &pb.QueueReserveRequest{
		QueueName: "test-queue",
		BatchSize: 1,
	}, &reserve))

	// Ensure we got the item we produced
	assert.Equal(t, 1, len(reserve.Items))
	item := reserve.Items[0]
	assert.Equal(t, ref, item.Reference)
	assert.Equal(t, enc, item.Encoding)
	assert.Equal(t, kind, item.Kind)
	assert.Equal(t, 0, item.Attempts)
	assert.Equal(t, body, item.Body)

	// TODO: Complete the queue item
	// TODO: Ensure it's gone from the database

}

// TODO: Defer
// TODO: Implement clock style thingy so we can freeze time and advance time in order to test Deadlines and such.
