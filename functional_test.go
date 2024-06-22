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

	// Produce a single message
	var produce pb.QueueProduceResponse
	require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
		QueueName:   "test-queue",
		Reference:   random.String("ref-", 10),
		Encoding:    random.String("enc-", 10),
		Kind:        random.String("kind-", 10),
		Deadline:    "1m",
		MaxAttempts: 10,
		Body:        []byte("I didn't learn a thing. I was right all along"),
	}, &produce))
	assert.Equal(t, "queue-p-12048123098", produce.MessageId)

	// TODO: Finish this

	// Reserve a single message
	var reserve pb.QueueReserveResponse
	require.NoError(t, c.QueueReserve(ctx, &pb.QueueReserveRequest{
		QueueName: "test-queue",
		BatchSize: 1,
	}, &produce))

}

// TODO: Implement clock style thingy so we can freeze time and advance time in order to test Deadlines and such.
