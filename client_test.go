package querator_test

import (
	"context"
	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	d, err := daemon.NewDaemon(ctx, daemon.Config{})
	require.NoError(t, err)
	defer func() { _ = d.Shutdown(context.Background()) }()
	c := d.MustClient()

	var resp pb.QueueProduceResponse
	require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{}, &resp))
	assert.Equal(t, "queue-p-12048123098", resp.MessageId)
}
