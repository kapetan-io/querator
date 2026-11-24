package main_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kapetan-io/querator"
	main "github.com/kapetan-io/querator/cmd/querator"
	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestInfoCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, err := daemon.NewDaemon(ctx, daemon.Config{ListenAddress: "localhost:0"})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Shutdown(context.Background())) }()
	endpoint := fmt.Sprintf("http://%s", d.Listener.Addr().String())

	c, err := querator.NewClient(querator.ClientConfig{Endpoint: endpoint})
	require.NoError(t, err)
	require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
		QueueName:           "queue-1",
		LeaseTimeout:        "1m0s",
		ExpireTimeout:       "24h0m0s",
		RequestedPartitions: 1,
	}))

	t.Run("PrintsQueueWithoutPendingWhenSettingsAgree", func(t *testing.T) {
		out, err := captureOutput(t, func() error {
			return main.RunInfo(main.FlagParams{Endpoint: endpoint}, "queue-1")
		})
		require.NoError(t, err)

		var printed pb.QueueInfo
		require.NoError(t, protojson.Unmarshal([]byte(out.Stdout), &printed))
		assert.Equal(t, "queue-1", printed.QueueName)
		assert.Equal(t, int32(1), printed.RequestedPartitions)
		assert.Len(t, printed.Partitions, 1)
		assert.Nil(t, printed.Pending)
		assert.NotContains(t, out.Stdout, "pending")
	})

	t.Run("PrintsPendingAfterRequestedPartitionsChange", func(t *testing.T) {
		require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{QueueName: "queue-1", RequestedPartitions: 2}))

		out, err := captureOutput(t, func() error {
			return main.RunInfo(main.FlagParams{Endpoint: endpoint}, "queue-1")
		})
		require.NoError(t, err)

		var printed pb.QueueInfo
		require.NoError(t, protojson.Unmarshal([]byte(out.Stdout), &printed))
		assert.Equal(t, int32(2), printed.RequestedPartitions)
		assert.Len(t, printed.Partitions, 1)
		require.NotNil(t, printed.Pending)
		require.Len(t, printed.Pending.Create, 1)
		assert.Equal(t, int32(1), printed.Pending.Create[0].Partition)
	})

	t.Run("UnknownQueueFails", func(t *testing.T) {
		_, err := captureOutput(t, func() error {
			return main.RunInfo(main.FlagParams{Endpoint: endpoint}, "no-such-queue")
		})
		require.ErrorContains(t, err, "no such queue")
	})

	t.Run("PrintsRebalanceRefusedWhenNoBackendCanTakePartitions", func(t *testing.T) {
		var backends pb.StorageBackendsListResponse
		require.NoError(t, c.StorageBackendsList(ctx, &backends))
		require.NoError(t, c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
			Backend:  backends.Backends[0].Backend,
			Affinity: new(0.0),
		}))

		out, err := captureOutput(t, func() error {
			return main.RunInfo(main.FlagParams{Endpoint: endpoint}, "queue-1")
		})
		require.NoError(t, err)

		var printed pb.QueueInfo
		require.NoError(t, protojson.Unmarshal([]byte(out.Stdout), &printed))
		assert.Nil(t, printed.Pending)
		assert.Contains(t, printed.RebalanceRefused, "rebalance is refused")
		assert.Contains(t, out.Stdout, "rebalance_refused")
	})
}
