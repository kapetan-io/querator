package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/kapetan-io/querator"
	main "github.com/kapetan-io/querator/cmd/querator"
	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRebalanceCommand(t *testing.T) {
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
		RequestedPartitions: 3,
	}))
	require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{QueueName: "queue-1", RequestedPartitions: 2}))

	type counts struct {
		QueuesChanged      *int32 `json:"queues_changed"`
		PartitionsCreated  *int32 `json:"partitions_created"`
		PartitionsDraining *int32 `json:"partitions_draining"`
	}

	t.Run("PrintsWhatChanged", func(t *testing.T) {
		out, err := captureOutput(t, func() error {
			return main.RunRebalance(main.FlagParams{Endpoint: endpoint})
		})
		require.NoError(t, err)

		var printed counts
		require.NoError(t, json.Unmarshal([]byte(out.Stdout), &printed))
		assert.Equal(t, counts{QueuesChanged: new(int32(1)), PartitionsCreated: new(int32(0)), PartitionsDraining: new(int32(1))}, printed)
		assert.Regexp(t, `"waiting_queues":\s+\[\]`, out.Stdout)

		var info pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: "queue-1"}, &info))
		require.Len(t, info.Partitions, 3)
		assert.Equal(t, "draining", info.Partitions[2].State)
	})

	t.Run("PrintsZerosWhenNothingChanges", func(t *testing.T) {
		out, err := captureOutput(t, func() error {
			return main.RunRebalance(main.FlagParams{Endpoint: endpoint})
		})
		require.NoError(t, err)

		var printed counts
		require.NoError(t, json.Unmarshal([]byte(out.Stdout), &printed))
		assert.Equal(t, counts{QueuesChanged: new(int32(0)), PartitionsCreated: new(int32(0)), PartitionsDraining: new(int32(0))}, printed)
	})

	t.Run("RefusedRebalanceFails", func(t *testing.T) {
		require.NoError(t, c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{Backend: "mem-0", Affinity: new(0.0)}))
		_, err := captureOutput(t, func() error {
			return main.RunRebalance(main.FlagParams{Endpoint: endpoint})
		})
		require.ErrorContains(t, err, "rebalance is refused; no storage backend can take partitions")
	})
}
