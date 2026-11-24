package daemon_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startFromYAML(t *testing.T, ctx context.Context, yaml string) *daemon.Daemon {
	t.Helper()
	file, err := daemon.ReadConfig(strings.NewReader(yaml))
	require.NoError(t, err)
	conf := daemon.Config{InMemoryListener: true}
	require.NoError(t, daemon.ApplyConfigFile(ctx, &conf, file, io.Discard))
	d, err := daemon.NewDaemon(ctx, conf)
	require.NoError(t, err)
	return d
}

func TestConfigFilePinnedQueueFollowsShareAtRebalance(t *testing.T) {
	ctx := context.Background()
	d := startFromYAML(t, ctx, `
partition-storage:
  - name: mem-00
    memory: {}
    affinity: 1
  - name: mem-01
    memory: {}
    affinity: 1
queue-storage:
  memory: {}
queues:
  - name: queue-1
    requested-partitions: 4
    lease-timeout: 1m
    expire-timeout: 1h
    partitions:
      - partition: 0
        storage-name: mem-00
      - partition: 1
        storage-name: mem-00
      - partition: 2
        storage-name: mem-00
      - partition: 3
        storage-name: mem-00
`)
	defer func() { require.NoError(t, d.Shutdown(ctx)) }()
	c := d.MustClient()

	// The queue starts where the config file pinned it
	var info pb.QueueInfo
	require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: "queue-1"}, &info))
	require.Len(t, info.Partitions, 4)
	for _, p := range info.Partitions {
		assert.Equal(t, "mem-00", p.StorageName)
		assert.Equal(t, "available", p.State)
	}

	// A draining partition that holds an item stays listed, so each partition gets one
	for range 4 {
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      "queue-1",
			RequestTimeout: "5s",
			Items:          []*pb.QueueProduceItem{{Reference: "held", Bytes: []byte("x")}},
		}))
	}

	var resp pb.QueuesRebalanceResponse
	require.NoError(t, c.QueuesRebalance(ctx, &pb.QueuesRebalanceRequest{}, &resp))
	assert.Equal(t, int32(1), resp.GetQueuesChanged())
	assert.Equal(t, int32(2), resp.GetPartitionsCreated())
	assert.Equal(t, int32(2), resp.GetPartitionsDraining())

	require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: "queue-1"}, &info))
	available := map[string]int{}
	draining := map[string]int{}
	for _, p := range info.Partitions {
		if p.State == "draining" {
			draining[p.StorageName]++
			continue
		}
		available[p.StorageName]++
	}
	assert.Equal(t, map[string]int{"mem-00": 2, "mem-01": 2}, available)
	assert.Equal(t, map[string]int{"mem-00": 2}, draining)
}
