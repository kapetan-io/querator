package daemon_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/duh-rpc/duh-go"
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

	var resp pb.QueuesRebalanceResponse
	require.NoError(t, c.QueuesRebalance(ctx, &resp))
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

func TestConfigFileInMemoryQueueStoreRefusesRebalance(t *testing.T) {
	ctx := context.Background()
	const queues = `
queues:
  - name: queue-1
    requested-partitions: 2
    lease-timeout: 1m
    expire-timeout: 1h
`

	t.Run("RefusedWhenBackendKeepsItems", func(t *testing.T) {
		d := startFromYAML(t, ctx, fmt.Sprintf(`
partition-storage:
  - name: mem-00
    memory: {}
    affinity: 1
  - name: badger-00
    badger:
      storage-dir: %q
    affinity: 1
queue-storage:
  memory: {}
%s`, t.TempDir(), queues))
		defer func() { require.NoError(t, d.Shutdown(ctx)) }()
		c := d.MustClient()

		require.NoError(t, c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
			Backend:  "badger-00",
			Affinity: new(0.0),
		}))
		var before pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: "queue-1"}, &before))

		err := c.QueuesRebalance(ctx, &pb.QueuesRebalanceResponse{})
		var e duh.Error
		require.ErrorAs(t, err, &e)
		assert.Equal(t, duh.CodeBadRequest, e.Code())
		assert.Contains(t, e.Message(), "the queue store is InMemory")

		var after pb.QueueInfo
		require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: "queue-1"}, &after))
		require.Len(t, after.Partitions, len(before.Partitions))
		for i, p := range after.Partitions {
			assert.Equal(t, before.Partitions[i].StorageName, p.StorageName)
			assert.Equal(t, "available", p.State)
		}

		// storage/backends.list reports nothing pending for a rebalance that would be refused
		var list pb.StorageBackendsListResponse
		require.NoError(t, c.StorageBackendsList(ctx, &list))
		for _, b := range list.Backends {
			assert.Empty(t, b.Pending.Create)
			assert.Empty(t, b.Pending.Drain)
		}
	})

	t.Run("AllowedWhenEveryBackendIsInMemory", func(t *testing.T) {
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
`+queues)
		defer func() { require.NoError(t, d.Shutdown(ctx)) }()
		c := d.MustClient()

		require.NoError(t, c.StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
			Backend:  "mem-01",
			Affinity: new(0.0),
		}))
		var resp pb.QueuesRebalanceResponse
		require.NoError(t, c.QueuesRebalance(ctx, &resp))
		assert.Equal(t, int32(1), resp.GetQueuesChanged())
		assert.Equal(t, int32(1), resp.GetPartitionsCreated())
		assert.Equal(t, int32(1), resp.GetPartitionsDraining())
	})
}
