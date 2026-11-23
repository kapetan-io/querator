package daemon_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigFileQueuesFollowSavedAffinity(t *testing.T) {
	ctx := context.Background()
	const template = `
partition-storage:
  - name: mem-00
    memory: {}
    affinity: 1
  - name: mem-01
    memory: {}
    affinity: 1
queue-storage:
  badger:
    storage-dir: %q
%s`
	dir := t.TempDir()
	start := func(t *testing.T, queues string) *daemon.Daemon {
		t.Helper()
		file, err := daemon.ReadConfig(strings.NewReader(fmt.Sprintf(template, dir, queues)))
		require.NoError(t, err)
		conf := daemon.Config{InMemoryListener: true}
		require.NoError(t, daemon.ApplyConfigFile(ctx, &conf, file, io.Discard))
		d, err := daemon.NewDaemon(ctx, conf)
		require.NoError(t, err)
		return d
	}

	first := start(t, "")
	require.NoError(t, first.MustClient().StorageBackendsUpdate(ctx, &pb.StorageBackendsUpdateRequest{
		Backend:  "mem-01",
		Affinity: new(0.0),
	}))
	require.NoError(t, first.Shutdown(ctx))

	second := start(t, `queues:
  - name: queue-1
    requested-partitions: 4
    lease-timeout: 1m
    expire-timeout: 1h
`)
	defer func() { require.NoError(t, second.Shutdown(ctx)) }()

	var info pb.QueueInfo
	require.NoError(t, second.MustClient().QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: "queue-1"}, &info))
	require.Len(t, info.Partitions, 4)
	for _, p := range info.Partitions {
		assert.Equal(t, "mem-00", p.StorageName)
	}
}

func TestRefusedStartSavesNoBackend(t *testing.T) {
	ctx := context.Background()
	const template = `
partition-storage:
  - name: mem-00
    memory: {}
%s
queue-storage:
  badger:
    storage-dir: %q
queues:
  - name: queue-1
    requested-partitions: 2
    lease-timeout: 1m
    expire-timeout: 1h
`
	dir := t.TempDir()
	apply := func(affinity string) (*daemon.Daemon, error) {
		file, err := daemon.ReadConfig(strings.NewReader(fmt.Sprintf(template, affinity, dir)))
		require.NoError(t, err)
		conf := daemon.Config{InMemoryListener: true}
		if err := daemon.ApplyConfigFile(ctx, &conf, file, io.Discard); err != nil {
			if conf.Service.StorageConfig.Queues != nil {
				require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx))
			}
			return nil, err
		}
		return daemon.NewDaemon(ctx, conf)
	}

	t.Run("AffinityLeftOut", func(t *testing.T) {
		_, err := apply("")
		require.ErrorContains(t, err, "partition storage 'mem-00' affinity is required")
	})

	_, err := apply("    affinity: 0")
	require.ErrorContains(t, err, "no storage backend can take partitions")

	d, err := apply("    affinity: 1")
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Shutdown(ctx)) }()

	var info pb.QueueInfo
	require.NoError(t, d.MustClient().QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: "queue-1"}, &info))
	assert.Len(t, info.Partitions, 2)
}
