package daemon_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestApplyConfigFileErrs(t *testing.T) {
	tests := []struct {
		name        string
		file        daemon.File
		expectedErr string
	}{
		{
			name: "InvalidLoggingHandler",
			file: daemon.File{
				Logging: daemon.Logging{
					Handler: "invalid",
				},
			},
			expectedErr: "invalid handler; 'invalid' is not one of (color, text, json)",
		},
		{
			name: "InvalidPartitionStorageDriver",
			file: daemon.File{
				PartitionStorage: []daemon.PartitionStorage{
					{
						Name:   "test",
						Driver: "invalid",
					},
				},
			},
			expectedErr: "invalid driver; 'invalid' is not one of (Memory, Badger)",
		},
		{
			name: "InvalidQueueStorageDriver",
			file: daemon.File{
				QueueStorage: daemon.QueueStorage{
					Driver: "invalid",
				},
			},
			expectedErr: "invalid driver; 'invalid' is not one of (Memory, Badger)",
		},
		{
			name: "InvalidPartitionStorageReference",
			file: daemon.File{
				PartitionStorage: []daemon.PartitionStorage{
					{
						Name:   "test",
						Driver: "memory",
					},
				},
				Queues: []daemon.Queue{
					{
						Name: "test-queue",
						Partitions: []daemon.Partition{
							{
								StorageName: "non-existent",
							},
						},
					},
				},
			},
			expectedErr: "invalid partition storage; queue 'test-queue' references 'non-existent' which is undefined",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &daemon.Config{}
			err := daemon.ApplyConfigFile(context.Background(), conf, tt.file, io.Discard)
			assert.EqualError(t, err, tt.expectedErr)
		})
	}
}

func TestApplyConfigFile(t *testing.T) {
	file := daemon.File{
		Logging: daemon.Logging{
			Level:   "debug",
			Handler: "json",
		},
		PartitionStorage: []daemon.PartitionStorage{
			{
				Name:     "mem-00",
				Driver:   "memory",
				Affinity: 0,
			},
		},
		QueueStorage: daemon.QueueStorage{
			Driver: "Memory",
		},
		Queues: []daemon.Queue{
			{
				Name:                "queue-1",
				LeaseTimeout:        10 * time.Minute,
				ExpireTimeout:       10 * time.Minute,
				MaxAttempts:         10,
				Reference:           "test",
				RequestedPartitions: 1,
				Partitions: []daemon.Partition{
					{
						Partition:   0,
						ReadOnly:    false,
						StorageName: "mem-00",
					},
				},
			},
		},
	}

	conf := &daemon.Config{}
	ctx := context.Background()
	err := daemon.ApplyConfigFile(ctx, conf, file, io.Discard)
	require.NoError(t, err)

	assert.Equal(t, true, conf.Service.Log.Handler().Enabled(ctx, slog.LevelDebug))
	assert.Len(t, conf.Service.StorageConfig.PartitionStorage, 1)
	assert.Equal(t, "mem-00", conf.Service.StorageConfig.PartitionStorage[0].Name)
	assert.Equal(t, float64(0), conf.Service.StorageConfig.PartitionStorage[0].Affinity)
	assert.IsType(t, &store.MemoryQueues{}, conf.Service.StorageConfig.Queues)

	var info types.QueueInfo
	require.NoError(t, conf.Service.StorageConfig.Queues.Get(ctx, "queue-1", &info))
	assert.Equal(t, "queue-1", info.Name)
	assert.Equal(t, 10*time.Minute, info.LeaseTimeout)
	assert.Equal(t, 10*time.Minute, info.ExpireTimeout)
	assert.Equal(t, "", info.DeadQueue)
	assert.Equal(t, 10, info.MaxAttempts)
	assert.Equal(t, "test", info.Reference)
	assert.Equal(t, 1, info.RequestedPartitions)
	assert.Len(t, info.PartitionInfo, 1)

	assert.Equal(t, 0, info.PartitionInfo[0].PartitionNum)
	assert.False(t, info.PartitionInfo[0].ReadOnly)
	assert.Equal(t, "mem-00", info.PartitionInfo[0].StorageName)
}

func TestApplyConfigFromYAML(t *testing.T) {
	validConfig := `
partition-storage:
  - name: mem-00
    driver: Memory
    affinity: 0

queue-storage:
  driver: Memory

queues:
  - name: queue-1
    lease-timeout: 10m
    expire-timeout: 10m
    dead-queue: queue-1-dead
    max-attempts: 10
    reference: test
    requested-partitions: 20
    partitions:
      - partition: 0
        read-only: false
        storage-name: mem-00
`
	var file daemon.File
	err := yaml.Unmarshal([]byte(validConfig), &file)
	require.NoError(t, err)

	conf := &daemon.Config{}
	err = daemon.ApplyConfigFile(context.Background(), conf, file, io.Discard)
	require.NoError(t, err)

	assert.Len(t, conf.Service.StorageConfig.PartitionStorage, 1)
	assert.Equal(t, "mem-00", conf.Service.StorageConfig.PartitionStorage[0].Name)
	assert.IsType(t, &store.MemoryQueues{}, conf.Service.StorageConfig.Queues)
	ctx := context.Background()

	var info types.QueueInfo
	require.NoError(t, conf.Service.StorageConfig.Queues.Get(ctx, "queue-1", &info))
	assert.Equal(t, "queue-1", info.Name)
	assert.Equal(t, 10*time.Minute, info.LeaseTimeout)
	assert.Equal(t, 10*time.Minute, info.ExpireTimeout)
	assert.Equal(t, "queue-1-dead", info.DeadQueue)
	assert.Equal(t, 10, info.MaxAttempts)
	assert.Equal(t, "test", info.Reference)
	assert.Equal(t, 20, info.RequestedPartitions)
	assert.Len(t, info.PartitionInfo, 1)
	assert.Equal(t, 0, info.PartitionInfo[0].PartitionNum)
	assert.False(t, info.PartitionInfo[0].ReadOnly)
	assert.Equal(t, "mem-00", info.PartitionInfo[0].StorageName)
}

func TestBadgerConfig(t *testing.T) {
	badgerConfig := `
partition-storage:
  - name: badger-00
    driver: Badger
    affinity: 0
    config:
      storage-dir: /tmp/badger1
queue-storage:
  driver: badger
  config:
    storage-dir: "/tmp/queue-storage"
`
	var file daemon.File
	err := yaml.Unmarshal([]byte(badgerConfig), &file)
	require.NoError(t, err)

	var conf daemon.Config
	ctx := context.Background()
	err = daemon.ApplyConfigFile(ctx, &conf, file, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, "/tmp/badger1",
		conf.Service.StorageConfig.PartitionStorage[0].PartitionStore.(*store.BadgerPartitionStore).Config().StorageDir)
	assert.Equal(t, "/tmp/queue-storage",
		conf.Service.StorageConfig.Queues.(*store.BadgerQueues).Config().StorageDir)
}
