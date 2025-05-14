package config_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/kapetan-io/querator/config"
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
		file        config.File
		expectedErr string
	}{
		{
			name: "InvalidLoggingHandler",
			file: config.File{
				Logging: config.Logging{
					Handler: "invalid",
				},
			},
			expectedErr: "invalid handler; 'invalid' is not one of (color, text, json)",
		},
		{
			name: "InvalidPartitionStorageDriver",
			file: config.File{
				PartitionStorage: []config.PartitionStorage{
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
			file: config.File{
				QueueStorage: config.QueueStorage{
					Driver: "invalid",
				},
			},
			expectedErr: "invalid driver; 'invalid' is not one of (Memory, Badger)",
		},
		{
			name: "InvalidPartitionStorageReference",
			file: config.File{
				PartitionStorage: []config.PartitionStorage{
					{
						Name:   "test",
						Driver: "memory",
					},
				},
				Queues: []config.Queue{
					{
						Name: "test-queue",
						Partitions: []config.Partition{
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
			err := config.ApplyConfigFile(context.Background(), conf, tt.file, io.Discard)
			assert.EqualError(t, err, tt.expectedErr)
		})
	}
}

func TestApplyConfigFile(t *testing.T) {
	file := config.File{
		Logging: config.Logging{
			Level:   "debug",
			Handler: "json",
		},
		PartitionStorage: []config.PartitionStorage{
			{
				Name:     "badger-00",
				Driver:   "Badger",
				Affinity: 0,
				Config: map[string]string{
					"storage-dir": "/tmp/badger1",
				},
			},
		},
		QueueStorage: config.QueueStorage{
			Driver: "Memory",
		},
		Queues: []config.Queue{
			{
				Name:                "queue-1",
				LeaseTimeout:        10 * time.Minute,
				ExpireTimeout:       10 * time.Minute,
				DeadQueue:           "queue-1-dead",
				MaxAttempts:         10,
				Reference:           "test",
				RequestedPartitions: 1,
				Partitions: []config.Partition{
					{
						Partition:   0,
						ReadOnly:    false,
						StorageName: "badger-00",
					},
				},
			},
		},
	}

	conf := &daemon.Config{}
	err := config.ApplyConfigFile(context.Background(), conf, file, io.Discard)
	require.NoError(t, err)
	ctx := context.Background()

	// Check if the config is reflected correctly
	assert.Equal(t, true, conf.Log.Handler().Enabled(ctx, slog.LevelDebug))
	assert.Len(t, conf.StorageConfig.PartitionStorage, 1)
	assert.Equal(t, "badger-00", conf.StorageConfig.PartitionStorage[0].Name)
	assert.Equal(t, float64(0), conf.StorageConfig.PartitionStorage[0].Affinity)
	assert.IsType(t, &store.MemoryQueues{}, conf.StorageConfig.Queues)

	// Check if the queue is created in daemon.Config
	var info types.QueueInfo
	require.NoError(t, conf.StorageConfig.Queues.Get(ctx, "queue-1", &info))
	assert.Equal(t, "queue-1", info.Name)
	assert.Equal(t, 10*time.Minute, info.LeaseTimeout)
	assert.Equal(t, 10*time.Minute, info.ExpireTimeout)
	assert.Equal(t, "queue-1-dead", info.DeadQueue)
	assert.Equal(t, 10, info.MaxAttempts)
	assert.Equal(t, "test", info.Reference)
	assert.Equal(t, 1, info.RequestedPartitions)
	assert.Len(t, info.PartitionInfo, 1)

	assert.Equal(t, 0, info.PartitionInfo[0].PartitionNum)
	assert.False(t, info.PartitionInfo[0].ReadOnly)
	assert.Equal(t, "badger-00", info.PartitionInfo[0].StorageName)
}

func TestApplyConfigFromYAML(t *testing.T) {
	var file config.File
	err := yaml.Unmarshal([]byte(validConfig), &file)
	require.NoError(t, err)

	conf := &daemon.Config{}
	err = config.ApplyConfigFile(context.Background(), conf, file, io.Discard)
	require.NoError(t, err)

	// Verify the configuration
	assert.Len(t, conf.StorageConfig.PartitionStorage, 1)
	assert.Equal(t, "badger-00", conf.StorageConfig.PartitionStorage[0].Name)
	assert.IsType(t, &store.MemoryQueues{}, conf.StorageConfig.Queues)
	ctx := context.Background()

	var info types.QueueInfo
	require.NoError(t, conf.StorageConfig.Queues.Get(ctx, "queue-1", &info))
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
	assert.Equal(t, "badger-00", info.PartitionInfo[0].StorageName)
}

const (
	validConfig = `
partition-storage:
  - name: badger-00
    driver: Badger 
    affinity: 0
    file:
      storage-dir: "/tmp/badger1"

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
        storage-name: badger-00
`
)
