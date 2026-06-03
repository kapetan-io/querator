package daemon_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadConfigErrs(t *testing.T) {
	for _, test := range []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			// A typo in a backend option must fail rather than silently fall back to the default.
			name: "MisspelledKey",
			config: `
partition-storage:
  - name: mongo-00
    mongo:
      connection-string: "mongodb://localhost:27017"
      max-poolsize: 50
`,
			wantErr: "field max-poolsize not found",
		},
		{
			// max-pool-size belongs to mongo; placing it under badger must fail.
			name: "MisplacedKey",
			config: `
partition-storage:
  - name: badger-00
    badger:
      storage-dir: /tmp/badger1
      max-pool-size: 50
`,
			wantErr: "field max-pool-size not found",
		},
		{
			name: "MistypedValue",
			config: `
queue-storage:
  mongo:
    connection-string: "mongodb://localhost:27017"
    max-pool-size: not-a-number
`,
			wantErr: "cannot unmarshal",
		},
		{
			name: "UnknownTopLevelKey",
			config: `
bogus-section: true
`,
			wantErr: "field bogus-section not found",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := daemon.ReadConfig(strings.NewReader(test.config))
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

// TestExampleConfigDecodes guards the shipped example.yaml against the strict decoder: every key it
// documents must be a key the config structs actually accept, or operators copying it would hit an
// "unknown field" error on startup.
func TestExampleConfigDecodes(t *testing.T) {
	f, err := os.Open("../example.yaml")
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	_, err = daemon.ReadConfig(f)
	require.NoError(t, err)
}

func TestApplyConfigFileErrs(t *testing.T) {
	for _, test := range []struct {
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
			name: "PartitionStorageNoBackend",
			file: daemon.File{
				PartitionStorage: []daemon.PartitionStorage{
					{Name: "test"},
				},
			},
			expectedErr: "partition storage 'test' must define exactly one backend (memory, badger, mongo, postgres)",
		},
		{
			name: "PartitionStorageMultipleBackends",
			file: daemon.File{
				PartitionStorage: []daemon.PartitionStorage{
					{
						Name:   "test",
						Memory: &daemon.MemoryConfig{},
						Badger: &daemon.BadgerConfig{StorageDir: "/tmp/badger1"},
					},
				},
			},
			expectedErr: "partition storage 'test' defines multiple backends; only one of (memory, badger, mongo, postgres) is allowed",
		},
		{
			name: "PartitionStorageMongoMissingConnectionString",
			file: daemon.File{
				PartitionStorage: []daemon.PartitionStorage{
					{
						Name:  "mongo-00",
						Mongo: &daemon.MongoConfig{Database: "querator"},
					},
				},
			},
			expectedErr: "mongo: 'connection-string' is required",
		},
		{
			name: "PartitionStoragePostgresMissingConnectionString",
			file: daemon.File{
				PartitionStorage: []daemon.PartitionStorage{
					{
						Name:     "postgres-00",
						Postgres: &daemon.PostgresConfig{MaxConns: 10},
					},
				},
			},
			expectedErr: "postgres: 'connection-string' is required",
		},
		{
			name: "PartitionStorageBadgerMissingStorageDir",
			file: daemon.File{
				PartitionStorage: []daemon.PartitionStorage{
					{
						Name:   "badger-00",
						Badger: &daemon.BadgerConfig{},
					},
				},
			},
			expectedErr: "badger: 'storage-dir' is required",
		},
		{
			name: "QueueStorageMultipleBackends",
			file: daemon.File{
				QueueStorage: daemon.QueueStorage{
					Memory: &daemon.MemoryConfig{},
					Mongo:  &daemon.MongoConfig{ConnectionString: "mongodb://localhost:27017"},
				},
			},
			expectedErr: "queue storage defines multiple backends; only one of (memory, badger, mongo, postgres) is allowed",
		},
		{
			name: "QueueStorageMongoMissingConnectionString",
			file: daemon.File{
				QueueStorage: daemon.QueueStorage{
					Mongo: &daemon.MongoConfig{Database: "querator"},
				},
			},
			expectedErr: "mongo: 'connection-string' is required",
		},
		{
			name: "QueueStoragePostgresMissingConnectionString",
			file: daemon.File{
				QueueStorage: daemon.QueueStorage{
					Postgres: &daemon.PostgresConfig{MaxConns: 10},
				},
			},
			expectedErr: "postgres: 'connection-string' is required",
		},
		{
			name: "QueueStorageBadgerMissingStorageDir",
			file: daemon.File{
				QueueStorage: daemon.QueueStorage{
					Badger: &daemon.BadgerConfig{},
				},
			},
			expectedErr: "badger: 'storage-dir' is required",
		},
		{
			name: "InvalidPartitionStorageReference",
			file: daemon.File{
				PartitionStorage: []daemon.PartitionStorage{
					{
						Name:   "test",
						Memory: &daemon.MemoryConfig{},
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
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := &daemon.Config{}
			err := daemon.ApplyConfigFile(context.Background(), conf, test.file, io.Discard)
			assert.EqualError(t, err, test.expectedErr)
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
				Memory:   &daemon.MemoryConfig{},
				Affinity: 0,
			},
		},
		QueueStorage: daemon.QueueStorage{
			Memory: &daemon.MemoryConfig{},
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
    memory: {}
    affinity: 0

queue-storage:
  memory: {}

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
	file, err := daemon.ReadConfig(strings.NewReader(validConfig))
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
    affinity: 0
    badger:
      storage-dir: /tmp/badger1
queue-storage:
  badger:
    storage-dir: "/tmp/queue-storage"
`
	file, err := daemon.ReadConfig(strings.NewReader(badgerConfig))
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

func TestMongoConfig(t *testing.T) {
	mongoConfig := `
partition-storage:
  - name: mongo-00
    affinity: 1
    mongo:
      connection-string: "mongodb://localhost:27017"
      database: querator
      max-pool-size: 50
queue-storage:
  mongo:
    connection-string: "mongodb://localhost:27017"
    database: querator
    max-pool-size: 25
`
	file, err := daemon.ReadConfig(strings.NewReader(mongoConfig))
	require.NoError(t, err)

	var conf daemon.Config
	ctx := context.Background()
	err = daemon.ApplyConfigFile(ctx, &conf, file, io.Discard)
	require.NoError(t, err)

	partitionConfig := conf.Service.StorageConfig.PartitionStorage[0].PartitionStore.(*store.MongoPartitionStore).Config()
	assert.Equal(t, "mongodb://localhost:27017", partitionConfig.ConnectionString)
	assert.Equal(t, "querator", partitionConfig.Database)
	assert.Equal(t, uint64(50), partitionConfig.MaxPoolSize)

	queueConfig := conf.Service.StorageConfig.Queues.(*store.MongoQueues).Config()
	assert.Equal(t, "mongodb://localhost:27017", queueConfig.ConnectionString)
	assert.Equal(t, "querator", queueConfig.Database)
	// queue-storage must honor max-pool-size, not silently drop it: queue and partition storage share
	// a process-global client keyed by connection string, so an unset size here would override the
	// partition-storage pool cap depending on which storage acquires the client first.
	assert.Equal(t, uint64(25), queueConfig.MaxPoolSize)
}

func TestPostgresConfig(t *testing.T) {
	postgresConfig := `
partition-storage:
  - name: postgres-00
    affinity: 1
    postgres:
      connection-string: "postgres://localhost:5432/querator"
      max-conns: 50
      scan-batch-size: 500
queue-storage:
  postgres:
    connection-string: "postgres://localhost:5432/querator"
    max-conns: 25
    scan-batch-size: 250
`
	file, err := daemon.ReadConfig(strings.NewReader(postgresConfig))
	require.NoError(t, err)

	var conf daemon.Config
	ctx := context.Background()
	err = daemon.ApplyConfigFile(ctx, &conf, file, io.Discard)
	require.NoError(t, err)

	partitionConfig := conf.Service.StorageConfig.PartitionStorage[0].PartitionStore.(*store.PostgresPartitionStore).Config()
	assert.Equal(t, "postgres://localhost:5432/querator", partitionConfig.ConnectionString)
	assert.Equal(t, int32(50), partitionConfig.MaxConns)
	assert.Equal(t, 500, partitionConfig.ScanBatchSize)

	queueConfig := conf.Service.StorageConfig.Queues.(*store.PostgresQueues).Config()
	assert.Equal(t, "postgres://localhost:5432/querator", queueConfig.ConnectionString)
	// queue-storage must honor max-conns, not silently drop it: queue and partition storage share a
	// process-global pgx pool keyed by connection string, so the first storage to acquire the pool
	// sets the cap. An unset value here would let one path override the other's pool size.
	assert.Equal(t, int32(25), queueConfig.MaxConns)
	assert.Equal(t, 250, queueConfig.ScanBatchSize)
}
