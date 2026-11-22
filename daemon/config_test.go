package daemon_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		{
			name: "QueueRequestsMoreThanMaxPartitions",
			file: daemon.File{
				PartitionStorage: []daemon.PartitionStorage{
					{Name: "test", Memory: &daemon.MemoryConfig{}, Affinity: 1},
				},
				Queues: []daemon.Queue{
					{Name: "test-queue", RequestedPartitions: 1_025},
				},
			},
			expectedErr: "invalid queue 'test-queue'; partitions is invalid; cannot be greater than 1024",
		},
		{
			name: "AuthWithoutStorage",
			file: daemon.File{Auth: &daemon.AuthConfig{}},
			expectedErr: "invalid auth; 'storage' is required and must be one of (memory, badger); " +
				"it holds namespaces, users, roles, role bindings and API keys",
		},
		{
			name: "AuthWithTwoStorages",
			file: daemon.File{Auth: &daemon.AuthConfig{Storage: daemon.AuthStorage{
				Memory: &daemon.MemoryConfig{},
				Badger: &daemon.BadgerConfig{StorageDir: "/tmp"},
			}}},
			expectedErr: "invalid auth; 'storage' defines multiple backends; only one of (memory, badger) is allowed",
		},
		{
			name: "AuthBadgerWithoutStorageDir",
			file: daemon.File{Auth: &daemon.AuthConfig{Storage: daemon.AuthStorage{
				Badger: &daemon.BadgerConfig{},
			}}},
			expectedErr: "invalid auth storage; badger: 'storage-dir' is required",
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
  - name: queue-1-dead
    lease-timeout: 10m
    expire-timeout: 10m
    requested-partitions: 1
    partitions:
      - partition: 0
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

func TestConfigQueuesAreListed(t *testing.T) {
	ctx := context.Background()
	file, err := daemon.ReadConfig(strings.NewReader(`
partition-storage:
  - name: mem-00
    memory: {}
    affinity: 1
queue-storage:
  memory: {}
queues:
  - name: from-config
    requested-partitions: 1
    lease-timeout: 1m
    expire-timeout: 1h
`))
	require.NoError(t, err)

	conf := daemon.Config{InMemoryListener: true}
	require.NoError(t, daemon.ApplyConfigFile(ctx, &conf, file, io.Discard))
	d, err := daemon.NewDaemon(ctx, conf)
	require.NoError(t, err)
	defer func() { _ = d.Shutdown(ctx) }()

	c := d.MustClient()
	require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
		QueueName:           "from-api",
		LeaseTimeout:        "1m",
		ExpireTimeout:       "1h",
		RequestedPartitions: 1,
	}))

	var list pb.QueuesListResponse
	require.NoError(t, c.QueuesList(ctx, &list, nil))
	names := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		names = append(names, item.QueueName)
	}
	assert.ElementsMatch(t, []string{"from-config", "from-api"}, names)
}

func TestQueuesStoredWithoutNamespaceBehaveAsSystem(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	start := func(t *testing.T, queues string) *daemon.Daemon {
		file, err := daemon.ReadConfig(strings.NewReader(fmt.Sprintf(`
partition-storage:
  - name: badger-00
    affinity: 1
    badger:
      storage-dir: %[1]s/partitions
queue-storage:
  badger:
    storage-dir: %[1]s/queues
queues:
%[2]s`, dir, queues)))
		require.NoError(t, err)
		conf := daemon.Config{InMemoryListener: true}
		require.NoError(t, daemon.ApplyConfigFile(ctx, &conf, file, io.Discard))
		d, err := daemon.NewDaemon(ctx, conf)
		require.NoError(t, err)
		return d
	}
	const both = `  - name: from-config
    requested-partitions: 1
    lease-timeout: 1m
    expire-timeout: 1h
  - name: removed-from-config
    requested-partitions: 1
    lease-timeout: 1m
    expire-timeout: 1h
`
	const one = `  - name: from-config
    requested-partitions: 1
    lease-timeout: 1m
    expire-timeout: 1h
`

	d := start(t, both)
	c := d.MustClient()
	require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
		QueueName:      "from-config",
		RequestTimeout: "5s",
		Items: []*pb.QueueProduceItem{
			{Bytes: []byte("leased")},
			{Bytes: []byte("unleased")},
			{Bytes: []byte("scheduled"), EnqueueAt: timestamppb.New(clock.Now().UTC().Add(time.Hour))},
		},
	}))
	var leased pb.QueueLeaseResponse
	require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
		QueueName: "from-config", ClientId: "client-1", BatchSize: 1, RequestTimeout: "1s",
	}, &leased))
	require.Len(t, leased.Items, 1)
	assert.Equal(t, []byte("leased"), leased.Items[0].Bytes)
	require.NoError(t, d.Shutdown(ctx))

	// Rewrite every stored queue into the shape an earlier version saved, with no namespace.
	db, err := badger.Open(badger.DefaultOptions(dir + "/queues/~queue-storage-partition").WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		for _, name := range []string{"from-config", "removed-from-config"} {
			found, err := txn.Get([]byte(name))
			require.NoError(t, err)
			var info types.QueueInfo
			require.NoError(t, found.Value(func(v []byte) error {
				return gob.NewDecoder(bytes.NewReader(v)).Decode(&info)
			}))
			assert.Equal(t, "_system", info.Namespace)
			info.Namespace = ""
			var buf bytes.Buffer
			require.NoError(t, gob.NewEncoder(&buf).Encode(info))
			require.NoError(t, txn.Set([]byte(name), buf.Bytes()))
		}
		return nil
	}))
	require.NoError(t, db.Close())

	for run := range 2 {
		d = start(t, one)
		c = d.MustClient()

		var list pb.QueuesListResponse
		require.NoError(t, c.QueuesList(ctx, &list, nil))
		names := make([]string, 0, len(list.Items))
		for _, item := range list.Items {
			names = append(names, item.QueueName)
		}
		assert.ElementsMatch(t, []string{"from-config", "removed-from-config"}, names)

		for _, name := range names {
			var info pb.QueueInfo
			require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: name}, &info))
			assert.Equal(t, "_system", info.Namespace)
		}

		if run == 0 {
			var lease pb.QueueLeaseResponse
			require.NoError(t, c.QueueLease(ctx, &pb.QueueLeaseRequest{
				QueueName: "from-config", ClientId: "client-2", BatchSize: 10, RequestTimeout: "1s",
			}, &lease))
			require.Len(t, lease.Items, 1)
			assert.Equal(t, []byte("unleased"), lease.Items[0].Bytes)

			// An update must not move a queue into another namespace.
			require.NoError(t, c.QueuesUpdate(ctx, &pb.QueuesUpdateRequest{
				QueueName: "removed-from-config", Namespace: "other", LeaseTimeout: "2m", ExpireTimeout: "1h",
			}))
			var other pb.QueuesListResponse
			require.NoError(t, c.QueuesList(ctx, &other, &querator.ListOptions{Namespace: "other"}))
			assert.Empty(t, other.Items)
		}
		require.NoError(t, d.Shutdown(ctx))
	}
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

func TestApplyConfigFilePlacesUnlistedPartitions(t *testing.T) {
	ctx := context.Background()
	apply := func(t *testing.T, queue daemon.Queue) types.QueueInfo {
		t.Helper()
		conf := &daemon.Config{}
		require.NoError(t, daemon.ApplyConfigFile(ctx, conf, daemon.File{
			PartitionStorage: []daemon.PartitionStorage{
				{Name: "mem-00", Memory: &daemon.MemoryConfig{}, Affinity: 1},
				{Name: "mem-01", Memory: &daemon.MemoryConfig{}, Affinity: 3},
				{Name: "mem-02", Memory: &daemon.MemoryConfig{}, Affinity: 0},
			},
			QueueStorage: daemon.QueueStorage{Memory: &daemon.MemoryConfig{}},
			Queues:       []daemon.Queue{queue},
		}, io.Discard))

		var info types.QueueInfo
		require.NoError(t, conf.Service.StorageConfig.Queues.Get(ctx, queue.Name, &info))
		return info
	}

	t.Run("PlacedByAffinity", func(t *testing.T) {
		info := apply(t, daemon.Queue{
			Name: "queue-1", RequestedPartitions: 4,
			LeaseTimeout: time.Minute, ExpireTimeout: time.Hour,
		})

		require.Len(t, info.PartitionInfo, 4)
		counts := map[string]int{}
		for idx, p := range info.PartitionInfo {
			counts[p.StorageName]++
			assert.Equal(t, idx, p.PartitionNum)
			assert.Equal(t, "queue-1", p.Queue.Name)
		}
		assert.Equal(t, map[string]int{"mem-00": 1, "mem-01": 3}, counts)
	})

	t.Run("ListedPartitionsKeptAsIs", func(t *testing.T) {
		info := apply(t, daemon.Queue{
			Name: "queue-2", RequestedPartitions: 20,
			LeaseTimeout: time.Minute, ExpireTimeout: time.Hour,
			Partitions: []daemon.Partition{{Partition: 0, StorageName: "mem-02"}},
		})

		require.Len(t, info.PartitionInfo, 1)
		assert.Equal(t, "mem-02", info.PartitionInfo[0].StorageName)
	})
}

func TestAffinityFromYAML(t *testing.T) {
	ctx := context.Background()
	const template = `
partition-storage:
  - name: mem-00
    memory: {}
    affinity: %s
  - name: mem-01
    memory: {}
    affinity: %s
queue-storage:
  memory: {}
queues:
  - name: queue-1
    requested-partitions: 4
    lease-timeout: 1m
    expire-timeout: 1h
`
	// Loads the YAML into a running daemon and reads each partition's backend from /queues.info.
	spread := func(t *testing.T, first, second string) (map[string]int, error) {
		t.Helper()
		file, err := daemon.ReadConfig(strings.NewReader(fmt.Sprintf(template, first, second)))
		require.NoError(t, err)
		conf := daemon.Config{InMemoryListener: true}
		if err := daemon.ApplyConfigFile(ctx, &conf, file, io.Discard); err != nil {
			return nil, err
		}
		d, err := daemon.NewDaemon(ctx, conf)
		if err != nil {
			return nil, err
		}
		defer func() { _ = d.Shutdown(ctx) }()

		var info pb.QueueInfo
		require.NoError(t, d.MustClient().QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: "queue-1"}, &info))
		counts := map[string]int{}
		for _, p := range info.Partitions {
			counts[p.StorageName]++
		}
		return counts, nil
	}

	for _, test := range []struct {
		name     string
		first    string
		second   string
		expected map[string]int
	}{
		{name: "Fractions", first: "0.5", second: "1.5", expected: map[string]int{"mem-00": 1, "mem-01": 3}},
		{name: "WholeNumbers", first: "1", second: "3", expected: map[string]int{"mem-00": 1, "mem-01": 3}},
		{name: "Zero", first: "0", second: "1", expected: map[string]int{"mem-01": 4}},
		{name: "Negative", first: "-1", second: "1", expected: map[string]int{"mem-01": 4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			counts, err := spread(t, test.first, test.second)
			require.NoError(t, err)
			assert.Equal(t, test.expected, counts)
		})
	}

	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "NaN", value: ".nan"},
		{name: "PositiveInfinity", value: ".inf"},
		{name: "NegativeInfinity", value: "-.inf"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := spread(t, "1", test.value)
			require.ErrorContains(t, err, "partition storage 'mem-01' affinity must be a finite number")
		})
	}
}

func TestDaemonRefusesUnusableAffinity(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name       string
		affinities []float64
		wantErr    string
	}{
		{name: "NaN", affinities: []float64{1, math.NaN()}, wantErr: "partition storage 'mem-01' affinity must be a finite number"},
		{name: "PositiveInfinity", affinities: []float64{math.Inf(1), math.Inf(1)}, wantErr: "partition storage 'mem-00' affinity must be a finite number"},
		{name: "NegativeInfinity", affinities: []float64{1, math.Inf(-1)}, wantErr: "partition storage 'mem-01' affinity must be a finite number"},
		{name: "SumOverflows", affinities: []float64{1e308, 1e308, 1}, wantErr: "sum of partition storage affinities is too large"},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := daemon.Config{InMemoryListener: true}
			conf.Service.StorageConfig.Queues = store.NewMemoryQueues(slog.Default())
			for i, affinity := range test.affinities {
				conf.Service.StorageConfig.PartitionStorage = append(conf.Service.StorageConfig.PartitionStorage,
					store.PartitionStorage{
						Name:           fmt.Sprintf("mem-%02d", i),
						Affinity:       affinity,
						PartitionStore: store.NewMemoryPartitionStore(store.Config{}, slog.Default()),
					})
			}
			_, err := daemon.NewDaemon(ctx, conf)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestConfigFileRefusesWhatItWouldIgnore(t *testing.T) {
	for _, test := range []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			name:    "SecondDocumentWithAuthStorage",
			config:  "address: localhost:0\n---\nauth:\n  storage:\n    memory: {}\n",
			wantErr: "holds one YAML document",
		},
		{
			name:    "SecondDocumentWithBareAuth",
			config:  "address: localhost:0\n---\nauth:\n",
			wantErr: "holds one YAML document",
		},
		{
			name:    "SecondDocumentWithCommentThenContent",
			config:  "address: localhost:0\n---\n# note\n---\nauth:\n",
			wantErr: "holds one YAML document",
		},
		{
			name:    "QueueStorageMongoWithoutSettings",
			config:  "queue-storage:\n  mongo:\n",
			wantErr: "connection-string",
		},
		{
			name:    "QueueStoragePostgresWithoutSettings",
			config:  "queue-storage:\n  postgres:\n",
			wantErr: "connection-string",
		},
		{
			name:    "QueueStorageBadgerWithoutSettings",
			config:  "queue-storage:\n  badger:\n",
			wantErr: "storage-dir",
		},
		{
			name:    "PartitionStorageBadgerWithoutSettings",
			config:  "partition-storage:\n  - name: p0\n    badger:\n",
			wantErr: "storage-dir",
		},
		{
			name:    "PartitionStorageMongoWithoutSettings",
			config:  "partition-storage:\n  - name: p0\n    mongo:\n",
			wantErr: "connection-string",
		},
		{
			name:    "PartitionStoragePostgresWithoutSettings",
			config:  "partition-storage:\n  - name: p0\n    postgres:\n",
			wantErr: "connection-string",
		},
		{
			name:    "PartitionStorageMemoryBesideBareBadger",
			config:  "partition-storage:\n  - name: p0\n    memory: {}\n    badger:\n",
			wantErr: "partition storage 'p0' defines multiple backends; only one of (memory, badger, mongo, postgres) is allowed",
		},
		{
			name:    "PartitionStorageTwoBareBackends",
			config:  "partition-storage:\n  - name: p0\n    memory:\n    badger:\n",
			wantErr: "multiple backends",
		},
		{
			name:    "PartitionStorageSecondEntryBareBadgerBesideMemory",
			config:  "partition-storage:\n  - name: p0\n    memory: {}\n  - name: p1\n    memory: {}\n    badger:\n",
			wantErr: "partition storage 'p1' defines multiple backends",
		},
		{
			name:    "PartitionStorageNullEntry",
			config:  "partition-storage:\n  - ~\n  - name: a\n    memory: {}\n  - name: b\n    badger:\n",
			wantErr: "partition-storage entry 1 is empty",
		},
		{
			name:    "PartitionStorageNullKeywordEntry",
			config:  "partition-storage:\n  - name: a\n    memory: {}\n  - null\n",
			wantErr: "partition-storage entry 2 is empty",
		},
		{
			name:    "PartitionStorageBareDashEntry",
			config:  "partition-storage:\n  -\n  - name: a\n    memory: {}\n",
			wantErr: "partition-storage entry 1 is empty",
		},
		{
			name:    "QueueStorageTwoEmptyBackends",
			config:  "queue-storage:\n  memory:\n  badger:\n",
			wantErr: "multiple backends",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := daemon.ReadConfig(strings.NewReader(test.config))
			if err == nil {
				err = daemon.ApplyConfigFile(context.Background(), &daemon.Config{}, file, io.Discard)
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}

	for _, test := range []struct{ name, config string }{
		{"TrailingSeparator", "address: localhost:0\n---\n"},
		{"TrailingSeparatorComment", "address: localhost:0\n--- # end\n"},
		{"TrailingCommentDocument", "address: localhost:0\n---\n# nothing here\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := daemon.ReadConfig(strings.NewReader(test.config))
			require.NoError(t, err)
			assert.Equal(t, "localhost:0", file.Address)
		})
	}

	t.Run("EmptyDocumentsAtTheCap", func(t *testing.T) {
		_, err := daemon.ReadConfig(strings.NewReader("address: localhost:0\n" + strings.Repeat("---\n", 16)))
		require.NoError(t, err)
		_, err = daemon.ReadConfig(strings.NewReader("address: localhost:0\n" + strings.Repeat("---\n", 17)))
		require.ErrorContains(t, err, "more than 16 empty YAML documents")
	})

	t.Run("QueueStorageMemoryWithoutSettings", func(t *testing.T) {
		file, err := daemon.ReadConfig(strings.NewReader("queue-storage:\n  memory:\n"))
		require.NoError(t, err)
		var conf daemon.Config
		require.NoError(t, daemon.ApplyConfigFile(context.Background(), &conf, file, io.Discard))
		assert.IsType(t, &store.MemoryQueues{}, conf.Service.StorageConfig.Queues)
	})
	t.Run("PartitionStorageMemoryWithoutSettings", func(t *testing.T) {
		file, err := daemon.ReadConfig(strings.NewReader("partition-storage:\n  - name: p0\n    memory:\n"))
		require.NoError(t, err)
		var conf daemon.Config
		require.NoError(t, daemon.ApplyConfigFile(context.Background(), &conf, file, io.Discard))
		require.Len(t, conf.Service.StorageConfig.PartitionStorage, 1)
		assert.IsType(t, &store.MemoryPartitionStore{}, conf.Service.StorageConfig.PartitionStorage[0].PartitionStore)
	})

	t.Run("PartitionStorageMemoryMapping", func(t *testing.T) {
		file, err := daemon.ReadConfig(strings.NewReader("partition-storage:\n  - name: p0\n    memory: {}\n"))
		require.NoError(t, err)
		var conf daemon.Config
		require.NoError(t, daemon.ApplyConfigFile(context.Background(), &conf, file, io.Discard))
		require.Len(t, conf.Service.StorageConfig.PartitionStorage, 1)
		assert.IsType(t, &store.MemoryPartitionStore{}, conf.Service.StorageConfig.PartitionStorage[0].PartitionStore)
	})
}

func TestConfigQueueFailingValidationFailsStart(t *testing.T) {
	for _, test := range []struct {
		name    string
		queue   string
		wantErr string
	}{
		{
			name: "MissingRequestedPartitions",
			queue: `
  - name: queue-1
    lease-timeout: 10m
    expire-timeout: 20m
`,
			wantErr: "queue-1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := daemon.ReadConfig(strings.NewReader(`
partition-storage:
  - name: mem-00
    memory: {}
    affinity: 1

queue-storage:
  memory: {}

queues:` + test.queue))
			require.NoError(t, err)

			err = daemon.ApplyConfigFile(context.Background(), &daemon.Config{}, file, io.Discard)
			require.ErrorContains(t, err, test.wantErr)
			require.ErrorContains(t, err, "partitions is invalid")
		})
	}
}

func TestQueueConfigRefusesNegativeTimeouts(t *testing.T) {
	for _, tc := range []struct {
		Name, Lease, Expire, Message string
	}{
		{"NegativeLease", "-1m", "1h", "lease timeout is invalid; cannot be negative"},
		{"NegativeExpire", "1m", "-1h", "expire timeout is invalid; cannot be negative"},
		{"BothNegative", "-2h", "-1h", "lease timeout is invalid; cannot be negative"},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			file, err := daemon.ReadConfig(strings.NewReader(fmt.Sprintf(`
partition-storage:
  - name: mem-00
    affinity: 1
    memory: {}
queue-storage:
  memory: {}
queues:
  - name: bad-queue
    lease-timeout: %s
    expire-timeout: %s
    requested-partitions: 1
`, tc.Lease, tc.Expire)))
			require.NoError(t, err)

			conf := daemon.Config{InMemoryListener: true}
			err = daemon.ApplyConfigFile(context.Background(), &conf, file, io.Discard)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "bad-queue")
			assert.Contains(t, err.Error(), tc.Message)
		})
	}
}
