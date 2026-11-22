package daemon_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const memoryStorage = `
partition-storage:
  - name: mem-00
    memory: {}
    affinity: 1
queue-storage:
  memory: {}
`

func TestConfigFileDeadLetterRules(t *testing.T) {
	ctx := context.Background()

	for _, test := range []struct {
		name    string
		queues  string
		unsaved []string
		wantErr string
	}{
		{
			name: "DeadQueueDoesNotExist",
			queues: `
queues:
  - name: src
    dead-queue: missing
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`,
			unsaved: []string{"src"},
			wantErr: "invalid queue 'src'; dead_queue does not exist; 'missing' was not found, create it first",
		},
		{
			name: "DeadQueueNameHasWhitespace",
			queues: `
queues:
  - name: src
    dead-queue: "bad name"
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`,
			unsaved: []string{"src"},
			wantErr: "invalid queue 'src'; dead queue is invalid; 'bad name' cannot contain whitespace",
		},
		{
			name: "DeadQueueNameTooLong",
			queues: `
queues:
  - name: src
    dead-queue: ` + strings.Repeat("d", 501) + `
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`,
			unsaved: []string{"src"},
			wantErr: "invalid queue 'src'; dead queue is invalid; cannot be greater than '500' characters",
		},
		{
			name: "DeadQueueReferencesItself",
			queues: `
queues:
  - name: src
    dead-queue: src
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`,
			unsaved: []string{"src"},
			wantErr: "invalid queue 'src'; dead_queue cannot reference itself",
		},
		{
			name: "DeadQueueNameHasTilde",
			queues: `
queues:
  - name: src
    dead-queue: bad~name
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`,
			unsaved: []string{"src"},
			wantErr: "invalid queue 'src'; dead queue is invalid; 'bad~name' cannot contain '~' character",
		},
		{
			name: "DeadQueueHasItsOwnDeadQueue",
			queues: `
queues:
  - name: src
    dead-queue: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
  - name: dlq
    dead-queue: last
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
  - name: last
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`,
			unsaved: []string{"src", "dlq", "last"},
			wantErr: "invalid queue 'src'; dead_queue is invalid; 'dlq' already has its own dead_queue configured, " +
				"clear or change dead_queue on 'dlq' first",
		},
		{
			name: "DeadQueueOfAnotherQueueHasItsOwnDeadQueue",
			queues: `
queues:
  - name: dlq
    dead-queue: last
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
  - name: last
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
  - name: src
    dead-queue: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`,
			unsaved: []string{"src", "dlq", "last"},
			wantErr: "invalid queue 'dlq'; dead_queue is invalid; 'dlq' is the dead_queue of 'src' and cannot have " +
				"its own dead_queue, clear or change dead_queue on 'src' first",
		},
		{
			name: "DeadQueueHasManyPartitions",
			queues: `
queues:
  - name: src
    dead-queue: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
  - name: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 4
`,
			unsaved: []string{"src", "dlq"},
			wantErr: "invalid queue 'src'; dead_queue is invalid; 'dlq' must have a single partition, but has '4'",
		},
		{
			name: "DeadQueueOfAnotherQueueHasManyPartitions",
			queues: `
queues:
  - name: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 4
  - name: src
    dead-queue: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`,
			unsaved: []string{"src", "dlq"},
			wantErr: "invalid queue 'dlq'; partitions is invalid; 'dlq' is the dead_queue of 'src' and must have " +
				"a single partition, clear or change dead_queue on 'src' first",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := daemon.ReadConfig(strings.NewReader(memoryStorage + test.queues))
			require.NoError(t, err)

			conf := &daemon.Config{}
			err = daemon.ApplyConfigFile(ctx, conf, file, io.Discard)
			require.EqualError(t, err, test.wantErr)

			for _, name := range test.unsaved {
				var stored types.QueueInfo
				assert.ErrorIs(t, conf.Service.StorageConfig.Queues.Get(ctx, name, &stored), store.ErrQueueNotExist)
			}
		})
	}

	t.Run("QueueDeclaredTwiceKeepsFirstDeclaration", func(t *testing.T) {
		file, err := daemon.ReadConfig(strings.NewReader(memoryStorage + `
queues:
  - name: src
    dead-queue: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
  - name: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 4
  - name: src
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
  - name: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`))
		require.NoError(t, err)

		conf := &daemon.Config{}
		err = daemon.ApplyConfigFile(ctx, conf, file, io.Discard)
		require.EqualError(t, err, "invalid queue 'src'; dead_queue is invalid; 'dlq' must have a single partition, but has '4'")

		var stored types.QueueInfo
		assert.ErrorIs(t, conf.Service.StorageConfig.Queues.Get(ctx, "src", &stored), store.ErrQueueNotExist)
		assert.ErrorIs(t, conf.Service.StorageConfig.Queues.Get(ctx, "dlq", &stored), store.ErrQueueNotExist)
	})

	t.Run("DeadQueueDefinedLaterInFile", func(t *testing.T) {
		file, err := daemon.ReadConfig(strings.NewReader(memoryStorage + `
queues:
  - name: src
    dead-queue: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 2
  - name: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`))
		require.NoError(t, err)

		conf := &daemon.Config{}
		require.NoError(t, daemon.ApplyConfigFile(ctx, conf, file, io.Discard))

		var src, dlq types.QueueInfo
		require.NoError(t, conf.Service.StorageConfig.Queues.Get(ctx, "src", &src))
		require.NoError(t, conf.Service.StorageConfig.Queues.Get(ctx, "dlq", &dlq))
		assert.Equal(t, "dlq", src.DeadQueue)
		assert.Equal(t, 1, dlq.RequestedPartitions)
	})
}

func TestConfigFileDeadLetterRulesWithStoredQueues(t *testing.T) {
	ctx := context.Background()

	apply := func(t *testing.T, dir string, queues string) (*daemon.Config, error) {
		t.Helper()
		file, err := daemon.ReadConfig(strings.NewReader(`
partition-storage:
  - name: mem-00
    memory: {}
    affinity: 1
queue-storage:
  badger:
    storage-dir: ` + dir + `
` + queues))
		require.NoError(t, err)
		conf := &daemon.Config{}
		return conf, daemon.ApplyConfigFile(ctx, conf, file, io.Discard)
	}

	saveStored := func(t *testing.T, dir string, info types.QueueInfo) {
		t.Helper()
		conf, err := apply(t, dir, "")
		require.NoError(t, err)
		require.NoError(t, conf.Service.StorageConfig.Queues.Add(ctx, info))
		require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx))
	}

	t.Run("FileStillDeclaresStoredBrokenQueue", func(t *testing.T) {
		dir := t.TempDir()
		saveStored(t, dir, types.QueueInfo{
			Name:                "src",
			DeadQueue:           "missing",
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       time.Hour,
			RequestedPartitions: 1,
		})

		conf, err := apply(t, dir, `
queues:
  - name: src
    dead-queue: missing
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`)
		require.EqualError(t, err, "invalid queue 'src'; dead_queue does not exist; 'missing' was not found, create it first")
		require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx))
	})

	t.Run("FileNoLongerDeclaresStoredBrokenQueue", func(t *testing.T) {
		dir := t.TempDir()
		saveStored(t, dir, types.QueueInfo{
			Name:                "src",
			DeadQueue:           "missing",
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       time.Hour,
			RequestedPartitions: 1,
		})

		conf, err := apply(t, dir, `
queues:
  - name: src
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`)
		require.NoError(t, err)
		defer func() { require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx)) }()

		var stored types.QueueInfo
		require.NoError(t, conf.Service.StorageConfig.Queues.Get(ctx, "src", &stored))
		assert.Equal(t, "missing", stored.DeadQueue)
	})

	t.Run("DeadQueueOnlyInStorageHasManyPartitions", func(t *testing.T) {
		dir := t.TempDir()
		saveStored(t, dir, types.QueueInfo{
			Name:                "dlq",
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       time.Hour,
			RequestedPartitions: 4,
		})

		conf, err := apply(t, dir, `
queues:
  - name: src
    dead-queue: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`)
		require.EqualError(t, err, "invalid queue 'src'; dead_queue is invalid; 'dlq' must have a single partition, but has '4'")
		defer func() { require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx)) }()

		var stored types.QueueInfo
		assert.ErrorIs(t, conf.Service.StorageConfig.Queues.Get(ctx, "src", &stored), store.ErrQueueNotExist)
	})

	t.Run("DeadQueueOnlyInStorage", func(t *testing.T) {
		dir := t.TempDir()
		saveStored(t, dir, types.QueueInfo{
			Name:                "dlq",
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       time.Hour,
			RequestedPartitions: 1,
		})

		conf, err := apply(t, dir, `
queues:
  - name: src
    dead-queue: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`)
		require.NoError(t, err)
		defer func() { require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx)) }()

		var stored types.QueueInfo
		require.NoError(t, conf.Service.StorageConfig.Queues.Get(ctx, "src", &stored))
		assert.Equal(t, "dlq", stored.DeadQueue)
	})

	t.Run("StoredQueueNamesDeclaredQueueAsDeadQueue", func(t *testing.T) {
		dir := t.TempDir()
		saveStored(t, dir, types.QueueInfo{
			Name:                "src",
			DeadQueue:           "dlq",
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       time.Hour,
			RequestedPartitions: 1,
		})

		conf, err := apply(t, dir, `
queues:
  - name: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 4
`)
		require.EqualError(t, err, "invalid queue 'dlq'; partitions is invalid; 'dlq' is the dead_queue of 'src' and must have "+
			"a single partition, clear or change dead_queue on 'src' first")
		defer func() { require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx)) }()

		var stored types.QueueInfo
		assert.ErrorIs(t, conf.Service.StorageConfig.Queues.Get(ctx, "dlq", &stored), store.ErrQueueNotExist)
	})

	t.Run("NewQueueNamesStoredDeadQueueWithManyPartitions", func(t *testing.T) {
		dir := t.TempDir()
		saveStored(t, dir, types.QueueInfo{
			Name:                "dlq",
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       time.Hour,
			RequestedPartitions: 4,
		})

		conf, err := apply(t, dir, `
queues:
  - name: src
    dead-queue: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
  - name: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`)
		require.EqualError(t, err, "invalid queue 'src'; dead_queue is invalid; 'dlq' must have a single partition, but has '4'")
		defer func() { require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx)) }()

		var stored types.QueueInfo
		assert.ErrorIs(t, conf.Service.StorageConfig.Queues.Get(ctx, "src", &stored), store.ErrQueueNotExist)
	})

	t.Run("FileNoLongerDeclaresStoredBrokenDeadQueue", func(t *testing.T) {
		dir := t.TempDir()
		conf, err := apply(t, dir, "")
		require.NoError(t, err)
		require.NoError(t, conf.Service.StorageConfig.Queues.Add(ctx, types.QueueInfo{
			Name:                "dlq",
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       time.Hour,
			RequestedPartitions: 4,
		}))
		require.NoError(t, conf.Service.StorageConfig.Queues.Add(ctx, types.QueueInfo{
			Name:                "src",
			DeadQueue:           "dlq",
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       time.Hour,
			RequestedPartitions: 1,
		}))
		require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx))

		conf, err = apply(t, dir, `
queues:
  - name: src
    dead-queue: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
  - name: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 1
`)
		require.NoError(t, err)
		defer func() { require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx)) }()

		var dlq types.QueueInfo
		require.NoError(t, conf.Service.StorageConfig.Queues.Get(ctx, "dlq", &dlq))
		assert.Equal(t, 4, dlq.RequestedPartitions)
	})

	t.Run("SourceStoredPastFirstPageOfQueues", func(t *testing.T) {
		dir := t.TempDir()
		conf, err := apply(t, dir, "")
		require.NoError(t, err)
		for i := range 1_000 {
			require.NoError(t, conf.Service.StorageConfig.Queues.Add(ctx, types.QueueInfo{
				Name:                fmt.Sprintf("filler-%04d", i),
				LeaseTimeout:        time.Minute,
				ExpireTimeout:       time.Hour,
				RequestedPartitions: 1,
			}))
		}
		require.NoError(t, conf.Service.StorageConfig.Queues.Add(ctx, types.QueueInfo{
			Name:                "src",
			DeadQueue:           "dlq",
			LeaseTimeout:        time.Minute,
			ExpireTimeout:       time.Hour,
			RequestedPartitions: 1,
		}))
		require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx))

		conf, err = apply(t, dir, `
queues:
  - name: dlq
    lease-timeout: 1m
    expire-timeout: 1h
    requested-partitions: 4
`)
		require.EqualError(t, err, "invalid queue 'dlq'; partitions is invalid; 'dlq' is the dead_queue of 'src' and must have "+
			"a single partition, clear or change dead_queue on 'src' first")
		defer func() { require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx)) }()

		var stored types.QueueInfo
		assert.ErrorIs(t, conf.Service.StorageConfig.Queues.Get(ctx, "dlq", &stored), store.ErrQueueNotExist)
	})
}
