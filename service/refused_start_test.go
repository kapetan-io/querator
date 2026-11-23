package service_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefusedStartSavesNoBackend(t *testing.T) {
	const template = `
partition-storage:
  - name: mem-00
    memory: {}
    affinity: %s
queue-storage:
%s
queues:
  - name: queue-1
    requested-partitions: 2
    lease-timeout: 1m
    expire-timeout: 1h
`
	badger := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}
	mongo := mongoTestSetup{}

	for _, tc := range []struct {
		queueStorage func() string
		tearDown     func()
		name         string
	}{
		{
			name:         "InMemory",
			queueStorage: func() string { return "  memory: {}" },
			tearDown:     func() {},
		},
		{
			name: "BadgerDB",
			queueStorage: func() string {
				badger.Setup(store.BadgerConfig{})
				return fmt.Sprintf("  badger:\n    storage-dir: %q", badger.data)
			},
			tearDown: badger.Teardown,
		},
		{
			name: "PostgreSQL",
			queueStorage: func() string {
				postgres.Setup(store.PostgresConfig{})
				return fmt.Sprintf("  postgres:\n    connection-string: %q", postgres.dsn)
			},
			tearDown: postgres.Teardown,
		},
		{
			name: "MongoDB",
			queueStorage: func() string {
				mongo.Setup(store.MongoConfig{})
				container, err := getSharedMongoContainer()
				require.NoError(t, err)
				return fmt.Sprintf("  mongo:\n    connection-string: %q\n    database: %q", container.uri, mongo.dbName)
			},
			tearDown: mongo.Teardown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			queueStorage := tc.queueStorage()
			defer tc.tearDown()

			apply := func(affinity string) (*daemon.Daemon, error) {
				file, err := daemon.ReadConfig(strings.NewReader(fmt.Sprintf(template, affinity, queueStorage)))
				require.NoError(t, err)
				conf := daemon.Config{InMemoryListener: true}
				if err := daemon.ApplyConfigFile(ctx, &conf, file, io.Discard); err != nil {
					require.NoError(t, conf.Service.StorageConfig.Queues.Close(ctx))
					return nil, err
				}
				return daemon.NewDaemon(ctx, conf)
			}

			_, err := apply("0")
			require.ErrorContains(t, err, "no storage backend can take partitions")

			d, err := apply("1")
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Shutdown(ctx)) }()

			var info pb.QueueInfo
			require.NoError(t, d.MustClient().QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: "queue-1"}, &info))
			assert.Len(t, info.Partitions, 2)
		})
	}
}
