package service_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestPostgresQueuesTableWithoutNextPartition(t *testing.T) {
	defer goleak.VerifyNone(t, goleakOptions...)

	var postgres postgresTestSetup
	defer postgres.Teardown()
	conf := postgres.Setup(store.PostgresConfig{})

	conn, err := pgx.Connect(context.Background(), postgres.dsn)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), `
		CREATE TABLE queues (
			name TEXT PRIMARY KEY,
			namespace TEXT NOT NULL DEFAULT '',
			lease_timeout_ns BIGINT NOT NULL,
			expire_timeout_ns BIGINT NOT NULL,
			dead_queue TEXT NOT NULL DEFAULT '',
			max_attempts INTEGER NOT NULL DEFAULT 0,
			reference TEXT NOT NULL DEFAULT '',
			requested_partitions INTEGER NOT NULL,
			partition_info JSONB NOT NULL DEFAULT '[]'::jsonb,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		)`)
	require.NoError(t, err)
	require.NoError(t, conn.Close(context.Background()))

	d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: conf})
	defer d.Shutdown(t)
	c, ctx := d.MustClient(), d.Context()

	name := random.String("queue-", 10)
	createQueueAndWait(t, ctx, c, &pb.QueueInfo{
		LeaseTimeout:        LeaseTimeout,
		ExpireTimeout:       ExpireTimeout,
		QueueName:           name,
		RequestedPartitions: 2,
	})

	var info pb.QueueInfo
	require.NoError(t, c.QueuesInfo(ctx, &pb.QueuesInfoRequest{QueueName: name}, &info))
	assert.Len(t, info.Partitions, 2)
}
