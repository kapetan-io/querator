package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
)

// TestPostgresCommit holds tests where PostgreSQL accepts every statement in a write and then fails
// its commit, by refusing it or by ending the connection. A constraint trigger deferred to commit
// causes the failure, and no other backend has one, so they run against PostgreSQL only.
func TestPostgresCommit(t *testing.T) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("QueueProduce", func(t *testing.T) {
		var postgres postgresTestSetup
		defer postgres.Teardown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: postgres.Setup(store.PostgresConfig{})})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(1),
		}))

		failCommits(t, ctx, postgres.dsn, insertStatement)
		err := c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(3),
		})
		var e duh.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, duh.CodeRetryRequest, e.Code())
		assert.Equal(t, "internal error, try your request again", e.Message())

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		assert.Len(t, list.Items, 1)
	})

	t.Run("QueueProduceConnectionLost", func(t *testing.T) {
		var postgres postgresTestSetup
		defer postgres.Teardown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: postgres.Setup(store.PostgresConfig{})})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(1),
		}))

		dropConnectionOnCommit(t, ctx, postgres.dsn, insertStatement)
		err := c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(3),
		})
		var e duh.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, duh.CodeRetryRequest, e.Code())
		assert.Equal(t, "internal error, try your request again", e.Message())

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		assert.Len(t, list.Items, 1)
	})

	t.Run("StorageItemsImport", func(t *testing.T) {
		var postgres postgresTestSetup
		defer postgres.Teardown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: postgres.Setup(store.PostgresConfig{})})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(1),
		}))

		failCommits(t, ctx, postgres.dsn, insertStatement)
		var resp pb.StorageItemsImportResponse
		err := c.StorageItemsImport(ctx, &pb.StorageItemsImportRequest{
			QueueName: queueName,
			Items: []*pb.StorageItem{
				{
					ExpireDeadline: timestamppb.New(clock.Now().UTC().Add(clock.Hour)),
					Reference:      "import",
					Payload:        []byte("x"),
				},
			},
		}, &resp)
		var e duh.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, duh.CodeInternalError, e.Code())

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		assert.Len(t, list.Items, 1)
	})

	t.Run("StorageItemsDelete", func(t *testing.T) {
		var postgres postgresTestSetup
		defer postgres.Teardown()
		d := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: postgres.Setup(store.PostgresConfig{})})
		defer d.Shutdown(t)
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			LeaseTimeout:        LeaseTimeout,
			ExpireTimeout:       ExpireTimeout,
			QueueName:           queueName,
			RequestedPartitions: 1,
		})
		require.NoError(t, c.QueueProduce(ctx, &pb.QueueProduceRequest{
			QueueName:      queueName,
			RequestTimeout: "5s",
			Items:          produceRandomItems(2),
		}))
		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		require.Len(t, list.Items, 2)

		failCommits(t, ctx, postgres.dsn, deleteStatement)
		err := c.StorageItemsDelete(ctx, &pb.StorageItemsDeleteRequest{
			QueueName: queueName,
			Ids:       querator.CollectIDs(list.Items),
		})
		var e duh.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, duh.CodeInternalError, e.Code())

		require.NoError(t, c.StorageItemsList(ctx, queueName, 0, &list, nil))
		assert.Len(t, list.Items, 2)
	})
}

// commitStatement is the SQL statement whose transaction a commit trigger fires for.
type commitStatement string

const (
	insertStatement commitStatement = "INSERT"
	deleteStatement commitStatement = "DELETE"
)

// failCommits makes PostgreSQL refuse to commit any transaction that runs the given statement
// on an item table. Each statement succeeds, and the refusal arrives only with the commit.
func failCommits(t *testing.T, ctx context.Context, dsn string, statement commitStatement) {
	t.Helper()
	runAtCommit(t, ctx, dsn, statement, `RAISE EXCEPTION 'commit refused by test';`)
}

// dropConnectionOnCommit makes PostgreSQL end the connection of any transaction that runs the
// given statement on an item table while that transaction commits. Each statement succeeds, and
// the connection closes before the commit result is sent.
func dropConnectionOnCommit(t *testing.T, ctx context.Context, dsn string, statement commitStatement) {
	t.Helper()
	runAtCommit(t, ctx, dsn, statement, `PERFORM pg_terminate_backend(pg_backend_pid()); PERFORM pg_sleep(10); RETURN NULL;`)
}

// runAtCommit installs a constraint trigger, deferred to commit, that runs body for each row
// the given statement touches on an item table.
func runAtCommit(t *testing.T, ctx context.Context, dsn string, statement commitStatement, body string) {
	t.Helper()

	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, `
		CREATE OR REPLACE FUNCTION run_at_commit() RETURNS trigger AS $$
		BEGIN
			`+body+`
		END
		$$ LANGUAGE plpgsql`)
	require.NoError(t, err)

	rows, err := conn.Query(ctx, `SELECT tablename FROM pg_tables WHERE tablename LIKE 'items\_%'`)
	require.NoError(t, err)
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.NotEmpty(t, tables)

	for _, table := range tables {
		_, err = conn.Exec(ctx, `CREATE CONSTRAINT TRIGGER run_at_commit AFTER `+string(statement)+` ON `+
			pgx.Identifier{table}.Sanitize()+` DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION run_at_commit()`)
		require.NoError(t, err)
	}
}
