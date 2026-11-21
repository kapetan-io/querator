package service_test

import (
	"context"
	"strings"
	"testing"

	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal/store"
	pb "github.com/kapetan-io/querator/proto"
	svc "github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongoopts "go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/goleak"
)

// TestMongoProduce holds produce tests that need MongoDB to refuse one item of a write, which
// only a collection validator set on MongoDB itself can do. They run against MongoDB only.
func TestMongoProduce(t *testing.T) {
	defer goleak.VerifyNone(t, goleakOptions...)

	t.Run("DeadLetterWriteFailingPartwayMovesEachItemOnce", func(t *testing.T) {
		now := clock.NewProvider()
		now.Freeze(clock.Now())
		defer now.UnFreeze()

		var mongoSetup mongoTestSetup
		d := newDaemon(t, 60*clock.Second, svc.Config{StorageConfig: mongoSetup.Setup(store.MongoConfig{}), Clock: now})
		defer func() {
			d.Shutdown(t)
			mongoSetup.Teardown()
		}()
		c, ctx := d.MustClient(), d.Context()

		queueName := random.String("queue-", 10)
		dlqName := random.String("dlq-", 10)
		createOnePartitionQueue(t, ctx, c, dlqName)
		createQueueAndWait(t, ctx, c, &pb.QueueInfo{
			QueueName:           queueName,
			LeaseTimeout:        "1m0s",
			ExpireTimeout:       "24h0m0s",
			MaxAttempts:         1,
			DeadQueue:           dlqName,
			RequestedPartitions: 1,
		})
		const count = 3
		produceInOrder(t, ctx, c, queueName, count)
		require.Len(t, leaseUpTo(t, ctx, c, queueName, count), count)

		// MongoDB refuses item-00001 from now on, so the move stores item-00000 and then fails.
		// The source deletes only item-00000, so a consumer that completes it before the next
		// pass does not see it moved again.
		refuseReference(t, ctx, &mongoSetup, "item-00001")
		now.Advance(clock.Minute + clock.Second)
		require.Eventually(t, func() bool {
			return partitionTotal(t, ctx, c, queueName) == count-1
		}, 5*clock.Second, 20*clock.Millisecond)
		assert.Equal(t, int32(1), partitionTotal(t, ctx, c, dlqName))

		moved := leaseOne(t, ctx, c, dlqName)
		require.Equal(t, "item-00000", moved.Reference)
		require.NoError(t, c.QueueComplete(ctx, &pb.QueueCompleteRequest{
			QueueName:      dlqName,
			RequestTimeout: "5s",
			Ids:            []string{moved.Id},
		}))

		refuseReference(t, ctx, &mongoSetup, "")
		now.Advance(10 * clock.Second)
		require.Eventually(t, func() bool {
			return partitionTotal(t, ctx, c, queueName) == 0
		}, 5*clock.Second, 20*clock.Millisecond)

		var list pb.StorageItemsListResponse
		require.NoError(t, c.StorageItemsList(ctx, dlqName, 0, &list, &querator.ListOptions{Limit: 2 * count}))
		references := make([]string, 0, len(list.Items))
		for _, item := range list.Items {
			references = append(references, item.Reference)
		}
		assert.ElementsMatch(t, []string{"item-00001", "item-00002"}, references)
	})
}

// refuseReference makes every item collection in the database refuse a new item whose reference
// is reference, and an empty reference lifts the refusal. Items already stored are not checked.
func refuseReference(t *testing.T, ctx context.Context, setup *mongoTestSetup, reference string) {
	t.Helper()
	container, err := getSharedMongoContainer()
	require.NoError(t, err)
	client, err := mongo.Connect(ctx, mongoopts.Client().ApplyURI(container.uri))
	require.NoError(t, err)
	defer func() { _ = client.Disconnect(ctx) }()

	db := client.Database(setup.dbName)
	names, err := db.ListCollectionNames(ctx, bson.M{})
	require.NoError(t, err)
	validator := bson.M{}
	if reference != "" {
		validator = bson.M{"reference": bson.M{"$ne": reference}}
	}
	var modified int
	for _, name := range names {
		if !strings.HasPrefix(name, "items_") {
			continue
		}
		require.NoError(t, db.RunCommand(ctx, bson.D{
			{Key: "collMod", Value: name},
			{Key: "validator", Value: validator},
			{Key: "validationLevel", Value: "moderate"},
		}).Err())
		modified++
	}
	// One item collection for each queue, since the source and the dead letter queue have one
	// partition each.
	require.Equal(t, 2, modified)
}
