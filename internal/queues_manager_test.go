package internal_test

import (
	"context"
	"testing"

	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
	"github.com/stretchr/testify/require"
)

func TestDeletedQueueAdoptsRecordQuietly(t *testing.T) {
	ctx := context.Background()
	conf := store.Config{
		Queues: store.NewMemoryQueues(nil),
		PartitionStorage: []store.PartitionStorage{{
			Name:     "memory-0",
			Affinity: 1,
		}},
	}
	conf.PartitionStorage[0].PartitionStore =
		store.NewMemoryPartitionStore(conf, nil)

	qm, err := internal.NewQueuesManager(internal.QueuesManagerConfig{
		StorageConfig: conf,
		LogicalConfig: internal.LogicalConfig{
			ReadTimeout:  clock.Second,
			WriteTimeout: clock.Second,
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, qm.Shutdown(ctx))
	})

	const name = "queue-deleted-during-adoption"
	_, err = qm.Create(ctx, types.QueueInfo{
		Name:                name,
		LeaseTimeout:        clock.Minute,
		ExpireTimeout:       clock.Hour,
		RequestedPartitions: 1,
	})
	require.NoError(t, err)

	q, err := qm.Get(ctx, name)
	require.NoError(t, err)

	var stored types.QueueInfo
	require.NoError(t, conf.Queues.Get(ctx, name, &stored))

	stored.Revision++

	held := q.Info()
	require.Equal(t, held.CreationID, stored.CreationID)
	require.Greater(t, stored.Revision, held.Revision)
	require.Len(t, q.GetAll(), 1)

	require.NoError(t, qm.Delete(ctx, name))
	require.NoError(t, qm.AdoptRecordInto(ctx, q, stored))
}
