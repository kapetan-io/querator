package internal

import (
	"context"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// This is a happy path unit test for internal.NewQueue() DO NOT ADD MORE TESTS
// Remove this test once functional tests cover this scenario.
func TestNewQueue(t *testing.T) {
	q := NewQueue(types.QueueInfo{
		PartitionInfo: []types.PartitionInfo{
			{
				Partition: 0,
			},
			{
				Partition: 1,
			},
			{
				Partition: 2,
			},
		},
	})

	l1, err := SpawnLogicalQueue(LogicalConfig{
		QueueInfo: types.QueueInfo{
			PartitionInfo: []types.PartitionInfo{
				{
					Partition: 0,
				},
			},
		},
	})
	require.NoError(t, err)
	defer func() { _ = l1.Shutdown(context.Background()) }()
	l2, err := SpawnLogicalQueue(LogicalConfig{
		QueueInfo: types.QueueInfo{
			PartitionInfo: []types.PartitionInfo{
				{
					Partition: 1,
				},
			},
		},
	})
	require.NoError(t, err)
	defer func() { _ = l2.Shutdown(context.Background()) }()
	l3, err := SpawnLogicalQueue(LogicalConfig{
		QueueInfo: types.QueueInfo{
			PartitionInfo: []types.PartitionInfo{
				{
					Partition: 2,
				},
			},
		},
	})
	require.NoError(t, err)
	defer func() { _ = l3.Shutdown(context.Background()) }()
	require.NotNil(t, q.AddLogical(l1, l2, l3))

	assert.Equal(t, 3, len(q.GetAll()))

	_, l, err := q.GetByPartition(0)
	require.NoError(t, err)
	assert.Equal(t, 0, l.conf.PartitionInfo[0].Partition)

	_, l, err = q.GetByPartition(1)
	require.NoError(t, err)
	assert.Equal(t, 1, l.conf.PartitionInfo[0].Partition)

	_, l, err = q.GetByPartition(2)
	require.NoError(t, err)
	assert.Equal(t, 2, l.conf.PartitionInfo[0].Partition)

	_, n := q.GetNext()
	assert.Equal(t, 0, n.conf.PartitionInfo[0].Partition)
	_, n = q.GetNext()
	assert.Equal(t, 1, n.conf.PartitionInfo[0].Partition)
	_, n = q.GetNext()
	assert.Equal(t, 2, n.conf.PartitionInfo[0].Partition)
	_, n = q.GetNext()
	assert.Equal(t, 0, n.conf.PartitionInfo[0].Partition)
	_, n = q.GetNext()
	assert.Equal(t, 1, n.conf.PartitionInfo[0].Partition)
	_, n = q.GetNext()
	assert.Equal(t, 2, n.conf.PartitionInfo[0].Partition)
	_, n = q.GetNext()
	assert.Equal(t, 0, n.conf.PartitionInfo[0].Partition)
}
