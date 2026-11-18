package internal_test

import (
	"context"
	"testing"

	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unreachable from the service: QueuesManager spawns one Logical per queue, so rotation across
// several Logicals can only be driven through the internal package's exported API.
func TestQueueGetNextRotatesOverLogicals(t *testing.T) {
	q := internal.NewQueue(types.QueueInfo{
		PartitionInfo: []types.PartitionInfo{
			{PartitionNum: 0},
			{PartitionNum: 1},
			{PartitionNum: 2},
		},
	})

	var logicals []*internal.Logical
	for num := range 3 {
		l, err := internal.SpawnLogicalQueue(internal.LogicalConfig{
			QueueInfo: types.QueueInfo{
				PartitionInfo: []types.PartitionInfo{{PartitionNum: num}},
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, l.Shutdown(context.Background())) })
		logicals = append(logicals, l)
	}
	require.NotNil(t, q.AddLogical(logicals...))
	// Adding the same Logical twice does not add it to the rotation twice.
	require.NotNil(t, q.AddLogical(logicals[0]))

	assert.Equal(t, logicals, q.GetAll())

	for num, want := range logicals {
		got, err := q.GetByPartition(num)
		require.NoError(t, err)
		assert.Same(t, want, got)
	}

	assert.Same(t, logicals[0], q.GetNext())
	assert.Same(t, logicals[1], q.GetNext())
	assert.Same(t, logicals[2], q.GetNext())
	assert.Same(t, logicals[0], q.GetNext())
	assert.Same(t, logicals[1], q.GetNext())
	assert.Same(t, logicals[2], q.GetNext())
	assert.Same(t, logicals[0], q.GetNext())
}
