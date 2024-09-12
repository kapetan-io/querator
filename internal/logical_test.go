package internal

import (
	"fmt"
	"slices"
	"testing"
)

// This test is just for my sanity, please feel free to remove
func TestLogicalDistributionSortOrder(t *testing.T) {
	var state QueueState
	state.PartitionDistributions = []PartitionDistribution{
		{Count: 10},
		{Count: 0},
		{Count: 500},
		{Count: 5},
	}
	slices.SortFunc(state.PartitionDistributions, func(a, b PartitionDistribution) int {
		if a.Count < b.Count {
			return -1
		}
		if a.Count > b.Count {
			return +1
		}
		return 0
	})

	for i := range state.PartitionDistributions {
		fmt.Printf("[%d] Count: %d\n", i, state.PartitionDistributions[i].Count)
	}

}
