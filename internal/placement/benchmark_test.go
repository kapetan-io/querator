package placement_test

import (
	"fmt"
	"testing"

	"github.com/kapetan-io/querator/internal/placement"
	"github.com/kapetan-io/querator/internal/types"
)

func BenchmarkProduceAndLeaseChoice(b *testing.B) {
	for _, count := range []int{1, 16, 1_024} {
		b.Run(fmt.Sprintf("Partitions_%d", count), func(b *testing.B) {
			queue := types.QueueInfo{Name: "queue-bench", RequestedPartitions: count}
			for i := range count {
				queue.PartitionInfo = append(queue.PartitionInfo, types.PartitionInfo{
					PartitionNum: i,
					StorageName:  "mem-00",
					Draining:     i%4 == 3,
				})
			}
			queue.LinkPartitions()
			unleased := func(p types.PartitionInfo) int { return p.PartitionNum % 7 }
			failing := func(p types.PartitionInfo) bool { return p.PartitionNum%5 == 4 }

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				placement.ProduceInto(queue, unleased, failing)
				placement.LeaseFrom(queue, unleased, failing)
			}
		})
	}
}
