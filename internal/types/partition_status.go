package types

type PartitionStatus int

const (
	// Partition is healthy, both produce and consume are allowed
	PartitionAvailable PartitionStatus = iota
	// No produce allowed, can consume until empty
	PartitionDraining
	// Storage issue, auto-recovery
	PartitionError
)

// statusRank is used to sort partitions by their status. Lower values are considered "better" and will be sorted first.
var StatusRank = map[PartitionStatus]int{
	PartitionAvailable: 0,
	PartitionDraining:  1,
	PartitionError:     2,
}
