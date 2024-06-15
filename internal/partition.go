package internal

// Partition is the synchronization point for all read and writes to the underlying data store. There MUST never
// be more than one unique instance of a partition running anywhere in the cluster.
//
// V0: Currently a queue can only have one partition. Eventually we will support many partitions per queue,
// managed and distributed by different nodes in the cluster.
type Partition interface {
	// TODO: Query the data base and wait for new things to show up.
	// TODO: First implementation of this partition should be in memory only partition.
}
