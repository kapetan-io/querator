// Package placement decides which storage backend holds each of a queue's partitions, and which
// partition a produce or a lease goes to. It reads queue records and backend affinities and holds
// no storage or network code, so the same saved facts always give the same answer.
//
// Shares hands a queue's requested partitions out one at a time to the backends with an affinity
// above 0, ranked by how far each is below its proportional share and by a hash of the queue and
// backend names. Rebalance keeps the lowest-numbered available partitions on each backend up to
// its share, marks the rest draining, and numbers new partitions from the queue's next partition
// number. Create is Rebalance on a queue with no partitions. The hash and the rank formula are a
// contract: changing either moves queues at the next rebalance (docs/adr/0031-partition-placement-and-rebalance.md).
//
// ProduceInto picks the available partition that is not failing with the fewest unleased items. LeaseFrom
// takes the lowest-numbered draining partition that holds unleased items first, then the available
// partition with the most unleased items. Ties go to the lower partition number. A queue that
// requests one partition keeps first-in-first-out order, so LeaseFrom waits instead of skipping a
// draining partition that is failing.
//
// The tests in this package simulate thousands of random sequences of produce, lease, retry,
// count changes, affinity changes and backend failures against this code and check the placement
// invariants after every step. They call the package directly because no client call can drive
// those sequences.
package placement
