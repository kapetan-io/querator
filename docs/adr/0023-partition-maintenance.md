# 23. Partition Maintenance

Date: 2025-03-25

## Status

Proposed

Superseded in part by [31. Partition placement and rebalance](0031-partition-placement-and-rebalance.md)

## Context

The ability to manage partitions is a key part of enabling zero downtime updates and highly 
efficient management of resources. Operators have the full control of how partitions are
managed and distributed which means Querator is capable of operating in very diverse and 
unique environments, and at extremely high scale.

## Partition States
These are the states the partition can be in

### Available
If a partition is marked "Available" the partition is available for all production and consumption 
operations. This is considered the "normal" operating state for the partition.

### Error
If a partition is marked as "Error" this means there was some issue with the underlying storage
such that the partition is unavailable for interaction. When in this state the Logical Queue's
lifecycle will continually attempt to recover the partition into an "Available" state, with
back-off. A partition is in this state while its failure count is not zero
(`PartitionState.Failures != 0`). There is no separate state value.

### Draining
> Superseded by [31. Partition placement and rebalance](0031-partition-placement-and-rebalance.md).

If a partition is marked as "Draining" the partition refuses production of new items into the partition,
but consumption and completion of items in the partition is allowed until all items in the partition
are completely drained. Once drained, the partition can be decommissioned, migrated to another storage
system, or have maintenance performed on its underlying storage without interrupting the queue.
Draining is the mechanism ADR-0016 calls a "read-only" partition.

TODO: Details. Open question: whether scheduled items and items waiting on retry count as remaining.

### Pausing is not a partition state
Querator does not pause partitions or queues. [13. Pausing a queue](0013-pausing-a-queue.md) was rejected in favor
of draining, and a partition "Unavailable" state was considered and dropped, because nothing needs it. Reload, the
only operation that would have used it, runs on the owning `Logical` request loop and does not need it.

## Partition Operations

### Migrating a Partition
> Superseded by [31. Partition placement and rebalance](0031-partition-placement-and-rebalance.md).

Migrate items in one partition to another partition. The source and destination partition may not exist
on the same underlying storage system. This allows for deprecation of existing storage systems, or for
extended maintenance cycles for underlying storage systems without impacting operation of the queue.

TODO: Details

### Reloading a Partition
Reloading is the process of syncing cached information about the underlying queue with actual stored
information on disk. This can include the number of items in partitions, or the number of leased
and un-leased items in a partition. Reloading can be useful if the underlying storage system has
changed out of band without `Querator` knowledge. For instance, due to a restored backup, or import
or export of items from the partition which were preformed out of band or via the `/storage` API.

##### Reloading Process
The reloading process is handled by `Logical` which owns the partition to be reloaded. The user can 
request all partitions be reloaded, or a subset of partitions be reloaded. In both cases the reload 
process is the same. 

> NOTE: A Partition cannot be re-assigned or rebalanced to a different `Logical` during the reload process.

The reload runs on the `Logical` request loop, which is the only writer of its partitions, so no other request
touches a partition while it reloads. For each partition requested the `Logical` reads the status of the entire
partition from the underlying storage and updates the in-memory counts of leased and un-leased items. A partition
whose storage cannot be read is marked "Error".

> NOTE: Outstanding leases are not invalidated by a reload. Complete and retry requests for them wait on the 
> `Logical` request loop and are handled once the reload finishes.

## Consequences

Partition maintenance can be both complex and powerful for systems that require no-downtime maintenance of underlying 
systems. (I'm sure there are some downsides, I'm just nothing thinking about them now)
