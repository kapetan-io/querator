# 23. Partition Maintenance

Date: 2025-03-25

## Status

Proposed

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
such that the partition is unavailable for interaction. When in this state `LifeCycle` will continually
attempt to recover the partition into an "Available" state.

TODO: Details

### Unavailable
If a partition is marked as "Unavailable" the partition is unavailable for any action. When in this
state no assumptions about the underlying status of the storage system is made, and no interaction
with the partition is possible.

TODO: Details

### Draining
If a partition is marked as "Draining" the partition refuses production of new items into the partition,
but consumption and completion of items in the partition is allowed until all items in the partition
are completely drained.

TODO: Details

### Paused
Pauses both the production and consumption of items to and from a partition. Pausing allows existing
leases to be completed, but new leases are not allowed, and new items cannot be produced to
the partition. This allows a partition to "rest" while maintenance is preformed, migration is completed
or the partition is reloaded.

TODO: Details

## Partition Operations

### Migrating a Partition
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

The reload process occurs sequentially by iterating through the partitions requested, reloading each until
the reload process for all requested partitions is complete. Any subsequent reload requests to the `Logical`
will fail until the current reload process is completed. The `Logical` queue is responsible for managing 
the reload process. To preform a reload the `Logical` preforms the following on each partition to be 
reloaded sequentially.

1. Mark the partition as "Unavailable" if it is not already marked as such.
2. Notify `LifeCycle` to read the status of the entire partition from the underlying storage
3. LifeCycle will call `Logical.PartitionStateChange()` updating the current state of the partition and marking the
partition as "Available".

> NOTE: Marking the partition as "Unavailable" means any outstanding leases given to clients will 
> not be allowed to mark the item as completed until the reload process is complete. As such it is 
> recommended to "Pause" the partitions you wish to reload first, allowing any outstanding item 
> leases to be completed before reload is run. This is not required however, and any outstanding 
> leases can be marked as completed after reload is completed.

## Consequences

Partition maintenance can be both complex and powerful for systems that require no-downtime maintenance of underlying 
systems. (I'm sure there are some downsides, I'm just nothing thinking about them now)
