# 24. scheduled items

Date: 2025-04-15

## Status

Proposed

## Context
Querator requires a mechanism to track items scheduled which are enqueued into a queue at a future date and time.
These items may originate from retries with a future `retry_at` set, or from produced items with `enqueue_at`
set to a time in the future.

## Decision
#### Scheduled Items
When a produced item has `enqueue_at` time set to a future date, the produced item will be assigned
a partition and written to the same data store as the chosen partition. Distribution of scheduled items
amongst the available partitions will use the same optimistic partition distribution system described in
[0019-partition-item-distribution](0019-partition-item-distribution.md) where the number of scheduled
items is combined with the number of enqueued items, and then used to calculate optimistic distribution.

An item whose `enqueue_at` is in the past or only moments away is produced as an ordinary item and its
`enqueue_at` is cleared. Clients often fill in every field of a produce request and set `enqueue_at` to
"now", which clock drift between machines puts a few milliseconds in the future, and scheduling such an
item costs a write, a scan and a move for nothing. An import (`/v1/storage/items.import`) restores
items as they were, so an imported item keeps the `enqueue_at` it was given.

#### Retried Items
When an item is retried with a `retry_at` timestamp set to a future date, it will be placed into the
scheduled queue of the same partition to which it was originally enqueued. As a result, retried items
cannot move to different partitions and will remain associated with their original partition for their
entire lifecycle. A `retry_at` in the past or only moments away is an immediate retry, by the same
rule as `enqueue_at`.

### Implementation
Querator uses the Logical Queue's lifecycle to find scheduled items that need to be enqueued at
their designated times. The lifecycle keeps a scheduled timer, independent of the main lifecycle
timer, to track when scheduled items are due for enqueueing. This avoids reading the partition for
other lifecycle actions when only scheduled items are due.

After each scheduled run the lifecycle asks storage for the earliest scheduled time and sets the
scheduled timer from it. A produce or retry that schedules an earlier time brings the timer forward.
The timer lives in memory and those writes are only the ones made since the queue loaded, so asking
storage is what keeps a scheduled item written before a restart from being forgotten. Scheduled runs
count their failures apart from the main lifecycle's, so neither raises or resets the other's back-off.
Unlike a failed lifecycle read, a failed scheduled run leaves the partition in the request
distribution.

The lifecycle generates scheduled actions of type `ActionQueueScheduledItem`, each containing the ID
of the scheduled item. `Logical` writes these actions to the storage implementation on its request
loop, one page at a time (see [22. Managing item lifecycles](0022-managing-item-lifecycles.md)). The
storage implementation is responsible for moving items from the scheduled queue to the partition
queue.

#### Data Store Implementation
The data store implementation should attempt to move items atomically whenever possible. If atomic
movement is not supported by the underlying store, a catastrophic failure during the non-atomic
transfer of scheduled items to the partition queue may result in duplicate scheduled items being enqueued.

It is the responsibility of the data store implementation to determine the most effective method for
storing and moving scheduled items from the scheduled queue to the partition queue.

> NOTE: If the item has a `unique_id` set, we might be able to use that to avoid duplication in the future.
#### Optimizations
Depending on support from data store implementations, it may be feasible to store scheduled items within the
same table or collection as the partition queued items. This would eliminate the need for an additional table
and database retrieval and reinsertion when transitioning from scheduled items to partition queue. It could
also provide atomic movement of scheduled items to partition queue if data storage supports transactions.

With SQL compatible data stores it is likely possible to create a table where the primary key is a composite of
the sorted key and the item type of `scheduled` or `queued`. When a scheduled item must be enqueued, the sorted
key and item type can be updated, thus simulating enqueuing the item into the table without needing to first
retrieve and then re-insert the item in the data store.

#### Linearizability
When moving scheduled items, data store implementations SHOULD insert items in the order in which
they were produced if they share the same scheduled time. This ensures that items scheduled first
are enqueued first, thereby preserving FIFO (First-In, First-Out) order for scheduled items.

#### Scheduled Items
We will introduce a new endpoint called `/v1/storage/scheduled.list` for viewing scheduled items
in a partition. Scheduled items will not be displayed via the `/v1/storage/items.list` endpoint.

## Consequences
#### Imbalanced Partitions
Since each scheduled item is assigned to a partition and remains associated with that partition for
its entire lifecycle, partitions may become imbalanced if a large number of retried or scheduled items
accumulate in a single partition.

Optimistic distribution should help minimize partition imbalance. Additionally, we could optionally
inspect the scheduled queue for items that are about to be enqueued and adjust the optimistic
distribution.

#### Optimistic Distribution
Because scheduled items and queued items are counted separately, the optimistic distribution for
scheduled items must be managed independently of the partition queue. This ensures that scheduled items
are distributed evenly across partitions, regardless of the number of non-scheduled items currently
enqueued in each partition, and helps prevent imbalanced partition assignment.

#### Reliability of Some Data Stores
Since the handling of scheduled items depends on the underlying data store, Querator cannot guarantee
transactional safety or linearizability for all implementations. The reliability and consistency of
scheduled items are determined entirely by the specific data store implementation used by Querator.