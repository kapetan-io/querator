# 24. scheduled items

Date: 2025-04-15

## Status

Accepted

## Context
Querator requires a mechanism to track and enqueue items scheduled for a future date and time. 
These items may originate from retries with a future `retry_at` set or from produced items
with `enqueue_at` set to a time in the future.

## Decision
Scheduled items will be stored in a separate table or collection distinct from the queued partition items. 
However, they will remain linked to the partition through a shared name. This design ensures that items 
scheduled for future enqueueing are partitioned across data stores and do not present a scaling issue by
sharing a central scheduled table shared by the entire queue.

Example:
- An item is scheduled for delivery 10 minutes from now and is placed in `queue-0001-scheduled`.
- After 10 minutes, the item is retrieved from `queue-0001-scheduled` enqueued to `queue-0001-partition`.

### Life Cycles
Querator will use `LifeCycle` to monitor and handle scheduled items that need to be enqueued at their 
designated times. The `LifeCycle` will implement a secondary timer, independent of the main timer, to track 
when scheduled items are due for enqueueing. This approach avoids scanning the main partition for lifecycle 
actions and streamlines the movement of scheduled items independent of the other lifecycle actions.

### Movement of Scheduled Items to Queued Items
Since partitions may reside on different physical data stores, Querator will retrieve scheduled items from 
the dedicated scheduled table and enqueue them as if they were submitted by a regular client. Although this
requires a retrieving the entire item from the database, and then writing the item back to the database, this 
implementation ensures consistency with the existing enqueueing process for non-scheduled items and wide
support of data stores which might not support movement of items without the need of re-insertion.

### Future Optimizations
Depending on demand and support from data store implementations, it may be feasible to store scheduled 
items within the same partition table or collection as queued items. This would eliminate the need for database 
retrieval and reinsertion during enqueue. Because enqueuing an item might not be possible without needing to 
re-insert the item for some data stores, this ADR states that we will support movement of the item first, 
then add support for avoiding excess data copy for data stores that might support this.

For instance, with SQL compatible data stores it is likely possible to create a table where the primary key is 
a composite of the sorted key and the item type of `scheduled` or `queued`. When a scheduled item must be enqueued,
the sorted key and item type can be updated, thus simulating enqueuing the item into the table without needing to
first retrieve and then re-insert the item in the data store.

## Consequences
The movement of scheduled items may not be efficient, but will remain simple. This allows Querator
to support the broadest set of data stores possible, while remaining easy for operators and developers to 
understand.

Currently, there is no strong preference regarding whether an item, once its scheduled time has arrived, 
should be enqueued into the same partition where it was originally scheduled. In the future, depending on 
the data store implementation, it may be possible for scheduled items to be enqueued into different partitions
based on the state of available partitions at the time they are ready to be enqueued.