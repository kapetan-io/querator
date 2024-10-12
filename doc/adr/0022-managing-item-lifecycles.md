# 22. managing item lifecycles

Date: 2024-10-04

## Status

Accepted

## Context

The life cycle of items in the queue and scheduled items need to be managed by querator in an efficient
manner.

## Decision

Item life cycles are managed by a separate Go routine for each partition. This Go routine functions 
similarly to a two-phase garbage collector. In the first phase, it identifies items that require 
action. In the second phase, it makes calls to either the Logical queue or the QueuesManager to 
remove, move, or update the status of these items.

Running the life cycle in a separate Go routine prevents any potential congestion in the main 
sync Go routine during the read phase. Since the life cycle is a client of both the logical queue
and the QueuesManager, either of these systems can optimize the actions taken by the life cycle
as they deem necessary.

For instance, in the case of a logical queue, items that need to be moved or updated can be
batched along with reserve or produce items by the logical queue to improve data storage 
access efficiency, if supported by the underlying storage. Item life cycles run in a separate
Go routine per partition. This allows partitions operating in a degraded state to do so without 
affecting non-degraded partitions.

When a logical queue starts, it will initiate the life cycles of the partitions it is responsible
for. Once started, the life cycle must immediately perform a phase one read of the partition's
storage, as there may be items that require action. If a life cycle fails to perform the read 
phase, the partition should be considered faulty and excluded from the logical queue's request
distribution until the life cycle can be restarted. This is to avoid unexpected behavior in a 
partition, such as handling reserved items that have exceeded their deadline in a timely manner.
If the life cycle is unable to start, the partition should not allow new items to be produced on it.

NOTE: The life cycle go routine will only READ from the partition directly. Any writes
preformed will be delegated to the logical queue or queues manager.

#### Life Cycle Responsibilities 
- Find all the reserve items in the partition which have exceeded their `reserve_deadline` and 
  increment `attempts` then reset the item to un-reserved state and clear the deadline.
- If any items have exceeded their `max_attempts`
    - If the queue has a dead letter, then send the item to the dead letter if possible
        - If sent to dead letter, then remove them from the partition
          (if this fails, the item will be found and moved at a later time, since it still exceeded
          the attempts)
    - If the queue has no dead letter, log the expired item and delete it.
- if any items have exceeded `dead_deadline`
    - If the queue has a dead letter, then send the item to the dead letter if possible
        - If send to dead letter is successful, then remove the item from the partition
          (if this fails, the item will be found and moved at a later time, since it still exceeded
          the attempts)
    - If the queue has no dead letter, log the expired item and delete it.
-  Find any items scheduled for enqueue and distribute them to the current Logical queue

### Maintaining FIFO Integrity
When an item exceeds its `reserved_deadline`, a straightforward solution might be to reset the
`reserved_deadline` and mark the item as `is_reserved=false`. However, this approach compromises
the First-In-First-Out (FIFO) nature of the queue. This is because un-reserved items typically have
item IDs that place them at the end of the queue, after newly queued items. If these un-reserved
items are NOT explicitly placed at the beginning of the queue, they will naturally take precedence over new
or existing items in the queue, thus violating the FIFO nature of the queue.

To maintain FIFO integrity, if an item fails to be completed before its `reserved_deadline` is exceeded, 
it should be placed at the beginning of the FIFO queue when it is returned to an un-reserved status. This
ensures that items already in the queue take precedence over such items and preserves FIFO behavior. 

Consider a failure scenario where thousands of reserved items exceed their `reserved_deadline` and are
returned to an un-reserved status by the lifecycle. If these previously reserved items are not placed
at the beginning of the queue, the flood of un-reserved items which appear at the end of the queue
could starve out items that exist further up the queue such that their processing is delayed. 
This could also create a cycle where reserved items never complete processing, while also
saturating all consumers, denying other items in the queue the chance to be processed and completed. 
As a result, items that would normally be processed do not get the opportunity to be handled by 
consumers and thus item processing is delayed.

## Consequences
Because the life cycle runs its first phase in a separate Go routine, all querator storage systems
must support concurrent read/write workloads. This ensures that both the life cycle and the sync
Go routines can access the storage system simultaneously. Most storage systems do support this.

Since the life cycle runs in a separate Go routine, it is possible that the routine might be
scheduled to run or blocked from a previous run, leading to a failure in updating expired
reservations. This means a reserved item may not be immediately available for delivery to a
different consumer at the exact moment one might expect. However, I believe that the efficiency
gained by this design outweighs any deficiencies incurred by the delayed handling of expired
reservations.

#### The appearance of compromised FIFO integrity
The decision to force items which exceed the `reserved_deadline` to be re-queued at the beginning of the queue
can have a negative impact on items which MUST be processed in a specific order.

Consider a queue with 3 items which must be completed in order
- Item 1 adds an item to a list in a database
- Item 2 processes the list in the database
- Item 3 deletes the item from the database

If `Item 1` exceeds the `reserved_deadline`, `Item 1` will be re-queued to run after `Item 3` completes,
which is likely not what the user wants. This behavior may appear to the user as if the FIFO integrity
is compromised. In this scenario it may be acceptable that an item which fails to be processed as
completed will block all other items from being processed until itself is processed to completion.

In the future, we may want to support marking reserved items which exceed their `reserved_deadline` without
placing them at the begining of the queue. Thus preserving their place at the end of the queue. This is
currently not a priority as it is my opinion that such scenario can be avoided by having `Item 1` enqueue
`Item 2` only when `Item 1` is complete, thus avoiding this problem, and ensuring order or user operation
is preserved while not blocking other items in the queue waiting to be processed.

