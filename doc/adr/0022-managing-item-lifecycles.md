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

