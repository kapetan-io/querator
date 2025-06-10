# 22. managing item lifecycles

Date: 2024-10-04

## Status

Accepted

## Context

The life cycle of items in the queue and scheduled items need to be managed by querator in an efficient
manner.

## Decision

Item life cycles are managed by a separate Go routine for each partition. This routine operates similarly
to a two-phase garbage collector. In the first phase, it identifies items that require action. In the
second phase, it makes calls to either the Logical Queue or the QueuesManager to remove, move, or update
the status of these items. 

Running the life cycle in a separate Go routine, distinct from the main sync
loop, prevents potential congestion in the main sync routine during the read phase. Since the life cycle
is a client of both the Logical Queue and the QueuesManager, either of these systems can optimize the
actions taken by the life cycle as necessary. For example, in the case of a logical queue, items that
need to be moved or updated can be batched along with lease or produce items by the logical queue.

This batching can improve data storage access efficiency, provided it is supported by the underlying
storage.

### The Life Cycle
When a logical queue starts, it will initiate the life cycles of all the partitions it is responsible
for. Once started, the life cycle must immediately perform a phase one read of the partition's
storage, as there may be items that require action. If a life cycle fails to perform the read 
phase, the partition should be considered faulty and excluded from the logical queue's request
distribution until the life cycle can be restarted. This is to avoid unexpected behavior in a 
partition, such as handling leased items that have exceeded their deadline in a timely manner.
If the life cycle is unable to start, the partition should not allow new items to be produced on it.

NOTE: The life cycle go routine will only READ from the partition directly. Any writes
preformed will be delegated to the logical queue or queues manager.

#### Life Cycle Responsibilities 
- Find all the lease items in the partition which have exceeded their `lease_deadline` and 
  increment `attempts` then reset the item to un-leased state and clear the deadline.
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

### Life Cycle per partition
Each partition is assigned a dedicated Go routine to manage its item life cycle. This routine periodically
reads from the data store to identify items that require life cycle handling. As the number of partitions
increases, so does the number of Go routines accessing the data store, potentially generating more IOPS
than necessary. 

Optimizing efficient access to the data store is the responsibility of individual data store
implementations. For instance, a data store implementation might cache life cycle item information in 
secondary in-memory indexes to avoid IOPs when scanning the data store.

We considered logically grouping partitions so that a single life cycle routine could scan multiple partitions,
provided they reside on the same physical data store or data store cluster. However, this approach can
significantly increase the complexity of the solution. Factors such as degraded partitions, availability zones,
and clustered data store systems can affect how partitions are handled within a specific data store
implementation, impacting the life cycle process. Due to this additional complexity, we made the decision
to move forward with the simpler, per-partition life cycle solution. We may revisit this in the
future if warranted.

### Avoid Head of line blocking
When an item exceeds its `leased_deadline`, the naive solution might be to simply reset the
`leased_deadline` and mark the item as `is_leased=false`. However, this approach compromises
the First-In-First-Out (FIFO) nature of the queue. If leased items which have exceeded their 
leases are NOT explicitly placed at the beginning of the queue, they will naturally take 
precedence over new or other existing items in the queue, thus violating the FIFO nature of the
queue. This is commonly referred to as [Head-of-line (HOL) blocking](https://en.wikipedia.org/wiki/Head-of-line_blocking).

In order to avoid HOL, and maintain FIFO integrity, if an item fails to be completed before its 
`leased_deadline` is exceeded, it should be placed at the beginning of the FIFO queue when it is 
returned to an un-leased status. This ensures that items already in the queue take precedence 
over such items and pleases FIFO behavior. 

Consider a failure scenario where thousands of leased items exceed their `leased_deadline` and are
returned to an un-leased status by the lifecycle. If these previously leased items are not placed
at the beginning of the queue, the flood of un-leased items which appear at the end of the queue
could starve out items that exist further up the queue such that their processing is delayed. 
This could also create a cycle where leased items never complete processing, while also
saturating all consumers, denying other items in the queue the chance to be processed and completed. 
As a result, items that would normally be processed do not get the opportunity to be handled by 
consumers and thus item processing is delayed.

## Consequences
Because the life cycle runs its first phase in a separate Go routine, all querator storage systems
must support concurrent read/write workloads. This ensures that both the life cycle and the sync
Go routines can access the storage system simultaneously. Most storage systems do support this.

Since the life cycle runs in a separate Go routine, it is possible that the routine might be
scheduled to run or is blocked from a previous run, leading to a delay in updating expired
leases. This means a leased item may not be immediately available for delivery to a
different consumer at the exact moment one might expect. However, I believe that the efficiency
gained by this design outweighs any deficiencies incurred by the delayed handling of expired
leases.

#### Order Of Operation Integrity
The decision to force items which exceed the `leased_deadline` to be re-queued at the beginning of the queue
can have a negative impact on items which MUST be processed in a specific order.

Consider a queue with 3 items which are commands to be completed in order
- Item 1 - Command: Add `derrick@wippler.dev` to a mailing list
- Item 2 - Command: Email all recipients in the mailing list
- Item 3 - Command: Delete `derrick@wippler.dev` from the mailing list

If `Item 1` exceeds the `leased_deadline`, `Item 1` will be re-queued to run after `Item 3` completes,
which means the commands run out of order and is likely not the intended behavior. This behavior may appear 
to the user as if the FIFO integrity is compromised. In this scenario it may be acceptable that an item 
which fails to be processed as completed will block all other items from being processed until itself
is processed to completion. 

In the future, we may want to support marking leased items which exceed their `leased_deadline` without
placing them at the beginning of the queue. Thus preserving their place at the end of the queue. This is
currently not a priority as it is my opinion that such scenario can be avoided by having `Item 1` enqueue
`Item 2` only when `Item 1` is complete, thus avoiding this problem, and ensuring order of operation
is preserved while not blocking other items in the queue waiting to be processed.

I consider the Order of Operation problem distinct from the FIFO (First In, First Out) problem. At first glance,
it might seem that a FIFO solution addresses this issue, but due to the impossibility of achieving Exactly
One Delivery (EOD), any FIFO system will eventually violate the Order of Operation. Therefore, Querator is
not currently designed to solve the Order of Operation problem.