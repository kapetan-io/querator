# 22. managing item lifecycles

Date: 2024-10-04

## Status

Accepted

## Context

The life cycle of items in the queue and scheduled items need to be managed by querator in an efficient
manner.

## Decision

Item life cycles are managed by the Logical Queue, on its request loop (see
[3. RW Sync Point](0003-rw-sync-point.md)). There is no separate life cycle Go routine. The life
cycle works like a two-phase garbage collector. In the first phase it reads a partition to identify
items that require action. In the second phase it writes those actions to the partition, or hands
items bound for a dead letter to the QueuesManager.

The life cycle must not hold the request loop for longer than one bounded storage call. Each life
cycle run reads one page of one partition and acts on what that page found. The Logical Queue
remembers a pivot, the last item the page read, and the next run continues after it. Between pages
the request loop handles any produce, lease, complete or retry requests that are waiting. A full
pass over a partition is therefore many short runs, not one long one.

Because the life cycle runs on the request loop, the Logical Queue remains the only reader and
writer of its partitions, and a partition store does not have to be safe for concurrent use.

### The Life Cycle
When a logical queue starts, it will initiate the life cycles of all the partitions it is responsible
for. Once started, the life cycle must immediately perform a phase one read of the partition's
storage, as there may be items that require action. If a life cycle fails to perform the read 
phase, the partition should be considered faulty and excluded from the logical queue's request
distribution until the life cycle can be restarted. This is to avoid unexpected behavior in a 
partition, such as handling leased items that have exceeded their deadline in a timely manner.
If the life cycle is unable to start, the partition should not allow new items to be produced on it.

NOTE: All life cycle writes to a partition are made by the Logical Queue that owns it. Items moved
to a dead letter are produced through the dead-letter queue's own request loop, and removed from
the source partition only after that produce is confirmed.

#### Life Cycle Responsibilities 
- Find all the leased items in the partition whose `lease_deadline` is at or before now, reset the
  item to un-leased state and clear the deadline. (`attempts` is incremented when the item is
  leased, not when the lease expires.)
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

If moving an item to the dead letter or writing an action fails, the life cycle runs again for that
partition with back-off. The partition is not marked faulty and stays in the request distribution,
since the fault may lie with the dead letter queue. A read that fails also backs off, and the
partition leaves the request distribution until a read succeeds. After a failed write or move,
the back-off is measured from the end of the run, because a move to the dead letter can block for
the whole write timeout. Every attempt count passed to the back-off is capped, because a large count
overflows the back-off's conversion to a duration and can return its shortest wait, which would put a
partition that keeps failing back to retrying at the fastest rate. A partition's failure count is capped
where it is passed in, not where it is kept, because queue stats report the count.

### Reading a partition
Each partition has its own life cycle timing, so a degraded partition backs off without delaying
the life cycle of healthy partitions on the same Logical Queue. The Logical Queue runs one page for
one partition at a time.

A pass ends when it reads the item that was last in the partition when the pass began. Items added
behind that point during the pass wait for the next pass, so a busy producer cannot keep a pass
running forever. The next pass starts at the earliest deadline seen during the pass, including the
deadline of any lease granted while the pass was running and of any item the pass moved to the
tail. When an immediate retry moves an item the pass has not read yet to the tail, the next pass
starts as soon as this one ends: the pass will not read that item, and a retry carries no expire
deadline to schedule the next pass from. A retry request that fails partway counts all its items,
because the items before the failure were moved. The cost is that a large partition under a steady
stream of immediate retries is read end to end without pause, one page at a time with requests
served between pages, rather than only when a deadline is due.
A failed move to the dead letter overrides both: the rest of the pass leaves dead items for the
next pass, and the next pass waits for the back-off, so a dead letter queue that hangs is called
once per pass, not once per page.

Optimizing access to the data store is the responsibility of individual data store
implementations. A data store that can find due items without reading the others, for instance
with an index on `lease_deadline` and `expire_deadline`, may return only those. A data store
that cannot must still examine no more than one page per call.

We considered running the life cycle read in a separate Go routine per partition so that it never
waits on the request loop. It required every data store to be safe for a reader concurrent with the
request loop's writes, and it needed a hand-off between the life cycle and the Logical Queue for
every write and every change in partition health. Once a read is one page, it is no larger than a
lease, and the separate Go routine buys nothing.

### Avoid Head of line blocking
When an item exceeds its `leased_deadline`, the naive solution might be to simply reset the
`leased_deadline` and mark the item as `is_leased=false`. However, this approach compromises
the First-In-First-Out (FIFO) nature of the queue. If leased items which have exceeded their 
leases are left at their original position in the queue, they will naturally take 
precedence over newer items waiting behind them, thus violating the FIFO nature of the
queue. This is commonly referred to as [Head-of-line (HOL) blocking](https://en.wikipedia.org/wiki/Head-of-line_blocking).

In order to avoid HOL, and maintain FIFO integrity, if an item fails to be completed before its 
`leased_deadline` is exceeded, it should be placed at the tail of the FIFO queue when it is 
returned to an un-leased status. This ensures that items already in the queue take precedence 
over such items and preserves FIFO behavior. 

Consider a failure scenario where thousands of leased items exceed their `leased_deadline` and are
returned to an un-leased status by the lifecycle. If these previously leased items are not placed
at the tail of the queue but instead remain at their original positions near the front, the flood 
of un-leased items could starve out newer items waiting further back in the queue such that their 
processing is delayed. This could also create a cycle where leased items never complete processing, 
while also saturating all consumers, denying other items in the queue the chance to be processed 
and completed. As a result, items that would normally be processed do not get the opportunity to 
be handled by consumers and thus item processing is delayed.

The same HOL blocking concern applies to client-initiated retry. When a consumer retries an item,
it should be assigned a new ID (per [ADR 0004](0004-item-id-is-not-immutable.md)) and placed at
the tail of the queue. If the retried item were left at its original position, it would take
precedence over newer items waiting behind it — the same HOL blocking problem described above.

## Consequences
A life cycle run holds the request loop for one page read and the writes that page found, plus a
dead-letter hand-off when the page found items bound for a dead letter. Requests for that Logical
Queue wait for that long, as they do for any other storage call.

On a data store that must examine every item, the time to notice an expired lease grows with the
size of the partition, because the pass has to come round to the item. An expired lease may be
given back up to one pass later than its deadline. This means a leased item may not be available
for delivery to a different consumer at the exact moment one might expect. I believe this is an
acceptable cost for keeping a single reader and writer per partition.

The pivot is held in memory. After a restart or a reload the pass begins again at the first item.
This is safe because every action is found again.

#### Order Of Operation Integrity
The decision to force items which exceed the `leased_deadline` to be re-queued at the tail of the queue
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
placing them at the tail of the queue. Thus preserving their original position in the queue. This is
currently not a priority as it is my opinion that such scenario can be avoided by having `Item 1` enqueue
`Item 2` only when `Item 1` is complete, thus avoiding this problem, and ensuring order of operation
is preserved while not blocking other items in the queue waiting to be processed.

I consider the Order of Operation problem distinct from the FIFO (First In, First Out) problem. At first glance,
it might seem that a FIFO solution addresses this issue, but due to the impossibility of achieving Exactly
One Delivery (EOD), any FIFO system will eventually violate the Order of Operation. Therefore, Querator is
not designed to solve the Order of Operation problem.
