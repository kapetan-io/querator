# 27. Dead-letter queues

Date: 2026-11-19

## Status

Accepted

## Context

A queue can name another queue as its dead-letter queue. When an item in `orders` expires or reaches
`max_attempts`, it moves to `orders-dead` ([22. Managing item lifecycles](0022-managing-item-lifecycles.md)).

The move runs while `orders` handles its own requests, and `orders` waits while `orders-dead` stores
the items. Two queues that wait on each other, or on a lock either one needs, can stop for good.

## Decision

**`orders-dead` stores the items itself.** `orders` hands the items over and never writes to
`orders-dead`'s partitions, so `orders-dead` keeps a single writer ([3. RW Sync
Point](0003-rw-sync-point.md)). `orders` deletes only the items `orders-dead` says it stored, so an
item is never lost between the two queues.

**A move is never stored together with a client's produce.** A move can be too big for one write to
storage, while a client's produce is stored in one write and its outcome goes back to that client
([28. Storage write failures](0028-storage-write-failures.md)). Keeping them apart means splitting a
move never splits a produce, and neither one's failure fails the other.

**A move that fails partway deletes the stored part from `orders` at once.** A failed move is sent
again, and `orders-dead` skips an item it already stored. It stores each item and the record of its
source ID together or not at all, because an item stored without that record is stored again by the
next move. The skip stops working once a consumer completes the stored copy, so waiting for the next
run can store the item twice. The code does not
meet this yet when `orders` stops waiting at the write timeout (ENG-844).

**A slow `orders-dead` never stalls `orders`.** `orders` waits a bounded time, keeps the items when the
wait runs out, and tries again later. The bound holds only if nothing holds the queues manager's lock
while it waits on a queue, because moving dead items needs that lock to find the dead-letter queue.
The code does not meet this yet when a queue is updated or deleted (ENG-782).

**Every dead-letter queue follows these rules.**

| Rule | Why |
|---|---|
| A queue cannot be its own dead-letter queue. | `orders` would wait on itself. |
| A dead-letter queue has no dead-letter queue of its own. | Items never move back, so a wait can never come back around. |
| A dead-letter queue has one partition. | `orders-dead` can skip an item it already stored only when every item lands in the same partition. |
| The rules hold from both sides. | Saving either queue is refused if it would break a rule for the other, and `orders-dead` cannot be deleted while `orders` names it. |

## Consequences

Saving a queue reads the stored queues to find any that name it as a dead-letter queue. Every
request that looks up a queue, dead-letter moves included, waits until that read is done.

A dead-letter queue handles only as much traffic as one partition can.
