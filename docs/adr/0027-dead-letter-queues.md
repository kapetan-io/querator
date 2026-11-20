# 27. Dead-letter queues

Date: 2026-11-19

## Status

Accepted

## Context

A queue can name another queue as its dead-letter queue. When an item in `orders` expires or reaches
`max_attempts`, it moves to `orders-dead` ([22. Managing item lifecycles](0022-managing-item-lifecycles.md)).

That move must not lose the item, must not store it twice, and must not let a slow `orders-dead`
stall `orders`.

## Decision

**An item leaves `orders` only after `orders-dead` has stored it.** `orders` produces the items to
`orders-dead` and deletes each one once `orders-dead` has stored it. The rest stay in `orders`, and
the move is tried again.

**A retried move does not store an item twice.** `orders-dead` keeps each item's ID from `orders` and
skips an item it already holds. It stores an item and that ID together or not at all, because an
item stored without its ID would be stored again by the next try.

That skip stops working once a consumer completes the copy in `orders-dead`. So when a move stores
only some of its items, `orders` deletes those items at once instead of leaving them for the next
try. The code does not meet this yet when `orders` stops waiting at the write timeout, or on a
backend that cannot say how many items it stored (ENG-844).

**Dead items are written to storage apart from client produces.** A failed storage call fails every
request in it, and each client hears the outcome of its own produce ([28. Storage write
failures](0028-storage-write-failures.md)). Writing dead items in their own call means a failed move
never fails a client's produce, and a failed client produce never fails a move.

**A stuck `orders-dead` never stalls `orders`.** `orders` waits for at most the write timeout, then
keeps the item and tries again later. That holds only if nothing holds the queues manager's lock
while it waits on a queue, because a move needs that lock to find `orders-dead`. The code does not
meet this yet when a queue is updated or deleted (ENG-782).

**Every dead-letter queue follows these rules.**

| Rule | Why |
|---|---|
| A queue cannot be its own dead-letter queue. | `orders` would wait on itself. |
| A dead-letter queue has no dead-letter queue of its own. | Items never move back, so two queues can never wait on each other. |
| A dead-letter queue has one partition. | `orders-dead` can skip an item it already holds only when every item lands in the same partition. |
| The rules hold from both sides. | Saving either queue is refused if it would break a rule for the other, and `orders-dead` cannot be deleted while `orders` names it. |

## Consequences

Saving a queue reads the stored queues to find any that name it as a dead-letter queue. Every
request that looks up a queue, moves included, waits until that read is done.

A dead-letter queue handles only as much traffic as one partition can.
