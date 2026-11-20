# 27. Dead-letter queues

Date: 2026-11-19

## Status

Accepted

## Context

A queue can name another queue as its dead-letter queue. When an item in `orders` expires or reaches
`max_attempts`, it moves to `orders-dead` ([22. Managing item lifecycles](0022-managing-item-lifecycles.md)).

Each queue has a request loop, the goroutine that writes to its partitions. The move runs on the
`orders` loop, and the `orders` loop waits while `orders-dead` stores the items. Any time two loops
wait on each other, or on a lock either one needs, both stop, for good unless one of them gives up.

## Decision

### How an item moves

1. The `orders` loop finds a dead item.
2. It hands the item to the `orders-dead` queue, which stores it through its own request loop like any
   other write. `orders` never writes to `orders-dead`'s partitions itself, so `orders-dead` keeps a
   single writer ([3. RW Sync Point](0003-rw-sync-point.md)).
3. `orders-dead` answers with how many items it stored.
4. `orders` deletes those items, and only those. An item is never lost between the two queues.

`orders` deletes them as a lifecycle action, not as a complete, because complete refuses an item
that is not leased, and an item can expire without ever being leased.

### A move is stored apart from client produces

A move can be bigger than one write to storage holds, so `orders-dead` may store it over several
writes. A client's produce goes to storage in one write, and its outcome goes back to that client
([28. Storage write failures](0028-storage-write-failures.md)). So `orders-dead` never puts a move
and a client produce in the same write. Splitting a move then never splits a client's produce, and
neither one's failure fails the other.

On BadgerDB an item and its source ID entry are written in the same transaction. An item stored
without its entry would be stored a second time by the next move.

### When a move fails partway

If `orders-dead` stored part of a move before failing, `orders` must delete that part at once and
keep the rest for the next run. Waiting is not safe. A consumer may complete a stored item before the
next run, and once it is completed its source ID no longer stops the next move from storing it
again. The code does not meet this yet when `orders` stops waiting at the write timeout. `orders`
then deletes nothing while `orders-dead` still stores the move (ENG-844).

### A slow dead-letter queue never stalls the source

`orders` waits on `orders-dead` for at most the write timeout. If `orders-dead` cannot take the request
at once, it answers that it is overloaded. Either way the items stay in `orders`, and the move is
tried again after a back-off. A stuck `orders-dead` costs `orders` at most one write timeout per run.

That bound holds only while nothing holds the queues manager's lock and waits on a request loop. A
loop that is moving dead items takes that lock to find the dead-letter queue, so a holder waiting on
that loop would wait forever. The queues manager must not hold its lock while it waits on a request
loop. The code does not meet this yet, because updating or deleting a queue holds the lock while it
waits (ENG-782).

### Rules for every dead-letter queue

| Rule | Why |
|---|---|
| A queue cannot be its own dead-letter queue. | The `orders` loop would wait on itself. |
| A dead-letter queue has no dead-letter queue of its own. | Items never move back, so a wait can never come back around. |
| A dead-letter queue has one partition. | A failed move is sent again, and each item carries its ID from `orders`. A partition skips an item whose source ID it already holds. That check is per partition, so a second partition would let the resent item land beside the first copy. |
| The rules are checked from both sides. | Saving `orders` or `orders-dead` is refused if it would break a rule for either one. `orders-dead` cannot be deleted while `orders` names it. |
| A dead-letter queue name is checked for format before it is looked up. | The lookup loads the queue from storage and can start its request loop. |

## Consequences

Checking from both sides means reading the stored queues to find any queue that names the one being
saved. Every request that looks up a queue, dead-letter moves included, waits until that read is done.

A dead-letter queue handles only as much traffic as one partition can.
