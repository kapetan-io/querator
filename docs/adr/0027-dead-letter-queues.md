# 27. Dead-letter queues

Date: 2026-11-19

## Status

Accepted

## Context

A queue may name another queue as its dead-letter queue. Items that expire or reach `max_attempts` move
there ([22. Managing item lifecycles](0022-managing-item-lifecycles.md)). The move runs on the source
queue's request loop, which waits on the dead-letter queue's own request loop to store the items. Two
request loops that wait on each other, or on a lock either of them needs, deadlock.

## Decision

A source queue never writes to the dead-letter queue's partitions. It hands the items to the dead-letter
queue's Logical Queue, which stores them through its request loop, so the dead-letter queue keeps a
single writer ([3. RW Sync Point](0003-rw-sync-point.md)). The source items are deleted only after the
dead-letter queue confirms what it stored, so an item is never lost between the two queues.

A move can be larger than one write to storage holds, so the dead-letter queue may store a move over
several writes. A client produce is sent to storage in one write, and its outcome is reported to its
client ([28. Storage write failures](0028-storage-write-failures.md)). The dead-letter queue therefore
never stores a move in the same write as a client produce, so splitting a move never splits a client
produce, and each is stored whatever the other's outcome. On BadgerDB an item and its source ID entry are
written in the same transaction, because an item stored without its entry would be stored again by the
next move.

When a move fails after part of it was stored, the source must delete the stored part at once and keep
the rest for the next run. Waiting would not be safe: a consumer may complete a stored item before the
move runs again, and a completed item's source ID no longer stops it being stored a second time. They are
deleted as a lifecycle action rather than through complete, because complete refuses an item that is not
leased and an item can expire without ever being leased. The code does not meet this yet when the source
stops waiting at the write timeout: the source deletes nothing while the dead-letter queue still stores
the move.

The source queue's wait on the dead-letter queue's request loop is bounded by the write timeout, and a
dead-letter queue that cannot take the request at once answers that it is overloaded. A stalled or
saturated dead-letter queue therefore costs the source queue at most one write timeout per run and a
back-off, as long as nothing holds the queues manager's lock while it waits on a Logical Queue (below).
The item stays in the source queue and the move is tried again.

The queues manager must not hold its lock while it waits on a Logical Queue. A request loop that is
moving dead items takes that lock to find the dead-letter queue, so a holder waiting on that loop
deadlocks with it. The code does not meet this yet: updating or deleting a queue holds the lock while it
waits (ENG-782).

These rules hold for every dead-letter queue:

* A queue cannot be its own dead-letter queue, and a dead-letter queue has no dead-letter queue of its
  own. The source loop and the dead-letter loop are then always different goroutines, and items never
  move back, so the source loop's wait cannot come back around to itself.
* A dead-letter queue has a single partition. A move that fails after the dead-letter queue stored it is
  sent again. Each item carries the ID it had in the source queue, and a partition skips an item whose
  source ID it already holds. That check is per partition, so a second partition would let the resent
  move land beside the first copy.
* The rules are checked from both sides. A create or update is refused when it would break them for the
  queue being saved or for any queue that already names it, and a queue cannot be deleted while another
  queue names it as its dead-letter queue.
* A dead-letter queue name is checked for format before it is looked up, because the lookup loads the
  queue from storage and can start a Logical Queue for it.

## Consequences

Checking from both sides reads the stored queues to find one that names the queue being saved. Every
request that looks up a queue, dead-letter moves included, waits for as long as that read takes.

A dead-letter queue is limited to the throughput of one partition.
