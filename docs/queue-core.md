# Queue Core

> **No implementation detail in this file.** This page holds only what the code cannot say: purpose,
> domain terms, why a choice was made, the rules that must stay true, and traps a reader would not
> see from one function. If reading the code would tell you this, it does not belong here. Step-by-step flows,
> lists of functions, fields or methods, timings, constants and per-backend mechanics stay in the
> code. A reason with no place on this page goes in an ADR under `docs/adr/` or in the package
> comment (`doc.go`).

This page records why the code in `internal/` (the files directly in the package, not its
sub-packages) works the way it does. Terms follow [CONTEXT.md](CONTEXT.md). Decisions already
recorded as ADRs are linked, not repeated.

## The Logical Queue is the only writer to its partitions

A Logical Queue is the read/write synchronization point for the partitions assigned to it
([ADR-0003](adr/0003-rw-sync-point.md)). Only one instance of a given Logical Queue may run anywhere
in the cluster at a time, and every produce, lease, complete and retry for its partitions goes
through that one instance's request loop.

Moving an item to a dead-letter queue is no exception. A source queue does not write to the
dead-letter queue's partitions. It hands the items to the dead-letter queue's own Logical Queue,
which takes them through its request loop exactly like a client produce. The design is in [ENG-55
dlq-single-writer](features/ENG-55-dlq-single-writer/blueprint.md).

Because there is one writer, the request loop is the one place where interaction with the data
store can be optimised, for example by writing to partitions in parallel.

`QueuesManager` runs one Logical Queue per queue. Running several per queue, as
[ADR-0016](adr/0016-queue-partitions.md) describes, is deferred until there is a demonstrated need.

## Hot and cold requests in the request loop

Hot requests are produce, lease, complete, retry and a dead-letter hand-off. They are the queue's
throughput path, so the loop batches them: one
wake-up does as much storage work as it can in as few transactions as it can
([ADR-0003](adr/0003-rw-sync-point.md)). Every other request is cold (pause, clear, stats, storage
inspection, queue info updates and the like). Cold requests are rare and are handled one at a time.

A batch stops at the first cold request. Stopping there keeps requests in arrival order, so a
pause, a clear or a queue info update is never applied ahead of a hot request that was sent before
it, and never behind one sent after it. The exception is a paused queue: hot requests are held while
cold requests still run, so a cold request sent during a pause is applied ahead of the hot requests
held before it.

## Pause

Pause exists only for tests ([ADR-0013](adr/0013-pausing-a-queue.md)). A paused queue holds hot
requests and stops its timers, and cancelling the pause applies what it held.

A paused queue drops each held dead-letter hand-off whose caller has stopped waiting. It checks
each time it holds another hot request, so the newest abandoned hand-off stays held until the next
check, and is applied when the pause is cancelled. The source queue sends a new hand-off each time
its lifecycle runs again with back-off, so without the drop a paused dead-letter queue would hold
one more copy every back-off interval for as long as the pause lasts. A held client produce is kept
even after its caller gives up, and is applied when the pause is cancelled.

## Shutdown

Shutdown must answer every caller, including requests the loop is holding because
the queue is paused or every partition has failed. A caller left unanswered keeps shutdown waiting
until its context ends. A caller can be counted as waiting before its request reaches the loop, so
shutdown waits for its count of waiting callers to reach zero, not for the request channel to
empty.

`QueuesManager.Shutdown` must not hold its mutex while it waits for a Logical Queue to stop. Holding
it would deadlock: a request loop in the middle of a lifecycle run can move dead items to a
dead-letter queue, and that path takes the same mutex. For the same reason the dead-letter path
holds the mutex only while it looks up the dead-letter queue, and releases it before it waits on
the dead-letter queue's loop.

`QueuesManager.Delete` holds the mutex while it shuts a queue's Logical Queue down, so it can hit
this deadlock (ENG-782).

## Partition distribution

Produce uses opportunistic distribution ([ADR-0019](adr/0019-partition-item-distribution.md)). A
produce request is meant to go to the healthy partition with the fewest unleased items, and a lease
request to the healthy partition with the most. A failed partition takes no new produce or lease
requests, and when every partition has failed a produce request waits unassigned.

Lease requests assigned in the same pass must spread by load, so each assignment counts its items
as leased before storage has leased them. That count is only an estimate. A failed lease write
would leave items nobody leased counted as leased, and a lease request that keeps waiting would
count them again on every later pass. So once storage answers, the part of the estimate it did not
lease is given back, after a successful write as well as a failed one.

When a write's outcome is unknown, the whole estimate is given back although storage may show some
of those items as leased, so the un-leased count overstates until the partition is next recounted
from storage. A failed produce in the same pass takes all its items out of the un-leased count,
even those the lease estimate already counted as leased. The estimate gives those back when
storage answers the lease, so the produce must not give them back as well.

A running queue does not write items for a client that stopped waiting
([ADR-0009](adr/0009-client-timeouts.md)). An unassigned client produce whose request timeout has
passed is answered with a retry error and dropped. One whose client has gone is dropped too.

Every pass that assigns produces must expire them first, including produces it has just taken
from the request channel, because a produce can sit there past its deadline while the loop is
busy. A pass that skips this writes items for a client that has already given up. The one
deliberate exception is the pass that cancels a pause, which applies what the pause held, as
described under Pause.

A dead-letter hand-off has no request timeout and its deadline is zero, so the deadline check must
skip it or it would expire at once.

Complete and retry requests name the partition the lease came from, so they cannot be moved to
another one. When that partition is missing or failing, the client gets a retry error.

The queue state's list of partitions (`QueueState.Partitions`) is re-ordered by load as requests
are assigned, so a position in it is not a partition number. Look a partition up by number with
`GetPartition`.

## Lifecycle and scheduled runs

The Lifecycle (see [CONTEXT.md](CONTEXT.md)) is two timed runs per partition, both on the request
loop. A lifecycle run handles expired leases, expired items and items at `MaxAttempts`. A scheduled
run enqueues scheduled items and retries whose time has come.

Writing to a partition can bring its next lifecycle or scheduled run forward: a lease sets a lease
deadline, a produce sets expire deadlines and may schedule items, and a retry may schedule a retry
time. The run moves forward when the write's time is earlier, so expiry is noticed on time without
scanning storage on every pass.

A lifecycle run that leaves due work undone runs again with back-off. That covers a dead-letter move
or a lifecycle action (`TakeAction`) that failed, and a deadline still at or before now after the
run. A partition reports only its earliest lease and expire deadlines, and the next run is set from
deadlines in the future. An item stuck in a failed move keeps the earliest deadline in the past, so
without counting it as undone work the lifecycle would not run again until a write brought it
forward, and every later deadline would wait for that write. The back-off is measured from the end
of the run, because a dead-letter move can block for `WriteTimeout`.

* An unfinished run leaves the partition in request distribution, because the fault may lie with
  the dead-letter queue and the partition can still serve requests. A failed scan or storage read
  takes the partition out of distribution until it recovers.
* Every attempt count passed to the back-off must be capped. After enough attempts the back-off
  overflows its conversion to a `Duration`. On amd64 the result is negative and the back-off returns
  its minimum, so an uncapped partition that kept failing would go back to retrying every 500ms.
  For the partition's `Failures` count, cap the value passed in, not the counter, because queue
  stats report the counter.
* A successful stats read during recovery resets `Failures`, so a scan or deadline read that keeps
  failing behind a working stats read never climbs past the first back-off step.
* A dead-letter queue that hangs blocks the source queue's loop for up to `WriteTimeout` once per
  back-off interval, and each retry rescans the source partition on the request loop while produce,
  lease and complete wait. This cost has not been measured.

Storage, not the writes that brought a run forward, decides when the next scheduled run happens.
Those writes are only the ones made since the queue loaded, so a scheduled item written before a
restart would be forgotten otherwise. For the same reason the queue runs one scheduled pass while it
loads, so items already overdue at startup are enqueued. Scheduled runs count their failures apart
from the lifecycle's, so neither run resets or raises the other's back-off, and a failed scheduled
run leaves the partition in request distribution.

After each lifecycle and scheduled run, lease requests that are waiting for items are tried again,
so items the run just enqueued or gave back reach those consumers without waiting for the next
client request. A waiting request is tried against one partition only, so on a queue with several
partitions it can miss an item that came due in another one (ENG-792).

## Dead-letter moves

Items reach a dead-letter queue when they expire or reach `MaxAttempts`.

* The source items are deleted only after the dead-letter queue's loop confirms the produce
  (delete-after-confirm), so an item is not lost between the two queues.
* A dead-letter queue whose request channel is full answers at once with an overloaded error
  instead of making the source queue wait. A saturated dead-letter queue never stalls the source
  queue's loop. The source item stays where it is and the lifecycle runs again with back-off.
* The wait for the dead-letter queue is bounded by `WriteTimeout`, so a stalled dead-letter queue
  cannot block the source loop indefinitely.
* The source items are deleted as a lifecycle action, not through `Complete`. `Complete` refuses an
  item that is not leased, and an item can expire without ever being leased.
* The move sets each item's `SourceID` to its ID in the source queue and clears its ID. It changes
  copies, because the delete from the source partition needs the original IDs. When deleting the
  source items fails after a confirmed move, the next run finds them again and the dead-letter queue
  drops the duplicate by that `SourceID`.

A dead-letter queue name is checked for format before the queue is looked up, because the lookup
loads the queue from storage and can start a Logical Queue for it. These rules govern a dead-letter
queue.

* A queue cannot be its own dead-letter queue, and the queue named as a dead-letter queue must not
  have a dead-letter queue of its own. These rules exist so the source queue's loop and the
  dead-letter queue's loop are always different goroutines and the dead-letter queue never
  dead-letters back, which keeps the source loop's wait on the dead-letter queue's loop from
  deadlocking. The wait is bounded by `WriteTimeout` either way.
* A dead-letter queue must have a single partition (ENG-57). `SourceID` dedup is per partition. With
  more than one partition, a move that failed and was retried could land on a different partition
  than the first attempt and slip past that partition's `SourceID` index, duplicating the item. One
  partition makes the dedup a guarantee.

The rules are checked for both queues in a pair. Checking only the queue being named lets a later
update break them: A names B, then B is updated to name C, giving the chain A→B→C (ENG-778). So a
create or update is also refused when another queue already names the queue being saved and the save
would give it a dead-letter queue or more than one partition. A queue cannot be deleted while another
queue names it, since the source queue's moves would then fail on every run.

* Finding the queues that name a queue reads up to every stored queue under the `QueuesManager` lock, so
  each create, update and delete that triggers it blocks queue loads, dead-letter moves included, for
  as long as that read takes.
* Chains that formed before ENG-778 stay in storage. An update replaces `dead_queue`, so an update
  that re-sends a link of such a chain is refused. Each update to a queue in the chain must clear its
  `dead_queue` or point it at a queue the rules allow, which breaks the chain.

## Queue creation and loading

`QueuesManager` decides where partitions go from the storage backends' affinities. A caller supplies
only the number of partitions, and any partition details it sends are ignored.

Two partition storage backends cannot share a name, because backends are found by name and a
repeated name would open some partitions on the wrong backend.

A queue stored with no partitions has nothing to run, so it has no Logical Queue. It refuses
produce, lease, complete and retry, and can still be inspected and deleted.

## Clearing a queue

The Logical Queue holds no cached items, so clearing a queue clears storage only. If it ever caches
items in memory, clearing a queue must flush that cache too.

## Authentication and authorization

Permission checks follow the cascade in [ADR-0025](adr/0025-multi-tenancy-and-auth.md), with one
addition. ADR-0025 says a key scoped to one namespace can never reach another. The code lets a
scoped key act on the `_system` namespace itself, using only the permissions its user holds through
`_system` role bindings. For any other namespace outside its scope, a scoped key is denied.

A permission check reads a bounded number of role bindings for each namespace it checks, so a user
with many bindings cannot cause an unbounded storage scan. The cost that remains is in
[known-issues.md](known-issues.md).

Concurrent lookups of the same API key share one storage lookup. That lookup runs on its own
context, not the first caller's, so one caller cancelling does not fail every caller sharing it.

Recording a key's last-used time must not slow authentication, so an update is dropped when the
updates waiting to be written fill their buffer.
