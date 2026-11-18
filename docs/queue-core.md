# Queue Core

This page records why the code in `internal/` (the files directly in the package, not its
sub-packages) works the way it does. The code says what it does. The reasons below are the parts it
cannot say. Terms follow [CONTEXT.md](CONTEXT.md). Decisions already recorded as ADRs are linked,
not repeated.

## The Logical Queue is the only writer to its partitions

A Logical Queue is the read/write synchronization point for the partitions assigned to it
([ADR-0003](adr/0003-rw-sync-point.md)). Only one instance of a given Logical Queue may run anywhere
in the cluster at a time, and every produce, lease, complete and retry for its partitions goes
through that one instance's `requestLoop` goroutine.

Moving an item to a dead-letter queue is no exception. A source queue does not write to the
dead-letter queue's partitions. It hands the items to the dead-letter queue's own Logical Queue
through `ProduceDeadLetter`, which enters that queue's `requestLoop` exactly like a client produce
(`MethodProduceInternal`). No goroutine other than a Logical Queue's `requestLoop` writes to its
partitions. The design is in [ENG-55
dlq-single-writer](features/ENG-55-dlq-single-writer/blueprint.md).

`QueuesManager` runs one Logical Queue per queue. Running several per queue, as
[ADR-0016](adr/0016-queue-partitions.md) describes, is deferred until there is a demonstrated need.

## Hot and cold requests in the request loop

`requestLoop` sorts each request into one of two kinds.

* **Hot requests** are produce, internal produce, lease, complete and retry. These are the queue's
  throughput path, so they are batched. When a hot request arrives, `handleHotRequests` pulls in
  every hot request already waiting in `requestCh` (`consumeHotCh`), assigns the whole batch to
  partitions (`assignToPartitions`), writes each partition's share in one call per operation
  (`applyToPartitions`), then answers every request that is done (`finalizeRequests`). Lease
  requests with nothing to lease stay in the loop's state for the next pass.
* **Cold requests** are storage list, import and delete, stats, pause, clear, update info, update
  partitions and reload. They are rare and are handled one at a time by `handleColdRequests`.

Batching is the point of the single-goroutine design ([ADR-0003](adr/0003-rw-sync-point.md)): one
wake-up of the loop does as much storage work as it can in as few transactions as it can.

The batch stops at the first cold request it pulls from `requestCh`. `handleHotRequests` finishes
the hot batch and returns that cold request, and the loop handles it next. Stopping there keeps
requests in arrival order, so a pause, a clear or a queue info update is never applied ahead of a
hot request that was sent before it, and never behind one sent after it. The exception is a paused
queue: hot requests are held while cold requests still run, so a cold request sent during a pause is
applied ahead of the hot requests held before it.

`consumeHotCh` reads at most `len(requestCh)` requests. Only the `requestLoop` goroutine receives
from `requestCh`, so the channel holds at least that many, and the bound makes the loop finite while
still taking everything that was waiting.

`handleHotRequests` called with no request (`nil`) pulls nothing from `requestCh`. It only processes
the requests already held in the loop's state. Un-pausing a queue uses this to apply the requests
held during the pause.

The single-writer rule above also means `applyToPartitions` is the one place where interaction with
the data store can be optimised (for example, writing to partitions in parallel). It calls each
partition in turn.

## Pause

Pause exists only for tests ([ADR-0013](adr/0013-pausing-a-queue.md)). While a queue is paused,
`requestLoop` holds hot requests in its state without processing them, and stops the lifecycle,
scheduled and request-timeout timers. It does this by selecting on `nil` channels in place of the
timers, because a `nil` channel never fires. Cancelling the pause applies the held requests.

## Shutdown

`Logical.inFlight` counts callers that have entered `Produce`, `Lease`, `Complete`, `Retry` or
`ProduceDeadLetter` and are waiting for an answer. Shutdown uses it to answer every one of them.

`handleShutdown` answers the requests held in the loop's state, then `rejectInFlight` answers what
is still in `requestCh` until `inFlight` reaches zero. `rejectInFlight` keeps checking even when
`requestCh` is empty, because a caller counted in `inFlight` may not have sent its request yet.

Produce, complete and retry requests are held in state while the queue is paused, and produce
requests are also held while every partition has failed. Shutdown must answer them too, or
`inFlight` never reaches zero and shutdown waits until its context ends.

`QueuesManager.Shutdown` copies its list of queues under the mutex, then releases the mutex before
it waits for any Logical Queue to stop. Holding the mutex while waiting would deadlock: a
`requestLoop` in the middle of `runLifecycle` can call `ProduceToQueue` to move dead items, and
`ProduceToQueue` takes the same mutex.

For the same reason `ProduceToQueue` holds the mutex only while it looks up the dead-letter queue,
and releases it before it waits on the dead-letter queue's loop.

`QueuesManager.Delete` holds the mutex while it shuts a queue's Logical Queue down, so it can hit
this deadlock (ENG-782).

## Partition distribution

Produce uses opportunistic distribution ([ADR-0019](adr/0019-partition-item-distribution.md)).
`sortPartitionsByLoad` orders partitions by failures, then by unleased item count, both ascending,
and the loop re-sorts after every assignment.

* A produce request goes to the first partition in that order, the healthy one with the fewest
  unleased items. Failed partitions sort last, so if the first partition has failed, every partition
  has failed, and the produce request is left unassigned.
* A lease request goes to the last healthy partition in that order (`lastHealthyPartition`), the one
  with the most unleased items. The lease records that partition's number, and the client sends it
  back with complete and retry.
* Complete and retry requests name their partition, so they cannot be moved to another one. When
  that partition is missing or failing, or its storage call fails, the client gets a retry error.

`QueueState.Partitions` is re-sorted continuously, so code that needs a partition by number looks it
up with `GetPartition` instead of indexing the slice. `handleStats` does this.

## Lifecycle and scheduled timers

Each partition tracks when it next needs a lifecycle run (`NextLifecycleRun`, for expired leases and
expired items) and a scheduled run (`NextScheduledRun`, for scheduled items and retries). The loop's
two timers fire at the earliest of these across all partitions.

Writing to a partition can bring a run forward. A lease sets a lease deadline, a produce sets item
expire deadlines and may add scheduled items, and a retry may schedule a retry time
(`applyToPartitions` with `notifyExpired`, `notifyScheduled` and `notifyScheduledRetry`). When any
of these is earlier than the partition's next run, the next run moves forward and the timer is
reset, so expiry is noticed on time without scanning storage on every pass. `runLifecycle` and
`runScheduled` run on the `requestLoop` goroutine, not a goroutine of their own.

## Dead-letter moves

Items reach a dead-letter queue when they expire or reach `MaxAttempts`.

* `ProduceDeadLetter` skips the client produce checks on request timeout and batch size, because
  `QueuesManager.LifeCycle` builds the items before handing them over. An empty batch returns `nil`
  instead of the error a client produce gets. The dead-letter queue's loop assigns each item a new
  `ExpireDeadline`, replacing the source queue's value.
* The hand-off waits for the dead-letter queue's loop to confirm the produce.
  `QueuesManager.LifeCycle` deletes the source items only after that confirmation
  (delete-after-confirm), so an item is never lost between the two queues.
* When the dead-letter queue's `requestCh` is full, `ProduceDeadLetter` returns at once with an
  overloaded error instead of waiting. A saturated dead-letter queue never stalls the source queue's
  loop. The source item stays where it is and a later lifecycle run finds it again. When no other
  deadline on the partition brings that run forward, the retry can wait indefinitely (ENG-779).
* `ProduceDeadLetter` stops waiting when its context ends, and the lifecycle bounds that context
  with `WriteTimeout`, so a stalled dead-letter queue cannot block the source loop indefinitely.
* Before the move, each item's `SourceID` is set to its original ID, its `ID` is cleared so the
  dead-letter queue's storage assigns a new one, and its lease state and attempts are reset. The
  original IDs are collected before `ID` is cleared, because deleting the items from the source
  partition after the move needs them.
* If deleting the source items fails after a confirmed move, `LifeCycle` logs the error and returns
  success. The items are already in the dead-letter queue. A later lifecycle run finds the source
  items again and the dead-letter queue drops the duplicate by `SourceID`.

`validateDeadQueue` checks the name's format before it looks the queue up, because the lookup
(`QueuesManager.get`) loads the queue from storage and can start a Logical Queue for it. Besides the
name and that the queue exists, it enforces these rules on the queue named as a dead-letter queue.

* A queue cannot be its own dead-letter queue, and the queue named as a dead-letter queue must not
  have a dead-letter queue of its own. These rules exist so the source queue's loop and the
  dead-letter queue's loop are always different goroutines and the dead-letter queue never
  dead-letters back, which keeps the source loop's wait on the dead-letter queue's loop from
  deadlocking. Updates can bypass the rules (ENG-778). The wait is bounded by `WriteTimeout` either
  way.
* A dead-letter queue must have a single partition (ENG-57). `SourceID` dedup is per partition. With
  more than one partition, a move that failed and was retried could land on a different partition
  than the first attempt and slip past that partition's `SourceID` index, duplicating the item. One
  partition makes the dedup a guarantee.

## Queue creation and loading

`QueuesManager.Create` decides where partitions go from the storage backends' affinities
(`store.PlacePartitions`). A caller supplies only the number of partitions. Any partition details it
sends are ignored.

`NewQueuesManager` rejects two partition storage backends with the same name, because `store.Find`
returns the first backend with a matching name, and a repeated name would open some partitions on
the wrong backend.

A queue stored with no partitions has nothing to run, so `get` registers it without a Logical Queue.
Produce and lease report that there are no available partitions, complete and retry reject every
partition number, and the queue can still be inspected and deleted.

## Clearing a queue

`handleClear` clears each partition in storage. The Logical Queue holds no cached items. If it ever
caches items in memory, clearing a queue must flush that cache too.

## Authentication and authorization

`AuthBackend.HasPermission` follows the cascade in [ADR-0025](adr/0025-multi-tenancy-and-auth.md),
with one addition. ADR-0025 says a key scoped to one namespace can never reach another. The code
lets a scoped key act on the `_system` namespace (`auth.SystemNamespace`) itself, using only the
permissions its user holds through `_system` role bindings. For any other namespace outside its
scope, a scoped key is denied.

A permission check reads at most `maxRoleBindingsPerCheck` (100) role bindings for each namespace it
checks (the target, then `_system`), so a user with many bindings cannot cause an unbounded storage
scan. The cost that remains is in [known-issues.md](known-issues.md).

`AuthCache` caches the principal for each API key for a TTL (default five minutes) so that most
requests authenticate without a storage read. `AuthCache.Authenticate` collapses concurrent lookups
of the same key into one storage lookup (`singleflight`). That lookup runs on its own context with a
10 second timeout instead of the first caller's context, so one caller cancelling does not fail
every caller sharing the lookup.

A storage error while looking up a key or its user is logged and returned to the caller as an
invalid key (HTTP 401) instead of a server error.

A cache miss also records the key's last-used time. That must not slow authentication, so the lookup
sends it to a buffered channel and drops the update when the buffer is full. `lastUsedLoop` writes
the updates to storage, and on `Close` it writes every update still buffered before it exits.

## Debugging

This helper prints the partition order. It is kept here because it is useful when debugging
distribution and is not part of the code.

```go
func (l *Logical) dumpPartitionOrder(state *QueueState) string {
	out := strings.Builder{}
	out.WriteString("Partition Order:\n")
	for _, req := range state.Partitions {
		fmt.Fprintf(&out, "  %02d - UnLeased: %d NumLeased: %d Failures: %d\n",
			req.Info.PartitionNum, req.State.UnLeased, req.State.NumLeased,
			req.State.Failures)
	}
	return out.String()
}
```
