# Storage Backend Internals

> **No implementation detail in this file.** This page holds only what the code cannot say: purpose,
> domain terms, why a choice was made, the rules that must stay true, and traps a reader would not
> see from one function. If reading the code would tell you this, it does not belong here. Step-by-step flows,
> lists of functions, fields or methods, timings, constants and per-backend mechanics stay in the
> code. A reason with no place on this page goes in an ADR under `docs/adr/` or in the package
> comment (`doc.go`).

This page is for people changing or adding a storage backend in `internal/store`. It records the
rules every backend must keep and why some backends are built the way they are. For how to
configure and run each backend, see the [backend overview](README.md). Decisions that have their
own ADR are linked, not repeated.

## The Partition contract

A `Partition` holds the items of one partition of one queue. Each backend implements it.

**One goroutine drives a partition.** Only the logical queue that owns the partition calls the item
methods, one call at a time ([ADR 0003](../adr/0003-rw-sync-point.md)). A `Partition` is therefore
not safe for concurrent use, and backends lean on this. MongoDB deduplicates `SourceID` with a read
followed by an insert, and Badger advances its item ID generator without a lock. Code that calls the
item methods from a second goroutine breaks both.

**`Info` and `UpdateQueueInfo` must be safe for concurrent use.** `UpdateQueueInfo` applies queue
settings a user changed, which must take effect at once. Every caller runs on the logical queue's
own request loop now, and the rule keeps them safe if a caller moves off it.

**Storage is contacted lazily.** Handing out a `Partition` never touches storage or fails. The first
call that needs the data creates tables, collections or connections
([ADR 0021](../adr/0021-storage-lazy-initialization.md)). The metadata stores follow the same rule.

**An error from `Complete` or `Retry` fails the whole batch.** The logical queue marks every request
in the batch for retry, and the client retries it. Each request touches one partition
([ADR 0020](../adr/0020-api-semantics.md)). A backend without transactions may already have removed
some of the items, so the retried request reports those as "does not exist".

**An error from `Produce` fails the whole batch.** The logical queue answers every request in the
batch with a retry error and counts none of their items, so a backend returns an error only when it
cannot say the batch was stored.

**A problem with one request goes on that request.** An invalid, missing, scheduled or not-leased
item ID is recorded on its request, and the method still returns nil. The returned error is only for
a storage failure.

**A lease spreads items across every waiting request.** Available items are shared evenly among the
lease requests in the batch, so one consumer does not take them all.

**A failed `Lease` hands out nothing.** Items are placed on their requests before the write commits,
so a backend whose write fails must take them back off before it returns the error. Otherwise a
consumer receives an item whose lease was not saved. Badger keeps this rule. PostgreSQL and MongoDB
break it (ENG-815).

**Scheduled items have their own scan.** Scheduled items run on their own timer, and a separate scan
lets the logical queue check them without walking every item for lease and expiry work.

**Removing an item frees its `SourceID`.** A produce or import skips an item whose `SourceID` is
already in the partition, so every way of removing an item (complete, delete, clear, a lifecycle
delete) must free its `SourceID` again.

**An item enqueued from the schedule stops counting as scheduled.** The earliest scheduled time a
partition reports includes items already due. If an enqueued item kept counting, the logical queue
would see a due item that was never enqueued and retry the scheduled run with back-off.

## The Queues contract

**A missing queue is `ErrQueueNotExist`.** The `QueuesManager` checks for that error when it reads a
queue from the `Queues` store. Deleting a missing queue succeeds.

## Rules every backend follows

**An `EnqueueAt` less than 100ms away is not scheduled.** A produced item whose `EnqueueAt` is in the
past or within 100ms of now is an ordinary item, and its `EnqueueAt` is cleared. Clients often fill
in every field of a produce request and set `EnqueueAt` to "now", which clock drift between machines
puts a few milliseconds in the future. Scheduling such an item costs a write, a scan and a move for
no benefit. A retry uses the same rule for `RetryAt`. An import restores items as they were, so an
imported item is exempt and keeps the `EnqueueAt` it was given.

**An unleased item records no lease deadline.** Expiry scans select leased items by the leased flag,
never by comparing the deadline against a sentinel value.

**A deadline equal to now is due.** The lifecycle scan yields an action when a deadline is at or
before the time it is given, never only after it. The logical queue treats a deadline at or before
now as work left undone, so a deadline equal to now that the scan skipped would cost a back-off
before the item is acted on. The same holds for a scheduled item's `EnqueueAt`.

**The lifecycle scan removes a leased item at `MaxAttempts`.** A leased item whose lease has not
expired but whose attempts reached the queue's `MaxAttempts` goes to the dead-letter queue, or is
deleted when the queue has none. Because a lease counts the attempt when it hands the item out, this
also matches an item a consumer holds on its final allowed lease. ENG-776 tracks whether that is
intended.

**Lifecycle actions may carry less than the whole item.** A backend that can move a scheduled item
into the queue by its ID alone does not need the rest of the item in the action, and may either
insert the item it is given or update the stored item in place, whichever the data store does best.

**An item reads back exactly as it was returned.** The functional suite compares an item returned
from an import with the same item read back through list, so a backend whose database keeps less
precision than Go's `time.Time` (PostgreSQL keeps microseconds) must return times at the precision it
stores. The exception is the lease deadline on items a lease hands out, which keeps full precision
while the stored copy is truncated. The
[MongoDB tech spec](../features/ENG-48-mongodb-backend/tech-spec.md) has the full reasoning under
"Time precision".

Backends differ today on how `Clear` and `Stats` treat scheduled items. See "Storage Backends
Disagree on Scheduled Items" in [known-issues.md](../known-issues.md).

## Badger

Badger keeps the queue list and the other metadata lists in databases whose directory names start
with `~`. Queue names cannot contain `~`, so no queue's partition directory can collide with them.

**A Badger item never becomes leasable again under the same ID.** `Lease` starts reading at the
lease pivot instead of the first item, so it does not read every leased item again on every
lease. That only works while an item that becomes leasable again (an expired lease, an immediate
retry, a scheduled item coming due) is written under a new ID
([ADR 0022](../adr/0022-managing-item-lifecycles.md)), and while every write that stores a leasable
item moves the pivot back to that item when it sorts earlier. IDs made after a restart can sort
before older ones, so new items need the pivot moved back too. Storage changed out of band (a queue
reload) makes the pivot wrong, so `Stats`, the read a reload or a partition recovery does, must reset
it, and the lifecycle scans, which do not, cannot be relied on to.

**A Badger lease stops at its context's deadline and keeps what it leased.** Produce and lease share
one write deadline. When it ends, the lease commits what it already assigned and the next lease
resumes where it stopped. A lone consumer on a quiet queue can therefore get "no items are in the
queue" while free items wait further on, and each new lease gets further.

**A Badger lifecycle delete needs the item's `SourceID` in the action.** Badger frees the `SourceID`
from the action's copy of the item, not the stored one, so a delete action without it leaves the
`SourceID` behind and a later produce with that `SourceID` is skipped.

## PostgreSQL

Table creation runs under its own context, not the caller's. The first call on a partition creates
its table, and that call may carry a short request deadline that would cancel the creation halfway
through.

**A PostgreSQL batch sent outside a transaction commits when it closes.** PostgreSQL runs the
statements of one batch as a single implicit transaction and commits it at the end, and the driver
reports that commit only when the batch is closed. A write that ignores the close error answers
success for a commit that failed.

## MongoDB

The [MongoDB tech spec](../features/ENG-48-mongodb-backend/tech-spec.md) records the design: no
multi-document transactions, no replica set required, insert-before-delete ordering, the
partial-index layout, the non-unique `source_id` index, and dropping the collection on a destructive
whole-queue `Clear`. Its account of `Complete` predates the code and is out of date. The rules it
does not cover follow.

**An empty lease batch skips the query.** MongoDB reads a limit of 0 as "no limit", unlike SQL
`LIMIT 0`, so a lease with nothing requested would otherwise read the whole collection.

**`Retry` reads the whole document before it writes.** It must tell a missing or scheduled item,
both reported as "does not exist", apart from one that is not leased, and an immediate retry
rebuilds the item at a new tail ID from the document's fields. A single `FindOneAndDelete` would
lose both.

**The process-wide client lock (`globalMongoMu`) is held across the connect retries.** Every
MongoDB call takes it on its way to the shared client. `mongo.Connect` returns at once and does no
I/O on the calling goroutine, so a healthy connect never holds the lock while waiting on the server.
When `Connect` itself returns an error, the sleeps between attempts (up to 1.5s in all) run under
the lock and hold up every MongoDB call in the process.

**A tail copy keeps every field of the item, `SourceID` included.** An expired lease moves its item
to the tail under a new ID ([ADR 0022](../adr/0022-managing-item-lifecycles.md)). Duplicate
detection after a dead-letter move depends on the `SourceID` surviving, and copying the whole item
carries along any field added later.

**Moving an item to the tail checks first that the item still exists.** The lifecycle scan and the
action that follows it are separate steps, so another path may have moved the item in between.
The check stays a separate read because the move inserts the new tail document before it deletes
the old one, so a crash leaves a duplicate rather than a lost item, and folding the check into the
delete would reverse that order.
