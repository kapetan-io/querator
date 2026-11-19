# Storage Backend Internals

This page is for people changing or adding a storage backend in `internal/store`. It records why
the backends are built the way they are. For how to configure and run each backend, see the
[backend overview](README.md). Decisions that have their own ADR are linked, not repeated.

## The Partition contract

A `Partition` holds the items of one partition of one queue. Each backend implements it.

**One goroutine drives a partition.** Only the logical queue that owns the partition calls
`Produce`, `Lease`, `Complete`, `Retry`, `TakeAction` and the other item methods, one call at a time
([ADR 0003](../adr/0003-rw-sync-point.md)). A `Partition` is therefore not safe for concurrent use,
and backends lean on this. The MongoDB backend runs without transactions and deduplicates
`SourceID` with a read followed by an insert, with nothing between them to stop a second caller.
Badger advances its item ID seed without a lock. Code that calls these methods from a second
goroutine breaks both.

**`Info` and `UpdateQueueInfo` must be safe for concurrent use.** `UpdateQueueInfo` runs when a
user changes queue settings that must take effect at once. Every backend guards both with the
partition's read-write lock. Today every caller of either method runs on the logical queue's own
request loop, and the lock keeps them safe if a caller moves off it.

**Storage is contacted lazily.** `PartitionStore.Get` returns a `Partition` without an error and
without touching storage. The first call that needs the data creates tables, collections or
connections ([ADR 0021](../adr/0021-storage-lazy-initialization.md)). The metadata stores
(`Queues`, `Namespaces`, `Users`, `APIKeys`, `Roles` and `RoleBindings`) follow the same rule.

**An error from `Complete` or `Retry` fails the whole batch.** When the backend returns an error,
the logical queue marks every request in the batch for retry, and the client retries it. Each
request touches one partition ([ADR 0020](../adr/0020-api-semantics.md)). Badger and Postgres
discard their transaction, so nothing in the batch applies. MongoDB has no transaction and keeps
the items it already removed, so the retried request reports those as "does not exist".

**`Lease` hands items out through `batch.Iterator()`.** The iterator spreads the available items
evenly across every waiting lease request in the batch, so one consumer does not take them all.

**A problem with one request goes on that request.** `Complete` and `Retry` record an invalid,
missing, scheduled or not-leased item ID on the request's `Err` and return nil. The returned error
is only for a storage failure.

**Scheduled items have their own scan.** `ScanForScheduled` returns only the actions that move a
scheduled item into the queue, separate from `ScanForActions`. Scheduled items run on their own
timer and their own times, and a separate scan lets the logical queue check them without walking
every item for lease and expiry work. Both scans stop at the first error they yield.

**The methods a name does not explain:**

- `Lease` sets `IsLeased` and `LeaseDeadline` and adds one to `Attempts` on each item it hands out.
- `Add` serves `/v1/storage/items.import`. It rejects an empty list and does not apply the 100ms
  `EnqueueAt` rule below, so an imported item keeps the `EnqueueAt` it was given.
- `Delete` skips IDs it cannot find and fails on an ID that is not valid. Postgres and MongoDB
  also reject an empty list.
- `Clear` removes what the `Queue`, `Scheduled` and `Destructive` flags of `types.ClearRequest`
  select, and the backends disagree on scheduled items. With `Queue` set, Postgres and MongoDB
  also remove scheduled items whose `EnqueueAt` has passed. InMemory and Badger keep every
  scheduled item on a non-destructive `Queue` clear, and empty the whole partition, scheduled
  items included, when `Queue` and `Destructive` are both set.
- `Produce` and `Add` skip an item whose `SourceID` matches one already in the partition. Removing
  an item by `Complete`, `Delete` or `Clear` frees its `SourceID` again. On InMemory and Badger, a
  lifecycle delete through `TakeAction` does not (ENG-777).
- `LifeCycleInfo` leaves `NextLeaseExpiry` or `NextExpireDeadline` zero when there is none.
- `PlacePartitions` returns no partitions when every backend's affinity is 0 or less. Its callers
  check `CanPlacePartitions` first, so a queue that requests partitions always gets them. The
  `QueuesManager` refuses to create a queue, and the daemon refuses a configured queue with no
  partitions listed, when no backend can take one.
- `Queues.Get` returns `ErrQueueNotExist` for a missing queue, which the `QueuesManager` checks
  for. `Queues.Delete` of a missing queue succeeds.

## Rules every backend follows

**An `EnqueueAt` less than 100ms away is not scheduled.** `Produce` treats an item whose
`EnqueueAt` is in the past or within 100ms of now as an ordinary item and clears `EnqueueAt`.
Clients often fill in every field of a produce request and set `EnqueueAt` to "now", which clock
drift between machines puts a few milliseconds in the future. Scheduling such an item costs a
write, a scan and a move for no benefit. `Retry` uses the same 100ms rule for `RetryAt`.

**An unleased item records no lease deadline.** Badger, InMemory and Postgres store a zero or
`NULL` lease deadline and MongoDB stores BSON `null`. Expiry scans select leased items by the
`is_leased` flag, never by comparing the deadline against a sentinel. `theFuture` in `store.go`
marks "nothing found yet" when InMemory and Badger compute `LifeCycleInfo`, and no backend writes
it.

**The lifecycle scan removes a leased item at `MaxAttempts`.** `ScanForActions` yields
`ActionItemMaxAttempts` for a leased item whose lease has not expired but whose `Attempts` reached
the queue's `MaxAttempts`, and the logical queue moves it to the dead letter queue, or deletes it
when the queue has none. Because `Lease` counts the attempt when it hands the item out, this also
matches an item a consumer holds on its final allowed lease. ENG-776 tracks whether that is
intended.

**Lifecycle actions may carry less than the whole item.** A backend that can move a scheduled
item into the queue by its ID alone does not need the rest of the item in the `Action`, and
`TakeAction` may either insert the item it is given or update the stored item in place, whichever
the data store does best.

**`Stats` counts a due scheduled item as ready.** Postgres and MongoDB count an item as active,
and so in `Total`, when it has no `EnqueueAt` or its `EnqueueAt` has passed, even before the
lifecycle moves it into the queue. InMemory and Badger count every item with an `EnqueueAt` as
scheduled until it moves.

## Timestamps are stored at microsecond precision

Postgres `TIMESTAMPTZ` keeps microseconds, so the Postgres backend truncates every timestamp to
the microsecond before it writes it and before it returns an item. The exception is the
`LeaseDeadline` on items `Lease` hands out, which keeps full precision while the stored copy is
truncated. MongoDB does the same. Without that, an item returned
from `Produce` would not equal the same item read back through `List`, and the functional suite
compares the two exactly. MongoDB's BSON `Date` keeps only milliseconds, so the MongoDB backend
stores timestamps as int64 microseconds since the epoch to match Postgres. The
[MongoDB tech spec](../features/ENG-48-mongodb-backend/tech-spec.md) has the full reasoning
under "Time precision".

## Shared connections

Postgres and MongoDB each keep one process-wide connection pool per connection string, shared by
every queue and partition that points at the same server and reference counted so the last
`Close` disconnects it (`postgresPoolManager` and `mongoClientManager`). A config takes one
reference on first use and reuses the pool on every later call, because a reference per call
would never be given back and the pool would never close. `MongoConfig` checks and sets its flag
under the manager's lock. `PostgresConfig` sets `poolAcquired` without a lock, so two first calls
at the same moment could each take a reference.

The MongoDB client manager holds its lock across the connect retry loop. The driver's
`mongo.Connect` returns at once and does no I/O on the calling goroutine, so the loop never waits
on the server while holding the lock. It still sleeps between attempts when `Connect` itself
returns an error. The backend stays lazy ([ADR 0021](../adr/0021-storage-lazy-initialization.md))
because the client is created on first use, not when the config is built.

Postgres and MongoDB read `ScanForActions` and `ScanForScheduled` results a page at a time,
`ScanBatchSize` items per page, 1,000 by default.

## Badger

The queue, namespace, user, API key, role and role binding lists each live in one Badger
database whose directory name starts with `~` (for example `~queue-storage-partition`). Queue
names cannot contain `~`, so no queue's partition directory can collide with these.

## Postgres

Table and index creation runs under its own context with a 30 second timeout instead of the
caller's context. The first call on a partition creates its table, and that call may carry a short request
deadline, which would cancel the creation halfway through.

## MongoDB

The [MongoDB tech spec](../features/ENG-48-mongodb-backend/tech-spec.md) records the design:
no multi-document transactions, no replica set required, insert-before-delete ordering, the
partial-index layout, the non-unique `source_id` index, and dropping the collection on a
destructive whole-queue `Clear` (a `Clear` with `Queue`, `Scheduled` and `Destructive` all set),
which also resets the partition's lazy-initialization flag (the spec calls it the once-guard; the
code calls it `initDone`). It does not cover the choices below, and its account of `Complete`
predates the code.

**An empty lease batch skips the query.** MongoDB reads `SetLimit(0)` as "no limit", unlike SQL
`LIMIT 0`, so a lease with nothing requested would otherwise read the whole collection.

**`Complete` deletes with `FindOneAndDelete` filtered on `is_leased`.** One round trip checks the
lease and deletes the item. When nothing matches, the item is either missing or not leased, and a
second read runs only on that error path to say which.

**`Retry` reads the whole document before writing.** It must tell a missing or scheduled item,
both reported as "does not exist", apart from one that is not leased, and an immediate retry
rebuilds the item at a new tail ID from every field of the document. A single `FindOneAndDelete`
would lose both.

**Moving an item to the tail checks first that the item still exists.** An
expired lease moves its item to the tail of the partition under a new ID
([ADR 0022](../adr/0022-managing-item-lifecycles.md)). The copy keeps every field of the item,
`SourceID` included, and resets only the lease, so a field added later is carried along. The lifecycle
scan and the action that follows it are separate steps, so another path may have moved the item in
between. Checking first avoids inserting a tail copy of an item that is already gone. The check
stays a separate read because the move inserts the new tail document before it deletes the old
one, so a crash leaves a duplicate rather than a lost item, and folding the check into the delete
would reverse that order.
