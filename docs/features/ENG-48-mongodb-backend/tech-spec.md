# MongoDB Storage Backend Tech Spec

_Source ticket: [ENG-48](https://linear.app/kapetan-io/issue/ENG-48/write-querator-a-mongodb-backend)_
_Reference implementation: `internal/store/postgres.go`_
_Related ADRs: 0003 (R/W sync point), 0004 (item id not immutable), 0014 (ordered storage), 0020 (api semantics), 0021 (lazy init), 0022 (item lifecycles)_

## Overview

Add a MongoDB storage backend to querator at parity with the PostgreSQL backend: `MongoQueues` (queue metadata) and `MongoPartitionStore`/`MongoPartition` (item storage). Auth stores (`Namespaces`, `Users`, `APIKeys`, `Roles`, `RoleBindings`) are **out of scope** — they remain on memory/badger, exactly as the Postgres backend leaves them. The backend must pass the existing functional suite unchanged with a new `MongoDB` backend-table entry, wire `mongo` into the daemon config, and ship operator docs.

The defining design decision is that this backend is **non-transactional and standalone-compatible**: it uses no multi-document transactions, requires no replica set, and upholds querator's correctness contracts through single-document atomic operations, insert-before-delete ordering, and the single-writer-per-partition guarantee (ADR-0003/0009).

Scope is **Phase 1 only**. Auth-store parity is a separate future ticket.

## Component Design

All components live in a new `internal/store/mongo.go`, modeled structurally on `postgres.go`.

### MongoConfig

Mirrors `PostgresConfig`:

```go
type MongoConfig struct {
    // ConnectionString is a MongoDB connection URI, e.g. "mongodb://user:pass@host:27017".
    ConnectionString string
    // Database is the logical database name within the Mongo server. Defaults to "querator".
    Database string
    // MaxPoolSize caps the driver connection pool per client. 0 = driver default.
    MaxPoolSize uint64
    // ScanBatchSize is the page size for ScanForActions/ScanForScheduled. Default 1000.
    ScanBatchSize int
    // Log defaults to slog.Default().
    Log *slog.Logger

    // private: shared-client bookkeeping (mirrors PostgresConfig.connString/poolAcquired)
}
```

### Shared client manager

`mongo.Client` is itself a connection pool and is safe for concurrent use. A process-global manager keyed by connection string shares one client across all partitions/queues pointed at the same server, ref-counted for shutdown — a direct port of `postgresPoolManager`:

```go
type mongoClientManager struct {
    client   *mongo.Client
    refCount atomic.Int32
}
var (
    globalMongoMu    sync.Mutex
    globalMongoClients = make(map[string]*mongoClientManager) // keyed by ConnectionString
)
```

`acquireClient(uri, maxPool, log)` connects with bounded exponential backoff on first use (mirroring the Postgres backoff), increments refcount on reuse; `releaseClient(uri)` decrements and `Disconnect()`s at zero. Lazy: constructors never contact Mongo (ADR-0021).

### Constructors and stores

```go
func NewMongoQueues(conf MongoConfig) *MongoQueues
func NewMongoPartitionStore(conf MongoConfig) *MongoPartitionStore
func (s *MongoPartitionStore) Get(info types.PartitionInfo) store.Partition // returns *MongoPartition; no error (ADR-0021)
```

- **`MongoQueues`** implements `store.Queues` (`Get/Add/Update/List/Delete/Close`) against a single `queues` collection (`_id` = queue name). Lazy collection+index creation guarded the same way as Postgres `ensureTable` (idempotent / on first call).
- **`MongoPartition`** implements `store.Partition`. Holds the `MongoConfig`, a `ksuid.KSUID` (`uid`) seed, a `sync.Mutex` (`mu`) guarding `uid` advancement only, and a `sync.Once`-style guard (`collOnce`) for lazy collection+index creation. One Mongo collection backs one partition (collection-per-partition, mirroring table-per-partition).

### Lazy initialization (ADR-0021)

`ensureCollection` runs once per `MongoPartition` (sync.Once with stored error), creating the collection (implicitly on first write) and its indexes via `createIndexes`. `Clear` with a destructive whole-queue request `Drop()`s the collection and **resets the once-guard** so the next use re-creates it lazily.

## Data Model

### Collection-per-partition naming

Reuse the existing Postgres naming helper: `items_<hash>_<partition>` where `<hash>` is the 10-char base-62 hash of the queue name and `<partition>` is the zero-padded partition number. Mongo namespace (`<db>.<collection>`) length stays well within limits. Queue metadata lives in a single `queues` collection.

### Item document (BSON)

```
{
  _id:             string,   // KSUID (lexicographically sortable) — the FIFO key
  source_id:       string,   // OMITTED when nil (DLQ/import provenance only)
  is_leased:       bool,
  lease_deadline:  int64|null, // microseconds-since-epoch; null when no lease deadline (see decision below)
  expire_deadline: int64,    // microseconds-since-epoch (see Time precision)
  enqueue_at:      int64,    // microseconds-since-epoch; OMITTED when the item is not scheduled
  created_at:      int64,    // microseconds-since-epoch
  attempts:        int32,
  max_attempts:    int32,
  reference:       string,
  encoding:        string,
  kind:            string,
  payload:         binary
}
```

> Timestamps are int64 microseconds rather than BSON `Date`; see **Time precision** below for why.

**Field-representation conventions** (these drive index design — see below):

- **`enqueue_at` is omitted (absent) when an item is not scheduled.** This lets the scheduled index use a clean `{enqueue_at: {$exists: true}}` partial filter.
- **`lease_deadline` is BSON `null` when an item is unleased** (decision: mirror Postgres semantics; the `theFuture` sentinel remains a Go-layer concept only and is never written to Mongo). The lease-expiry index keys on `is_leased: true`, so it does not depend on `lease_deadline` being present.
- **`source_id` is omitted when nil.** Normally-produced items always have no `source_id`.

### Indexes

MongoDB `partialFilterExpression` supports only equality, `$exists: true`, `$gt/$gte/$lt/$lte`, `$type`, and `$and` of these — **it does not support `$exists: false` or `$or`**. The Postgres partial predicates therefore do not all map 1:1; the field-representation conventions above are chosen so the mappable ones use equality / `$exists: true`, and the remaining predicate is applied as a residual query filter.

| Purpose | Index key | Partial filter | Notes |
|---|---|---|---|
| FIFO / primary | `_id` | (implicit) | KSUID ascending → FIFO for free (ADR-0014) |
| Lease selection | `{is_leased: 1, _id: 1}` | `{is_leased: false}` | Query adds residual `enqueue_at: {$exists: false}` and sorts `_id`. |
| Scheduled | `{enqueue_at: 1}` | `{enqueue_at: {$exists: true}}` | Clean — only scheduled items carry the field. |
| Lease expiry | `{lease_deadline: 1}` | `{is_leased: true}` | Clean — equality on `is_leased`. |
| Item expiry | `{expire_deadline: 1}` | (none) | Plain index. |
| Dedup lookup | `{source_id: 1}` | `{source_id: {$exists: true}}` | **Non-unique** (see Correctness). Optional but recommended for the check-before-insert lookup. |

**There is no unique index on `source_id`** — a deliberate divergence from Postgres, justified in Correctness.

## Correctness

The PRD-equivalent correctness constraints are querator's storage contracts: FIFO ordering (ADR-0014), at-least-once delivery (no item loss), single-lease-per-item, and `source_id` dedup. Because MongoDB is schemaless, **none of these invariants are enforced structurally** — all are enforced by application logic, so each needs the conformance suite's coverage. The arguments below show why the chosen operations preserve them.

### Invariant: FIFO ordering

Every read (`Lease` candidate select, `List`, `ListScheduled`, `ScanForActions`, `ScanForScheduled`) sorts by `{_id: 1}`. New ids come from `uid.Next()` under `mu`, producing the lexicographic successor, so insertion order == `_id` order == FIFO. Requeued/retried items get a fresh `uid.Next()` and thus sort to the tail (ADR-0004, ADR-0022 HOL-blocking avoidance). **Preserved on every operation that assigns an id.**

### Invariant: at-least-once (no item loss) under the non-transactional model

The contract (ADR-0022 "found later") is that partial failures degrade to **duplicates**, never to loss. Every operation that *moves* an item to a new `_id` (immediate `Retry`, lease-expiry requeue in `TakeAction`, DLQ move) MUST **insert the new tail document first, then delete the old**:

- Crash between insert and delete → the old document still exists and is re-found by the next lifecycle scan (lease still expired) → re-requeued → at worst a duplicate. No loss.
- The reverse order (delete-then-insert) is **prohibited** — it opens a loss window the contract forbids.

`Produce`/`Complete`/`Lease` need no multi-document atomicity: `Produce` is independent inserts (`insertMany(ordered:false)`, partial-tolerant like Postgres); `Complete` is partial-tolerant deletes (the client retries the whole request, and unknown/already-complete ids are silently ignored); `Lease` claims are per-document atomic (below). **Preserved without transactions.**

### Invariant: single lease per item

`Lease` reads candidates (`{is_leased: false, enqueue_at: {$exists: false}}`, sort `{_id: 1}`, limit = `batch.TotalRequested`), distributes them via `batch.Iterator()`, then marks them leased with a `bulkWrite` of **conditional** updates: `updateOne({_id: id, is_leased: false}, {$set: {is_leased: true, lease_deadline: deadline}, $inc: {attempts: 1}})`. The `is_leased: false` filter makes each claim atomic on its single document — even if two writers ever raced the same partition (misconfig/failover overlap), the loser's update matches zero documents and that item is simply not handed out. This is the standalone-Mongo equivalent of Postgres `SELECT … FOR UPDATE SKIP LOCKED`. **Preserved per-document without transactions.**

### Invariant: source_id dedup (no unique index)

Dedup is implemented exactly as InMemory and BadgerDB already do it — an **application-level check-before-insert**, not a DB unique constraint (only Postgres uses a unique index, an incidental artifact of its `ON CONFLICT` idiom). On `Produce`/`Add`, for each item whose `source_id` is set, a `FindOne({source_id: ...})` (and a within-batch seen-set) gates the insert; an existing match is silently skipped. This is safe because a `Partition` is driven by a **single logical-queue goroutine** (ADR-0003/0009) — the same single-writer property that already makes InMemory's map check and Badger's get-before-put correct. Dropping the unique index is what lets the insert-first requeue ordering work with `source_id` preserved on the tail copy (no possible key collision), keeping behavioral parity with InMemory/Postgres (which both preserve `source_id` across requeue). Trade-off accepted: no hard DB backstop if the single-writer invariant is ever violated.

### Behavioral constraint: no replica set / no transactions

Every operation above uses only single-document atomic writes or partial-tolerant batches. The backend connects to and passes the suite against a **standalone `mongod`**. Satisfied by construction.

### Time precision

BSON `Date` is millisecond-precision; Postgres (and the conformance suite) work at microsecond precision. The initial design called for millisecond truncation, but the existing functional suite forecloses it: `storage_test`'s `CRUDCompare` truncates to **microsecond** before comparing, and `compareStorageItem` requires the import-response and a subsequent `List` read to be **exactly** equal. Millisecond-truncated storage fails both. (This is the conformance implication the Testing section flags: an assertion *does* expect sub-millisecond precision.)

The backend therefore stores item timestamps as **int64 microseconds-since-epoch** rather than BSON `Date`, matching Postgres's microsecond precision exactly so the suite passes unchanged. FIFO ordering is unaffected (it rides on `_id`/KSUID, not timestamps). The index design is unaffected because the partial filters rely only on equality and `$exists`, which behave identically on an int64 field. Round-tripping is bit-stable because reads return the same microsecond values that were written.

### Operation → Mongo mechanism (summary)

| Method | Mechanism |
|---|---|
| `Produce` | per-item KSUID under `mu`; check-before-insert for `source_id`; `insertMany(ordered:false)` |
| `Lease` | find candidates sort `_id` limit N → distribute → `bulkWrite` conditional `updateOne` claims |
| `Complete` | per-id leased-check + `deleteOne`; partial-tolerant; per-request `Err` on not-found/not-leased |
| `Retry` (immediate) | insert new tail doc (new KSUID, unleased, `source_id` preserved) → delete old |
| `Retry` (scheduled) | `updateOne` `$set enqueue_at`, `is_leased:false`, `lease_deadline:null` |
| `Retry` (dead) | `deleteOne` |
| `TakeAction` (lease expired) | insert new tail doc → delete old |
| `TakeAction` (expired/delete) | `deleteOne` |
| `TakeAction` (scheduled ready) | `updateOne` `$unset enqueue_at` |
| `ScanForActions/Scheduled` | paginated `find` sort `_id`, key-set on `_id`; READ-ONLY (lifecycle goroutine) |
| `Stats` | aggregation `$group` with `$sum`/`$cond` |
| `LifeCycleInfo` | aggregation: min `lease_deadline` where leased, min `expire_deadline` where active |
| `List/ListScheduled` | `find` filtered, sort `_id`, key-set pagination on `_id` |
| `Clear` | `Drop()` collection (destructive whole-queue) + reset once-guard; else `deleteMany` |

## API Design

YAML config mirrors Postgres (`daemon/config.go`):

```yaml
queue-storage:
  driver: mongo
  config:
    connection-string: "mongodb://user:pass@localhost:27017"
    database: querator

partition-storage:
  - name: mongo-01
    driver: mongo
    affinity: 1
    config:
      connection-string: "mongodb://user:pass@localhost:27017"
      database: querator
      max-pool-size: "50"
```

`setupPartitionStorage` and `setupQueueStorage` gain a `case "mongo":` constructing `MongoConfig` from the `config` map. Update the two error strings to include `Mongo`. **Postgres wiring is explicitly not bundled into this ticket** (separate concern).

## Dependencies

Add `go.mongodb.org/mongo-driver` (the official driver) to `go.mod`. No other new runtime dependencies. Test-only: the `testcontainers-go` mongodb module (already have `testcontainers-go`).

## Error Handling

Follow the Postgres backend's two-tier model:

- **Function-level errors** (connection/write failures) are wrapped and returned; the caller assumes the batch did not apply. Map "queue not found" to `store.ErrQueueNotExist`; map duplicate `queues._id` on `Add` to an invalid-option error (`queue already exists`).
- **Per-request validation errors** are set on `batch.Requests[i].Err` (e.g. `reply.NewInvalidOption("invalid storage id…")` for not-found/not-leased in `Complete`/`Retry`) and do not fail the whole call.

There is no general Mongo-error→store-error table; only the two cases above are mapped, mirroring Postgres.

## Observability

Reuse the optional `OnQueryComplete(operation, duration, err)` hook pattern from `PostgresConfig` if carried over. `Stats` and `LifeCycleInfo` already expose partition-level counts via the existing `types.PartitionStats`/`types.LifeCycleInfo` surfaces — no new observability API is required because the functional suite asserts behavior through these existing surfaces.

## Performance and Scale

- One shared `mongo.Client` (pool) per connection string; `max-pool-size` caps it.
- Indexes above keep lease selection, scheduled scans, and expiry scans index-backed.
- `Clear` via `Drop()` is O(1) vs `deleteMany` O(n).
- Collection-per-partition: many queues × partitions ⇒ many collections. MongoDB/WiredTiger handles large collection counts but each carries namespace + index overhead; documented as an operational consideration (not a correctness limit) in `docs/storage/mongodb.md`.

## Testing

Testing follows the `surface-testing` skill — the existing functional suite already tests entirely through the public client/daemon surface, and the MongoDB backend joins it via the backend table.

Key surfaces:
- **Backend-table entry**: add `{Name: "MongoDB", Setup: …, TearDown: …}` to the table in each of `service/queue_test.go`, `queues_test.go`, `retry_test.go`, `partition_test.go`, `shutdown_test.go`, `storage_test.go`. No test bodies change — passing them unchanged is the definition of done (including the FIFO ordering assertions, the strongest correctness check).
- **`mongoTestSetup`** in `service/common_test.go`, mirroring `postgresTestSetup`: a shared MongoDB testcontainer (`getSharedMongoContainer`, `sync.Once`) plus a per-test **database** for isolation (counter-suffixed, like `CreateDatabase`/`DropDatabase`). A **standalone** mongod container suffices — no replica-set configuration is required, which is the practical proof that the non-transactional design holds.
- **External dependency / substitute tier**: MongoDB via testcontainers (same tier as the Postgres suite). No in-process fake.
- **Time**: existing suite injects a clock via `svc.Config`; the backend takes `now` as a parameter on the relevant methods (as Postgres does) — no new clock plumbing.

Conformance risks to verify during implementation:
- **Millisecond truncation** vs the suite's `compareStorageItem`/`Truncate` assertions: ensure every write truncates to millisecond so import→read round-trips are bit-stable. Confirm no assertion expects sub-millisecond precision.
- **`source_id` parity**: confirm no functional test asserts a unique-constraint *error* on duplicate produce (the suite asserts silent-skip behavior, which check-before-insert provides).

## Migration and Deployment

- New backend only; no data migration. New collections/indexes are created lazily on first use (ADR-0021).
- Daemon wiring: `mongo` becomes a selectable driver for both queue and partition storage, validated by `daemon/config_test.go` (add a Mongo case).
- Docs: add `docs/storage/mongodb.md` (mirror `postgres.md`, documenting the standalone-OK / non-transactional model, collection-per-partition naming, and index list); update the comparison table in `docs/storage/README.md`; add a working Mongo block to `example.yaml`.
- Update the `theFuture` comment in `internal/store/store.go` to note that storing BSON `null` (as Postgres/Mongo do) is an acceptable per-backend alternative to the sentinel.

## Open Questions

None blocking. Index tuning (whether the lease-selection residual `enqueue_at` filter warrants a compound `{is_leased:1, enqueue_at:1, _id:1}` index) is an implementation-time profiling decision, not a contract.
