# 26. MongoDB backend consistency model

Date: 2026-06-01

## Status

Accepted

## Context

Querator is adding a MongoDB storage backend at parity with the existing PostgreSQL backend. The Postgres backend wraps its multi-item operations in transactions and uses a unique partial index for deduplication. MongoDB multi-document transactions require a replica set or sharded cluster; a standalone `mongod` cannot run them. Mandating a replica set would exclude single-node, edge, and simple development deployments.

Three properties of querator's existing design make database transactions unnecessary for correctness:

- A partition is read and written by a single Logical Queue goroutine. Concurrent access to one partition is not part of the model, so the only failure to guard against is a partial write from a mid-operation crash, not a data race.
- Delivery is at-least-once. A leased item whose lease expires is re-queued and re-delivered, so duplicate delivery is already tolerated by every consumer.
- The lifecycle is a two-phase garbage collector that re-scans for items still needing action. An operation that fails partway is retried on the next scan, provided the source item still exists ("found later").

A tension exists only for operations that move an item to the tail of the FIFO queue. The tail position is encoded in the item id, which is immutable in MongoDB, so a move is an insert of a new document plus a delete of the old one — two writes with no transaction to bind them.

A separate tension exists for deduplication. The `source_id` field is set only for dead-letter provenance and admin import; normally produced items have none. A unique index on `source_id` would reject the insert half of a tail-move while the old document still holds the same value. The in-memory and BadgerDB backends already deduplicate with an application-level existence check rather than a database constraint; only Postgres relies on a unique index.

## Decision

The MongoDB backend will run against a standalone `mongod` and use no multi-document transactions.

- Correctness rests on single-document atomic operations and the single-writer-per-partition guarantee.
- Every operation that moves an item to a new id — client retry, lease-expiry re-queue, and dead-letter move — will insert the new tail document first, then delete the old. Delete-before-insert is prohibited.
- Batch operations use only single-document primitives and tolerate partial application: produce inserts independently, complete deletes independently and never rolls back (the client retries the whole request and unknown or already-completed ids are ignored), and lease claims each item with a conditional update filtered on the unleased state so a lost race claims nothing.
- Deduplication uses an application-level check before insert, with no unique index on `source_id`. This lets a tail-move preserve `source_id` on the new document without a key collision.

## Consequences

- The backend deploys on any MongoDB topology, including single-node and managed clusters, with no replica-set requirement.
- A mid-operation crash during a tail-move leaves the original item in place to be re-found and re-queued, yielding at worst a duplicate delivery and never a lost item.
- There is no database-level backstop against a duplicate `source_id` or a double claim. If the single-writer-per-partition invariant is ever violated — overlapping failover, or a misconfiguration that runs two goroutines against one partition — the existence check and the conditional claim become time-of-check-to-time-of-use races that a unique index or row lock would have caught.
- The on-disk shape diverges from the Postgres backend: no unique index on `source_id`, and an unleased lease deadline stored as null rather than as a far-future sentinel.
- Batch operations carry no transaction overhead, reducing latency in the common case.
