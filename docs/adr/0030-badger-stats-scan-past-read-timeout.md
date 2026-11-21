# 30. BadgerDB stats scan runs past the read timeout

Date: 2026-11-20

## Status

Accepted

## Context

A partition's stats are the number of items it holds, how many are leased and scheduled, and their
average ages. Querator reads them from storage for `/v1/queue.stats`, on a reload, when a queue starts
and when a failed partition recovers. The last three give the call the read timeout (`ReadTimeout`,
3 seconds by default), and mark the partition failed when the call does not finish in time. Recovery
tries again with the same timeout.

PostgreSQL and MongoDB answer the stats call with one counting query. BadgerDB has no such query, so it
scans every item in the partition. Measured through the service on a laptop, that scan covers about
75,000 items a second:

```
items=50000  stats=661ms
items=200000 stats=2.68s
items=500000 stats=6.65s
```

Every other BadgerDB read loop stops when its context ends. Tiger, the static checker, accepts a loop
bounded that way and flags one that is not. If the stats scan stopped at the read timeout too, a
partition holding more than about 200,000 items would fail at every start-up and every recovery after,
and never load. The partition is not broken, and the scan always finishes when it runs out of items.

## Decision

**BadgerDB's stats scan ignores its context's deadline and stops only when the context is cancelled.**
`BadgerPartition.Stats` in `internal/store/badger.go` hands `countPartition` a context that drops the
caller's deadline and is cancelled when the caller's context is. A scan that outlasts the read timeout
finishes and returns the same counts it always did. A context cancelled before the scan ends gives an
error, so a client that gives up on `/v1/queue.stats` stops it.

**Callers keep the read timeout for every backend.** PostgreSQL and MongoDB stop at it, so a hung
database marks its partition failed after 3 seconds and the queue keeps serving its other partitions. A
context that ends only at shutdown would let a hung database hold the queue until the server stops.

**The loop that scans the items takes the deadline-free context as a parameter** and checks it in the
loop header. Tiger rejected the same check on a context derived inside `Stats`, and on
`!errors.Is(ctx.Err(), context.Canceled)` in the header.

## Consequences

The stats scan on a large BadgerDB partition runs on the queue's request loop for as long as it takes,
so the queue's other requests wait for it. The operating limits page lists this.

The contexts that start-up and recovery pass are never cancelled, so their scans always run to the end.
The one a reload passes carries the read timeout and the reload request's own context. A client that
gives up on a reload while a partition is being scanned, within the read timeout that began for that
partition, stops the scan. After the timeout passes, the context's error stays `DeadlineExceeded`, so a
later cancel is never seen and the scan runs to the end.

A reload cancelled this way marks the partition it was scanning failed, and each partition after it in
that reload fails too, because each gets the cancelled context. The lifecycle then recovers each failed
partition with a context no client can cancel, and the partition serves again. PostgreSQL and MongoDB
already mark a partition failed when a cancelled reload stops their count, so BadgerDB behaves the same.
The cost is a second scan of the partition. `CancelledReloadRecovers` in `service/badger_stats_test.go`
holds the failure and the recovery.

Shutdown cancels none of these contexts. A shutdown waits for a scan in progress, or gives up at
its own deadline while the scan runs on.

Keeping a partition's counts as items come and go would make the full scan unnecessary and remove this
exception. ENG-867 designs that.
