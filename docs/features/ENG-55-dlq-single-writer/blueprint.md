# Dead-Letter Single-Writer Routing Blueprint

> Fixes [ENG-55](https://linear.app/kapetan-io/issue/ENG-55/dlq-item-movement-violates-single-writer-per-partition) · Reaffirms [ADR-0003](../../adr/0003-rw-sync-point.md) · Touches [ADR-0019](../../adr/0019-partition-items-distribution.md), [ADR-0022](../../adr/0022-managing-item-lifecycles.md)

## Objective

Dead-letter movement currently writes directly to the dead-letter queue's storage
partition from the **source** queue's `requestLoop` goroutine, bypassing the dead-letter queue's own
`requestLoop`. This breaks querator's load-bearing single-writer-per-partition invariant
(ADR-0003) and produces two rider defects: the dead-letter partition's in-memory lifecycle/expiry
timer is not advanced, and consumers blocked on the dead-letter queue are not woken when an item is
dead-lettered.

The fix routes dead-letter production **through the dead-letter queue's own `requestLoop`** via a new
internal produce path, restoring single-writer, fixing GC scheduling drift, and waking blocked consumers
in one move. No storage-backend changes are required; the defect and the fix are core querator
behavior affecting all backends (InMemory, BadgerDB, PostgreSQL, MongoDB) equally.

## Mental Model

Every storage partition has exactly one writer: the `requestLoop` of the `Logical` that owns it.
All mutations — client Produce/Lease/Complete/Retry, lifecycle actions, scheduled-item promotion —
funnel through that one goroutine via `requestCh`. The `requestLoop` is the only place that mutates
a partition *and* keeps the in-memory `QueueState`/`Partition.State` consistent with storage.

Dead-lettering is just another producer to the dead-letter queue. Today it cheats by reaching past the
dead-letter queue's loop straight into storage. After this change it behaves like any other produce: the
source loop hands the pre-built items to the dead-letter queue's loop and waits for confirmation, exactly
as an external client's produce would — except it skips client-facing validation because the items are
already built.

The deadlock that this hand-off might appear to risk cannot occur: `validateDeadQueue` forbids a
queue from being its own dead-letter queue and forbids dead-letter chains, so the source loop and the
dead-letter queue loop are always distinct goroutines, and the dead-letter queue never dead-letters back.

## Correctness Constraints

### State Invariants

- **Single writer per partition (ADR-0003).** A storage partition is mutated by exactly one
  goroutine: the `requestLoop` of the `Logical` that owns it.
  - *Violated by:* any code path that writes to a partition's storage off that partition's
    `requestLoop`. Today's `Logical.ProduceInternal` is such a path — it runs on the *source*
    queue's loop and writes the *dead-letter queue's* partition.
  - *Enforcement (structural):* remove `ProduceInternal`. With it gone there is no exported method
    on `Logical` that writes to storage off-loop; the only entry to a partition write is
    `requestCh`. This makes the illegal state hard to reintroduce by accident — a new bypass would
    have to add a brand-new storage write, not merely call an existing one.

- **In-memory partition accounting matches storage after a produce.** After items are produced to a
  partition, that partition's in-memory `State.UnLeased` reflects them and its
  `Lifecycle.NextLifecycleRun` is no later than the earliest produced item's `ExpireDeadline`.
  - *Violated by:* the direct write, which updates neither (`distribution.go` `assignProduceRequests` /
    `applyToPartitions` / `notifyExpired` are skipped).
  - *Enforcement (application logic):* routing the internal produce through `handleHotRequests`
    reuses the exact same assignment/apply/notify path as a client produce. Because it is enforced by
    code (not a schema constraint), it is covered by the secondary test below.

### Behavioral Constraints

- **Never delete a source item before its dead-letter copy is durably produced.** The source item is
  removed only after a successful dead-letter produce (`queues_manager.go` `LifeCycle`). A failed or
  dropped internal produce must leave the source item in place for the next lifecycle pass (which
  re-finds it, since it still exceeds attempts/deadline; the dead-letter queue dedupes via `SourceID`).
  *Already honored today and must remain so — the change must not reorder delete-before-confirm.*

- **A consumer blocked on the dead-letter queue is offered a newly dead-lettered item promptly.**
  Producing through `handleHotRequests` runs `assignToPartitions` (which re-assigns waiting lease
  requests) and `applyToPartitions` (which applies Produce *then* Lease to the same partition in one
  pass), so a blocked lease is satisfied in the same loop iteration the item arrives — not at an
  unrelated later wake.

- **Dead-lettering never deadlocks.** The source loop blocks on the dead-letter request's `ReadyCh`
  while the dead-letter queue loop services it. These are guaranteed-distinct goroutines
  (`validateDeadQueue` forbids self-reference and dead-letter chains), and the dead-letter queue never
  produces back to the source, so there is no cycle.

- **Backpressure leaves no item lost.** When the dead-letter queue's `requestCh` is full, the internal
  produce returns an error rather than blocking; the source item is retained and retried on the next
  lifecycle pass. The source loop is never stalled waiting on a saturated dead-letter queue.

## Acceptance Criteria

1. A consumer blocked on `QueueLease` against a dead-letter queue receives an item driven to dead-letter
   in the source queue **promptly** (well within the lease request timeout), not after an unrelated wake.
   This fails on `main` and passes after the change.
2. After an item is dead-lettered **with no consumer leasing it** (so it stays un-leased), the
   dead-letter partition's public `QueueStats` reports `unLeased == total` and a `nextLifecycleRun`
   advanced toward the item's expiry. This fails on `main` (in-memory accounting stays stale) and passes
   after the change. (A leasing consumer would drop `unLeased` to 0 while `total` stays 1, so this
   criterion must be measured without one — see the secondary test below.)
3. `Logical.ProduceInternal` no longer exists; dead-letter production flows over `requestCh` through
   the dead-letter queue's `requestLoop`.
4. The delete-after-confirm ordering is preserved: a forced internal-produce failure leaves the source
   item in the source partition (verified via stats), and a later pass moves it without duplication.
5. The doc comment at `logical.go:97-100` no longer contradicts the dead-letter path.
6. `make ci` passes (`-race -p=1`) across all backends.

## Scope

### In Scope

- A new internal produce `Method` on `Logical` carrying pre-built items, routed through `requestCh`
  and handled as a hot request.
- Removal of `Logical.ProduceInternal`.
- Two new first-class fields on the public `QueueStats` partition contract: `unLeased` and
  `nextLifecycleRun`, sourced from in-memory `QueueState`.
- The primary (liveness) and secondary (state-drift) surface tests.
- Corrected doc comment / brief note reaffirming ADR-0003.

### Out of Scope / Non-Goals

- Any storage-backend changes — this is backend-agnostic core behavior.
- Changing how the lifecycle *detects* dead items, or the delete-after-confirm sequencing in
  `QueuesManager.LifeCycle` (only the produce mechanism it calls changes).
- Cross-Logical or cluster-level coordination beyond the existing single-Logical guarantees.
- Reworking opportunistic distribution itself (ADR-0019) — the dead-letter queue simply gains it for
  free by using the normal produce path.

## Dependencies and Constraints

- ADR-0003 (R/W sync point) — the invariant being restored.
- ADR-0019 (partition item distribution) — the dead-letter produce now distributes opportunistically.
- ADR-0022 (item lifecycles) — the lifecycle goroutine reads directly but delegates all writes; this
  change makes the dead-letter write a true delegation.
- `validateDeadQueue` (`queues_manager.go`) — the no-self-reference / no-chain guarantee the
  deadlock-freedom argument rests on. Must remain in force.

---

## Architecture

### Current (buggy) path

All on the **source** queue's `requestLoop` goroutine:

```
runLifecycle (lifecycle.go)
  -> QueuesManager.LifeCycle           queues_manager.go
  -> ProduceToQueue(deadQueueName)     queues_manager.go
  -> dlqLogical.ProduceInternal(items) logical.go      <-- runs on SOURCE loop
  -> dlqPartition.Produce(...)         direct storage write to dead-letter partition
```

### New path

```
runLifecycle (source loop)
  -> QueuesManager.LifeCycle
  -> ProduceToQueue(deadQueueName)
  -> dlqLogical.ProduceInternal-replacement(items)
       enqueues a Request{Method: MethodProduceInternal} on dlqLogical.requestCh
       and blocks on Request.ReadyCh
                                  ── goroutine boundary ──
  Dead-letter loop: requestLoop -> handleRequest -> handleHotRequests
       -> consumeHotCh/reqToState   (item batch added to state.Producers)
       -> assignToPartitions        (assignProduceRequests: opportunistic partition pick,
                                       ExpireDeadline assigned; waiting leases re-assigned)
       -> applyToPartitions         (Produce then Lease on the partition; notifyExpired advances
                                       NextLifecycleRun; State.UnLeased updated)
       -> finalizeRequests          (close ReadyCh -> source loop unblocks with the result)
```

### Key design decisions

- **New dedicated `Method` (e.g. `MethodProduceInternal`)**, treated as a *hot* request alongside
  `MethodProduce`/`Lease`/`Complete`/`Retry`. It is added to `state.Producers` in `reqToState` and is
  indistinguishable from a normal produce once inside the loop, so it inherits assignment, apply,
  `notifyExpired`/`notifyScheduled`, and waiting-lease matching for free. Chosen over a boolean flag on
  the existing produce request to keep the hot-path `switch` statements explicit and matching the
  established method-per-operation pattern. **Three switch sites in `logical.go` must each gain a
  `MethodProduceInternal` case:** `handleRequest` (routes it to `handleHotRequests`), `reqToState`
  (maps it to `state.Producers`), and `isHotRequest` (gates the `consumeHotCh` drain loop and the
  pause-loop fast path in `handlePause`). Missing `isHotRequest` specifically does **not** fail to
  compile — under concurrent load a drained `MethodProduceInternal` would be treated as a cold request
  and panic in `handleColdRequests`. All three must be updated together.

- **Synchronous hand-off.** The source loop blocks on the request's `ReadyCh` (bounded by the lifecycle
  `WriteTimeout` context already in `runLifecycle`). This is required by the delete-after-confirm
  invariant: the source must know the dead-letter produce succeeded before deleting the source item. This
  is no worse than the status quo, where `ProduceInternal` already performs a synchronous storage write
  on the source loop.

- **Skip client-facing validation.** The internal produce does not enforce `RequestTimeout`, `ClientID`,
  or batch-size limits — the items are already built and validated upstream. It still flows through
  `handleHotRequests`. `ExpireDeadline` continues to be assigned in `assignProduceRequests`, so the
  internal path should *not* pre-assign it (avoids double assignment).

- **Opportunistic distribution.** Because the internal produce uses `assignProduceRequests`, items are
  spread across the dead-letter queue's partitions by load (ADR-0019), replacing the old
  always-`StoragePartitions[0]` behavior. This is a deliberate improvement and is correct for
  multi-partition dead-letter queues.

- **Backpressure: return error on full `requestCh`.** Mirrors the client `Produce` non-blocking send
  (`select { case requestCh <- ...: default: return overloaded }`). On a full channel the internal
  produce returns an error; `QueuesManager.LifeCycle` already treats a produce error as "leave the
  source item in place," so the item is retried next pass.

- **Remove `ProduceInternal`.** It is the bypass; deleting it is the structural enforcement of the
  single-writer invariant (no off-loop storage write remains to call).

## Data Design

### Internal request

The internal produce reuses the existing `types.ProduceRequest` (it already carries `Items`,
`Context`, `ReadyCh`, `Err`, `Assigned`). The `RequestTimeout`/`RequestDeadline` fields are simply
not validated on this path. No new request struct is required; the new `MethodProduceInternal`
constant distinguishes it at the loop boundary.

### Public stats contract additions

Two fields are added to the wire contract `QueuePartitionStats` (`proto/queue.proto`), to the internal
`types.PartitionStats`, and populated in `handleStats` from the live `QueueState`:

| Field | Type | Source | Meaning |
|---|---|---|---|
| `unLeased` | int32 | in-memory `p.State.UnLeased` | Count of un-leased items the Logical believes are available in the partition. Pairs with the storage-derived `total`; divergence indicates in-memory drift. |
| `nextLifecycleRun` | string (duration) | in-memory `p.Lifecycle.NextLifecycleRun` | When the partition's lifecycle/GC routine will next run. Exposes GC cadence and the expiry-scheduling the dead-letter path must advance. |

`handleStats` already overrides storage stats with in-memory `Failures`/`NumLeased`; these two fields
extend that same in-memory override. They are genuinely useful operator signals (distribution skew and
GC cadence), not test-only scaffolding. Being proto fields, they are a permanent contract.

See [observability-scope.html](./observability-scope.html) for the field-by-field rationale, the
storage-vs-in-memory distinction, and the alternatives considered.

### Invariant Preservation

- **Single writer.** After the change, the operations that mutate the dead-letter partition are exactly
  those applied inside the dead-letter queue's `applyToPartitions` — all on the dead-letter queue loop.
  The source loop's only interaction is enqueuing a request and reading a result channel; it performs no
  storage write. The removal of `ProduceInternal` means no exported off-loop write path exists.
- **Accounting matches storage.** The internal produce traverses `assignProduceRequests`
  (`State.UnLeased += len(items)` via `Partition.Produce`) and `applyToPartitions`/`notifyExpired`
  (advances `NextLifecycleRun`), the same code that keeps client produces consistent. Enforced by
  application logic; covered by acceptance criterion 2.

### Illegal State Analysis

The single-writer invariant cannot be made fully unrepresentable in Go's type system (any code with a
partition handle could call `Store.Produce`). The strongest available structural enforcement is
**narrowing the surface**: removing `ProduceInternal` leaves `requestCh` as the sole partition-write
entry point on `Logical`. Remaining enforcement is by convention and covered by the single-writer
acceptance tests. This is noted explicitly because it is an application-logic invariant, not a
schema-enforced one.

### Component-boundary contract — `QueuesManager.ProduceToQueue` → Dead-Letter `Logical`

- **Preconditions (caller):** items are fully built; each item's `SourceID` is set to its original ID
  and `ID` is cleared for dead-letter-side generation; lease state is reset (`IsLeased=false`, deadline
  cleared, `Attempts=0`) — all as `QueuesManager.LifeCycle` does today.
- **Postconditions (callee):** on success, items are durably produced to a dead-letter partition and the
  dead-letter queue's in-memory accounting reflects them; on failure, no source-side state is changed and
  an error is returned so the caller retains the source item.
- **Dedup scope.** The primary anti-duplication guarantee is the ordering above (delete the source item
  only after a confirmed dead-letter produce). `SourceID` dedup is a *secondary* net and is
  **partition-scoped** — each partition's storage keeps its own `SourceID` index (`memory.go`,
  `badger.go`, `mongo.go`). Because the internal produce now distributes opportunistically (ADR-0019), a
  failed-then-retried move could in principle land on a different dead-letter partition than the first
  attempt and escape the partition-local dedup. Delete-after-confirm makes that window narrow, but
  consumers that need dedup as a hard guarantee should use a single-partition dead-letter queue. This is
  not a new limitation — `SourceID` dedup was partition-scoped before this change — but the opportunistic
  distribution makes it worth stating explicitly.

## Testing

Testing follows the `surface-testing` skill. Both tests live in the standard nested
`testXXXX()` structure and run automatically against every backend (InMemory, BadgerDB, …).

Key surfaces:
- **integration (primary / liveness):** create a queue with a configured dead-letter queue; start a
  consumer blocked on `QueueLease` against the **dead-letter queue**; drive a source item to dead-letter
  (exceed `MaxAttempts` / expire via the injectable clock); assert the blocked dead-letter consumer
  receives the item promptly. Fails on `main`, passes after the change. Verifies the liveness behavioral
  constraint and acceptance #1.
- **integration (secondary / state-drift):** use a **separate setup with no consumer** leasing the
  dead-lettered item (do *not* share the primary test's blocked-consumer setup — a leasing consumer
  drops in-memory `unLeased` to 0 and defeats the assertion). Drive an item to dead-letter, then poll
  the **dead-letter queue's public `QueueStats`** with `require.Eventually`; assert the dead-letter
  partition reports `unLeased == total` and a `nextLifecycleRun` advanced toward the item's expiry. Fails
  on `main` (stale in-memory accounting), passes after the change. Verifies the accounting invariant and
  acceptance #2.
- **integration (backpressure / no-loss):** with its own setup, force the internal produce to fail
  (e.g. saturate the dead-letter `requestCh` or a partition failure) and assert via stats that the source
  item is retained and a subsequent pass moves it exactly once. The primary anti-duplication guarantee
  here is delete-after-confirm (the source item is removed only after a confirmed dead-letter produce),
  not `SourceID` dedup — which is a partition-scoped secondary net (see the component-boundary contract).
  To keep the no-duplicate assertion deterministic, use a **single-partition dead-letter queue** for this
  test. Verifies the delete-after-confirm and backpressure constraints and acceptance #4.
- **time handling:** dead-letter timing is driven through the existing injectable `clock.Provider`, not
  wall-clock sleeps.
- **fakes needed:** none beyond the existing multi-backend harness; no new external dependency.

## Limitations & Future Work

- The single-writer invariant remains enforced by convention plus tests, not by the type system. A
  future hardening could move all `Store` write methods behind an unexported, loop-only seam.
- Opportunistic distribution to dead-letter partitions is now in effect; if a future requirement wants
  dead-lettered items co-located deterministically, that would be a separate decision.

## Open Questions

None blocking. All design decisions for this change are resolved (see decision log in the PR /
session); the secondary test's observability surface is settled as first-class `QueueStats` fields.
