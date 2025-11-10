# Logical Queue Simplification Research

**Date:** 2025-11-09
**Status:** Research Complete
**Recommendation:** Option B - Simplify Logical Implementation, Keep Abstraction

---

## Executive Summary

After comprehensive analysis of the Querator codebase and architecture decision records (ADRs), this document recommends **keeping the Logical Queue abstraction** while **radically simplifying its implementation**. This approach:

- **Reduces complexity by ~1000 lines of code** (40% reduction in core components)
- **Simplifies Dead Letter Queue implementation** dramatically
- **Maintains all design goals** from ADR 0002 (simple clients, efficiency-first)
- **Preserves the solution to Kafka's partition problem** (ADR 0016)
- **Keeps single-threaded efficiency model** (Redis-style, ADR 0003)

The key insight: **Logical Queue is not optional complexity** — it's a fundamental abstraction that hides partition complexity from consumers. The complexity problem is in the *implementation*, not the *design*.

---

## Current Architecture Analysis

### Three-Layer Hierarchy

```
Service (service.go)
    ↓
QueuesManager (queues_manager.go)
    - Manages queue lifecycle & persistence
    - Handles partition assignment across storage backends
    ↓
Queue (queue.go)
    - Lightweight wrapper
    - Routes requests to logical queues
    ↓
Logical (logical.go - 1440 lines)
    - Single-threaded synchronization point
    - Manages subset of partitions
    - Dual-channel architecture (hot/cold requests)
    - Opportunistic partition distribution
    ↓
LifeCycle (lifecycle.go - per partition)
    - Separate goroutine for each partition
    - Handles expired leases
    - Moves scheduled items
    - Sends items to dead letter queue
    ↓
Partitions (store/*.go)
    - Physical storage backends
    - InMemory, BadgerDB, (future: PostgreSQL, MySQL, etc.)
```

### Complexity Drivers

**A. Logical Queue Synchronization** (`internal/logical.go` - 1440 lines)

- **Dual-channel architecture:**
  - `hotRequestCh` - Congestion-aware (Produce, Lease, Complete, Retry)
  - `coldRequestCh` - Management operations (Stats, Pause, Clear, etc.)
  - Rationale: Separate "hot path" from management for congestion detection
  - Problem: Adds complexity, congestion detection not yet implemented

- **Request batching and distribution:**
  - Opportunistic Partition Distribution (OD) algorithm
  - Partitions sorted by failures, then UnLeased count
  - Complex assignment logic per request type (produce, lease, complete, retry)

- **State management:**
  - `QueueState` struct holds in-flight request state
  - Must synchronize with `l.conf.StoragePartitions`
  - TODO at line 1351: "We should merge these two into one"

- **Partition health tracking:**
  - LifeCycle sends async updates via `PartitionStateChange`
  - Logical maintains synchronous state in `QueueState.Partitions`
  - Storage backends report failures via error returns
  - Client requests trigger partition re-sorting

**B. LifeCycle Management** (`internal/lifecycle.go`)

- **Per-partition goroutine:** Each partition gets dedicated lifecycle manager (N goroutines)
- **Dual-timer system:**
  - `timer` - Main lifecycle actions (expired leases, dead items)
  - `scheduleTimer` - Scheduled item enqueueing (ADR 0024)
- **Action batching:** LifeCycle generates actions, sends to Logical for batched writes
- **State propagation:** Complex handshake: LifeCycle → Logical → Storage

**C. Unimplemented Features Creating Complexity**

1. **Multi-Logical Queue Rebalancing** (ADR 0016, 0017)
   - Currently hardcoded to single logical queue (queues_manager.go:170)
   - Congestion detection framework not implemented
   - TODO: "adjust the number of Logical Queues depending on the number of consumers"

2. **Partition Migration** (ADR 0023)
   - Read-only partition concept defined (queue.go:9-11)
   - Rebalancing operations planned but not implemented
   - Validation of external partition modifications missing (logical.go:572)

3. **Cluster Operation** (ADR 0017)
   - Leader election proposed
   - Evaluation loop and reconciliation loop designed
   - Not yet implemented

4. **Dead Letter Queue** (ADR 0022)
   - Mentioned throughout codebase
   - API spec includes `dead_queue` field
   - CLI supports `--dead-queue` flag
   - **No actual implementation of moving items to DLQ**
   - No validation preventing circular references

---

## Dead Letter Queue Implementation Challenges

### Current State

| Component | Status | Notes |
|-----------|--------|-------|
| API Spec | ✅ Defined | `openapi.yaml` includes `dead_queue` field |
| CLI | ✅ Defined | `--dead-queue` flag exists |
| ADR | ✅ Written | ADR 0022 describes responsibilities |
| Implementation | ❌ Missing | No actual item movement code |
| Validation | ❌ Missing | No circular reference checks |
| Tests | ❌ TODO | `queues_test.go:296-303` has TODOs |

### Implementation Gaps (from ADR 0022:42-56)

When items exceed `max_attempts` or `dead_deadline`, LifeCycle should:

1. **Send item to dead letter queue** if configured
2. **Remove item from source partition** on success
3. **Log and delete item** if no DLQ configured

### Current Complexity Blockers

**1. Cross-Queue Coordination**
```go
// LifeCycle needs to call another queue's Logical instance
// Problem: Creates circular dependency chain
Logical → LifeCycle → QueuesManager → Logical
```

- LifeCycle running in separate goroutine per partition
- Must call `QueuesManager.get()` to access DLQ
- Potential for lock contention and deadlocks
- Requires careful error handling across goroutine boundaries

**2. Partition Assignment**
```go
// DLQ items need opportunistic distribution to DLQ partitions
// Problem: Requires calling DLQ's Logical.Produce() from source LifeCycle
```

- DLQ has its own partitions and Logical Queue
- Source LifeCycle must produce to DLQ's Logical
- Deadlock risk if DLQ's Logical is busy/unavailable
- No clear ownership of DLQ produce request lifecycle

**3. Transaction Boundaries**
```go
// Delete from source + Produce to DLQ must be atomic
// Problem: Not all storage backends support cross-partition transactions
```

- Failure modes create zombie items or duplicates
- Source partition delete succeeds, DLQ produce fails → item lost
- DLQ produce succeeds, source delete fails → duplicate
- Requires complex compensation logic

**4. Validation Complexity**

Current validation missing (from `queues_test.go:296-303`):
- ❌ Prevent circular DLQ chains (A → B → A)
- ❌ Prevent self-referencing DLQ (A → A)
- ❌ Verify DLQ exists before queue creation
- ❌ Verify DLQ has no DLQ of its own

---

## Why Logical Queue Must Remain

### Critical Insight from ADR 0016

**The Kafka Problem:**
```
Queue with 100 partitions + Kafka-style design = 100 consumers required
```

Kafka requires one consumer per partition. This creates massive client complexity:
- Clients must track all partition assignments
- Clients must implement partition-aware load balancing
- Adding partitions requires client code changes
- Consumer group coordination protocol complexity

**Querator's Solution: Logical Queue**

From ADR 0016:59-65:

> "The Logical Queue is designed to avoid the issue seen in Kafka, where one consumer is required per partition. For example, if a Queue has 100 partitions and only one consumer, Querator will create a single Logical Queue that round-robins across all 100 partitions."

```
Queue with 100 partitions + Logical Queue = 1 consumer works perfectly
```

### Logical Queue Provides

1. **Consumer Abstraction**
   - Clients connect to ONE Logical Queue
   - Logical handles partition distribution transparently
   - Clients have no partition awareness

2. **Cross-Partition Load Balancing**
   - Single Logical serves items from all assigned partitions
   - Opportunistic distribution balances load automatically
   - Failed partitions handled transparently

3. **Simple Client Complexity** (ADR 0002 design goal)
   - Clients just call Lease() and Complete()
   - No partition routing logic needed
   - No consumer group protocol needed

4. **Cross-Partition Batching**
   - Logical can batch writes across multiple partitions
   - Optimize transaction usage per storage backend
   - Reduce network round trips

5. **Future Horizontal Scaling** (ADR 0017)
   - Multiple Logical Queues can serve same Queue
   - Distribute logicals across Querator instances
   - Scale without client changes

### Design Principle Alignment

**ADR 0002: Design Goals**
> "Complexity is handled by the server, with very simple client implementations."

**Eliminating Logical Queue would:**
- ❌ Push partition awareness to clients
- ❌ Recreate Kafka's consumer complexity
- ❌ Violate core design principle
- ❌ Require client updates when partitions change

**Keeping Logical Queue:**
- ✅ Hides all partition complexity
- ✅ Maintains simple client implementation
- ✅ Enables transparent scaling
- ✅ Aligns with efficiency goals

---

## Recommended Simplification: Option B (Modified)

**Goal:** Keep Logical Queue abstraction, radically simplify implementation

### Change 1: Integrate LifeCycle into Logical requestLoop

**Current Architecture:**
```
Logical.requestLoop() (1 goroutine)
    +
LifeCycle[partition_0].requestLoop() (1 goroutine)
    +
LifeCycle[partition_1].requestLoop() (1 goroutine)
    +
...
    +
LifeCycle[partition_N].requestLoop() (1 goroutine)

Total: N+1 goroutines
```

**Simplified Architecture:**
```
Logical.requestLoop() (1 goroutine) {
    // Handle client requests
    // Handle lifecycle actions inline
    // Scan partitions for expired leases
    // Move scheduled items
    // Send to dead letter queue
}

Total: 1 goroutine
```

**Implementation:**
```go
func (l *Logical) requestLoop() {
    state := QueueState{
        nextLifecycleRun: l.conf.Clock.Now().UTC().Add(30 * time.Second),
        lifecycleTimer:   l.conf.Clock.NewTimer(30 * time.Second),
    }

    for {
        select {
        case req := <-l.requestCh:
            l.handleRequest(&state, req)

        case <-state.lifecycleTimer.C:
            // Inline lifecycle: scan all partitions
            l.runLifecycle(&state)
            state.lifecycleTimer.Reset(l.calculateNextLifecycle(&state))

        case <-l.shutdownCh:
            return
        }
    }
}

func (l *Logical) runLifecycle(state *QueueState) {
    for _, partition := range state.Partitions {
        // Scan for expired leases
        l.handleExpiredLeases(partition)

        // Scan for scheduled items
        l.handleScheduledItems(partition)

        // Scan for dead items (max_attempts, dead_deadline)
        l.handleDeadItems(partition)
    }
}
```

**Benefits:**
- ✅ Eliminate N goroutines (one per partition)
- ✅ Single goroutine owns all state (no synchronization needed)
- ✅ Easier DLQ implementation (no cross-goroutine coordination)
- ✅ Deterministic execution order (easier testing)
- ✅ ~500 lines removed from lifecycle.go

**Trade-offs:**
- ⚠️ Lifecycle scanning blocks request processing momentarily
- ⚠️ Can't scan partitions in parallel (but probably unnecessary)
- Mitigation: Keep lifecycle scans fast, limit items per scan

### Change 2: Remove Dual-Channel Complexity

**Current:**
```go
hotRequestCh  chan *Request  // Congestion-aware (Produce, Lease, etc.)
coldRequestCh chan *Request  // Management (Stats, Pause, etc.)
```

**Simplified:**
```go
requestCh chan *Request  // All requests
```

**Rationale:**
- Congestion detection not yet implemented
- Adding complexity for future feature
- Can be added back when actually needed
- ~200 lines simplified

### Change 3: Accept Single-Logical Reality

**Current state:**
- Code supports multiple logical queues per queue
- `QueuesManager.get()` always creates single logical (queues_manager.go:170)
- Multi-logical rebalancing not implemented
- Dead code paths exist for multi-logical support

**Simplification:**
```go
// Remove all multi-logical code paths until demonstrated need:
// - Remove rebalancing logic
// - Remove congestion detection stubs
// - Remove logical queue distribution code

// Add prominent TODO:
// TODO: Multi-logical support can be added later when single-logical
// becomes a demonstrated bottleneck in production. Current design
// supports this extension, but implementation is deferred until needed.
```

**Benefits:**
- ✅ ~300 lines removed
- ✅ Clearer code (no unused branches)
- ✅ Easier maintenance
- ✅ Clear extension point for future

### Change 4: Simplified DLQ Implementation

**With integrated lifecycle (single goroutine):**

```go
func (l *Logical) handleDeadItems(partition *Partition) error {
    // Scan partition for items exceeding max_attempts or dead_deadline
    deadItems := partition.Storage.ScanForDeadItems(l.conf.ReadTimeout)

    for _, item := range deadItems {
        if l.conf.DeadQueue != "" {
            // Produce to DLQ via QueuesManager
            // Simple because we're in the same goroutine
            err := l.conf.Manager.ProduceToQueue(ctx, types.ProduceRequest{
                QueueName: l.conf.DeadQueue,
                Items:     []*types.Item{item},
            })
            if err != nil {
                l.log.Error("failed to send to dead letter queue",
                    "item_id", item.ID, "dead_queue", l.conf.DeadQueue, "error", err)
                continue // Retry on next lifecycle run
            }
        }

        // Remove from source partition (logged if no DLQ)
        if err := partition.Storage.Delete(ctx, item.ID); err != nil {
            l.log.Error("failed to delete dead item",
                "item_id", item.ID, "error", err)
            continue // Retry on next lifecycle run
        }

        if l.conf.DeadQueue == "" {
            l.log.Warn("item exceeded max_attempts, deleted (no dead_queue configured)",
                "item_id", item.ID, "attempts", item.Attempts)
        }
    }

    return nil
}
```

**Validation at queue creation:**
```go
func (qm *QueuesManager) validateDeadQueue(ctx context.Context, info types.QueueInfo) error {
    if info.DeadQueue == "" {
        return nil
    }

    // DLQ must exist
    dlq, err := qm.get(ctx, info.DeadQueue)
    if err != nil {
        return transport.NewInvalidOption(
            "dead_queue '%s' does not exist; create it first", info.DeadQueue)
    }

    // Prevent self-reference
    if info.DeadQueue == info.Name {
        return transport.NewInvalidOption(
            "dead_queue cannot reference itself")
    }

    // Prevent circular reference
    if dlq.conf.DeadQueue != "" {
        return transport.NewInvalidOption(
            "dead_queue '%s' cannot have its own dead_queue; dead letter queues must have dead_queue=''",
            info.DeadQueue)
    }

    return nil
}
```

**Benefits:**
- ✅ No cross-goroutine coordination needed
- ✅ No deadlock risk (single thread)
- ✅ Simple error handling (retry on next cycle)
- ✅ Clear validation rules
- ✅ ~300 lines for complete implementation

### Change 5: Lifecycle as Timer in requestLoop

**Dual-timer for lifecycle and scheduled items:**

```go
type QueueState struct {
    // ... existing fields ...

    // Lifecycle timing
    nextLifecycleRun   time.Time
    lifecycleTimer     *clock.Timer

    // Scheduled items timing
    nextScheduledRun   time.Time
    scheduledTimer     *clock.Timer

    // Partitions
    Partitions         []*Partition
}

func (l *Logical) requestLoop() {
    state := l.prepareQueueState()

    for {
        select {
        case req := <-l.requestCh:
            l.handleRequest(&state, req)

        case <-state.lifecycleTimer.C:
            l.runLifecycle(&state)
            state.lifecycleTimer.Reset(l.calculateNextLifecycle(&state))

        case <-state.scheduledTimer.C:
            l.runScheduled(&state)
            state.scheduledTimer.Reset(l.calculateNextScheduled(&state))

        case req := <-l.shutdownCh:
            l.handleShutdown(&state, req)
            return
        }
    }
}
```

**Benefits:**
- ✅ Maintains ADR 0024 scheduled items design
- ✅ All timing in one place
- ✅ Easy to adjust lifecycle frequency based on partition state
- ✅ Deterministic for testing

---

## Implementation Roadmap

### Phase 1: Lifecycle Integration (~2-3 weeks)

**Complexity Reduction: ~40%**

1. **Refactor LifeCycle into Logical**
   - Move `runLifecycle()` logic into `logical.go`
   - Add lifecycle timer to `QueueState`
   - Remove per-partition `LifeCycle` struct
   - Update tests to verify lifecycle behavior

2. **Update Partition Interface**
   - Add `ScanForExpiredLeases()` iterator method
   - Add `ScanForScheduledItems()` iterator method
   - Add `ScanForDeadItems()` iterator method
   - Implement for InMemory and BadgerDB

3. **Testing**
   - Verify lifecycle timing
   - Verify expired lease handling
   - Verify scheduled item enqueueing
   - Benchmark performance impact

**Expected Results:**
- ~500 lines removed (lifecycle.go mostly eliminated)
- Simpler goroutine model (N+1 → 1)
- Foundation for DLQ implementation

### Phase 2: DLQ Implementation (~1-2 weeks)

**New Code: ~300 lines**

1. **Add Validation**
   - Implement `validateDeadQueue()` in QueuesManager
   - Add to `Create()` and `Update()` flows
   - Write validation tests

2. **Implement Dead Item Handling**
   - Add `handleDeadItems()` to Logical
   - Integrate into `runLifecycle()`
   - Add `QueuesManager.ProduceToQueue()` helper

3. **Testing**
   - Test max_attempts exhaustion
   - Test dead_deadline expiration
   - Test circular reference prevention
   - Test DLQ not found error handling
   - Integration tests across storage backends

**Expected Results:**
- Working dead letter queue feature
- ~300 new lines of well-tested code
- Closes major feature gap

### Phase 3: Remove Multi-Logical Stubs (~1 week)

**Complexity Reduction: ~20%**

1. **Identify Dead Code**
   - Search for multi-logical support code
   - Identify rebalancing stubs
   - Find congestion detection placeholders

2. **Safe Removal**
   - Remove unused code paths
   - Add TODO comments for future extension
   - Update ADR 0016 implementation notes

3. **Simplify Dual-Channel**
   - Merge `hotRequestCh` and `coldRequestCh`
   - Simplify request routing
   - Update tests

**Expected Results:**
- ~300 lines removed
- Clearer codebase
- Easier maintenance

### Total Impact

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| logical.go | 1440 lines | ~900 lines | -37% |
| lifecycle.go | ~600 lines | ~100 lines | -83% |
| Total core | 2040 lines | ~1000 lines | -51% |
| Goroutines (10 partitions) | 11 | 1 | -91% |
| DLQ implementation | Missing | Complete | ✅ |

---

## Single-Thread Design Validation

### Current Design Already Correct

**Your single-threaded Logical Queue design is sound** and aligns with proven patterns:

**Redis-Style Efficiency (ADR 0003):**
- ✅ Single requestLoop goroutine per logical
- ✅ Opportunistic batching of requests
- ✅ Minimize lock contention
- ✅ Efficient I/O bundling

**Scalability Model:**
- ✅ Scale via partitions (not threads)
- ✅ Single partition = FIFO guarantee
- ✅ Multiple partitions = higher throughput, relaxed ordering
- ✅ Future: Multiple logicals = horizontal scale

**Design Goal Alignment (ADR 0002):**
- ✅ Efficiency over raw performance
- ✅ Simple client implementation
- ✅ Complexity handled by server

### Don't Change the Threading Model

**Recommendation: Keep single-threaded Logical, simplify implementation.**

**What to do:**
- ✅ Integrate lifecycle into requestLoop (remove goroutines)
- ✅ Remove unneeded complexity (multi-logical stubs)
- ✅ Implement DLQ properly (now straightforward)
- ✅ Trust the design (ADR 0003, 0016 are sound)

**What NOT to do:**
- ❌ Add threading to Logical Queue
- ❌ Make lifecycle parallel across partitions
- ❌ Introduce goroutine pools or work queues
- ❌ Break single-threaded synchronization point

### Why Single-Thread Works

**From ADR 0003:**
> "The code path to get an item into or out of the queue should result in as little blocking or mutex lock contention as possible."

**From ADR 0016:**
> "The Logical Queue acts as a single-threaded synchronization point for all the partitions it manages, thus allowing many of the optimizations and benefits described in [3. RW Sync Point]"

**Key insight:** Redis proved single-threaded I/O can be extremely efficient. Querator applies this to distributed queues by:

1. Single thread handles all synchronization (no locks)
2. Batching maximizes I/O efficiency
3. Partitions provide horizontal scaling
4. Multiple logicals (future) provide instance scaling

This is **not a limitation** — it's an **intentional design choice** for efficiency and simplicity.

---

## References

### Architecture Decision Records

- **ADR 0002: Design Goals** - Efficiency-first, simple clients
- **ADR 0003: R/W Sync Point** - Single-threaded synchronization rationale
- **ADR 0016: Queue Partitions** - Logical Queue design, solving Kafka problem
- **ADR 0017: Cluster Operation** - Future multi-logical distribution
- **ADR 0019: Partition Item Distribution** - Opportunistic distribution algorithm
- **ADR 0022: Managing Item Lifecycles** - LifeCycle responsibilities, DLQ handling
- **ADR 0023: Partition Maintenance** - Partition states and operations
- **ADR 0024: Scheduled Items** - Scheduled item timing and distribution

### Key Codebase Locations

- `internal/logical.go` (1440 lines) - Main Logical Queue implementation
- `internal/lifecycle.go` (~600 lines) - Per-partition lifecycle management
- `internal/queues_manager.go` - Queue lifecycle and persistence
- `internal/queue.go` - Queue wrapper and routing
- `internal/store/memory.go` - InMemory storage backend
- `internal/store/badger.go` - BadgerDB storage backend
- `internal/types/` - Shared types and request/response structs

### Test Coverage

- `queue_test.go` - Functional tests across storage backends
- `queues_test.go:296-303` - DLQ test TODOs
- `retry_test.go:192` - DeadLetter test case

---

## Conclusion

The Logical Queue abstraction is **fundamental to Querator's design**, not optional complexity. It solves the Kafka partition problem, maintains simple client implementation, and enables future horizontal scaling.

**The complexity is in the implementation, not the design.**

**Recommended Action:** Implement Option B (Modified)
1. Integrate LifeCycle into Logical requestLoop
2. Implement Dead Letter Queue
3. Remove multi-logical stubs

This achieves:
- ✅ 50% code reduction in core components
- ✅ Working DLQ feature
- ✅ Maintains all design goals
- ✅ Simpler, more maintainable codebase
- ✅ Clear path to future scaling

**Next Steps:** Review this research with team, begin Phase 1 implementation planning.
