# Logical Queue Simplification Implementation Plan

## Overview

This plan implements **Option B** from the logical-simplification-research.md: keep the Logical Queue abstraction while radically simplifying its implementation. The goal is to reduce complexity by consolidating goroutines and removing unused stubs.

**Note**: Dead Letter Queue implementation is handled in a separate plan (`dlq-implementation-plan.md`). This plan maintains existing DLQ behavior (routing to manager stub which returns nil).

## Current State Analysis

### What Exists Now

**Logical Queue (`internal/logical.go` - 1440 lines)**
- Dual-channel architecture (`hotRequestCh`/`coldRequestCh`) for congestion detection (not implemented)
- Single `requestLoop()` goroutine per logical queue
- Opportunistic Distribution algorithm for partition assignment

**LifeCycle (`internal/lifecycle.go` - 445 lines)**
- Per-partition goroutine model (N+1 total goroutines per queue)
- Dual-timer system (main lifecycle + scheduled items)
- Routes dead items to QueuesManager but manager stub returns nil

**QueuesManager (`internal/queues_manager.go`)**
- Single logical queue hardcoded (line 169-170)
- `LifeCycle()` method is a stub returning nil (line 233-235)

### Key Discoveries

- `internal/logical.go:648-684` - Request loop pattern with select statement
- `internal/lifecycle.go:110-149` - Timer-based lifecycle loop pattern
- `internal/store/store.go:56-134` - Storage interface with `ScanForActions()` and `ScanForScheduled()`
- `internal/types/actions.go:3-9` - Action types: `ActionItemExpired`, `ActionItemMaxAttempts`
- `internal/types/requests.go:178-182` - `LifeCycleRequest` struct

## Desired End State

After implementation:

1. **Single goroutine per logical queue** - LifeCycle logic runs inline in `requestLoop()`
2. **Unified request channel** - Remove dual-channel complexity
3. **~40% code reduction** in core components
4. **Existing behavior preserved** - Including current DLQ stub behavior

### Verification

- All existing tests pass
- Goroutine count reduced from N+1 to 1 per queue
- `logical.go` reduced to ~900 lines
- `lifecycle.go` reduced to ~100 lines (types/helpers only)

## What We're NOT Doing

- **NOT changing the Opportunistic Distribution algorithm** - It's the right design per ADR 0019
- **NOT adding multi-logical queue support** - Deferred until demonstrated need
- **NOT implementing congestion detection** - Remove stubs, add back when needed
- **NOT changing the storage interface** - `ScanForActions()` and `ScanForScheduled()` already exist
- **NOT modifying partition assignment logic** - OD works correctly
- **NOT implementing Dead Letter Queue** - Separate plan handles this

## Implementation Approach

The implementation follows a phased approach where each phase produces a working, testable increment:

1. **Phase 1**: Integrate lifecycle into Logical's requestLoop (removes N goroutines)
2. **Phase 2**: Remove dual-channel and multi-logical stubs

Each phase maintains all existing functionality while incrementally simplifying.

---

## Phase 1: Lifecycle Integration

### Overview

Move lifecycle management from separate per-partition goroutines into the main `requestLoop()`. This eliminates N goroutines per queue while maintaining partition isolation through per-partition timing state.

### Design Decision: Per-Partition Timing with Single Timer

**Why per-partition timing?** A degraded partition (slow, unavailable, timed out) must not block lifecycle processing for healthy partitions. Each partition tracks its own `NextLifecycleRun` time, and failed partitions back off independently while healthy ones continue processing.

**How it works:**
1. Each partition has its own `NextLifecycleRun` and `NextScheduledRun` timestamps
2. A single timer is set to the minimum `NextRun` across all partitions
3. When timer fires, only partitions with `NextRun <= now` are scanned
4. Failed partition scans increment that partition's failure count and back off
5. Healthy partitions are unaffected by degraded ones

### Changes Required

#### 0. Fix Missing Manager Reference

**File**: `internal/queues_manager.go`

**Changes**: Pass QueuesManager reference to LogicalConfig

**Issue Found**: `LogicalConfig.Manager` is currently never set (nil). The current code doesn't panic because the manager's `LifeCycle()` stub is never called with a nil check. This needs to be fixed for lifecycle to route actions to the manager.

At `queues_manager.go:171-186`, add Manager to LogicalConfig:
```go
l, err := SpawnLogicalQueue(LogicalConfig{
    // ... existing fields ...
    Manager: qm,  // ADD THIS
})
```

#### 1. Per-Partition Lifecycle State

**File**: `internal/logical.go`

**Changes**: Add lifecycle state to existing `Partition` struct and queue-level timers to `QueueState`

```go
// PartitionLifecycleState tracks lifecycle timing per partition
type PartitionLifecycleState struct {
    NextLifecycleRun  clock.Time  // When this partition's lifecycle should next run
    NextScheduledRun  clock.Time  // When this partition's scheduled scan should next run
    Failures          int         // Consecutive failure count for backoff
}

// Partition extended with lifecycle state
type Partition struct {
    // ... existing fields (Info, State, Storage, etc.) ...

    Lifecycle PartitionLifecycleState
}

// QueueState with queue-level timers (set to minimum across partitions)
type QueueState struct {
    // ... existing fields ...

    // Single timer set to minimum NextLifecycleRun across all partitions
    LifecycleTimer    clock.Timer
    // Single timer set to minimum NextScheduledRun across all partitions
    ScheduledTimer    clock.Timer
}
```

**Function Responsibilities:**

`prepareQueueState()` - Initialize lifecycle state:
- For each partition, set `Lifecycle.NextLifecycleRun` to `now`
- For each partition, set `Lifecycle.NextScheduledRun` to `now + humanize.LongTime`
- Set `Lifecycle.Failures` to 0
- Create `LifecycleTimer` set to fire immediately (first run)
- Create `ScheduledTimer` set to `humanize.LongTime`
- Follow existing timer pattern from `lifecycle.go:114-115`

#### 2. Request Loop Modification

**File**: `internal/logical.go`

**Changes**: Add lifecycle timer cases to the select statement

```go
func (l *Logical) requestLoop()
```

**Function Responsibilities:**

- Add `case <-state.LifecycleTimer.C():` to select statement
- Call new `runLifecycle()` method when timer fires
- Reset timer to `minNextLifecycleRun(state)` after lifecycle run
- Add `case <-state.ScheduledTimer.C():` for scheduled items
- Call `runScheduled()` and reset timer to `minNextScheduledRun(state)`
- **Keep existing `NextMaintenanceCh` case** - it handles client request timeouts, which is different from item lifecycle
- Follow existing select pattern from `logical.go:648-684`

#### 3. Backoff Configuration

**File**: `internal/logical.go`

**Changes**: Define a package-level backoff for lifecycle operations

```go
var lifecycleBackOff = retry.IntervalBackOff{
    Min:    500 * time.Millisecond,
    Max:    5 * time.Second,
    Factor: 1.5,
    Jitter: 0.2,
}
```

Import `github.com/kapetan-io/tackle/retry` and remove `github.com/duh-rpc/duh-go/retry`.

#### 4. Lifecycle Runner Methods

**File**: `internal/logical.go`

**Changes**: Add methods to run lifecycle actions inline with per-partition isolation

```go
func (l *Logical) runLifecycle(state *QueueState)

func (l *Logical) runScheduled(state *QueueState)

func (l *Logical) minNextLifecycleRun(state *QueueState) clock.Duration

func (l *Logical) minNextScheduledRun(state *QueueState) clock.Duration

func (l *Logical) scanPartitionLifecycle(ctx context.Context, partition *Partition) ([]types.Action, error)

func (l *Logical) scanPartitionScheduled(ctx context.Context, partition *Partition) ([]types.Action, error)
```

**Function Responsibilities:**

`runLifecycle()`:
- Get current time: `now := l.conf.Clock.Now().UTC()`
- Iterate over `state.Partitions`
- **Skip partitions not due**: if `partition.Lifecycle.NextLifecycleRun.After(now)`, continue
- Call `scanPartitionLifecycle()` with per-partition timeout context
- **On error**:
  - Increment `partition.Lifecycle.Failures`
  - Set `NextLifecycleRun` to `now.Add(lifecycleBackOff.Next(partition.Lifecycle.Failures))`
  - Log warning: `l.log.Warn("lifecycle scan failed; retrying...", "partition", partitionNum, "retry", retryIn, "error", err)`
  - Update partition state: `partition.State.Failures = partition.Lifecycle.Failures`
  - Continue to next partition
- **On success**: reset `partition.Lifecycle.Failures` to 0
- Route actions by type (preserve existing behavior from `lifecycle.go:228-248`):
  - `ActionItemExpired`/`ActionItemMaxAttempts` with no DLQ → convert to `ActionDeleteItem`
  - `ActionItemExpired`/`ActionItemMaxAttempts` with DLQ → route to `l.conf.Manager.LifeCycle()` (stub returns nil, items stay - existing behavior)
  - `ActionLeaseExpired` → storage's `TakeAction()`
  - `ActionDeleteItem` → storage's `TakeAction()`
  - `ActionQueueScheduledItem` → storage's `TakeAction()`
- Query `partition.Storage.LifeCycleInfo()` to get `NextLeaseExpiry`
- Update `partition.Lifecycle.NextLifecycleRun` based on `NextLeaseExpiry`
- Follow action routing pattern from `lifecycle.go:218-262`

`runScheduled()`:
- Get current time: `now := l.conf.Clock.Now().UTC()`
- Iterate over `state.Partitions`
- **Skip partitions not due**: if `partition.Lifecycle.NextScheduledRun.After(now)`, continue
- Call `scanPartitionScheduled()` with per-partition timeout context
- **On error**:
  - Increment `partition.Lifecycle.Failures`
  - Set `NextScheduledRun` to `now.Add(lifecycleBackOff.Next(partition.Lifecycle.Failures))`
  - Log warning: `l.log.Warn("scheduled scan failed; retrying...", "partition", partitionNum, "retry", retryIn, "error", err)`
  - Continue to next partition
- **On success**: reset `partition.Lifecycle.Failures` to 0, call storage's `TakeAction()` to enqueue scheduled items
- Update `partition.Lifecycle.NextScheduledRun`
- Follow pattern from `lifecycle.go:169-188`

`minNextLifecycleRun()`:
- Iterate all partitions, find minimum `partition.Lifecycle.NextLifecycleRun`
- Return duration from now to that minimum time
- If minimum is in the past, return 0 (fire immediately)
- If no partitions have pending lifecycle work, return `humanize.LongTime` (timer resets when new items arrive)

`minNextScheduledRun()`:
- Iterate all partitions, find minimum `partition.Lifecycle.NextScheduledRun`
- Return duration from now to that minimum time
- If no partitions have pending scheduled items, return `humanize.LongTime`

`scanPartitionLifecycle()`:
- Create context with `l.conf.ReadTimeout`
- Call `partition.Storage.ScanForActions(timeout, now)`
- Collect actions into slice, return actions and any error
- Follow iterator pattern from `lifecycle.go:227-248`

`scanPartitionScheduled()`:
- Create context with `l.conf.ReadTimeout`
- Call `partition.Storage.ScanForScheduled(timeout, now)`
- Collect `ActionQueueScheduledItem` actions
- Return actions and any error
- Follow pattern from `lifecycle.go:176-188`

#### 5. Remove LifeCycle Goroutine Creation

**File**: `internal/logical.go`

**Changes**: Remove LifeCycle instantiation and startup

Current code at `logical.go:585-605` creates and starts LifeCycle per partition:
```go
for _, p := range l.conf.StoragePartitions {
    lc := NewLifeCycle(...)
    // ...
    lc.Start()
}
```

**Function Responsibilities:**

- Remove the loop that creates `LifeCycle` instances
- Remove `LifeCycle.Start()` calls
- Keep partition setup (storage assignment, state initialization)
- Remove `l.lifeCycles` field from Logical struct

**Shutdown Changes in `handleShutdown()`:**

- Remove the loop calling `p.LifeCycle.Shutdown()` (lines 1342-1349)
- Add timer cleanup before closing partitions:
  ```go
  state.LifecycleTimer.Stop()
  state.ScheduledTimer.Stop()
  ```
- Keep existing logic: cancel leases, drain in-flight requests, close storage partitions

#### 6. Simplify LifeCycle File

**File**: `internal/lifecycle.go`

**Changes**: Reduce to types and helper functions only

**Function Responsibilities:**

- Remove `LifeCycle` struct
- Remove `NewLifeCycle()` constructor
- Remove `requestLoop()` goroutine
- Remove `Start()`, `Shutdown()` methods
- Keep `lifeCycleState` struct (rename to package-level if needed)
- Keep action batching helpers: `batchAction()`, `writeBatch()`
- Keep notification handling logic (move to logical.go or keep as helpers)

### Testing Requirements

**Existing tests that must continue to pass:**
```go
func TestQueue(t *testing.T)           // queue_test.go - All functional tests
func TestQueuesStorage(t *testing.T)   // queues_test.go - Queue CRUD tests
```

**Test Objectives:**
- Verify expired leases are still detected and released
- Verify scheduled items are still enqueued at correct times
- Verify lease timeout behavior unchanged
- Verify partition health tracking still works
- Verify shutdown is clean (no goroutine leaks)
- Verify DLQ actions still route to manager (existing stub behavior preserved)

**Key scenarios to cover:**
- Lease expires → item becomes available for re-lease
- Scheduled item reaches `enqueue_at` → appears in queue
- Multiple partitions → all scanned in single lifecycle run
- Timer fires → lifecycle runs → timer resets correctly
- Degraded partition → backs off independently, other partitions unaffected

### Validation Commands

```bash
# Run all tests
go test ./... -v

# Run with race detector
go test ./... -race

# Verify no goroutine leaks
go test ./... -v -run "TestQueue"

# Check for compilation errors
go build ./...
```

### Context for Implementation

- Request loop pattern: `internal/logical.go:648-684`
- Timer pattern: `internal/lifecycle.go:110-149`
- Action handling: `internal/lifecycle.go:218-262`
- Scheduled handling: `internal/lifecycle.go:169-188`
- Storage interface: `internal/store/store.go:87-103` (`ScanForActions`, `ScanForScheduled`)
- Action types: `internal/types/actions.go:3-9`
- Backoff: `lifecycleBackOff.Next(failures)` using `github.com/kapetan-io/tackle/retry.IntervalBackOff` (500ms min, 5s max, factor 1.5, 20% jitter)

### Implementation Issues Discovered (Not in Original Plan)

The following issues were discovered during implementation and required additional fixes:

#### Issue 1: Lifecycle Actions Not Written to Storage

**Problem**: `runLifecycle()` added actions to `partition.LifeCycleRequests`, but the actions were never executed against storage. In the old design, the lifecycle goroutine sent actions to the logical queue via `l.logical.LifeCycle()`, which triggered `applyToPartitions()` to call `TakeAction()`. With the inline approach, there was no mechanism to execute the collected actions.

**Solution**: Added explicit `TakeAction()` calls in both `runLifecycle()` and `runScheduled()` after collecting actions:
```go
if len(partition.LifeCycleRequests.Requests) > 0 {
    ctx, cancel := context.WithTimeout(context.Background(), l.conf.WriteTimeout)
    partition.Store.TakeAction(ctx, partition.LifeCycleRequests, &partition.State)
    cancel()
    partition.LifeCycleRequests.Reset()
}
```

#### Issue 2: Timer Not Reset After Lease Notification

**Problem**: When items are leased, the old code called `p.LifeCycle.Notify(leaseDeadline)` to inform the lifecycle when to wake up for expired leases. The new implementation updated `p.Lifecycle.NextLifecycleRun` but did not reset the timer, so the timer would fire at its previously scheduled time (potentially hours in the future).

**Solution**: Added timer reset in `handleHotRequests()` after updating partition lifecycle state:
```go
lifecycleUpdated := false
for _, p := range state.Partitions {
    if p.State.NumLeased != 0 {
        now := l.conf.Clock.Now().UTC()
        if p.State.MostRecentDeadline.After(now) && p.State.MostRecentDeadline.Before(p.Lifecycle.NextLifecycleRun) {
            p.Lifecycle.NextLifecycleRun = p.State.MostRecentDeadline
            lifecycleUpdated = true
        }
    }
    p.Reset()
}
if lifecycleUpdated {
    state.LifecycleTimer.Reset(l.minNextLifecycleRun(state))
}
```

#### Issue 3: Existing Notification Code in handleHotRequests

**Discovery**: The codebase already had lifecycle notification code at lines 757-766 in the original `handleHotRequests()`. This handled updating `NextLifecycleRun` when leases were created, but the timer reset was missing. No duplication was needed in `applyToPartitions()`.

---

## Phase 2: Remove Dual-Channel and Multi-Logical Stubs

### Overview

Remove complexity added for features not yet implemented: dual-channel architecture and multi-logical queue support stubs.

### Changes Required

#### 1. Merge Request Channels

**File**: `internal/logical.go`

**Changes**: Replace `hotRequestCh` and `coldRequestCh` with single `requestCh`

```go
type Logical struct {
    // ... other fields ...
    requestCh chan *Request  // Replaces hotRequestCh and coldRequestCh
}
```

**Function Responsibilities:**

In `NewLogical()`:
- Create single `requestCh` instead of two channels
- Remove congestion detection comments/stubs

In `requestLoop()`:
- Replace two channel cases with single `case req := <-l.requestCh:`
- Remove `handleHotRequests()` and `handleColdRequests()` distinction
- Create unified `handleRequest()` method
- Follow simplified select pattern

In public methods (`Produce()`, `Lease()`, `Complete()`, etc.):
- Send to `l.requestCh` instead of `l.hotRequestCh` or `l.coldRequestCh`
- Remove channel selection logic

#### 2. Remove Multi-Logical Stubs

**File**: `internal/queues_manager.go`

**Changes**: Remove code paths for multiple logical queues

Current code at `queues_manager.go:169-186`:
```go
// We currently start with a single logical queue...
```

**Function Responsibilities:**

- Remove TODO comments about adjusting logical queue count
- Remove congestion detection references (lines 41-46)
- Simplify `get()` to always create exactly one logical queue
- Add clear comment: "Single logical queue per queue. Multi-logical support deferred until demonstrated need."

#### 3. Remove Unused Rebalancing Code

**File**: `internal/logical.go`

**Changes**: Remove any rebalancing stubs or partition migration code

**Function Responsibilities:**

- Search for and remove TODO comments about rebalancing
- Remove any unused partition distribution code
- Keep OD algorithm intact (it's used and working)
- Add comment at extension point for future multi-logical support

#### 4. Extract Duplicate Sort Function

**File**: `internal/logical.go`

**Changes**: Replace 3 duplicate sort functions with single helper

```go
func sortPartitionsByLoad(partitions []*Partition)
```

**Function Responsibilities:**

- Extract sort logic from `logical.go:608-620`, `864-876`, `913-925`
- Single function sorts by failures (ascending) then UnLeased (ascending)
- Replace all three call sites with `sortPartitionsByLoad(state.Partitions)`
- Reduces ~26 lines of duplication

### Testing Requirements

**Existing tests that must continue to pass:**
```go
func TestQueue(t *testing.T)           // All functional tests
func TestQueuesStorage(t *testing.T)   // Queue operations
```

**Test Objectives:**
- Verify all request types still processed correctly
- Verify produce/lease/complete/retry work unchanged
- Verify stats and pause operations work
- Verify shutdown is clean

**Key scenarios:**
- All existing test scenarios continue to pass
- No behavior changes, only internal simplification

### Validation Commands

```bash
# Full test suite
go test ./... -v

# Race detector
go test ./... -race

# Verify line count reduction
wc -l internal/logical.go internal/lifecycle.go
```

### Context for Implementation

- Dual-channel definition: `internal/logical.go:99-101`
- Channel creation: `internal/logical.go:127-136`
- Hot/cold handling: `internal/logical.go:696-746`
- Multi-logical TODO: `internal/queues_manager.go:41-46`, `169-170`
- Duplicate sorts: `internal/logical.go:608-620`, `864-876`, `913-925`

---

## Summary

| Phase | Primary Goal | Lines Changed | Risk Level |
|-------|--------------|---------------|------------|
| 1 | Lifecycle Integration | -400 (lifecycle.go), +150 (logical.go) | Medium |
| 2 | Remove Stubs | -200 lines | Low |

**Expected Final State:**
- `logical.go`: ~950 lines (from 1440)
- `lifecycle.go`: ~100 lines (from 445)
- Goroutines per queue: 1 (from N+1)
- All existing tests: Passing
- DLQ behavior: Unchanged (stub returns nil, items stay in source)

**Next Step:** After this plan is complete, implement `dlq-implementation-plan.md` to add full Dead Letter Queue functionality.
