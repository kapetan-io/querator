# Lifecycle Timeout Testing Implementation Plan

## Overview

This plan adds comprehensive tests for the MaxAttempts/LeaseTimeout/ExpireTimeout lifecycle to verify all timeout-based behaviors work correctly across all storage backends (InMemory, BadgerDB, PostgreSQL).

**Scope:** Write tests AND fix any implementation issues discovered during testing. If tests fail due to incomplete or incorrect implementation, fix the implementation.

## Current State Analysis

### Implementation Files (All Backends)
- **Type definitions**: `internal/types/items.go:161-186` - `QueueInfo` with `MaxAttempts`, `LeaseTimeout`, `ExpireTimeout`
- **Lifecycle scan** (all backends implement `ScanForActions()`):
  - `internal/store/badger.go:708-789`
  - `internal/store/memory.go:404-459`
  - `internal/store/postgres.go:1432-1565`
- **Take action** (all backends implement `TakeAction()`):
  - `internal/store/badger.go:791+`
  - `internal/store/memory.go:461-536`
  - `internal/store/postgres.go:1567+`
- **Action routing**: `internal/lifecycle.go:44-90` - Routes actions to DLQ or delete
- **DLQ handling**: `internal/queues_manager.go:231-315` - `LifeCycle()` produces to dead queue

### Existing Tests
- `queue_test.go:2089-2150` - `MaxAttempts` test (no DLQ, item deleted)
- `queue_test.go:2161-2197` - `ExpireTimeout` test (no DLQ, item deleted)

### Key Implementation Details

**Attempts increment on lease** (all backends):
- `badger.go:183`
- `memory.go:92`
- `postgres.go:807`
```go
item.Attempts++
```

**Lease expiry re-queue** (all backends clear lease state and assign new ID):
- `badger.go:826-832`
- `memory.go:482-494`
- `postgres.go:1606-1623`
```go
item.LeaseDeadline = clock.Time{}
item.IsLeased = false
uid = uid.Next()
item.ID = []byte(uid.String())
```

**MaxAttempts check after lease expiry** (`lifecycle.go:46-67`):
```go
case types.ActionLeaseExpired:
    if partition.Info.Queue.MaxAttempts != 0 && a.Item.Attempts >= partition.Info.Queue.MaxAttempts {
        if partition.Info.Queue.DeadQueue != "" {
            // Route to DLQ
            a.Action = types.ActionItemMaxAttempts
            dlqActions = append(dlqActions, a)
        } else {
            // No DLQ - delete
            a.Action = types.ActionDeleteItem
        }
    }
```

## Desired End State

A comprehensive test suite that validates:
1. **LeaseTimeout re-queue** - Item returns to queue with new ID after lease expiry
2. **MaxAttempts with DLQ** - Item routed to dead letter queue when max attempts exceeded
3. **ExpireTimeout with DLQ** - Item routed to dead letter queue when expired
4. **MaxAttempts=0** - Unlimited attempts behavior (item never deleted for max attempts)
5. All tests run against InMemory, BadgerDB, and PostgreSQL backends

### Verification
- Run `go test ./service/... -v -run "TestQueue/.*/Lifecycle"` to execute all lifecycle tests
- Each test verifies item state transitions through public API only
- Tests use frozen clock for deterministic timeout triggering

## What We're NOT Doing

- Adding unit tests for internal functions
- Testing storage layer directly (functional tests via API only)
- Testing error recovery/retry logic for lifecycle failures

**Note:** If tests reveal implementation bugs, we WILL fix them. The tests define expected behavior.

## Implementation Approach

Add new tests to `service/queue_test.go` under a `Lifecycle` test group. Follow existing patterns:
- Use `clock.NewProvider().Freeze()` for deterministic time control
- Use `retry.On()` with `RetryTenTimes` for async state validation
- Use `StorageItemsList` to verify item state
- Use `createQueueAndWait()` for queue setup

## Phase 1: LeaseTimeout Re-queue Test

### Overview
Test that when a leased item's lease expires (without hitting MaxAttempts), it is returned to the queue with:
- `IsLeased = false`
- `LeaseDeadline` cleared
- New ID assigned (moved to front of queue)

### Attempts Counter Behavior
Understanding when `Attempts` changes is critical (same across all backends):
1. Item produced: `Attempts = 0`
2. First lease: `Attempts = 1` (incremented at `badger.go:183`, `memory.go:92`, `postgres.go:807`)
3. Lease expires: `Attempts = 1` (unchanged during re-queue)
4. Second lease: `Attempts = 2` (incremented again)

### Changes Required

#### 1. Add LeaseTimeout Re-queue Test
**File**: `service/queue_test.go`
**Location**: Add within `testQueue()` function, in new `t.Run("Lifecycle", ...)` block

```go
func testQueue(...) {
    // ... existing tests ...

    t.Run("Lifecycle", func(t *testing.T) {
        t.Run("LeaseTimeoutRequeue", func(t *testing.T) { ... })
    })
}
```

**Test Signature:**
```go
t.Run("LeaseTimeoutRequeue", func(t *testing.T)
```

**Test Objectives:**
- Produce a single item to queue (`Attempts = 0`)
- Lease the item (`Attempts` increments to 1) and record its ID
- Advance clock by `2 * clock.Minute` (past LeaseTimeout of 1m)
- Verify item is re-queued: `IsLeased=false`, new ID differs from original, `Attempts` still 1
- Lease the item again and verify `Attempts` incremented to 2

**Context for implementation:**
- Follow pattern from `queue_test.go:2089-2150` (MaxAttempts test)
- Use `clock.NewProvider().Freeze()` and `now.Advance(2 * clock.Minute)` for time control
- Use `findInStorageList()` helper to locate item by reference
- Queue config: `MaxAttempts: 10` (high value to avoid deletion), `LeaseTimeout: "1m0s"`
- Lifecycle scans run asynchronously; use `retry.On(ctx, RetryTenTimes, ...)` to wait for processing

### Validation
- [ ] Run: `go test ./service/... -v -run "TestQueue/.*/Lifecycle/LeaseTimeoutRequeue"`
- [ ] Verify: Item re-queued with new ID, `IsLeased=false`, `Attempts=1` after expiry, `Attempts=2` after second lease

---

## Phase 2: MaxAttempts with Dead Letter Queue

### Overview
Test that when an item exceeds MaxAttempts AND a DeadQueue is configured, the item is moved to the dead letter queue instead of being deleted.

### Attempt Sequence for MaxAttempts=2
1. Lease 1: `Attempts` becomes 1
2. Timeout 1: `Attempts` stays 1, item re-queued
3. Lease 2: `Attempts` becomes 2
4. Timeout 2: `Attempts >= MaxAttempts`, item goes to DLQ

### Changes Required

#### 1. Add MaxAttempts DLQ Test
**File**: `service/queue_test.go`
**Location**: Within `t.Run("Lifecycle", ...)` block

**Test Signature:**
```go
t.Run("MaxAttemptsWithDLQ", func(t *testing.T)
```

**Test Objectives:**
- Create DLQ first, then main queue referencing DLQ
- Produce item to main queue
- Lease/timeout cycle twice (exhaust MaxAttempts)
- Verify item removed from main queue
- Verify item appears in DLQ with correct state

**Queue Setup Pattern:**
```go
// Create DLQ first
dlqName := random.String("dlq-", 10)
createQueueAndWait(t, ctx, c, &pb.QueueInfo{
    QueueName:           dlqName,
    LeaseTimeout:        LeaseTimeout,
    ExpireTimeout:       ExpireTimeout,
    RequestedPartitions: 1,
})

// Create main queue referencing DLQ
queueName := random.String("queue-", 10)
createQueueAndWait(t, ctx, c, &pb.QueueInfo{
    QueueName:           queueName,
    LeaseTimeout:        "1m0s",
    ExpireTimeout:       ExpireTimeout,
    RequestedPartitions: 1,
    MaxAttempts:         2,
    DeadQueue:           dlqName,
})
```

**DLQ Verification Criteria:**
- Item NOT present in main queue (use `StorageItemsList`)
- Item present in DLQ with `SourceID` = original item ID
- Item in DLQ has: `IsLeased=false`, `LeaseDeadline` cleared
- Item fields preserved: `Reference`, `Bytes`, `Encoding`, `Kind`

**Context for implementation:**
- Reference DLQ handling at `lifecycle.go:49-52` and `queues_manager.go:276-295`
- Dead queue items have `SourceID` set (`queues_manager.go:279`)
- Advance clock by `2 * clock.Minute` after each lease to trigger timeout

### Validation
- [ ] Run: `go test ./service/... -v -run "TestQueue/.*/Lifecycle/MaxAttemptsWithDLQ"`
- [ ] Verify: Item transferred to DLQ with SourceID preserved, item fields intact

---

## Phase 3: ExpireTimeout with Dead Letter Queue

### Overview
Test that when an item's ExpireDeadline passes AND a DeadQueue is configured, the item is moved to the dead letter queue. This tests item expiry WITHOUT leasing - the item simply sits in the queue until it expires.

### Changes Required

#### 1. Add ExpireTimeout DLQ Test
**File**: `service/queue_test.go`
**Location**: Within `t.Run("Lifecycle", ...)` block

**Test Signature:**
```go
t.Run("ExpireTimeoutWithDLQ", func(t *testing.T)
```

**Test Objectives:**
- Create DLQ first, then main queue with short ExpireTimeout referencing DLQ
- Produce item to main queue (do NOT lease it)
- Advance clock by `15 * clock.Second` (past ExpireTimeout of 10s)
- Verify item removed from main queue
- Verify item appears in DLQ

**Queue Setup Pattern:**
```go
// Create DLQ first
dlqName := random.String("dlq-", 10)
createQueueAndWait(t, ctx, c, &pb.QueueInfo{
    QueueName:           dlqName,
    LeaseTimeout:        LeaseTimeout,
    ExpireTimeout:       ExpireTimeout,
    RequestedPartitions: 1,
})

// Create main queue with short ExpireTimeout
queueName := random.String("queue-", 10)
createQueueAndWait(t, ctx, c, &pb.QueueInfo{
    QueueName:           queueName,
    LeaseTimeout:        "5s",
    ExpireTimeout:       "10s",
    RequestedPartitions: 1,
    DeadQueue:           dlqName,
})
```

**DLQ Verification Criteria:**
- Item NOT present in main queue
- Item present in DLQ with `SourceID` = original item ID
- Item in DLQ has: `Attempts=0`, `IsLeased=false`
- Item fields preserved: `Reference`, `Bytes`, `Encoding`, `Kind`

**Context for implementation:**
- Reference expiry handling at `lifecycle.go:68-78`
- Expire check at `badger.go:769-780`
- Follow pattern from existing `ExpireTimeout` test at `queue_test.go:2161-2197`

### Validation
- [ ] Run: `go test ./service/... -v -run "TestQueue/.*/Lifecycle/ExpireTimeoutWithDLQ"`
- [ ] Verify: Expired item transferred to DLQ with SourceID and fields preserved

---

## Phase 4: MaxAttempts=0 Unlimited Behavior

### Overview
Test that when `MaxAttempts=0`, items are never deleted due to attempt limits (only ExpireTimeout can remove them).

### Why 5 Cycles?
We use 5 lease/timeout cycles (arbitrary number > 2) to prove the behavior is truly unlimited, not just a high limit. If MaxAttempts were accidentally set to a default like 3, this test would catch it.

### Changes Required

#### 1. Add Unlimited Attempts Test
**File**: `service/queue_test.go`
**Location**: Within `t.Run("Lifecycle", ...)` block

**Test Signature:**
```go
t.Run("UnlimitedAttempts", func(t *testing.T)
```

**Test Objectives:**
- Create queue with `MaxAttempts: 0` (unlimited)
- Produce item (`Attempts = 0`)
- Loop 5 times:
  - Lease the item (`Attempts` increments)
  - Advance clock by `2 * clock.Minute`
  - Wait for re-queue (use `retry.On()`)
  - Verify item is still in queue with expected `Attempts` count
- After 5 cycles: `Attempts = 5`, item still present

**Context for implementation:**
- MaxAttempts=0 check at `lifecycle.go:48`: `if partition.Info.Queue.MaxAttempts != 0 && ...`
- When MaxAttempts=0, the condition is false, so items are re-queued indefinitely
- Queue config: `MaxAttempts: 0`, `LeaseTimeout: "1m0s"`, `ExpireTimeout: "24h0m0s"`

### Validation
- [ ] Run: `go test ./service/... -v -run "TestQueue/.*/Lifecycle/UnlimitedAttempts"`
- [ ] Verify: Item persists through 5 lease/timeout cycles, `Attempts` increments to 5

---

## Phase 5: Test Organization and Structure

### Overview
Organize all lifecycle tests under a single `Lifecycle` test group and move existing tests into this structure for consistency.

### Daemon/Clock Setup Approach
Following the existing pattern at `queue_test.go:1918-2197`:
- **Single frozen clock setup** for the `Lifecycle` group
- **Separate queues** for each test within the group (using `random.String("queue-", 10)`)
- Each test creates its own queue(s) with specific configuration
- Tests are independent and can run in any order (random queue names ensure isolation)

### Changes Required

#### 1. Reorganize Existing Lifecycle Tests
**File**: `service/queue_test.go`

Move existing tests (`MaxAttempts`, `ExpireTimeout`) into the new `Lifecycle` group:

**Final Test Structure:**
```go
t.Run("Lifecycle", func(t *testing.T) {
    // Shared frozen clock setup
    now := clock.NewProvider()
    now.Freeze(clock.Now())
    defer now.UnFreeze()

    d, c, ctx := newDaemon(t, 60*clock.Second, svc.Config{
        StorageConfig: setup(),
        Clock:         now,
    })
    defer func() {
        d.Shutdown(t)
        tearDown()
    }()

    t.Run("LeaseTimeoutRequeue", func(t *testing.T) { ... })      // Phase 1
    t.Run("MaxAttempts", func(t *testing.T) { ... })              // Move existing
    t.Run("MaxAttemptsWithDLQ", func(t *testing.T) { ... })       // Phase 2
    t.Run("ExpireTimeout", func(t *testing.T) { ... })            // Move existing
    t.Run("ExpireTimeoutWithDLQ", func(t *testing.T) { ... })     // Phase 3
    t.Run("UnlimitedAttempts", func(t *testing.T) { ... })        // Phase 4
})
```

#### 2. Remove Commented Code
**File**: `service/queue_test.go`
**Location**: Lines 2199-2210

Remove the commented-out test stubs:
```go
// t.Run("UntilDeadLetter", func(t *testing.T) { ... })
// t.Run("RequestTimeouts", func(t *testing.T) {})
// t.Run("ExpireTimeout", func(t *testing.T) { ... })
```

**Context for implementation:**
- Existing `MaxAttempts` test at lines 2089-2150
- Existing `ExpireTimeout` test at lines 2161-2197
- Queue creation pattern at lines 2079-2087 and 2152-2159
- Both tests share the same frozen clock setup from line 1983

### Validation
- [ ] Run: `go test ./service/... -v -run "TestQueue/.*/Lifecycle"`
- [ ] Verify: All 6 tests pass across InMemory, BadgerDB, and PostgreSQL backends
- [ ] Verify: No commented-out test code remains

---

## Test Infrastructure

### Clock Management Pattern
All lifecycle tests should use frozen clock:
```go
now := clock.NewProvider()
now.Freeze(clock.Now())
defer now.UnFreeze()

d, c, ctx := newDaemon(t, 60*clock.Second, svc.Config{
    StorageConfig: setup(),
    Clock:         now,
})
```

### Retry Policy for Async Validation
```go
err := retry.On(ctx, RetryTenTimes, func(ctx context.Context, i int) error {
    var resp pb.StorageItemsListResponse
    if err := c.StorageItemsList(ctx, queueName, 0, &resp, nil); err != nil {
        return err
    }
    // Validate expected state
    return nil
})
require.NoError(t, err)
```

### Helper Functions
- `findInStorageList(ref string, resp *pb.StorageItemsListResponse)` - Find item by reference
- `createQueueAndWait(t, ctx, c, &pb.QueueInfo{...})` - Create queue and wait for ready
- `RetryTenTimes` - Standard retry policy (20 attempts, 100ms interval)

## Summary

| Phase | Test | What It Validates |
|-------|------|-------------------|
| 1 | LeaseTimeoutRequeue | Lease expiry re-queues item with new ID |
| 2 | MaxAttemptsWithDLQ | Max attempts routes to dead letter queue |
| 3 | ExpireTimeoutWithDLQ | Expiry routes to dead letter queue |
| 4 | UnlimitedAttempts | MaxAttempts=0 allows infinite retries |
| 5 | Organization | Clean test structure, remove dead code |
