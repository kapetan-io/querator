# QueueUpdate Behavioral Tests Implementation Plan

## Overview

This plan implements comprehensive behavioral tests for `QueuesUpdate` to verify that updated queue settings (MaxAttempts, LeaseTimeout, ExpireTimeout, DeadQueue) are correctly respected by subsequent queue operations. The existing tests only verify metadata storage; these new tests verify the runtime behavior.

## Current State Analysis

**Existing Tests** (`service/queues_test.go:139-290`):
- Verify metadata updates are stored correctly
- Verify `UpdatedAt` timestamp is updated
- Verify error validation for invalid inputs

**Missing**: Behavioral tests that verify updated settings affect item lifecycle.

**Key Design Principle**: Queue metadata is the source of truth at evaluation time. Updates are immediate and affect any items during lifecycle evaluation without retroactively modifying items in storage.

### Key Discoveries:
- Lifecycle tests use a controlled clock for time manipulation (`queue_test.go:2082-2088`):
  ```go
  now := clock.NewProvider()
  now.Freeze(clock.Now())
  defer now.UnFreeze()

  d, c, ctx := newDaemon(t, 60*clock.Second, svc.Config{
      StorageConfig: setup(),
      Clock:         now,  // Pass controlled clock to daemon
  })
  ```
- Use `now.Advance()` to simulate time passing (`queue_test.go:2151`)
- Use `retry.On()` to wait for async lifecycle operations (`queue_test.go:2154-2167`)
- `findInStorageList()` helper locates items by reference (`service/common_test.go`)
- `createQueueAndWait()` helper creates queues synchronously
- Tests run against all storage backends automatically (InMemory, BadgerDB, PostgreSQL)
- Tests must be in `package service_test` (external test package)
- Attempts counter increments only on lease, not on re-queue after timeout

### Critical Architecture Constraint:
Behavioral tests that require a controlled clock **MUST NOT** be nested inside the `CRUD` block. The CRUD block has its own daemon running without a controlled clock. If behavioral tests create a second daemon inside CRUD:
- Both daemons may share the same storage backend
- The CRUD daemon's lifecycle scanner (using real time) could interfere with items created by the behavioral test daemon
- This causes race conditions and "item not found" errors

**Solution**: Create a new top-level `t.Run("UpdateBehavior", ...)` block that runs AFTER the CRUD block completes, following the same pattern as the existing `t.Run("Lifecycle", ...)` tests in `queue_test.go`.

## Desired End State

After implementation, the test suite will verify:
1. MaxAttempts updates immediately affect item lifecycle evaluation
2. LeaseTimeout updates affect new/renewed leases only
3. ExpireTimeout updates affect newly produced items only
4. DeadQueue updates affect where failed items are routed

**Verification**: Run `go test ./service/... -v -run "TestQueuesStorage/.*/UpdateBehavior"` and all behavioral tests pass.

## What We're NOT Doing

- Retroactive item updates (explicitly avoided per design)
- Testing concurrent update scenarios
- Performance/stress testing of updates
- Testing partition-related update behavior (RequestedPartitions)

## Implementation Approach

Create a new top-level `t.Run("UpdateBehavior", ...)` block in `testQueues()` that:
1. Creates its own daemon using `newDaemon()` with a controlled clock
2. Runs AFTER all other test blocks (CRUD, List, Errors) complete
3. Contains all behavioral tests for queue updates
4. Follows the exact pattern of the working `Lifecycle` tests in `queue_test.go`

**Structure**:
```go
func testQueues(t *testing.T, setup NewStorageFunc, tearDown func()) {
    // ... existing CRUD, List, Errors blocks ...

    t.Run("UpdateBehavior", func(t *testing.T) {
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

        t.Run("MaxAttempts", func(t *testing.T) {...})
        t.Run("LeaseTimeout", func(t *testing.T) {...})
        t.Run("ExpireTimeout", func(t *testing.T) {...})
        t.Run("DeadQueue", func(t *testing.T) {...})
    })
}
```

The existing TODO stubs inside the CRUD block will be removed (they were placeholders).

## Phase 1: Create UpdateBehavior Block and MaxAttempts Tests

### Overview
Create the new `UpdateBehavior` test block and implement MaxAttempts behavioral tests verifying that MaxAttempts updates immediately affect lifecycle evaluation.

### Changes Required:

#### 1. Remove Existing TODO Stubs from CRUD Block
**File**: `service/queues_test.go`
**Action**: Remove the `t.Run("Respected", ...)` blocks with TODOs from inside the CRUD/Update tests. Keep only the metadata update tests.

#### 2. Add New UpdateBehavior Block
**File**: `service/queues_test.go`
**Location**: At the end of `testQueues()`, after the `Errors` block (around line 1383)

**Test Structure:**
```go
t.Run("UpdateBehavior", func(t *testing.T) {
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

    t.Run("MaxAttempts", func(t *testing.T) {
        t.Run("DecreaseAffectsExistingItems", func(t *testing.T) {...})
        t.Run("IncreaseAllowsMoreAttempts", func(t *testing.T) {...})
        t.Run("SetToUnlimited", func(t *testing.T) {...})
    })
})
```

**Test Responsibilities:**

**DecreaseAffectsExistingItems**:
- Create queue with MaxAttempts=5, DLQ configured
- Produce item, lease it (Attempts becomes 1)
- Update queue to MaxAttempts=2
- Advance time past LeaseTimeout, wait for re-queue
- Lease again (Attempts becomes 2)
- Advance time past LeaseTimeout
- Verify item routes to DLQ (2 >= MaxAttempts)
- Pattern reference: Follow `MaxAttemptsWithDLQ` test at `queue_test.go:2200-2336`

**IncreaseAllowsMoreAttempts**:
- Create queue with MaxAttempts=2, DLQ configured
- Produce item, lease it (Attempts becomes 1)
- Advance time, let lease timeout, wait for re-queue (Attempts stays 1)
- Lease again (Attempts becomes 2) - would normally route to DLQ on next timeout
- Update queue to MaxAttempts=5
- Advance time, let lease timeout
- Verify item is re-queued (NOT in DLQ) because MaxAttempts is now 5
- Verify item can be leased again (Attempts becomes 3)

**SetToUnlimited**:
- Create queue with MaxAttempts=2, DLQ configured
- Produce item, lease it (Attempts becomes 1)
- Update queue to MaxAttempts=0 (unlimited)
- Loop 5+ times: lease, advance time, verify item re-queues
- Verify item never routes to DLQ
- Pattern reference: Follow `UnlimitedAttempts` test at `queue_test.go:2338-2415`

**Testing Requirements:**
```go
func TestQueuesStorage/.../UpdateBehavior/MaxAttempts/DecreaseAffectsExistingItems
func TestQueuesStorage/.../UpdateBehavior/MaxAttempts/IncreaseAllowsMoreAttempts
func TestQueuesStorage/.../UpdateBehavior/MaxAttempts/SetToUnlimited
```

**Test Objectives:**
- Verify MaxAttempts decrease causes items to fail sooner
- Verify MaxAttempts increase allows items more retry attempts
- Verify MaxAttempts=0 (unlimited) prevents DLQ routing regardless of attempts

**Context for Implementation:**
- Use `now.Advance(2 * clock.Minute)` to move past 1m LeaseTimeout
- Use `retry.On(ctx, RetryTenTimes, ...)` to wait for lifecycle operations
- `RetryTenTimes` is defined in `common_test.go`
- Use `findInStorageList(ref, &resp)` to locate items by reference
- DLQ must be created before main queue: `queue_test.go:2201-2208`
- Attempts counter: increments on lease (not on re-queue after timeout)

### Validation
- [ ] Run: `go test ./service/... -v -run "TestQueuesStorage/.*/UpdateBehavior/MaxAttempts"`
- [ ] Verify: All three sub-tests pass for all backends (InMemory, BadgerDB, PostgreSQL)

---

## Phase 2: LeaseTimeout Update Behavioral Tests

### Overview
Add tests verifying that LeaseTimeout updates affect new leases but not existing ones. Items already leased keep their original deadline; new leases use the updated timeout.

### Changes Required:

#### 1. Add LeaseTimeout Tests to UpdateBehavior Block
**File**: `service/queues_test.go`
**Location**: Inside `t.Run("UpdateBehavior", ...)`, after MaxAttempts tests

**Test Structure:**
```go
t.Run("LeaseTimeout", func(t *testing.T) {
    t.Run("NewLeasesUseUpdatedTimeout", func(t *testing.T) {...})
    t.Run("ExistingLeasesKeepOriginalDeadline", func(t *testing.T) {...})
})
```

**Test Responsibilities:**

**NewLeasesUseUpdatedTimeout**:
- Create queue with LeaseTimeout="5m"
- Produce item
- Update queue to LeaseTimeout="1m"
- Lease item, verify LeaseDeadline is ~1 minute from now (not 5)
- Advance time 2 minutes
- Wait for item to be re-queued (lease expired at ~1m mark)
- Verify item is no longer leased

**ExistingLeasesKeepOriginalDeadline**:
- Create queue with LeaseTimeout="5m"
- Produce item
- Lease item (LeaseDeadline set to ~5m from now)
- Update queue to LeaseTimeout="1m"
- Advance time 2 minutes
- Verify item is STILL leased (original 5m deadline not yet passed)
- Advance time 4 more minutes (total 6m, past original deadline)
- Wait for item to be re-queued
- Verify item is no longer leased
- **Note**: This verifies the natural consequence of not retroactively modifying items in the queue - not a special "protection" feature

**Testing Requirements:**
```go
func TestQueuesStorage/.../UpdateBehavior/LeaseTimeout/NewLeasesUseUpdatedTimeout
func TestQueuesStorage/.../UpdateBehavior/LeaseTimeout/ExistingLeasesKeepOriginalDeadline
```

**Test Objectives:**
- Verify new leases respect the updated LeaseTimeout
- Verify existing items are not retroactively modified (LeaseDeadline unchanged)

**Context for Implementation:**
- Uses same daemon/clock from UpdateBehavior parent block
- LeaseDeadline visible via `StorageItemsList`: `queue_test.go:1963-1968`
- Compare deadline: `item.LeaseDeadline.AsTime()` against `now.Now().Add(expectedDuration)`
- Wait for re-queue using `retry.On()` pattern after `now.Advance()`
- Use `assert.True(t, item.LeaseDeadline.AsTime().Before(expectedTime))` for comparisons

### Validation
- [ ] Run: `go test ./service/... -v -run "TestQueuesStorage/.*/UpdateBehavior/LeaseTimeout"`
- [ ] Verify: Both sub-tests pass for all backends

---

## Phase 3: ExpireTimeout Update Behavioral Tests

### Overview
Add tests verifying that ExpireTimeout updates affect newly produced items only. Existing items keep their original ExpireDeadline.

### Changes Required:

#### 1. Add ExpireTimeout Tests to UpdateBehavior Block
**File**: `service/queues_test.go`
**Location**: Inside `t.Run("UpdateBehavior", ...)`, after LeaseTimeout tests

**Test Structure:**
```go
t.Run("ExpireTimeout", func(t *testing.T) {
    t.Run("NewItemsUseUpdatedTimeout", func(t *testing.T) {...})
    t.Run("ExistingItemsUnaffected", func(t *testing.T) {...})
})
```

**Test Responsibilities:**

**NewItemsUseUpdatedTimeout**:
- Create queue with ExpireTimeout="10m"
- Update queue to ExpireTimeout="2m"
- Produce item AFTER the update
- Verify item's ExpireDeadline is ~2 minutes from now (not 10)
- Use StorageItemsList to inspect the item's ExpireDeadline

**ExistingItemsUnaffected**:
- Create queue with ExpireTimeout="10m"
- Produce item (ExpireDeadline set to ~10m)
- Record the original ExpireDeadline
- Update queue to ExpireTimeout="2m"
- Verify item's ExpireDeadline is UNCHANGED (still ~10m from creation)
- Use StorageItemsList to verify ExpireDeadline matches original

**Testing Requirements:**
```go
func TestQueuesStorage/.../UpdateBehavior/ExpireTimeout/NewItemsUseUpdatedTimeout
func TestQueuesStorage/.../UpdateBehavior/ExpireTimeout/ExistingItemsUnaffected
```

**Test Objectives:**
- Verify newly produced items use the updated ExpireTimeout
- Verify existing items retain their original ExpireDeadline

**Context for Implementation:**
- Uses same daemon/clock from UpdateBehavior parent block
- ExpireDeadline visible via `StorageItemsList`: `queue_test.go:294-296`
- Use `item.ExpireDeadline.AsTime()` to compare deadlines
- Produce items via `c.QueueProduce()`: `queue_test.go:93-102`
- Compare times with tolerance for test execution latency

### Validation
- [ ] Run: `go test ./service/... -v -run "TestQueuesStorage/.*/UpdateBehavior/ExpireTimeout"`
- [ ] Verify: Both sub-tests pass for all backends

---

## Phase 4: DeadQueue Update Behavioral Tests

### Overview
Add tests verifying that DeadQueue updates affect where failed items are routed. Items that exceed MaxAttempts or ExpireTimeout should route to the currently configured DLQ.

### Changes Required:

#### 1. Add DeadQueue Tests to UpdateBehavior Block
**File**: `service/queues_test.go`
**Location**: Inside `t.Run("UpdateBehavior", ...)`, after ExpireTimeout tests

**Test Structure:**
```go
t.Run("DeadQueue", func(t *testing.T) {
    t.Run("AddDLQAfterCreation", func(t *testing.T) {...})
    t.Run("ChangeDLQRoutesNewFailures", func(t *testing.T) {...})
    t.Run("RemoveDLQDeletesFailedItems", func(t *testing.T) {...})
})
```

**Test Responsibilities:**

**AddDLQAfterCreation**:
- Create DLQ queue
- Create main queue WITHOUT DeadQueue, MaxAttempts=2
- Produce item
- Update main queue to set DeadQueue=dlqName
- Exhaust attempts (lease, timeout, lease, timeout)
- Verify item routes to DLQ (not deleted)
- Pattern reference: `MaxAttemptsWithDLQ` at `queue_test.go:2200-2336`

**ChangeDLQRoutesNewFailures**:
- Create DLQ-A and DLQ-B queues
- Create main queue with DeadQueue=DLQ-A, MaxAttempts=2
- Produce item-1
- Exhaust item-1 attempts, verify it routes to DLQ-A
- Update main queue to set DeadQueue=DLQ-B
- Produce item-2
- Exhaust item-2 attempts, verify it routes to DLQ-B (not DLQ-A)

**RemoveDLQDeletesFailedItems**:
- Create DLQ queue
- Create main queue with DeadQueue=dlqName, MaxAttempts=2
- Update main queue to remove DeadQueue (set to "")
- Produce item
- Exhaust attempts (lease, timeout, lease, timeout)
- Verify item is deleted (not in main queue, not in DLQ)

**Testing Requirements:**
```go
func TestQueuesStorage/.../UpdateBehavior/DeadQueue/AddDLQAfterCreation
func TestQueuesStorage/.../UpdateBehavior/DeadQueue/ChangeDLQRoutesNewFailures
func TestQueuesStorage/.../UpdateBehavior/DeadQueue/RemoveDLQDeletesFailedItems
```

**Test Objectives:**
- Verify adding a DLQ enables routing for subsequent failures
- Verify changing DLQ routes new failures to the updated queue
- Verify removing DLQ causes failed items to be deleted

**Context for Implementation:**
- Uses same daemon/clock from UpdateBehavior parent block
- DLQ routing verified via `StorageItemsList` on both queues: `queue_test.go:2318-2331`
- Use `findInStorageList()` to check item presence/absence
- SourceId is set when item moves to DLQ: `queue_test.go:2324-2325`
- To clear DeadQueue: `c.QueuesUpdate(ctx, &pb.QueueInfo{QueueName: name, DeadQueue: ""})`
- Wait for item removal using `retry.On()` pattern

### Validation
- [ ] Run: `go test ./service/... -v -run "TestQueuesStorage/.*/UpdateBehavior/DeadQueue"`
- [ ] Verify: All three sub-tests pass for all backends

---

## Phase 5: Test Organization Cleanup

### Overview
Remove stale TODO placeholders from CRUD block and verify final test structure.

### Changes Required:

#### 1. Remove Stale TODOs from CRUD Block
**File**: `service/queues_test.go`
**Action**: Remove the `t.Run("Respected", ...)` blocks containing TODOs from:
- `t.Run("MaxAttempts", ...)` - remove lines 167-552 (the Respected block with behavioral tests)
- `t.Run("LeaseTimeout", ...)` - remove the TODO at line 582-583
- `t.Run("ExpireTimeout", ...)` - remove the TODO at line 611-613

The CRUD/Update tests should only contain metadata verification tests, not behavioral tests.

#### 2. Verify Final Test Structure
**File**: `service/queues_test.go`

Ensure final structure is:
```
testQueues()
├── t.Run("CRUD", func(t *testing.T) {
│   ├── t.Run("Create", ...)
│   ├── t.Run("GetByPartition", ...)
│   ├── t.Run("Update", func(t *testing.T) {
│   │   ├── t.Run("MaxAttempts", ...)      // Metadata test only
│   │   ├── t.Run("LeaseTimeout", ...)     // Metadata test only
│   │   ├── t.Run("ExpireTimeout", ...)    // Metadata test only
│   │   ├── t.Run("Reference", ...)
│   │   └── t.Run("Everything", ...)
│   │   })
│   ├── t.Run("Delete", ...)
│   └── t.Run("DeadLetterQueue", ...)
│   })
├── t.Run("List", ...)
├── t.Run("Errors", ...)
└── t.Run("UpdateBehavior", func(t *testing.T) {    // NEW BLOCK
    │   // Controlled clock daemon created here
    ├── t.Run("MaxAttempts", func(t *testing.T) {
    │   ├── t.Run("DecreaseAffectsExistingItems", ...)
    │   ├── t.Run("IncreaseAllowsMoreAttempts", ...)
    │   └── t.Run("SetToUnlimited", ...)
    │   })
    ├── t.Run("LeaseTimeout", func(t *testing.T) {
    │   ├── t.Run("NewLeasesUseUpdatedTimeout", ...)
    │   └── t.Run("ExistingLeasesKeepOriginalDeadline", ...)
    │   })
    ├── t.Run("ExpireTimeout", func(t *testing.T) {
    │   ├── t.Run("NewItemsUseUpdatedTimeout", ...)
    │   └── t.Run("ExistingItemsUnaffected", ...)
    │   })
    └── t.Run("DeadQueue", func(t *testing.T) {
        ├── t.Run("AddDLQAfterCreation", ...)
        ├── t.Run("ChangeDLQRoutesNewFailures", ...)
        └── t.Run("RemoveDLQDeletesFailedItems", ...)
        })
    })
```

### Validation
- [ ] Run: `go test ./service/... -v -run "TestQueuesStorage"` (comprehensive - all backends, all tests)
- [ ] Verify: All tests pass for all backends (InMemory, BadgerDB, PostgreSQL)
- [ ] Run: `go test ./... -v -count=1` to ensure no regressions across entire codebase
- [ ] Verify: No TODO comments remain in the Update test section (grep for "TODO" in queues_test.go)
- [ ] Verify: CRUD block no longer contains behavioral tests with controlled clocks
