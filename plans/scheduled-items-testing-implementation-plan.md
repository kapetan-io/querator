# Scheduled Items Testing Implementation Plan

## Overview

This plan completes the testing coverage for scheduled items in Querator. Scheduled items are queue items that are set to be enqueued at a future time via `EnqueueAt` (during produce) or `RetryAt` (during retry). The lifecycle system monitors these items and moves them to the ready queue when their scheduled time arrives.

## Current State Analysis

### Existing Test Coverage
- **`Produce/Scheduled` test** (`queue_test.go:358-442`): Tests producing items with `EnqueueAt`, verifies items appear in `StorageScheduledList()`, clock advancement, and items becoming leasable.
- **`ScheduledRetry` test** (`retry_test.go:166-197`): Tests retrying with `RetryAt` but **does NOT verify** items become available after scheduled time (incomplete).

### Key Discoveries
- `stats.Scheduled` field exists internally (`types/requests.go:175`) but is NOT exposed in proto (`proto.QueuePartitionStats` lacks field)
- `QueueClear.Scheduled` has `// TODO: Implement` comment - not implemented
- Items scheduled <100ms from now are treated as immediate (optimization at `badger.go:94-105`, `memory.go:53-64`)
- Scheduled items get new IDs when moved to ready queue (Badger/Memory), but same ID preserved (PostgreSQL)

## Desired End State

After this plan is complete:
- Full lifecycle test for `ScheduledRetry` (retry with `RetryAt` -> clock advance -> item leasable)
- `QueuePartitionStats` exposes `scheduled` count via proto
- `QueueClear` with `Scheduled=true` clears only scheduled items
- Tests verify FIFO ordering for multiple items with same scheduled time
- Tests verify near-immediate (<100ms) optimization
- Tests verify past timestamp handling
- Tests verify `StorageScheduledList` pagination
- Tests verify scheduled items + DLQ interaction

### Verification
- `go test ./...` passes all tests
- All storage backends (InMemory, BadgerDB, PostgreSQL) pass scheduled item tests
- Stats API returns accurate `scheduled` count

## What We're NOT Doing

- Not changing the core lifecycle timer mechanism
- Not modifying how scheduled items are stored (same table, `EnqueueAt` field)
- Not implementing `QueueClear.Retry` (separate TODO)
- Not adding new scheduled item APIs beyond what exists

## Implementation Approach

Four phases:
1. **Phase 1**: Complete `ScheduledRetry` lifecycle test
2. **Phase 2**: Expose `stats.Scheduled` in proto and add stats test
3. **Phase 3**: Implement `QueueClear.Scheduled` and add tests
4. **Phase 4**: Add edge case tests (linearizability, near-immediate, past timestamps, pagination)

---

## Phase 1: Complete ScheduledRetry Lifecycle Test

### Overview
Complete the existing `ScheduledRetry` test in `retry_test.go` to verify the full lifecycle: item retried with future `RetryAt` -> appears in `StorageScheduledList` -> clock advances -> item moves to ready queue -> item becomes leasable.

### Changes Required

#### 1. service/retry_test.go
**File**: `service/retry_test.go`
**Changes**: **REPLACE** the existing `ScheduledRetry` test with a complete lifecycle test using frozen clock

**Current test** (lines 166-197) only verifies item is not immediately available. **Delete and replace entirely** with:

```go
t.Run("ScheduledRetry", func(t *testing.T) {
	// Test setup, clock freeze, queue creation, produce, lease
	// ...
	// Retry with future RetryAt
	// Verify item in StorageScheduledList
	// Verify item NOT in StorageItemsList
	// Advance clock
	// Wait for item to appear in StorageItemsList
	// Verify item removed from StorageScheduledList
	// Verify item is leasable with incremented attempts
})
```

**Function responsibilities:**
- Create daemon with frozen clock (follow pattern from `queue_test.go:358-367`)
- Produce item, lease it
- Retry with `RetryAt` set to 1 minute in future
- Verify item appears in `StorageScheduledList()`
- Verify item does NOT appear in `StorageItemsList()`
- Advance clock past `RetryAt` time
- Wait (with retry) for item to appear in `StorageItemsList()`
- Verify item removed from `StorageScheduledList()`
- Lease item again and verify `Attempts` incremented

**Testing Requirements:**
```go
func TestRetry(t *testing.T)
// Existing test function - modify t.Run("ScheduledRetry", ...) inside testRetry()
```

**Test Objectives:**
- Verify scheduled retry items appear in `StorageScheduledList()`
- Verify scheduled retry items do NOT appear in `StorageItemsList()` until time passes
- Verify lifecycle moves items to ready queue after scheduled time
- Verify `Attempts` count is incremented on retry

**Context for implementation:**
- Follow clock freezing pattern from `queue_test.go:358-367`
- Follow retry pattern from existing `retry_test.go` setup
- Use `retry.On()` pattern for waiting (see `queue_test.go:414-424`)

### Validation
- [ ] Run: `go test ./service -run "TestRetry/.*/ScheduledRetry" -v`
- [ ] Verify: Test passes for InMemory, BadgerDB, PostgreSQL backends

---

## Phase 2: Expose Stats.Scheduled in Proto

### Overview
Add `scheduled` field to `QueuePartitionStats` proto message and map the internal `PartitionStats.Scheduled` value to it.

### Changes Required

#### 1. proto/queue.proto
**File**: `proto/queue.proto`
**Changes**: Add `scheduled` field to `QueuePartitionStats` message

Add after line 286 (`averageLeasedAge`):
```protobuf
// Scheduled is the number of scheduled items in the partition
int32 scheduled = 7;
```

#### 2. Regenerate Proto
**Command**: `make proto`

#### 3. service/service.go
**File**: `service/service.go`
**Changes**: Map `stat.Scheduled` to proto response

Update the stats mapping at lines 506-513:
```go
&proto.QueuePartitionStats{
	AverageLeasedAge: stat.AverageLeasedAge.String(),
	Partition:        int32(stat.Partition),
	TotalLeased:      int32(stat.NumLeased),
	Failures:         int32(stat.Failures),
	AverageAge:       stat.AverageAge.String(),
	Total:            int32(stat.Total),
	Scheduled:        int32(stat.Scheduled),  // Add this line
}
```

#### 4. service/queue_test.go
**File**: `service/queue_test.go`
**Changes**: Add test for scheduled count in stats

Add new test within `testQueue()`:
```go
t.Run("Stats/Scheduled", func(t *testing.T) {
	// Setup with frozen clock, create queue
	// Produce scheduled items with future EnqueueAt
	// Call QueueStats and verify Scheduled count
	// Advance clock, wait for items to move
	// Call QueueStats and verify Scheduled count is 0
})
```

**Testing Requirements:**
```go
// New test to add inside testQueue()
t.Run("Stats/Scheduled", func(t *testing.T))
```

**Test Objectives:**
- Verify `QueueStats` returns accurate `Scheduled` count after producing scheduled items
- Verify `Scheduled` count decreases after items move to ready queue

**Context for implementation:**
- Follow existing `Stats` test pattern at `queue_test.go:825-874`
- Use frozen clock pattern from `Produce/Scheduled` test

### Validation
- [ ] Run: `make proto`
- [ ] Verify: Proto regeneration succeeds
- [ ] Run: `go test ./service -run "TestQueue/.*/Stats/Scheduled" -v`
- [ ] Verify: Test passes for all backends

---

## Phase 3: Implement QueueClear.Scheduled

### Overview
Implement the `Scheduled` flag in `QueueClear` to allow clearing only scheduled items from a queue partition.

### Changes Required

#### 1. internal/store/store.go
**File**: `internal/store/store.go`
**Changes**: Update `Clear` interface signature to accept `ClearRequest`

Current signature (line 91):
```go
Clear(ctx context.Context, destructive bool) error
```

New signature:
```go
Clear(ctx context.Context, req types.ClearRequest) error
```

#### 2. internal/store/memory.go
**File**: `internal/store/memory.go`
**Changes**: Implement `ClearRequest.Scheduled` handling

Update `Clear()` method signature and add scheduled item handling:
- When `req.Queue=true`: Existing behavior - clear queue items (respect `Destructive`)
- When `req.Scheduled=true`: Clear scheduled items (items with non-zero `EnqueueAt`)
- Note: Scheduled items are never leased, so `Destructive` flag doesn't affect them

**Function responsibilities:**
- Update method signature to accept `types.ClearRequest`
- Check `req.Scheduled` flag
- If true, iterate items and remove those with non-zero `EnqueueAt`
- Update SourceID index when removing items
- Preserve existing `req.Queue` flag behavior

#### 3. internal/store/badger.go
**File**: `internal/store/badger.go`
**Changes**: Implement `ClearRequest.Scheduled` handling

Update `Clear()` at line 577 with same logic as memory.go but using Badger transaction.

#### 4. internal/store/postgres.go
**File**: `internal/store/postgres.go`
**Changes**: Implement `ClearRequest.Scheduled` handling

Update `Clear()` at line 1251. Use SQL DELETE with `WHERE enqueue_at IS NOT NULL` condition when `req.Scheduled=true`.

#### 5. internal/handlers.go
**File**: `internal/handlers.go`
**Changes**: Update `handleClear()` to pass full `ClearRequest` to storage

Current code at line 139:
```go
if err := l.conf.StoragePartitions[0].Clear(req.Context, cr.Destructive); err != nil {
```

Updated code - pass full ClearRequest and handle Scheduled flag:
```go
if cr.Queue || cr.Scheduled {
    if err := l.conf.StoragePartitions[0].Clear(req.Context, *cr); err != nil {
        req.Err = err
    }
}
```

Also remove the TODO comment at line 143: `// TODO(thrawn01): Support clearing retry and scheduled queues`

#### 6. service/queue_test.go
**File**: `service/queue_test.go`
**Changes**: Add tests for `QueueClear` with `Scheduled` flag

Add within the existing `t.Run("QueueClear", ...)` block (after line 946):
```go
t.Run("Scheduled", func(t *testing.T) {
	// Produce regular items AND scheduled items (with future EnqueueAt)
	// Call QueueClear with Scheduled=true, Queue=false
	// Verify scheduled items removed via StorageScheduledList
	// Verify regular items remain via StorageItemsList
	// Verify stats.Scheduled is 0 after clear
})

t.Run("ScheduledAndQueue", func(t *testing.T) {
	// Produce regular items AND scheduled items
	// Call QueueClear with Scheduled=true, Queue=true
	// Verify both scheduled and regular items removed
})
```

**Testing Requirements:**
```go
// New tests to add inside t.Run("QueueClear", ...) block
t.Run("Scheduled", func(t *testing.T))
t.Run("ScheduledAndQueue", func(t *testing.T))
```

**Test Objectives:**
- Verify `QueueClear` with `Scheduled=true` removes only scheduled items
- Verify regular queue items are preserved when only `Scheduled=true`
- Verify `stats.Scheduled` is updated to 0 after clear
- Verify both flags can be combined

**Context for implementation:**
- Follow existing `QueueClear` test patterns at `queue_test.go:876-946`
- Use frozen clock pattern from `Produce/Scheduled` test to create scheduled items
- Use `StorageScheduledList()` to verify scheduled items
- Use `StorageItemsList()` to verify regular items
- Use `QueueStats()` to verify `stats.Scheduled` count

### Validation
- [ ] Run: `go build ./...`
- [ ] Verify: No compilation errors
- [ ] Run: `go test ./service -run "TestQueue/.*/QueueClear/Scheduled" -v`
- [ ] Verify: Tests pass for all backends
- [ ] Run: `grep "TODO.*Implement" internal/types/requests.go | grep -i scheduled`
- [ ] Verify: `Scheduled` TODO comment removed (line 114)

---

## Phase 4: Edge Case Tests

### Overview
Add tests for edge cases: linearizability (FIFO ordering), near-immediate optimization, past timestamps, and pagination.

### Test Organization

All new tests should be added as **siblings** to the existing `t.Run("Produce/Scheduled", ...)` test at `queue_test.go:358`, NOT nested inside it. The test path will be:
- `TestQueue/<Backend>/Produce/Scheduled/Linearizability`
- `TestQueue/<Backend>/Produce/Scheduled/NearImmediate`
- etc.

### Changes Required

#### 1. service/queue_test.go - Linearizability Test
**File**: `service/queue_test.go`
**Changes**: Add test for FIFO ordering of scheduled items with same timestamp

Add **after** the existing `t.Run("Produce/Scheduled", ...)` block (after line 442):
```go
t.Run("Produce/Scheduled/Linearizability", func(t *testing.T) {
	// Setup with frozen clock, create queue
	// Produce 10 items with SAME EnqueueAt timestamp, each with unique Reference
	// Advance clock past scheduled time
	// Wait for items to appear in StorageItemsList
	// Lease all items in batch
	// Verify items leased in FIFO order (by Reference matching production order)
})
```

**Test Objectives:**
- Verify items with same scheduled time are enqueued in production order
- Verify FIFO ordering preserved when multiple items become ready simultaneously
- Use `Reference` field to track production order

**Specific assertions:**
- `lease.Items[0].Reference` == first produced item's reference
- `lease.Items[9].Reference` == last produced item's reference

#### 2. service/queue_test.go - Near-Immediate Optimization Test
**File**: `service/queue_test.go`
**Changes**: Add test for <100ms optimization

Add after Linearizability test:
```go
t.Run("Produce/Scheduled/NearImmediate", func(t *testing.T) {
	// Setup with frozen clock, create queue
	// Produce item with EnqueueAt = now + 50ms
	// Verify item appears in StorageItemsList immediately (count == 1)
	// Verify item NOT in StorageScheduledList (count == 0)
	// Verify item is immediately leasable
})
```

**Test Objectives:**
- Verify items scheduled <100ms from now are treated as immediate
- Verify they appear in `StorageItemsList()` not `StorageScheduledList()`

**Specific assertions:**
- `len(scheduledList.Items) == 0`
- `len(itemsList.Items) == 1`
- Lease succeeds immediately without clock advance

#### 3. service/queue_test.go - Past Timestamp Test
**File**: `service/queue_test.go`
**Changes**: Add test for past `EnqueueAt` timestamp

```go
t.Run("Produce/Scheduled/PastTimestamp", func(t *testing.T) {
	// Setup with frozen clock, create queue
	// Produce item with EnqueueAt = now - 1 minute (in past)
	// Verify item appears in StorageItemsList immediately
	// Verify item NOT in StorageScheduledList
	// Verify item is immediately leasable
})
```

**Test Objectives:**
- Verify items with past scheduled time are treated as immediate
- Verify they don't appear in `StorageScheduledList()`

#### 4. service/queue_test.go - Pagination Test
**File**: `service/queue_test.go`
**Changes**: Add test for `StorageScheduledList` pagination

```go
t.Run("Produce/Scheduled/Pagination", func(t *testing.T) {
	// Setup with frozen clock, create queue
	// Produce 50 scheduled items with future EnqueueAt
	// List first page: Limit=20, Pivot=""
	// Assert len(page1.Items) == 20
	// List second page: Limit=20, Pivot=page1.Items[19].Id
	// Assert len(page2.Items) == 20
	// List third page: Limit=20, Pivot=page2.Items[19].Id
	// Assert len(page3.Items) == 10 (remaining)
	// Collect all IDs and verify no duplicates
	// Verify total unique items == 50
})
```

**Test Objectives:**
- Verify `StorageScheduledList` pagination works correctly
- Verify `Pivot` parameter allows iterating through all scheduled items
- Verify no duplicate items across pages
- Verify correct item counts per page

#### 5. service/retry_test.go - Past RetryAt Test
**File**: `service/retry_test.go`
**Changes**: Add test for past `RetryAt` timestamp

Add within `testRetry()` function, after `t.Run("QueueRetry", ...)` block:
```go
t.Run("RetryWithPastTimestamp", func(t *testing.T) {
	// Setup queue, produce item, lease it
	// Retry with RetryAt = now - 1 minute (in past)
	// Verify item NOT in StorageScheduledList
	// Verify item immediately available for lease
	// Lease and verify Attempts incremented
})
```

**Test Objectives:**
- Verify retry with past `RetryAt` is treated as immediate retry
- Verify item doesn't go to scheduled queue

#### 6. service/queue_test.go - Scheduled + DLQ Test
**File**: `service/queue_test.go`
**Changes**: Add test within the existing `t.Run("DeadLetterQueue", ...)` block (after line 2081)

```go
t.Run("ScheduledMaxAttempts", func(t *testing.T) {
	// Setup with frozen clock
	// Create DLQ queue (no DLQ of its own)
	// Create source queue with DLQ, MaxAttempts=2, LeaseTimeout=1m
	// Produce item with future EnqueueAt
	// Advance clock past EnqueueAt, wait for item to be ready
	// Lease item (attempt 1)
	// Advance clock to expire lease
	// Wait for item to be re-queued (attempts=1)
	// Lease item (attempt 2)
	// Advance clock to expire lease (now at MaxAttempts)
	// Wait for item to appear in DLQ
	// Verify item removed from source queue
	// Verify DLQ item has correct Reference, SourceId set
})
```

**Test Objectives:**
- Verify scheduled items correctly track attempts across lifecycle
- Verify scheduled items move to DLQ when max attempts exceeded
- Verify DLQ receives item with correct metadata (SourceId set)

**Testing Requirements:**
```go
// New tests to add - all as top-level t.Run() within testQueue() or testRetry()
t.Run("Produce/Scheduled/Linearizability", func(t *testing.T))
t.Run("Produce/Scheduled/NearImmediate", func(t *testing.T))
t.Run("Produce/Scheduled/PastTimestamp", func(t *testing.T))
t.Run("Produce/Scheduled/Pagination", func(t *testing.T))
t.Run("RetryWithPastTimestamp", func(t *testing.T))  // in retry_test.go
t.Run("DeadLetterQueue/ScheduledMaxAttempts", func(t *testing.T))
```

**Context for implementation:**
- Follow existing test patterns in `queue_test.go` and `retry_test.go`
- Use frozen clock for timing-sensitive tests (see `queue_test.go:359-361`)
- Use `retry.On()` with `RetryTenTimes` policy for waiting on lifecycle actions
- Reference field should be unique per item for tracking purposes

### Validation
- [ ] Run: `go test ./service -run "TestQueue/.*/Produce/Scheduled" -v`
- [ ] Verify: All scheduled produce tests pass
- [ ] Run: `go test ./service -run "TestRetry/.*/RetryWithPastTimestamp" -v`
- [ ] Verify: Past timestamp retry test passes
- [ ] Run: `go test ./service -run "TestQueue/.*/DeadLetterQueue/ScheduledMaxAttempts" -v`
- [ ] Verify: Scheduled + DLQ test passes
- [ ] Run: `go test ./...`
- [ ] Verify: Full test suite passes for all backends
