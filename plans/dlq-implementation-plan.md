# Dead Letter Queue Implementation Plan

## Overview

This plan implements full Dead Letter Queue (DLQ) functionality for Querator. Items that exceed `max_attempts` or `expire_deadline` are moved to a configured dead letter queue for manual inspection and reprocessing.

**Prerequisite**: This plan assumes `logical-simplification-plan.md` has been completed. The simplified codebase makes DLQ implementation cleaner.

## Current State Analysis

### What Exists Now

- `DeadQueue` field exists on `QueueInfo` (`internal/types/items.go:158`)
- CLI supports `--dead-queue` flag
- API spec includes `dead_queue` field in queue creation
- `QueuesManager.LifeCycle()` is a stub returning nil (`queues_manager.go:219-221`)
- Lifecycle identifies dead items and routes to manager, but manager does nothing
- No validation prevents circular DLQ references
- Test TODOs at `queues_test.go:296-303`

### The Problem

Items are identified as dead but never moved:
1. Storage returns `ActionItemExpired` or `ActionItemMaxAttempts`
2. Lifecycle routes to manager via `writeManagerActions()` (or inline after simplification)
3. Manager's `LifeCycle()` returns nil (stub)
4. Items remain in partition forever (silent failure)

## Desired End State

After implementation:

1. **DLQ Validation** - Circular references and self-references prevented at queue creation/modification
2. **Item Movement** - Dead items moved to DLQ with new FIFO-ordered ID
3. **Idempotency** - `SourceID` field prevents duplicates on crash recovery
4. **Safety** - Items never lost, never duplicated

### Verification

- All existing tests pass
- New DLQ functional tests pass
- Items exceeding `max_attempts` appear in DLQ
- Items exceeding `expire_deadline` appear in DLQ
- Duplicate `SourceID` items rejected by storage

## What We're NOT Doing

- **NOT implementing DLQ-to-DLQ chains** - A DLQ cannot have its own DLQ
- **NOT adding automatic retry from DLQ** - Manual intervention required
- **NOT changing the storage interface** - Only adding `SourceID` field handling

## Design Decisions

### Decision 1: SourceID for Idempotency

**Problem**: When moving items to DLQ, we need:
1. **New ID** for FIFO ordering (KSUIDs are time-ordered, reusing old ID breaks FIFO)
2. **Idempotency** to prevent duplicates if crash occurs between DLQ produce and source delete

**Solution**: Add `SourceID` field to `Item` struct:
- DLQ items get a new time-ordered KSUID for proper FIFO ordering
- `SourceID` stores the original item ID for duplicate detection
- Storage backends enforce uniqueness on `SourceID` within partitions
- Enables tracing DLQ items back to their source

**Safety guarantees**:
1. Delete from source only AFTER successful DLQ produce
2. If crash after DLQ produce but before source delete:
   - Next lifecycle finds item in source, tries to move again
   - DLQ storage detects duplicate `SourceID`, ignores insert
   - Source delete succeeds on retry
3. Items are never lost, never duplicated in DLQ

### Decision 2: No DLQ Chains

**Rule**: A queue configured as a DLQ cannot have its own DLQ.

**Enforcement**: At queue creation/modification time:
- If `info.DeadQueue != ""`, fetch the target queue
- Check if target's `DeadQueue` is empty
- If target has a DLQ, reject with error

**Rationale**: Items in a DLQ have already "failed" - they require manual intervention, not automatic cascading through multiple queues.

### Decision 3: ProduceToQueue via QueuesManager

**Architecture**:
```
Logical.runLifecycle()
    ↓ identifies dead items
    ↓ calls l.conf.Manager.LifeCycle(ctx, &request)
    ↓
QueuesManager.LifeCycle()
    ↓ calls qm.ProduceToQueue(ctx, deadQueueName, items)
    ↓
QueuesManager.ProduceToQueue()
    ↓ gets DLQ queue, calls logical.ProduceInternal()
    ↓
Logical.ProduceInternal() [on DLQ]
    ↓ writes directly to storage (bypasses requestCh)
```

**Why direct storage write?** Called from different queue's context (different goroutine), so safe. Avoids channel round-trip latency.

## Implementation Approach

The implementation follows a phased approach:

1. **Phase 1**: Add `SourceID` field and storage backend support
2. **Phase 2**: DLQ Validation at queue creation/modification
3. **Phase 3**: DLQ Item Movement (implement `LifeCycle()` method)

### Testing Philosophy

**All tests must use the public client API** following the functional testing philosophy documented in `CLAUDE.md`. This means:
- Tests use client methods like `c.QueueProduce()`, `c.QueueLease()`, `c.StorageItemsList()`, etc.
- Tests never call internal storage methods directly
- Tests are placed in `queue_test.go` within `testQueue()` or `queues_test.go` within `testQueues()`
- Tests verify behavior through observable outcomes, not internal state

---

## Phase 1: SourceID Field and Storage Support

### Overview

Add the `SourceID` field to `Item` and update all storage backends to handle duplicate detection.

### Changes Required

#### 1. Add SourceID to Item

**File**: `internal/types/items.go`

**Changes**: Add `SourceID` field for DLQ idempotency

```go
type Item struct {
    ID        ItemID  // Storage-generated KSUID, unique within partition
    SourceID  ItemID  // Original ID from source queue (only set for DLQ items, nil otherwise)
    // ... existing fields unchanged
}
```

**Function Responsibilities:**

- Add `SourceID ItemID` field after `ID` field
- Update `ToProto()` to include `SourceID`: `in.SourceId = string(i.SourceID)`
- Update `FromProto()` to include `SourceID`: `i.SourceID = []byte(in.SourceId)`
- Update `Compare()` to include `SourceID` comparison

#### 2. Update Proto Definition

**File**: `proto/querator.proto`

**Changes**: Add `source_id` field to `StorageItem` message

```protobuf
message StorageItem {
    string id = 1;
    string source_id = 2;  // NEW: Original ID for DLQ items
    // ... existing fields
}
```

**Function Responsibilities:**

- Add field to proto
- Run `make proto` to regenerate

#### 3. Memory Storage Update

**File**: `internal/store/memory.go`

**Changes**: Handle `SourceID` duplicate detection on produce

```go
type MemoryPartition struct {
    // ... existing fields
    bySourceID map[string]struct{}  // Index for SourceID deduplication
}
```

**Function Responsibilities:**

In `Produce()`:
- Before inserting item, check `if item.SourceID != nil`
- If `SourceID` set and exists in `bySourceID`, skip item (no error)
- If `SourceID` set and not exists, add to `bySourceID` after insert

In `Delete()`:
- If deleted item has `SourceID`, remove from `bySourceID`

In constructor:
- Initialize `bySourceID: make(map[string]struct{})`

#### 4. BadgerDB Storage Update

**File**: `internal/store/badger.go`

**Changes**: Handle `SourceID` duplicate detection via secondary index

**Function Responsibilities:**

In `Produce()`:
- If `item.SourceID != nil`:
  - Check for existing key: `source:{sourceID}`
  - If exists, skip item (no error, idempotent)
  - If not exists, write secondary index key alongside item

In `Delete()`:
- If deleted item has `SourceID`, delete secondary index key

Key format: `source:{sourceID}` → `{itemID}`

#### 5. PostgreSQL Storage Update

**File**: `internal/store/postgres.go`

**Changes**: Add `source_id` column with conditional unique index

**Function Responsibilities:**

In `ensureTable()`:
- Add column: `source_id TEXT`
- Add partial unique index: `CREATE UNIQUE INDEX IF NOT EXISTS {table}_source_id_idx ON {table} (source_id) WHERE source_id IS NOT NULL`

In `Produce()`:
- Include `source_id` in INSERT
- Use `ON CONFLICT (source_id) DO NOTHING` for idempotency

In `List()` and scan methods:
- Include `source_id` in SELECT and scan

### Testing Requirements

**Important**: All tests must use the public client API following the functional testing philosophy. Tests should be added to `queue_test.go` within `testQueue()`, using `c.StorageItemsImport()` and `c.StorageItemsList()` to test SourceID functionality.

**New tests (nested under existing test structure in `testQueue()`):**
```go
t.Run("SourceID", func(t *testing.T) {
    t.Run("ImportWithSourceID", func(t *testing.T) {
        // Use c.StorageItemsImport() to import item with SourceID set
        // Use c.StorageItemsList() to verify SourceID is preserved
    })
    t.Run("DuplicateSourceIDIgnored", func(t *testing.T) {
        // Import item with SourceID via c.StorageItemsImport()
        // Import second item with same SourceID
        // Verify only one item exists via c.StorageItemsList()
    })
    t.Run("SourceIDPreservedOnList", func(t *testing.T) {
        // Import item with SourceID
        // List via c.StorageItemsList()
        // Verify SourceID field is populated in response
    })
})
```

**Test Objectives:**
- Verify items with `SourceID` can be imported via public API
- Verify duplicate `SourceID` items are silently ignored (no error)
- Verify `SourceID` is preserved when listing items via public API
- Verify `SourceID` is nil/empty for regular (non-DLQ) items

### Validation Commands

```bash
# Regenerate proto
make proto

# Run all tests
go test ./... -v

# Run storage-specific tests
go test ./... -v -run "TestQueue/.*/SourceID"
```

### Context for Implementation

- Item struct: `internal/types/items.go:22-58`
- Memory storage: `internal/store/memory.go`
- BadgerDB storage: `internal/store/badger.go`
- PostgreSQL storage: `internal/store/postgres.go`
- Proto definition: `proto/querator.proto`

---

## Phase 2: DLQ Validation

### Overview

Add validation at queue creation/modification time to prevent invalid DLQ configurations.

### Changes Required

#### 1. Add Validation Function

**File**: `internal/queues_manager.go`

**Changes**: Add validation function for DLQ configuration

```go
func (qm *QueuesManager) validateDeadQueue(ctx context.Context, info types.QueueInfo) error
```

**Function Responsibilities:**

- Return nil if `info.DeadQueue == ""`
- Prevent self-reference: if `info.DeadQueue == info.Name`, return error:
  - `transport.NewInvalidOption("dead_queue cannot reference itself")`
- Fetch DLQ info: call `qm.get(ctx, info.DeadQueue)` to load the target queue
- If DLQ not found, return error:
  - `transport.NewInvalidOption("dead_queue '%s' does not exist; create it first", info.DeadQueue)`
- **Enforce no DLQ chains**: inspect fetched DLQ's `DeadQueue` field
  - If `dlq.Info().DeadQueue != ""`, return error:
  - `transport.NewInvalidOption("dead_queue '%s' cannot have its own dead_queue", info.DeadQueue)`
- Return nil if all validations pass

#### 2. Integrate into Create

**File**: `internal/queues_manager.go`

**Changes**: Call validation before creating queue

In `Create()`, before `qm.conf.StorageConfig.Queues.Add()`:
```go
if err := qm.validateDeadQueue(ctx, info); err != nil {
    return nil, err
}
```

#### 3. Integrate into Update

**File**: `internal/queues_manager.go`

**Changes**: Call validation before updating queue

In `Update()`, before applying changes:
```go
if err := qm.validateDeadQueue(ctx, info); err != nil {
    return err
}
```

### Testing Requirements

**Important**: All tests must use the public client API. Tests should be added to `queues_test.go` within `testQueues()`, under the existing `t.Run("DeadLetterQueue"...)` section at line 296.

**New tests (nested under existing `testQueues()` DeadLetterQueue section):**
```go
t.Run("DeadLetterQueue", func(t *testing.T) {
    t.Run("Validation", func(t *testing.T) {
        t.Run("NonExistentDLQ", func(t *testing.T) {
            // Use c.QueuesCreate() with non-existent DeadQueue
            // Verify error via require.Error() and assert error message
        })
        t.Run("SelfReference", func(t *testing.T) {
            // Use c.QueuesCreate() with DeadQueue = queue name
            // Verify error message contains "cannot reference itself"
        })
        t.Run("DLQHasDLQ", func(t *testing.T) {
            // Create queue C (no DLQ)
            // Create queue B with DeadQueue = C
            // Attempt to create queue A with DeadQueue = B
            // Verify error message contains "cannot have its own dead_queue"
        })
        t.Run("ValidDLQ", func(t *testing.T) {
            // Create DLQ queue (no DLQ of its own)
            // Create source queue with valid DeadQueue reference
            // Verify success via require.NoError()
        })
    })
})
```

**Test Objectives:**
- Create queue with non-existent DLQ → error with message "does not exist"
- Create queue with self-reference DLQ → error with message "cannot reference itself"
- Create queue A with DLQ B where B has DLQ C → error with message "cannot have its own dead_queue"
- Create queue with valid DLQ (exists, no DLQ of its own) → success

### Validation Commands

```bash
# Run all tests
go test ./... -v

# Run DLQ validation tests
go test ./... -v -run "DeadLetterQueue/Validation"
```

### Context for Implementation

- Queue creation: `internal/queues_manager.go:83-126`
- Queue update: `internal/queues_manager.go:205-231`
- Error patterns: `transport.NewInvalidOption()` usage throughout
- Test TODOs: `queues_test.go:296-303`

---

## Phase 3: DLQ Item Movement

### Overview

Implement the actual movement of dead items from source queue to DLQ.

### Changes Required

#### 1. Add ProduceToQueue Helper

**File**: `internal/queues_manager.go`

**Changes**: Add centralized method for producing to any queue by name

```go
func (qm *QueuesManager) ProduceToQueue(ctx context.Context, queueName string, items []*types.Item) error
```

**Function Responsibilities:**

- Get queue by name: `q, err := qm.get(ctx, queueName)`
- Return error if queue not found
- Get next logical queue: `_, logical := q.GetNext()`
- Call `logical.ProduceInternal(ctx, items)`
- Return error if produce fails

#### 2. Add ProduceInternal to Logical

**File**: `internal/logical.go`

**Changes**: Add method for internal produce (bypasses client validation)

```go
func (l *Logical) ProduceInternal(ctx context.Context, items []*types.Item) error
```

**Function Responsibilities:**

- **Does NOT go through requestCh** - writes directly to storage
- Safe because called from different queue's context (different goroutine)
- Use OD algorithm to select partition:
  - Sort partitions by failures then UnLeased (reuse `sortPartitionsByLoad`)
  - Select first healthy partition
- For each item:
  - Assign new KSUID for FIFO ordering (storage will generate)
  - `SourceID` is already set by caller
- Batch write to selected partition's storage via `partition.Storage.Produce()`
- Return error on storage failure (caller will retry)

#### 3. Implement LifeCycle Method

**File**: `internal/queues_manager.go`

**Changes**: Replace stub with actual implementation

```go
func (qm *QueuesManager) LifeCycle(ctx context.Context, req *types.LifeCycleRequest) error
```

**Function Responsibilities:**

- Get source queue info from request's partition info to find `DeadQueue` name
- Group actions: collect all `ActionItemExpired` and `ActionItemMaxAttempts`
- If no dead actions, return nil

For dead actions:
- Get source partition storage for deletion
- If `DeadQueue` is empty:
  - Log warning for each item: "item '%s' exceeded max_attempts, deleting (no dead_queue configured)"
  - Delete items from source partition via storage
  - Return nil

- If `DeadQueue` is set:
  - Prepare items for DLQ:
    - Set `SourceID` = original `ID`
    - Clear `ID` (new one will be assigned by DLQ storage)
    - Reset state: `IsLeased = false`, `LeaseDeadline = zero`, `Attempts = 0`
  - Call `qm.ProduceToQueue(ctx, deadQueueName, items)`
  - If produce fails:
    - Log error: "failed to produce to dead_queue '%s': %v"
    - Return error (items stay in source, will retry next lifecycle)
  - If produce succeeds:
    - Delete items from source partition
    - If delete fails, log error but don't fail (items are in DLQ, source cleanup will happen next cycle)
  - Return nil

#### 4. Ensure Lifecycle Routes to Manager

**File**: `internal/logical.go`

**Changes**: Verify `runLifecycle()` routes DLQ actions to manager

In `runLifecycle()` (from simplification plan), ensure:
- `ActionItemExpired` and `ActionItemMaxAttempts` with DLQ configured → call `l.conf.Manager.LifeCycle()`
- Batch actions by partition before calling manager
- Handle manager errors: log and continue (retry next cycle)

### Testing Requirements

**Important**: All tests must use the public client API. Tests should be added to `queue_test.go` within `testQueue()` in a new `t.Run("DeadLetterQueue"...)` section.

**New tests (nested under `testQueue()`):**
```go
t.Run("DeadLetterQueue", func(t *testing.T) {
    t.Run("MaxAttempts", func(t *testing.T) {
        // Use c.QueuesCreate() to create DLQ and source queues
        // Use c.QueueProduce() to add item
        // Use c.QueueLease() and c.QueueRetry() to exhaust attempts
        // Use c.StorageItemsList() to verify item moved to DLQ
    })
    t.Run("ExpireDeadline", func(t *testing.T) {
        // Similar pattern using public API
    })
    t.Run("NoDLQConfigured", func(t *testing.T) {
        // Verify item deleted when no DLQ configured
    })
    t.Run("Idempotency", func(t *testing.T) {
        // Use c.StorageItemsImport() with duplicate SourceID
        // Verify deduplication via c.StorageItemsList()
    })
})
```

**Test Objectives:**

MaxAttempts test:
- Create DLQ queue via `c.QueuesCreate()` (no DLQ of its own)
- Create source queue with DLQ configured, `max_attempts = 2`
- Produce item via `c.QueueProduce()`
- Lease item via `c.QueueLease()`, retry via `c.QueueRetry()` (attempts = 1)
- Lease item, retry (attempts = 2)
- Lease item, retry → should trigger DLQ movement
- Verify item appears in DLQ via `c.StorageItemsList()` with `SourceID` set
- Verify item no longer in source queue
- Verify DLQ item has new ID (different from `SourceID`)

ExpireDeadline test:
- Create DLQ queue via `c.QueuesCreate()`
- Create source queue with DLQ configured, short `expire_timeout`
- Produce item via `c.QueueProduce()`
- Wait for expiry (use clock manipulation or short timeout)
- Verify item appears in DLQ via `c.StorageItemsList()`
- Verify item no longer in source

NoDLQConfigured test:
- Create queue with no DLQ via `c.QueuesCreate()`
- Produce item, exhaust attempts via public API
- Verify item is deleted via `c.StorageItemsList()` (not in source, not in any DLQ)

Idempotency test:
- Import item to DLQ with `SourceID = "abc"` via `c.StorageItemsImport()`
- Attempt to import another item with same `SourceID`
- Verify second import succeeds (no error)
- Verify DLQ contains exactly one item via `c.StorageItemsList()`

### Validation Commands

```bash
# Run all tests
go test ./... -v

# Run DLQ functional tests
go test ./... -v -run "DeadLetterQueue"

# Run with race detector
go test ./... -race -run "DeadLetterQueue"
```

### Context for Implementation

- LifeCycle stub: `internal/queues_manager.go:219-221`
- Action routing: `internal/logical.go:1346-1367` (runLifecycle method)
- Queue access: `internal/queues_manager.go:167-186`
- Partition produce: `internal/store/store.go:60-62`

---

## Summary

| Phase | Primary Goal | Risk Level |
|-------|--------------|------------|
| 1 | SourceID field + storage support | Low |
| 2 | DLQ validation | Low |
| 3 | DLQ item movement | Medium |

**Expected Final State:**
- DLQ validation prevents invalid configurations
- Dead items move to DLQ automatically
- `SourceID` prevents duplicates on crash recovery
- Items are never lost
- All existing tests pass
- New DLQ tests pass

**Dependencies:**
- Phase 1 must complete before Phase 3 (storage needs `SourceID` support)
- Phase 2 can run in parallel with Phase 1
- Phase 3 requires both Phase 1 and Phase 2
