# Remove Logical.LifeCycle() Method Implementation Plan

## Overview

Remove the unused public `Logical.LifeCycle()` method and the `MethodLifeCycle` constant with all associated request routing code. This method was designed to allow external triggering of lifecycle actions but is never called - lifecycle processing is handled internally by the lifecycle timer (`runLifecycle()`).

## Current State Analysis

The `Logical.LifeCycle()` method exists at `internal/logical.go:380-404` but is never called externally. It:
- Accepts a `LifeCycleRequest` and sends it to the request channel with `MethodLifeCycle`
- Gets routed through the main request loop as a "hot request"
- Eventually calls `partition.Store.TakeAction()` with the batched requests

The internal lifecycle timer (`runLifecycle()`) handles all lifecycle actions automatically, making this public API redundant.

### Key Discoveries:
- `MethodLifeCycle` constant at `internal/logical.go:40`
- `Logical.LifeCycle()` method at `internal/logical.go:380-404`
- Multiple switch case handlers reference `MethodLifeCycle`
- **IMPORTANT**: `QueuesManager.LifeCycle()` at `internal/queues_manager.go:219-221` IS called from `runLifecycle()` at line 1452 for dead letter queue routing - this must be PRESERVED

## Desired End State

After this plan is complete:
- No `MethodLifeCycle` constant exists
- No `Logical.LifeCycle()` public method exists
- All switch cases handling `MethodLifeCycle` are removed
- `QueuesManager.LifeCycle()` is PRESERVED for future DLQ implementation
- Internal lifecycle processing continues to work via `runLifecycle()` and timers
- All existing tests pass

### Verification:
- `go build ./...` succeeds with no errors
- `go test ./...` passes all tests
- `grep -r "MethodLifeCycle" internal/` returns no results
- `grep "func (l \*Logical) LifeCycle" internal/logical.go` returns no results
- `grep "func (qm \*QueuesManager) LifeCycle" internal/queues_manager.go` DOES return a result (preserved)

## What We're NOT Doing

- NOT removing `types.LifeCycleRequest` struct (used by internal `runLifecycle()`)
- NOT removing `types.LifeCycleInfo` struct (used by store's `LifeCycleInfo()`)
- NOT removing `Partition.LifeCycleRequests` batch field (used internally)
- NOT removing `TakeAction()` or `LifeCycleInfo()` store interface methods
- NOT removing `state.LifeCycles` from `QueueState` (used by internal processing)
- NOT removing `runLifecycle()`, `runScheduled()`, or timer infrastructure
- **NOT removing `QueuesManager.LifeCycle()`** - it is called at line 1452 and serves as a placeholder for future dead letter queue implementation

## Implementation Approach

Single phase removal - all changes are localized to `internal/logical.go`.

## Phase 1: Remove LifeCycle Method and MethodLifeCycle Constant

### Overview
Remove the public `Logical.LifeCycle()` method and `MethodLifeCycle` constant with all switch case handlers from `internal/logical.go`.

### Changes Required:

#### 1. internal/logical.go
**File**: `internal/logical.go`
**Changes**: Remove constant, method, and all switch case handlers

**Removals:**

1. **Remove `MethodLifeCycle` constant** (line 40):
   ```go
   // DELETE this line from the iota block:
   MethodLifeCycle
   ```
   Note: Go handles iota renumbering automatically; no manual adjustment needed.

2. **Remove `Logical.LifeCycle()` method** (lines 380-404):
   ```go
   // DELETE entire method (25 lines):
   func (l *Logical) LifeCycle(ctx context.Context, req *types.LifeCycleRequest) error {
       if l.inShutdown.Load() {
           return ErrQueueShutdown
       }

       r := Request{
           ReadyCh: make(chan struct{}),
           Method:  MethodLifeCycle,
           Context: ctx,
           Request: req,
       }

       select {
       case l.requestCh <- &r:
       case <-ctx.Done():
           return ctx.Err()
       }

       select {
       case <-r.ReadyCh:
           return r.Err
       case <-ctx.Done():
           return ctx.Err()
       }
   }
   ```

3. **Update `handleRequest()` switch case** (line 690):
   ```go
   // CHANGE FROM:
   case MethodProduce, MethodLease, MethodComplete, MethodRetry, MethodLifeCycle:
   // CHANGE TO:
   case MethodProduce, MethodLease, MethodComplete, MethodRetry:
   ```

4. **Update `isHotRequest()` switch case** (line 771):
   ```go
   // CHANGE FROM:
   case MethodProduce, MethodLease, MethodComplete, MethodRetry, MethodLifeCycle:
   // CHANGE TO:
   case MethodProduce, MethodLease, MethodComplete, MethodRetry:
   ```

5. **Remove `MethodLifeCycle` case in `reqToState()`** (lines 840-842):
   ```go
   // DELETE these 3 lines:
   case MethodLifeCycle:
       state.LifeCycles.Add(req.Request.(*types.LifeCycleRequest))
       close(req.ReadyCh)
   ```

6. **Remove `MethodLifeCycle` case in `handleShutdown()`** (lines 1367-1370):
   ```go
   // DELETE these 4 lines:
   case MethodLifeCycle:
       o := r.Request.(*Request)
       o.Err = ErrQueueShutdown
       close(o.ReadyCh)
   ```

### What is NOT Changed

**File**: `internal/queues_manager.go`
**PRESERVE** the `QueuesManager.LifeCycle()` method (lines 219-221):
```go
// KEEP - called from runLifecycle() at logical.go:1452 for future DLQ implementation
func (qm *QueuesManager) LifeCycle(ctx context.Context, req *types.LifeCycleRequest) error {
    return nil // TODO(lifecycle)
}
```

This method is called from `runLifecycle()` when items expire or reach max attempts AND a dead letter queue is configured. It serves as the hook point for future dead letter queue routing.

### Testing Requirements

No new tests required. This is a removal of unused code.

**Existing tests that verify continued functionality:**
- All tests in `queue_test.go` that exercise lifecycle behavior (lease expiration, item expiration) will validate that internal lifecycle processing still works correctly

**Test Objectives:**
- Ensure all existing tests pass after removal
- Verify no compilation errors
- Confirm internal lifecycle timer processing is unaffected

### Validation
- [ ] Run: `go build ./...`
- [ ] Verify: No compilation errors
- [ ] Run: `go test ./...`
- [ ] Verify: All tests pass
- [ ] Run: `grep -r "MethodLifeCycle" internal/`
- [ ] Verify: No results returned
- [ ] Run: `grep "func (l \*Logical) LifeCycle" internal/logical.go`
- [ ] Verify: No results returned
- [ ] Run: `grep "func (qm \*QueuesManager) LifeCycle" internal/queues_manager.go`
- [ ] Verify: Method still exists (preserved for DLQ)

### Context for Implementation
- `internal/logical.go` contains the `Logical` struct and main request loop
- The `MethodLifeCycle` constant is part of the `MethodKind` iota enum starting at line 28
- Internal lifecycle processing uses `partition.LifeCycleRequests` batch populated by `runLifecycle()`, not through the public `Logical.LifeCycle()` method
- The `runLifecycle()` method at line 1398 creates `LifeCycleRequest` structs internally and adds them to partition's `LifeCycleRequests` batch directly
- Dead letter queue routing at line 1452 calls `l.conf.Manager.LifeCycle()` - this code path is UNCHANGED
