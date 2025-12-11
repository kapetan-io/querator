# QueueReload Implementation Plan

## Overview

Complete the QueueReload feature to sync in-memory partition state with underlying storage. This is used when storage changes out-of-band (backup restoration, StorageItemsImport, StorageItemsDelete) and the cached item counts become stale.

## Current State Analysis

**Existing infrastructure (partially implemented):**
- Service method: `service/service.go:227` - `QueueReload()` exists but passes empty partitions list
- Logical method: `internal/logical.go:413` - `ReloadPartitions()` routes request correctly
- Handler stub: `internal/handlers.go:33-35` - **EMPTY**, only has `// TODO(reload)` comment
- Request type: `internal/types/requests.go:102-105` - `ReloadRequest` struct exists

**Missing components:**
- Handler implementation in `handlers.go`
- HTTP endpoint `/v1/queue.reload`
- Client method `QueueReload()`
- Service interface method
- Tests

### Key Discoveries:
- `prepareQueueState()` at `internal/logical.go:573-588` shows the pattern for reading Stats() and updating partition state
- `MethodUpdateInfo` handler at `internal/handlers.go:21-28` shows how to iterate `state.Partitions`
- Handler must close `req.ReadyCh` to unblock the waiting `queueRequest()` caller
- Stats() returns `Total` and `NumLeased`; `UnLeased` is calculated as `Total - NumLeased`

## Desired End State

After implementation:
1. `QueueReload` handler reads partition stats from storage and updates in-memory state
2. HTTP endpoint `/v1/queue.reload` exposes the functionality
3. Client method `c.QueueReload()` allows programmatic access
4. Tests verify that after out-of-band storage changes, `QueueReload` syncs the counts

**Verification:**
- `go test ./... -run "^TestQueue/.*/QueueReload"` passes
- All existing tests continue to pass

## What We're NOT Doing

- Partition state transitions (Available → Unavailable → Available) - ADR suggests this but current implementation doesn't require it
- Selective partition reload - will reload all partitions (matching current `QueueClear` behavior)
- Pause/drain coordination - out of scope for initial implementation
- Proto message changes - reuse existing `QueueClearRequest` (has `queueName` field which is all we need)
  - **Note**: The `retry`, `scheduled`, `queue`, and `destructive` fields in `QueueClearRequest` are ignored by `QueueReload` - only `queueName` is used

## Implementation Approach

Follow existing patterns:
1. `MethodUpdateInfo` handler pattern (lines 21-28) for iterating `state.Partitions`
2. `prepareQueueState()` pattern (lines 573-588) for calling Stats() and updating state
3. `QueueClear` HTTP handler pattern (line 326) for HTTP endpoint
4. `QueueClear` client pattern (lines 141-156) for client method

## Phase 1: Handler Implementation

### Overview
Implement `handleReload()` to read partition stats from storage and update in-memory `PartitionState`.

### Changes Required:

#### 1. Handler Function
**File**: `internal/handlers.go`
**Changes**: Add `handleReload()` function and update the switch case

```go
func (l *Logical) handleReload(state *QueueState, req *Request)
```

**Function Responsibilities:**
- Extract `*types.ReloadRequest` from `req.Request` via type assertion (not currently used, but extract for future selective reload support)
- Iterate through `state.Partitions` (follow `MethodUpdateInfo` pattern at `handlers.go:24-27`)
- For each partition:
  - Create timeout context: `ctx, cancel := context.WithTimeout(req.Context, l.conf.ReadTimeout)`
  - Call `partition.Store.Stats(ctx, &stats, l.conf.Clock.Now().UTC())`
  - Call `cancel()` after Stats() returns
  - On success: Update `partition.State.UnLeased = stats.Total - stats.NumLeased` and `partition.State.NumLeased = stats.NumLeased`, reset `partition.State.Failures = 0`
  - On error: Set `partition.State.Failures = 1`, log warning, continue to next partition
- Always `close(req.ReadyCh)` at the end (even if errors occurred)

**Pattern References:**
- Partition iteration: `internal/handlers.go:21-28` (MethodUpdateInfo)
- Stats call and state update: `internal/logical.go:573-588` (prepareQueueState)

**Important**: Unlike `handleClear()` which only operates on partition 0, `handleReload()` must iterate ALL partitions in `state.Partitions`.

#### 2. Update Switch Case
**File**: `internal/handlers.go`
**Changes**: Replace TODO stub with handler call at line 33-35

```go
case MethodReloadPartitions:
	l.handleReload(state, req)
```

**Testing Requirements:**

No new test functions in this phase - testing covered in Phase 3.

### Validation
- [ ] Run: `go build ./...`
- [ ] Verify: No compilation errors

---

## Phase 2: HTTP Endpoint, Service Interface, and Client

### Overview
Expose QueueReload via HTTP API and client library.

### Changes Required:

#### 1. HTTP Endpoint Constant
**File**: `transport/http.go`
**Changes**: Add constant after `RPCQueueClear` (around line 44)

```go
RPCQueueReload = "/v1/queue.reload"
```

#### 2. Router Case
**File**: `transport/http.go`
**Changes**: Add case in `ServeHTTP` switch statement (after `RPCQueueClear` case around line 145)

```go
case RPCQueueReload:
	h.QueueReload(ctx, w, r)
	return
```

#### 3. HTTP Handler Function
**File**: `transport/http.go`
**Changes**: Add handler function after `QueueClear` handler (after line 338)

```go
func (h *HTTPHandler) QueueReload(ctx context.Context, w http.ResponseWriter, r *http.Request)
```

**Function Responsibilities:**
- Read request with `duh.ReadRequest(r, &req, 512*duh.Bytes)` - small request, only queue name needed
- Call `h.service.QueueReload(ctx, &req)`
- On error: `h.ReplyError(w, r, err)`
- On success: `duh.Reply(w, r, duh.CodeOK, &v1.Reply{Code: duh.CodeOK})`

**Pattern Reference**: `QueueClear` handler at `transport/http.go:326-338`

#### 4. Service Interface
**File**: `transport/interfaces.go`
**Changes**: Add method to `QueueOps` interface (after `QueueClear`)

```go
QueueReload(context.Context, *pb.QueueClearRequest) error
```

#### 5. Client Method
**File**: `client.go`
**Changes**: Add client method after `QueueClear` method (after line 156)

```go
func (c *Client) QueueReload(ctx context.Context, req *pb.QueueClearRequest) error
```

**Function Responsibilities:**
- Marshal request with `proto.Marshal(req)`
- Create POST request to `transport.RPCQueueReload` endpoint
- Set `Content-Type` header to `duh.ContentTypeProtoBuf`
- Execute with `c.client.Do(r, &res)` where `res` is `v1.Reply`
- Return `error` only (no response object, same as `QueueClear`)

**Pattern Reference**: `QueueClear` client at `client.go:141-156`

#### 6. Fix Service Method
**File**: `service/service.go`
**Changes**: Remove TODO comment at line 236

The current implementation at line 235-236:
```go
r := types.ReloadRequest{
    Partitions: make([]int, 0), // TODO
}
```

Remove the `// TODO` comment. Empty partitions means "reload all partitions" which is the correct behavior.

**Testing Requirements:**

No new test functions in this phase - testing covered in Phase 3.

### Validation
- [ ] Run: `go build ./...`
- [ ] Verify: No compilation errors
- [ ] Run: `go test ./... -run "^TestQueue/.*/QueueClear"` (ensure similar patterns still work)

---

## Phase 3: Testing

### Overview
Add comprehensive tests for QueueReload functionality.

### Changes Required:

#### 1. Main Test Block
**File**: `service/queue_test.go`
**Changes**: Add `t.Run("QueueReload", ...)` block inside `testQueue()` function, after the QueueClear tests (around line 1420)

```go
t.Run("QueueReload", func(t *testing.T) {
    // Test implementations here
})
```

**Test Objectives:**

1. **BasicReload** - Reload empty queue succeeds without error
2. **ReloadPreservesItems** - Items in queue preserved after reload, stats unchanged
3. **StorageSyncAfterImport** - Main use case: stats sync after StorageItemsImport
4. **ReloadWithLeasedItems** - Leased items preserved, counts correct after reload

**Testing Pattern Reference:**
- Setup pattern from `queue_test.go:1249-1264` (QueueClear test)
- StorageItemsImport usage from `queue_test.go:1294-1297`
- Stats verification from `queue_test.go:1162-1169`

#### 2. Error Handling Tests
**File**: `service/queue_test.go`
**Changes**: Add error tests under existing `t.Run("Errors", ...)` block

```go
t.Run("QueueReload", func(t *testing.T) {
    t.Run("InvalidQueueName", func(t *testing.T) { ... })
    t.Run("NonExistentQueue", func(t *testing.T) { ... })
})
```

**Test Objectives:**
- Invalid queue name (e.g., contains `~`) returns `duh.CodeBadRequest`
- Non-existent queue returns `duh.CodeRequestFailed`

**Note**: No additional validation needed in handler - the service method at `service/service.go:228-231` already validates via `s.queues.Get()`.

**Pattern Reference**: Error test pattern from `queues_test.go:740-881`

**Context for implementation:**
- Use `newDaemon()` helper for test setup
- Use `createQueueAndWait()` to ensure partitions are ready before testing
- Use `writeRandomItems()` to populate queue with test data
- Use `c.StorageItemsImport()` for out-of-band item addition (bypasses normal produce)
- Use `c.QueueStats()` to verify counts before/after reload
- Use `require` for critical assertions (errors, nil checks), `assert` for value comparisons

### Validation
- [ ] Run: `go test ./... -run "^TestQueue/.*/QueueReload"`
- [ ] Run: `go test ./... -run "^TestQueue/.*/Errors/QueueReload"`
- [ ] Run: `go test ./...` (all tests pass)
- [ ] Run: `make ci` (full CI passes)

---

## Test Scenarios Detail

### Scenario: StorageSyncAfterImport (Primary Use Case)

This is the core test validating the purpose of QueueReload:

1. Create queue with 1 partition
2. Produce 100 items normally via `QueueProduce`
3. Verify stats show `Total: 100, TotalLeased: 0`
4. Import 50 items directly via `StorageItemsImport` (bypasses normal flow, doesn't update in-memory counts)
5. Verify stats STILL show `Total: 100` (stale - cached counts not updated)
6. Call `QueueReload`
7. Verify stats NOW show `Total: 150` (synced with actual storage)

This demonstrates the reload syncs cached counts with actual storage state.

### Scenario: ReloadWithLeasedItems

1. Create queue, produce 100 items
2. Lease 20 items via `QueueLease`
3. Verify stats: `Total: 100, TotalLeased: 20`
4. Call `QueueReload`
5. Verify stats unchanged: `Total: 100, TotalLeased: 20` (reload preserves lease state)
6. Complete 10 leased items via `QueueComplete`
7. Call `QueueReload` again
8. Verify stats: `Total: 90, TotalLeased: 10`

This ensures reload correctly reflects lease state from storage.

### Scenario: BasicReload

1. Create queue with 1 partition
2. Call `QueueReload` immediately (empty queue)
3. Verify no error returned
4. Verify stats show `Total: 0, TotalLeased: 0`

This ensures reload works on empty queues without error.
