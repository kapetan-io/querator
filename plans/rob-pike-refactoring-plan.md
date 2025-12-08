# Rob Pike Style Refactoring Implementation Plan

## Overview

This plan addresses code style improvements identified through a "Rob Pike review" of the Querator codebase. The changes focus on Go idioms: smaller interfaces, explicit composition, type safety, and splitting large files into focused components.

## Current State Analysis

### Key Issues Identified

1. **`logical.go` is 1731 lines** - Too large to reason about; handles request routing, batching, lifecycle management, timeout handling, and pause/resume
2. **`Service` interface has 15+ methods** (`transport/http.go:85-104`) - Violates "small interfaces" principle
3. **Config embedding** (`daemon/config.go:15-17`) - Implicit composition makes field origins unclear
4. **Generic `Batch[T]`** (`internal/types/batch.go`) - Abstraction doesn't fit when `LeaseBatch` has different semantics

### Not Addressing

- **`Request.Request any`** (`internal/structs.go:78`) - While type-unsafe, the type assertions are localized to a few switch statements and not a source of bugs. The cure (union types with ~60 lines of boilerplate) is worse than the disease.

### Key Discoveries
- `internal/logical.go:72-83`: Request struct uses `any` for Request field
- `internal/types/batch.go:59-95`: LeaseBatch has unique methods (MarkNil, FilterNils, TotalRequested) not in generic Batch[T]
- `transport/http.go:85-104`: Service interface mixes queue ops, admin, and storage inspection
- `daemon/config.go:15-17`: Config embeds querator.ServiceConfig implicitly
- `internal/logical.go:627-668`: requestLoop is the main coordination point
- `internal/logical.go:670-681`: handleRequest routes to hot vs cold handlers

## Desired End State

After this plan is complete:

1. **`logical.go` split into focused files** - Each under 500 lines with clear responsibilities
2. **Service interface split into 3 smaller interfaces** - QueueOps, QueueAdmin, StorageInspector
3. **Config uses explicit composition** - `daemon.Config.Service` instead of embedding
4. **Batch types are concrete** - Remove generic Batch[T] in favor of three concrete types

### Verification

- All existing tests pass: `go test ./...`
- No new linter warnings: `make lint` (if available)
- Code compiles cleanly: `go build ./...`

## What We're NOT Doing

- Adding new functionality
- Changing public API contracts
- Modifying test behavior (only updating tests to match refactored code)
- Performance optimizations
- Changing storage interfaces

## Implementation Approach

The refactoring follows this sequence:
1. First, fix batch type safety (concrete Batch types)
2. Then, split large files (logical.go)
3. Then, refactor interfaces (Service)
4. Finally, refactor config (explicit composition)

Each phase produces working code that passes all tests.

**Note**: This project is in alpha (no v1 release). Breaking changes to `daemon.Config` in Phase 4 are acceptable.

---

## Phase 1: Replace Generic Batch[T] with Concrete Types

### Overview
Remove the generic `Batch[T]` abstraction and replace with three concrete, focused batch types. This addresses Rob Pike's observation that "if the abstraction doesn't quite fit, you don't have an abstraction."

### Changes Required

#### 1. Refactor Batch Types
**File**: `internal/types/batch.go`
**Changes**: Replace generic `Batch[T]` with concrete `CompleteBatch` and `RetryBatch` types

```go
// CompleteBatch is a batch of complete requests
type CompleteBatch struct {
    Requests []*CompleteRequest
}

func (b *CompleteBatch) Add(req *CompleteRequest)
func (b *CompleteBatch) Remove(req *CompleteRequest)
func (b *CompleteBatch) Reset()

// RetryBatch is a batch of retry requests
type RetryBatch struct {
    Requests []*RetryRequest
}

func (b *RetryBatch) Add(req *RetryRequest)
func (b *RetryBatch) Remove(req *RetryRequest)
func (b *RetryBatch) Reset()

// LifeCycleBatch is a batch of lifecycle requests
type LifeCycleBatch struct {
    Requests []*LifeCycleRequest
}

func (b *LifeCycleBatch) Add(req *LifeCycleRequest)
func (b *LifeCycleBatch) Remove(req *LifeCycleRequest)
func (b *LifeCycleBatch) Reset()
```

**Function Responsibilities:**
- Each batch type manages its own slice of requests
- `Add`: Append non-nil request to slice
- `Remove`: Filter-in-place algorithm to remove specific request
- `Reset`: Truncate slice to zero length preserving capacity
- Follow existing patterns from `ProduceBatch` at `internal/types/batch.go:33-54`

#### 2. Update Partition and QueueState
**File**: `internal/structs.go`
**Changes**: Update field types from `Batch[T]` to concrete types

```go
type Partition struct {
    // Change from types.Batch[types.CompleteRequest] to types.CompleteBatch
    CompleteRequests types.CompleteBatch
    // Change from types.Batch[types.RetryRequest] to types.RetryBatch
    RetryRequests    types.RetryBatch
    // Change from types.Batch[types.LifeCycleRequest] to types.LifeCycleBatch
    LifeCycleRequests types.LifeCycleBatch
    // ... other fields unchanged
}
```

**File**: `internal/logical.go`
**Changes**: Update QueueState struct (around line 606)

```go
type QueueState struct {
    LifeCycles types.LifeCycleBatch  // was types.Batch[types.LifeCycleRequest]
    Completes  types.CompleteBatch   // was types.Batch[types.CompleteRequest]
    Retries    types.RetryBatch      // was types.Batch[types.RetryRequest]
    // ... other fields unchanged
}
```

### Testing Requirements

**Existing tests that may require updates:**
```go
// No new tests needed - existing tests validate batch behavior through public APIs
// Verify all tests pass after type changes
```

**Test Objectives:**
- All existing queue tests continue to pass
- Batch operations (add, remove, reset) work identically to before
- No behavioral changes, only type changes

### Validation Commands
```bash
go build ./...
go test ./...
```

### Context for Implementation
- `internal/types/batch.go:33-54`: ProduceBatch pattern to follow
- `internal/types/batch.go:4-31`: Generic Batch[T] to remove after migration
- `internal/structs.go:26-34`: Partition struct uses Batch[T]
- `internal/logical.go:606-616`: QueueState struct uses Batch[T]

---

## Phase 2: Split logical.go into Focused Files

### Overview
Extract distinct responsibilities from `logical.go` (1731 lines) into separate files. Each file will be under 500 lines with a single responsibility.

### Changes Required

#### 1. Extract Lifecycle Processing
**File**: `internal/lifecycle.go` (new file)
**Changes**: Move lifecycle-related code from logical.go

```go
package internal

// runLifecycle processes lifecycle actions for all partitions
func (l *Logical) runLifecycle(state *QueueState)

// runScheduled processes scheduled items for all partitions
func (l *Logical) runScheduled(state *QueueState)

// scanPartitionLifecycle scans partition for actions needed
func (l *Logical) scanPartitionLifecycle(ctx context.Context, p *Partition) ([]types.Action, error)

// recoverPartition attempts to recover a failed partition
func (l *Logical) recoverPartition(p *Partition, now time.Time) bool

// minNextLifecycleRun calculates minimum next lifecycle run time
func (l *Logical) minNextLifecycleRun(state *QueueState) clock.Duration

// minNextScheduledRun calculates minimum next scheduled run time
func (l *Logical) minNextScheduledRun(state *QueueState) clock.Duration
```

**Function Responsibilities:**
- All lifecycle timer and action processing
- Dead letter queue routing
- Partition recovery logic
- Move lines ~1357-1731 from logical.go

#### 2. Extract Partition Distribution
**File**: `internal/distribution.go` (new file)
**Changes**: Move partition assignment and distribution logic

```go
package internal

// assignToPartitions assigns requests to appropriate partitions
func (l *Logical) assignToPartitions(state *QueueState)

// assignProduceRequests assigns produce requests using opportunistic distribution
func (l *Logical) assignProduceRequests(state *QueueState)

// assignLeaseRequests assigns lease requests to partitions
func (l *Logical) assignLeaseRequests(state *QueueState)

// assignCompleteRequests assigns complete requests to their target partition
func (l *Logical) assignCompleteRequests(state *QueueState)

// assignRetryRequests assigns retry requests to their target partition
func (l *Logical) assignRetryRequests(state *QueueState)

// sortPartitionsByLoad sorts partitions for opportunistic distribution
func sortPartitionsByLoad(partitions []*Partition)

// applyToPartitions applies batched requests to storage partitions
func (l *Logical) applyToPartitions(state *QueueState)

// finalizeRequests closes ready channels for completed requests
func (l *Logical) finalizeRequests(state *QueueState)
```

**Function Responsibilities:**
- Opportunistic partition distribution algorithm
- Partition sorting by load
- Request-to-partition mapping
- Storage interaction coordination
- Move lines ~829-1138 from logical.go

#### 3. Extract Request Handlers
**File**: `internal/handlers.go` (new file)
**Changes**: Move cold request handlers

```go
package internal

// handleColdRequests routes cold requests to specific handlers
func (l *Logical) handleColdRequests(state *QueueState, req *Request)

// handleStorageRequests handles storage list/import/delete
func (l *Logical) handleStorageRequests(req *Request)

// handleStats gathers queue statistics
func (l *Logical) handleStats(state *QueueState, req *Request)

// handlePause enters/exits pause mode
func (l *Logical) handlePause(state *QueueState, req *Request)

// handleClear clears queue items
func (l *Logical) handleClear(state *QueueState, req *Request)

// handleShutdown gracefully shuts down the logical queue
func (l *Logical) handleShutdown(state *QueueState, req *types.ShutdownRequest)
```

**Function Responsibilities:**
- Cold request routing and handling
- Storage operations (list, import, delete)
- Queue administration (stats, pause, clear)
- Graceful shutdown coordination
- Move lines ~1144-1355 from logical.go

#### 4. Remaining in logical.go
**File**: `internal/logical.go`
**Changes**: Keep core coordination logic

```go
package internal

// Logical struct definition and constants (~lines 1-100)
// Public methods: Produce, Lease, Complete, Retry (~lines 142-400)
// requestLoop - main coordination (~lines 627-700)
// handleRequest, handleHotRequests (~lines 670-750)
// consumeHotCh, reqToState (~lines 778-827)
// prepareQueueState, QueueState (~lines 537-625)
```

**Function Responsibilities:**
- Logical struct and configuration
- Public API methods
- Main request loop coordination
- Hot request batching
- State initialization

### Testing Requirements

**Existing tests that may require updates:**
```go
// No test changes - this is a file reorganization only
// All functions remain methods on *Logical
```

**Test Objectives:**
- All tests pass without modification
- No behavioral changes
- File organization is purely structural

### Validation Commands
```bash
go build ./...
go test ./...
```

### Context for Implementation
- `internal/logical.go:1357-1494`: Lifecycle code to extract
- `internal/logical.go:1496-1731`: Scheduled processing to extract
- `internal/logical.go:829-1138`: Distribution code to extract
- `internal/logical.go:1144-1355`: Handler code to extract
- Maintain all receiver methods on `*Logical`

---

## Phase 3: Split Service Interface

### Overview
Split the 15-method Service interface into three focused interfaces following the "small interfaces" principle.

### Changes Required

#### 1. Define Focused Interfaces
**File**: `transport/interfaces.go` (new file)
**Changes**: Create three focused interfaces

```go
package transport

// QueueOps handles high-frequency queue operations
type QueueOps interface {
    QueueProduce(context.Context, *pb.QueueProduceRequest) error
    QueueLease(context.Context, *pb.QueueLeaseRequest, *pb.QueueLeaseResponse) error
    QueueComplete(context.Context, *pb.QueueCompleteRequest) error
    QueueRetry(context.Context, *pb.QueueRetryRequest) error
    QueueStats(context.Context, *pb.QueueStatsRequest, *pb.QueueStatsResponse) error
    QueueClear(context.Context, *pb.QueueClearRequest) error
}

// QueueAdmin handles queue lifecycle management
type QueueAdmin interface {
    QueuesCreate(context.Context, *pb.QueueInfo) error
    QueuesList(context.Context, *pb.QueuesListRequest, *pb.QueuesListResponse) error
    QueuesUpdate(context.Context, *pb.QueueInfo) error
    QueuesDelete(context.Context, *pb.QueuesDeleteRequest) error
    QueuesInfo(context.Context, *pb.QueuesInfoRequest, *pb.QueueInfo) error
}

// StorageInspector handles direct storage access
type StorageInspector interface {
    StorageItemsList(context.Context, *pb.StorageItemsListRequest, *pb.StorageItemsListResponse) error
    StorageItemsImport(context.Context, *pb.StorageItemsImportRequest, *pb.StorageItemsImportResponse) error
    StorageItemsDelete(context.Context, *pb.StorageItemsDeleteRequest) error
    StorageScheduledList(context.Context, *pb.StorageItemsListRequest, *pb.StorageItemsListResponse) error
}

// Service combines all interfaces for backward compatibility
type Service interface {
    QueueOps
    QueueAdmin
    StorageInspector
}
```

**Function Responsibilities:**
- QueueOps: Hot path operations (produce, lease, complete, retry)
- QueueAdmin: Queue CRUD and info
- StorageInspector: Direct storage access for debugging/inspection
- Service: Composite interface maintains backward compatibility

#### 2. Update HTTPHandler
**File**: `transport/http.go`
**Changes**: HTTPHandler can now accept smaller interfaces if needed

```go
type HTTPHandler struct {
    // Keep service as Service for now (backward compatible)
    service Service
    // ... other fields unchanged
}

// Future: Could accept QueueOps only for a read-only handler
// func NewQueueOpsHandler(ops QueueOps) *HTTPHandler
```

**Function Responsibilities:**
- No immediate changes to HTTPHandler implementation
- Interface split enables future flexibility
- Consumers can now accept smaller interfaces

### Testing Requirements

**Existing tests that may require updates:**
```go
// No test changes - Service interface is a superset
// Existing implementations satisfy all three interfaces
```

**Test Objectives:**
- querator.Service satisfies transport.Service
- Interface composition works correctly
- No breaking changes to existing code

### Validation Commands
```bash
go build ./...
go test ./...
```

### Context for Implementation
- `transport/http.go:85-104`: Current Service interface
- `service.go:63-66`: Service struct implements all methods
- Interface split is additive, not breaking

---

## Phase 4: Explicit Config Composition

### Overview
Replace embedded `querator.ServiceConfig` in `daemon.Config` with explicit composition. This makes field origins clear and follows "don't make me guess where a field comes from."

### Changes Required

#### 1. Update Daemon Config
**File**: `daemon/config.go`
**Changes**: Use explicit composition instead of embedding

```go
type Config struct {
    // Explicit composition instead of embedding
    Service querator.ServiceConfig

    // TLS is the TLS config used for public server and clients
    TLS *duh.TLSConfig
    // ListenAddress is the address:port that Querator will listen on
    ListenAddress string
    // MaxProducePayloadSize is the maximum size in bytes for produce requests
    MaxProducePayloadSize int64
    // InMemoryListener enables in-memory connections for testing
    InMemoryListener bool
}

func (c *Config) ClientTLS() *tls.Config
func (c *Config) ServerTLS() *tls.Config
func (c *Config) SetDefaults()
```

**Function Responsibilities:**
- All ServiceConfig fields accessed via `c.Service.FieldName`
- SetDefaults updates `c.Service.Log`, `c.Service.Clock`, etc.
- Clear separation between daemon-specific and service config

#### 2. Update Daemon and Tests
**File**: `daemon/daemon.go`
**Changes**: Update field access patterns

```go
// Before: d.conf.Log
// After:  d.conf.Service.Log

// Before: d.conf.StorageConfig
// After:  d.conf.Service.StorageConfig

// Before: d.conf.MaxLeaseBatchSize
// After:  d.conf.Service.MaxLeaseBatchSize
```

**Function Responsibilities:**
- Update all field access in daemon.go
- Update test configurations in daemon tests
- Update any code that creates daemon.Config

#### 3. Update Test Helpers
**File**: `common_test.go` and related test files
**Changes**: Update daemon config creation in tests

```go
// Before:
conf := daemon.Config{
    Log: logger,
    StorageConfig: store.Config{...},
}

// After:
conf := daemon.Config{
    Service: querator.ServiceConfig{
        Log: logger,
        StorageConfig: store.Config{...},
    },
}
```

**Function Responsibilities:**
- Update all test files that create daemon.Config
- Explicit `Service:` field makes config source clear

### Testing Requirements

**Existing tests that may require updates:**
```go
// Tests that create daemon.Config need field path updates:
func TestDaemon(t *testing.T)           // daemon/daemon_test.go (if exists)
func TestQueue(t *testing.T)            // queue_test.go - uses testDaemon helper
func TestPartition(t *testing.T)        // partition_test.go
func TestStorage(t *testing.T)          // storage_test.go
func TestRetry(t *testing.T)            // retry_test.go
```

**Test Objectives:**
- All config fields accessible via explicit path
- Test configurations updated to use `Service:` field
- No behavioral changes

### Validation Commands
```bash
go build ./...
go test ./...
```

### Context for Implementation
- `daemon/config.go:15-34`: Current Config with embedding
- `daemon/config.go:50-67`: SetDefaults accesses embedded fields
- `daemon/daemon.go`: Uses conf.FieldName pattern
- `common_test.go`: Test helper creates daemon configs

---

## Summary

| Phase | Focus | Files Changed | Risk |
|-------|-------|---------------|------|
| 1 | Batch types | `internal/types/batch.go`, `internal/structs.go`, `internal/logical.go` | Low |
| 2 | File split | `internal/logical.go` -> 4 files | Low |
| 3 | Interface split | `transport/http.go`, new `transport/interfaces.go` | Low |
| 4 | Config composition | `daemon/config.go`, `daemon/daemon.go`, tests | Low (breaking) |

**Notes:**
- Phase 4 is a breaking change to `daemon.Config` (acceptable in alpha)
- `Request.Request any` intentionally left unchanged - pragmatic tradeoff

Each phase is independently deployable and all tests must pass before proceeding to the next phase.
