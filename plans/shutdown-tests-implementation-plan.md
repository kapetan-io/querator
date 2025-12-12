# Shutdown Tests and Config Consolidation Implementation Plan

## Overview

This plan consolidates the `config/` package into `daemon/` and adds comprehensive shutdown behavior tests. The config package functionality is more appropriately located in daemon since it directly populates `daemon.Config` and handles application startup concerns.

## Current State Analysis

### Config Package (`config/config.go`)
- YAML types: `File`, `Logging`, `QueueStorage`, `PartitionStorage`, `Queue`, `Partition`
- `ApplyConfigFile()` function that populates `daemon.Config`
- Helper functions: `setupLogger()`, `setupPartitionStorage()`, `setupQueueStorage()`
- Conversion methods: `ToQueueInfo()`, `ToPartitionInfo()`
- Only used by `cmd/querator/server.go`

### Shutdown Mechanism (Already Implemented)
- `daemon.Shutdown()` → `service.Shutdown()` → `QueuesManager.Shutdown()` → `Logical.Shutdown()`
- `handleShutdown()` drains waiting lease requests and in-flight requests with `ErrQueueShutdown`
- `inShutdown` atomic flags prevent new operations during shutdown
- Partition stores are closed during logical queue shutdown

### Key Discoveries
- `internal/handlers.go:167-221` - `handleShutdown()` implementation
- `internal/queues_manager.go:407-444` - `QueuesManager.Shutdown()` with storage close
- `internal/logical.go:515-534` - `Logical.Shutdown()` sends request to loop
- Error constants: `MsgQueueInShutdown`, `MsgServiceInShutdown`, `ErrQueueShutdown`, `ErrServiceShutdown`

## Desired End State

After this plan is complete:
1. The `config/` package no longer exists
2. All YAML config types and `ApplyConfigFile()` live in `daemon/config.go`
3. `cmd/querator/server.go` imports only `daemon` (not `config`)
4. New `service/shutdown_test.go` contains comprehensive shutdown behavior tests
5. All tests pass against InMemory, BadgerDB, and PostgreSQL backends

### Verification
- `go build ./...` succeeds with no import of `github.com/kapetan-io/querator/config`
- `go test ./...` passes including new shutdown tests
- `grep -r "querator/config" --include="*.go"` returns no results (except in git history)

## What We're NOT Doing

- Not changing the shutdown logic itself (already well-implemented)
- Not modifying `service.Config` structure
- Not adding new error types or HTTP codes
- Not changing the daemon/service architecture relationship

## Implementation Approach

Move config code into daemon package first (simpler, isolated change), then add shutdown tests. This order ensures we have a clean codebase before adding new test files.

---

## Phase 1: Consolidate Config into Daemon Package

### Overview
Move all `config/` package contents into `daemon/config.go` and update the single consumer (`cmd/querator/server.go`).

### Changes Required

#### 1. Merge config types into daemon/config.go
**File**: `daemon/config.go`
**Changes**: Add YAML config types and ApplyConfigFile function from config/config.go

```go
// Add these imports (merge with existing)
import (
    "context"
    "fmt"
    "io"
    "strings"
    "time"

    "github.com/kapetan-io/errors"
    "github.com/kapetan-io/querator/internal/store"
    "github.com/kapetan-io/querator/internal/types"
    "github.com/kapetan-io/tackle/color"
)

// YAML config file types
type File struct {
    Address          string             `yaml:"address"`
    Logging          Logging            `yaml:"logging"`
    PartitionStorage []PartitionStorage `yaml:"partition-storage"`
    QueueStorage     QueueStorage       `yaml:"queue-storage"`
    Queues           []Queue            `yaml:"queues"`
    ConfigFile       string
}

type Logging struct {
    Level   string `yaml:"level"`
    Handler string `yaml:"handler"`
}

type QueueStorage struct {
    Name   string            `yaml:"name"`
    Driver string            `yaml:"driver"`
    Config map[string]string `yaml:"config"`
}

type PartitionStorage struct {
    Name     string            `yaml:"name"`
    Driver   string            `yaml:"driver"`
    Affinity int               `yaml:"affinity"`
    Config   map[string]string `yaml:"config"`
}

type Queue struct {
    Name                string        `yaml:"name"`
    DeadQueue           string        `yaml:"dead-queue"`
    LeaseTimeout        time.Duration `yaml:"lease-timeout"`
    ExpireTimeout       time.Duration `yaml:"expire-timeout"`
    MaxAttempts         int           `yaml:"max-attempts"`
    Reference           string        `yaml:"reference"`
    RequestedPartitions int           `yaml:"requested-partitions"`
    Partitions          []Partition   `yaml:"partitions"`
}

type Partition struct {
    Partition   int    `yaml:"partition"`
    ReadOnly    bool   `yaml:"read-only"`
    StorageName string `yaml:"storage-name"`
}

func ApplyConfigFile(ctx context.Context, conf *Config, file File, w io.Writer) error

func (q Queue) ToQueueInfo() types.QueueInfo

func (p Partition) ToPartitionInfo() types.PartitionInfo
```

**Function Responsibilities:**
- `ApplyConfigFile()`: Parse File struct, setup logger, partition storage, queue storage, apply to Config
- `setupLogger()`: Create slog.Logger based on Logging config (color/text/json handlers)
- `setupPartitionStorage()`: Create store.PartitionStore instances based on driver type
- `setupQueueStorage()`: Create store.Queues instance based on driver type
- `toLogLevel()`: Convert string log level to slog.Level
- `ToQueueInfo()`: Convert daemon.Queue to types.QueueInfo
- `ToPartitionInfo()`: Convert daemon.Partition to types.PartitionInfo

**Context for implementation:**
- Copy functions from `config/config.go:73-229`
- Imports to add: `context`, `fmt`, `io`, `strings`, `time`, `github.com/kapetan-io/errors`, `github.com/kapetan-io/querator/internal/types`, `github.com/kapetan-io/tackle/color`
- Follow existing code style in daemon/config.go

#### 2. Update cmd/querator/server.go
**File**: `cmd/querator/server.go`
**Changes**: Replace `config.` references with `daemon.`

```go
// Remove this import
// "github.com/kapetan-io/querator/config"

// Change variable declaration
var file daemon.File  // was: config.File

// Change function call
err := daemon.ApplyConfigFile(ctx, &conf, file, w)  // was: config.ApplyConfigFile
```

**Context for implementation:**
- Line 13: Remove config import
- Line 44: Change `config.File` to `daemon.File`
- Line 60: Change `config.ApplyConfigFile` to `daemon.ApplyConfigFile`

#### 3. Move config tests to daemon package
**File**: `daemon/config_test.go`
**Changes**: Move tests from `config/config_test.go`, update package and imports

```go
package daemon_test

import (
    "context"
    "io"
    "log/slog"
    "testing"
    "time"

    "github.com/kapetan-io/querator/daemon"
    "github.com/kapetan-io/querator/internal/store"
    "github.com/kapetan-io/querator/internal/types"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "gopkg.in/yaml.v3"
)

func TestApplyConfigFileErrs(t *testing.T)

func TestApplyConfigFile(t *testing.T)

func TestApplyConfigFromYAML(t *testing.T)

func TestBadgerConfig(t *testing.T)
```

**Test Objectives:**
- Verify error handling for invalid logging handlers, drivers, and storage references
- Verify successful config application with all fields populated correctly
- Verify YAML parsing produces correct daemon.Config
- Verify Badger-specific configuration is applied correctly

**Context for implementation:**
- Copy from `config/config_test.go`
- Replace all `config.` references with `daemon.`
- Package declaration: `package daemon_test`

#### 4. Delete config package
**Files to delete**:
- `config/config.go`
- `config/config_test.go`
- `config/` directory

### Validation
- [ ] Run: `go build ./...`
- [ ] Verify: No compilation errors
- [ ] Run: `go test ./daemon/...`
- [ ] Verify: All config tests pass
- [ ] Run: `grep -r "querator/config" --include="*.go" .`
- [ ] Verify: No results (config package no longer referenced)

---

## Phase 2: Shutdown Behavior Tests

### Overview
Add comprehensive shutdown tests covering data persistence, lease integrity, and graceful request handling. Tests run against all storage backends with appropriate scenario filtering.

### Critical: Backend-Specific Test Matrix

**Not all scenarios apply to all backends:**

| Scenario | InMemory | BadgerDB | PostgreSQL |
|----------|----------|----------|------------|
| Scenario 1: ProduceThenShutdown | SKIP | RUN | RUN |
| Scenario 2A: LeaseThenShutdown | SKIP | RUN | RUN |
| Scenario 2B: PartialCompleteThenShutdown | SKIP | RUN | RUN |
| Scenario 3: ShutdownDuringActiveRequests | RUN | RUN | RUN |

**Why**: InMemory storage loses all data when daemon shuts down - restart/persistence tests are meaningless.

### Critical: Storage Reuse Pattern for Restart Tests

**IMPORTANT**: The normal test pattern calls `tearDown()` after each test, which deletes storage. For restart tests, we must NOT call `tearDown()` until the very end.

```go
// WRONG - This deletes storage between daemon instances
d1, c1, ctx1 := newDaemon(t, timeout, svc.Config{StorageConfig: setup()})
// ... produce items ...
d1.Shutdown(t)
tearDown()  // WRONG: Deletes storage!
d2, c2, ctx2 := newDaemon(t, timeout, svc.Config{StorageConfig: setup()})  // WRONG: Creates NEW storage

// CORRECT - Reuse the same storage config
storage := setup()  // Call setup() ONCE to get persistent storage
d1, c1, ctx1 := newDaemon(t, timeout, svc.Config{StorageConfig: storage})
// ... produce items ...
d1.Shutdown(t)  // Shutdown daemon but storage directory/container remains

// Create second daemon with SAME storage config
d2, c2, ctx2 := newDaemon(t, timeout, svc.Config{StorageConfig: storage})
// ... verify items still exist ...
d2.Shutdown(t)
tearDown()  // NOW cleanup storage (only call once at end)
```

### Changes Required

#### 1. Create shutdown test file
**File**: `service/shutdown_test.go`
**Changes**: New file with shutdown behavior tests

```go
package service_test

import (
    "strings"
    "sync"
    "testing"
    "time"

    "github.com/kapetan-io/querator/internal/store"
    pb "github.com/kapetan-io/querator/proto"
    svc "github.com/kapetan-io/querator/service"
    "github.com/kapetan-io/tackle/clock"
    "github.com/kapetan-io/tackle/random"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestShutdown(t *testing.T)

func testShutdown(t *testing.T, setup NewStorageFunc, tearDown func(), persistent bool)
```

**Function Responsibilities:**
- `TestShutdown()`: Entry point running testShutdown against all backends, passing `persistent` flag
- `testShutdown()`: Contains all shutdown test scenarios, skips persistence tests when `persistent=false`

### Test Entry Point Structure

```go
func TestShutdown(t *testing.T) {
    badgerdb := badgerTestSetup{Dir: t.TempDir()}
    postgres := postgresTestSetup{}

    for _, tc := range []struct {
        Setup      NewStorageFunc
        TearDown   func()
        Name       string
        Persistent bool  // NEW: indicates if backend persists data
    }{
        {
            Name:       "InMemory",
            Persistent: false,  // InMemory does NOT persist
            Setup: func() store.Config {
                return setupMemoryStorage(store.Config{})
            },
            TearDown: func() {},
        },
        {
            Name:       "BadgerDB",
            Persistent: true,  // BadgerDB persists to disk
            Setup: func() store.Config {
                return badgerdb.Setup(store.BadgerConfig{})
            },
            TearDown: func() {
                badgerdb.Teardown()
            },
        },
        {
            Name:       "PostgreSQL",
            Persistent: true,  // PostgreSQL persists to database
            Setup: func() store.Config {
                return postgres.Setup(store.PostgresConfig{})
            },
            TearDown: func() {
                postgres.Teardown()
            },
        },
    } {
        t.Run(tc.Name, func(t *testing.T) {
            testShutdown(t, tc.Setup, tc.TearDown, tc.Persistent)
        })
    }
}
```

### Scenario 1: ProduceThenShutdown (Persistent backends only)

**Test Objectives:**
- Verify items produced before shutdown survive restart
- Verify storage correctly persists queue data

**Detailed Steps:**
1. Call `setup()` ONCE to get storage config
2. Create daemon with storage config
3. Create queue with standard timeouts
4. Produce 5 items to queue
5. Shutdown daemon (NOT tearDown)
6. Create NEW daemon with SAME storage config
7. List items via `StorageItemsList` API
8. Verify: All 5 items exist in storage
9. Call `tearDown()` to cleanup

```go
t.Run("ProduceThenShutdown", func(t *testing.T) {
    if !persistent {
        t.Skip("skipping persistence test for non-persistent backend")
    }

    const numItems = 5
    queueName := random.String("queue-", 10)
    storage := setup()  // Get storage ONCE

    // First daemon: produce items
    d1, c1, ctx1 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
    createQueueAndWait(t, ctx1, c1, &pb.QueueInfo{
        QueueName:           queueName,
        LeaseTimeout:        "1m0s",
        ExpireTimeout:       "24h0m0s",
        RequestedPartitions: 1,
    })

    require.NoError(t, c1.QueueProduce(ctx1, &pb.QueueProduceRequest{
        QueueName:      queueName,
        RequestTimeout: "5s",
        Items:          produceRandomItems(numItems),
    }))

    d1.Shutdown(t)  // Shutdown but keep storage

    // Second daemon: verify items persisted
    d2, c2, ctx2 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
    defer func() {
        d2.Shutdown(t)
        tearDown()  // Only cleanup at very end
    }()

    var listResp pb.StorageItemsListResponse
    require.NoError(t, c2.StorageItemsList(ctx2, &pb.StorageItemsListRequest{
        QueueName: queueName,
        Partition: 0,
        Limit:     100,
    }, &listResp))

    assert.Len(t, listResp.Items, numItems)
})
```

### Scenario 2A: LeaseThenShutdown (Persistent backends only)

**Test Objectives:**
- Verify leased items return to queue after lease expires
- Verify lease state is not persisted (items become available again)

**Detailed Steps:**
1. Call `setup()` ONCE
2. Create daemon, create queue with SHORT LeaseTimeout (100ms)
3. Produce 5 items
4. Lease all 5 items (items now "owned" by client)
5. Shutdown daemon WITHOUT calling Complete
6. Wait for lease timeout to expire (sleep 200ms)
7. Create NEW daemon with SAME storage
8. Lease items again
9. Verify: All 5 items available (leases expired during shutdown)
10. Call `tearDown()` to cleanup

```go
t.Run("LeaseThenShutdown", func(t *testing.T) {
    if !persistent {
        t.Skip("skipping persistence test for non-persistent backend")
    }

    const numItems = 5
    const leaseTimeout = "100ms"
    queueName := random.String("queue-", 10)
    storage := setup()

    // First daemon: produce and lease items
    d1, c1, ctx1 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
    createQueueAndWait(t, ctx1, c1, &pb.QueueInfo{
        QueueName:           queueName,
        LeaseTimeout:        leaseTimeout,
        ExpireTimeout:       "24h0m0s",
        RequestedPartitions: 1,
    })

    require.NoError(t, c1.QueueProduce(ctx1, &pb.QueueProduceRequest{
        QueueName:      queueName,
        RequestTimeout: "5s",
        Items:          produceRandomItems(numItems),
    }))

    var leaseResp pb.QueueLeaseResponse
    require.NoError(t, c1.QueueLease(ctx1, &pb.QueueLeaseRequest{
        QueueName:      queueName,
        RequestTimeout: "5s",
        NumRequested:   int32(numItems),
        ClientId:       random.String("client-", 10),
    }, &leaseResp))
    require.Len(t, leaseResp.Items, numItems)

    d1.Shutdown(t)  // Shutdown WITHOUT completing items

    // Wait for lease to expire
    time.Sleep(200 * time.Millisecond)

    // Second daemon: verify items returned to queue
    d2, c2, ctx2 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
    defer func() {
        d2.Shutdown(t)
        tearDown()
    }()

    // Items should be available again (lease expired)
    var leaseResp2 pb.QueueLeaseResponse
    require.NoError(t, c2.QueueLease(ctx2, &pb.QueueLeaseRequest{
        QueueName:      queueName,
        RequestTimeout: "5s",
        NumRequested:   int32(numItems),
        ClientId:       random.String("client-", 10),
    }, &leaseResp2))
    assert.Len(t, leaseResp2.Items, numItems)
})
```

### Scenario 2B: PartialCompleteThenShutdown (Persistent backends only)

**Test Objectives:**
- Verify completed items are removed from queue
- Verify uncompleted leased items return to queue

**Detailed Steps:**
1. Produce 10 items
2. Lease 10 items
3. Complete 5 items
4. Shutdown daemon
5. Wait for lease expiration
6. Restart daemon
7. Verify: Only 5 items remain (the uncompleted ones)

```go
t.Run("PartialCompleteThenShutdown", func(t *testing.T) {
    if !persistent {
        t.Skip("skipping persistence test for non-persistent backend")
    }

    const numItems = 10
    const completeCount = 5
    const leaseTimeout = "100ms"
    queueName := random.String("queue-", 10)
    storage := setup()

    // First daemon: produce, lease, partially complete
    d1, c1, ctx1 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
    createQueueAndWait(t, ctx1, c1, &pb.QueueInfo{
        QueueName:           queueName,
        LeaseTimeout:        leaseTimeout,
        ExpireTimeout:       "24h0m0s",
        RequestedPartitions: 1,
    })

    require.NoError(t, c1.QueueProduce(ctx1, &pb.QueueProduceRequest{
        QueueName:      queueName,
        RequestTimeout: "5s",
        Items:          produceRandomItems(numItems),
    }))

    var leaseResp pb.QueueLeaseResponse
    require.NoError(t, c1.QueueLease(ctx1, &pb.QueueLeaseRequest{
        QueueName:      queueName,
        RequestTimeout: "5s",
        NumRequested:   int32(numItems),
        ClientId:       random.String("client-", 10),
    }, &leaseResp))
    require.Len(t, leaseResp.Items, numItems)

    // Complete only first 5 items
    idsToComplete := make([]string, completeCount)
    for i := 0; i < completeCount; i++ {
        idsToComplete[i] = leaseResp.Items[i].Id
    }

    require.NoError(t, c1.QueueComplete(ctx1, &pb.QueueCompleteRequest{
        QueueName:      queueName,
        Partition:      leaseResp.Partition,
        RequestTimeout: "5s",
        Ids:            idsToComplete,
    }))

    d1.Shutdown(t)
    time.Sleep(200 * time.Millisecond)  // Wait for lease expiration

    // Second daemon: verify only uncompleted items remain
    d2, c2, ctx2 := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: storage})
    defer func() {
        d2.Shutdown(t)
        tearDown()
    }()

    var leaseResp2 pb.QueueLeaseResponse
    require.NoError(t, c2.QueueLease(ctx2, &pb.QueueLeaseRequest{
        QueueName:      queueName,
        RequestTimeout: "5s",
        NumRequested:   int32(numItems),
        ClientId:       random.String("client-", 10),
    }, &leaseResp2))

    // Only the 5 uncompleted items should be available
    assert.Len(t, leaseResp2.Items, numItems-completeCount)
})
```

### Scenario 3: ShutdownDuringActiveRequests (All backends)

**Test Objectives:**
- Verify in-flight requests receive shutdown error
- Verify no requests hang indefinitely
- Verify no panics or race conditions

**Detailed Steps:**
1. Start daemon
2. Create queue
3. Launch 10 goroutines making produce requests in tight loop
4. After brief delay (50ms), call Shutdown()
5. Each goroutine captures error and signals completion
6. Verify: All goroutines completed (no hangs)
7. Verify: Errors are either nil (succeeded before shutdown) OR contain "shutting down"

**Error Validation Pattern:**
Since error message constants are in the internal package, use string matching:
```go
// Check if error indicates shutdown
func isShutdownError(err error) bool {
    if err == nil {
        return false
    }
    errStr := err.Error()
    return strings.Contains(errStr, "shutting down")
}
```

```go
t.Run("ShutdownDuringActiveRequests", func(t *testing.T) {
    queueName := random.String("queue-", 10)
    storage := setup()
    defer tearDown()

    d, c, ctx := newDaemon(t, 30*clock.Second, svc.Config{StorageConfig: storage})
    createQueueAndWait(t, ctx, c, &pb.QueueInfo{
        QueueName:           queueName,
        LeaseTimeout:        "1m0s",
        ExpireTimeout:       "24h0m0s",
        RequestedPartitions: 1,
    })

    const numGoroutines = 10
    var wg sync.WaitGroup
    errChan := make(chan error, numGoroutines*100)  // Buffer for multiple requests per goroutine
    stopChan := make(chan struct{})

    // Launch goroutines making requests
    for i := 0; i < numGoroutines; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for {
                select {
                case <-stopChan:
                    return
                default:
                    err := c.QueueProduce(ctx, &pb.QueueProduceRequest{
                        QueueName:      queueName,
                        RequestTimeout: "5s",
                        Items:          produceRandomItems(1),
                    })
                    if err != nil {
                        errChan <- err
                    }
                }
            }
        }(i)
    }

    // Let goroutines run briefly
    time.Sleep(50 * time.Millisecond)

    // Trigger shutdown while requests in flight
    close(stopChan)
    d.Shutdown(t)

    // Wait for goroutines with timeout
    done := make(chan struct{})
    go func() {
        wg.Wait()
        close(done)
    }()

    select {
    case <-done:
        // All goroutines completed - good
    case <-time.After(5 * time.Second):
        t.Fatal("goroutines did not complete within timeout - possible hang")
    }

    // Verify errors are either success or shutdown-related
    close(errChan)
    for err := range errChan {
        // Error should indicate shutdown, not some other failure
        errStr := err.Error()
        isShutdown := strings.Contains(errStr, "shutting down") ||
                      strings.Contains(errStr, "context canceled")
        assert.True(t, isShutdown)
    }
})
```

### Validation
- [ ] Run: `go test ./service/... -run TestShutdown -v`
- [ ] Verify: InMemory runs only Scenario 3 (others skipped)
- [ ] Verify: BadgerDB runs all scenarios
- [ ] Verify: PostgreSQL runs all scenarios
- [ ] Run: `go test ./service/... -race -run TestShutdown`
- [ ] Verify: No race conditions detected

---

## Phase 3: Final Validation

### Overview
Run full test suite and verify no regressions.

### Validation Commands
- [ ] Run: `go build ./...`
- [ ] Run: `go test ./... -race`
- [ ] Run: `go vet ./...`
- [ ] Verify: All tests pass, no race conditions, no vet warnings

---

## Key File References

### Shutdown Implementation
- `daemon/daemon.go:90-103` - Daemon.Shutdown()
- `service/service.go:553-556` - Service.Shutdown()
- `internal/queues_manager.go:407-444` - QueuesManager.Shutdown()
- `internal/logical.go:515-534` - Logical.Shutdown()
- `internal/handlers.go:167-221` - handleShutdown() drains requests

### Error Constants
- `internal/queues_manager.go:20-22` - MsgServiceInShutdown, ErrServiceShutdown
- `internal/logical.go:52-57` - MsgQueueInShutdown, ErrQueueShutdown

### Test Patterns
- `service/common_test.go:69-107` - testDaemon helper
- `service/common_test.go:113-146` - badgerTestSetup for persistent storage
- `service/queue_test.go:25-67` - Multi-backend test pattern

### Config (to be moved)
- `config/config.go:27-72` - YAML types to move
- `config/config.go:73-97` - ApplyConfigFile to move
- `config/config.go:99-200` - Helper functions to move
- `config/config_test.go` - Tests to move
