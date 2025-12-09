# PostgreSQL Test Coverage Expansion Implementation Plan

## Overview

This plan expands PostgreSQL backend test coverage by adding PostgreSQL to the test backend loops in three test files that currently only test InMemory and BadgerDB backends. The PostgreSQL implementation already exists and passes tests in `queue_test.go` and `storage_test.go` - this work extends coverage to the remaining test suites.

## Current State Analysis

### PostgreSQL Implementation
- **Location**: `internal/store/postgres.go` (1688 lines)
- **Status**: Complete implementation of `PostgresQueues`, `PostgresPartitionStore`, and `PostgresPartition`
- **Test Infrastructure**: `postgresTestSetup` in `service/common_test.go:152-205` using testcontainers-go

### Test Coverage Matrix

| Test File | Tests | PostgreSQL Status |
|-----------|-------|-------------------|
| `queue_test.go` | Core queue operations (produce, lease, complete) | ✅ Enabled |
| `storage_test.go` | Storage CRUD operations (import, list, delete) | ✅ Enabled |
| `queues_test.go` | Queue metadata CRUD (create, update, delete, list) | ❌ Not enabled |
| `retry_test.go` | Retry and dead-letter functionality | ❌ Not enabled |
| `partition_test.go` | Multi-partition operations and consumer distribution | ❌ Not enabled |

### Key Discoveries
- PostgreSQL backend uses testcontainers-go pattern at `common_test.go:157-195`
- Existing pattern for adding backends: declare setup struct, add to test case slice
- Tests use `goleak.VerifyNone(t)` for goroutine leak detection
- Each test file follows identical backend registration pattern

## Desired End State

All five test files run against all three backends (InMemory, BadgerDB, PostgreSQL) with all tests passing. Verification command:

```bash
go test ./service/... -v -run "TestQueue|TestQueueStorage|TestQueuesStorage|TestRetry|TestPartitions"
```

Expected output shows all backends for each test:
- `TestQueue/InMemory/...`, `TestQueue/BadgerDB/...`, `TestQueue/PostgreSQL/...`
- `TestQueuesStorage/InMemory/...`, `TestQueuesStorage/BadgerDB/...`, `TestQueuesStorage/PostgreSQL/...`
- `TestRetry/InMemory/...`, `TestRetry/BadgerDB/...`, `TestRetry/PostgreSQL/...`
- `TestPartitions/InMemory/...`, `TestPartitions/BadgerDB/...`, `TestPartitions/PostgreSQL/...`

## What We're NOT Doing

- Adding new test cases
- Modifying the PostgreSQL implementation unless tests fail
- Changing test infrastructure or patterns
- Adding new storage backends

## Implementation Approach

Follow the established pattern from `queue_test.go` and `storage_test.go` to add PostgreSQL to each remaining test file. Fix any PostgreSQL-specific issues discovered during test execution.

---

## Phase 1: Add PostgreSQL to queues_test.go

### Overview
Enable PostgreSQL backend for queue metadata CRUD tests (create, update, delete, list operations).

### Changes Required

#### 1. Update TestQueuesStorage Function
**File**: `service/queues_test.go`
**Changes**: Add PostgreSQL to the backend test loop

```go
func TestQueuesStorage(t *testing.T) {
	badger := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		// ... existing InMemory and BadgerDB entries ...
		{
			Name: "PostgreSQL",
			Setup: func() store.Config {
				return postgres.Setup(store.PostgresConfig{})
			},
			TearDown: func() {
				postgres.Teardown()
			},
		},
	} {
		// ... existing test loop ...
	}
}
```

**Function Responsibilities:**
- Declare `postgres := postgresTestSetup{}` at function start
- Add PostgreSQL case to the test case slice (follow pattern from `queue_test.go:50-58`)
- Remove commented-out PostgreSQL placeholder (lines 44-49)

**Testing Requirements:**
```go
func TestQueuesStorage(t *testing.T)
```

**Test Objectives:**
- Verify queue metadata CRUD operations work with PostgreSQL
- Validate queue listing with pagination
- Confirm error handling for invalid operations

**Context for Implementation:**
- Follow exact pattern from `service/queue_test.go:25-67`
- `postgresTestSetup` already defined in `common_test.go:152-205`

### Validation
- [ ] Run: `go test ./service/... -v -run "^TestQueuesStorage$" -timeout 5m`
- [ ] Verify: All three backends (InMemory, BadgerDB, PostgreSQL) show in output
- [ ] Verify: All tests pass for PostgreSQL backend

---

## Phase 2: Add PostgreSQL to retry_test.go

### Overview
Enable PostgreSQL backend for retry and dead-letter functionality tests.

### Changes Required

#### 1. Update TestRetry Function
**File**: `service/retry_test.go`
**Changes**: Add PostgreSQL to the backend test loop

```go
func TestRetry(t *testing.T) {
	badgerdb := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		// ... existing InMemory and BadgerDB entries ...
		{
			Name: "PostgreSQL",
			Setup: func() store.Config {
				return postgres.Setup(store.PostgresConfig{})
			},
			TearDown: func() {
				postgres.Teardown()
			},
		},
	} {
		// ... existing test loop ...
	}
}
```

**Function Responsibilities:**
- Declare `postgres := postgresTestSetup{}` at function start
- Add PostgreSQL case to the test case slice
- Remove commented-out PostgreSQL placeholder (lines 44-49)

**Testing Requirements:**
```go
func TestRetry(t *testing.T)
```

**Test Objectives:**
- Verify immediate retry works with PostgreSQL
- Validate scheduled retry with future timestamps
- Confirm dead-letter marking removes items from queue
- Test error handling for invalid retry operations

**Context for Implementation:**
- Follow exact pattern from `service/queue_test.go:25-67`
- Retry tests at `retry_test.go:60-218` exercise `QueueRetry` API

### Validation
- [ ] Run: `go test ./service/... -v -run "^TestRetry$" -timeout 5m`
- [ ] Verify: All three backends show in output
- [ ] Verify: All tests pass for PostgreSQL backend

---

## Phase 3: Add PostgreSQL to partition_test.go

### Overview
Enable PostgreSQL backend for multi-partition operation tests including consumer distribution.

### Changes Required

#### 1. Update TestPartitions Function
**File**: `service/partition_test.go`
**Changes**: Add PostgreSQL to the backend test loop

```go
func TestPartitions(t *testing.T) {
	badger := badgerTestSetup{Dir: t.TempDir()}
	postgres := postgresTestSetup{}

	for _, tc := range []struct {
		Setup    NewStorageFunc
		TearDown func()
		Name     string
	}{
		// ... existing InMemory and BadgerDB entries ...
		{
			Name: "PostgreSQL",
			Setup: func() store.Config {
				return postgres.Setup(store.PostgresConfig{})
			},
			TearDown: func() {
				postgres.Teardown()
			},
		},
	} {
		// ... existing test loop ...
	}
}
```

**Function Responsibilities:**
- Declare `postgres := postgresTestSetup{}` at function start
- Add PostgreSQL case to the test case slice
- Remove commented-out PostgreSQL placeholder (lines 44-49)

**Testing Requirements:**
```go
func TestPartitions(t *testing.T)
```

**Test Objectives:**
- Verify two-partition queue operations with PostgreSQL
- Validate opportunistic lease distribution across partitions
- Confirm one-partition-many-consumers scenario
- Test many-partitions-one-consumer scenario
- Verify waiting consumers receive items when produced

**Context for Implementation:**
- Follow exact pattern from `service/queue_test.go:25-67`
- Partition tests at `partition_test.go:65-428` are more complex with multiple partition scenarios

### Validation
- [ ] Run: `go test ./service/... -v -run "^TestPartitions$" -timeout 5m`
- [ ] Verify: All three backends show in output
- [ ] Verify: All tests pass for PostgreSQL backend

---

## Phase 4: Fix PostgreSQL-Specific Issues (If Any)

### Overview
Address any test failures discovered when running the newly-enabled PostgreSQL tests. This phase may not be needed if all tests pass.

### Potential Issue Areas

Based on the PostgreSQL implementation and test patterns, potential issues may arise in:

1. **Timestamp Precision**: PostgreSQL uses microsecond precision for `TIMESTAMPTZ`. Pattern already handled in `storage_test.go:119-137` using `Truncate(clock.Microsecond)`.

2. **Transaction Isolation**: PostgreSQL has different transaction semantics than BadgerDB. Watch for race conditions in concurrent tests.

3. **Connection Pool Exhaustion**: Tests create many daemons. The pool manager at `postgres.go:40-101` uses reference counting - verify pools are properly released.

4. **Query Performance**: Large batch operations in partition tests (50+ items) may need index optimization.

### Diagnostic Commands
```bash
# Run with verbose output to identify specific failures
go test ./service/... -v -run "TestQueuesStorage/PostgreSQL" 2>&1 | tee postgres-queues.log
go test ./service/... -v -run "TestRetry/PostgreSQL" 2>&1 | tee postgres-retry.log
go test ./service/... -v -run "TestPartitions/PostgreSQL" 2>&1 | tee postgres-partitions.log
```

### Fix Pattern
For any failures:
1. Identify the specific test and error message
2. Check if it's a PostgreSQL-specific behavior difference
3. Fix in `internal/store/postgres.go` if implementation bug
4. Adjust test expectations only if PostgreSQL behavior is correct but different

**Context for Implementation:**
- PostgreSQL implementation: `internal/store/postgres.go`
- Timestamp handling pattern: `storage_test.go:119-137`
- Pool management: `postgres.go:40-101`

### Validation
- [ ] Run: `go test ./service/... -v -timeout 10m`
- [ ] Verify: All tests pass for all backends
- [ ] Verify: No goroutine leaks (goleak passes)

---

## Phase 5: Final Verification

### Overview
Run the complete test suite to verify all backends work correctly.

### Validation Commands

```bash
# Run all service tests with all backends
go test ./service/... -v -timeout 10m

# Verify specific PostgreSQL coverage
go test ./service/... -v -run "/PostgreSQL" -timeout 10m

# Run with race detector for concurrency issues
go test ./service/... -race -timeout 15m
```

### Success Criteria
- [ ] All tests pass for InMemory backend
- [ ] All tests pass for BadgerDB backend
- [ ] All tests pass for PostgreSQL backend
- [ ] No goroutine leaks detected
- [ ] No race conditions detected

### Validation
- [ ] Run: `go test ./service/... -timeout 10m`
- [ ] Verify: Exit code 0, all tests pass
