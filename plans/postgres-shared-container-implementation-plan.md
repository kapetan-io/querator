# PostgreSQL Shared Container Implementation Plan

## Overview

Optimize PostgreSQL test performance by starting a single container once per `go test` run via `TestMain`, rather than starting/stopping a container for each test function. Each `postgres.Setup()` call creates a new database within the shared container to maintain test isolation.

## Current State Analysis

**Current Implementation (`service/common_test.go:152-205`):**
- `postgresTestSetup.Setup()` starts a new PostgreSQL container via `postgres.Run()`
- `postgresTestSetup.Teardown()` terminates the container via `container.Terminate()`
- Each test file runs the container lifecycle multiple times
- Container startup takes ~3-7 seconds each time
- Total overhead: ~20-40+ seconds across all test files

**Existing Infrastructure:**
- `globalPools` in `internal/store/postgres.go:35-37` already implements connection pool sharing by DSN
- Random queue names and hashed table names (`items_{hash}_{partition}`) provide some isolation
- The `queues` table is shared across all queues within a database
- Each database gets its own connection pool (different DSN = different pool entry)

## Desired End State

After implementation:
1. A single PostgreSQL container starts in `TestMain` before any tests run
2. Each `postgres.Setup()` call creates a new database within the shared container
3. Each `postgres.Teardown()` call drops the database (not the container)
4. The container is terminated in `TestMain` after all tests complete
5. Test isolation is maintained via separate databases
6. All existing tests pass without modification to test logic

**Verification:**
- Run `go test ./service/... -v` and observe only ONE "Creating container for image postgres:16-alpine" log
- All tests pass
- No goroutine leaks detected by goleak

## What We're NOT Doing

- Not changing the test structure or test logic
- Not modifying the PostgreSQL storage implementation (`internal/store/postgres.go`)
- Not changing how other backends (InMemory, BadgerDB) work
- Not implementing connection pool sharing across databases (each database gets its own pool, which is intentional for isolation)
- Not adding infrastructure tests (existing tests implicitly verify the shared container works)

## Implementation Approach

1. Create a package-level shared container manager using `sync.Once`
2. Modify `postgresTestSetup` to create/drop databases instead of containers
3. Update `TestMain` to handle container lifecycle (note: `goleak.VerifyTestMain` never returns)
4. Ensure proper cleanup on test failure/panic (Ryuk handles this automatically)

## Phase 1: Implement Shared Container Manager

### Overview
Create a shared container manager that starts the PostgreSQL container once and provides connection strings for new databases.

### Changes Required:

#### 1. Add Required Imports
**File**: `service/common_test.go`
**Changes**: Add new imports at top of file

```go
import (
    "sync/atomic"
    "github.com/jackc/pgx/v5"
    // ... existing imports remain
)
```

#### 2. Add Shared Container Manager
**File**: `service/common_test.go`
**Changes**: Add new types and functions after the existing imports section, before `TestMain`

```go
type sharedPostgresContainer struct {
    container *postgres.PostgresContainer
    host      string
    port      string
    dbCounter atomic.Int64
}

var (
    sharedPostgres     *sharedPostgresContainer
    sharedPostgresOnce sync.Once
    sharedPostgresErr  error
)

func getSharedPostgresContainer() (*sharedPostgresContainer, error)

func (s *sharedPostgresContainer) Start(ctx context.Context) error

func (s *sharedPostgresContainer) Stop(ctx context.Context) error

func (s *sharedPostgresContainer) CreateDatabase(ctx context.Context) (dsn string, dbName string, err error)

func (s *sharedPostgresContainer) DropDatabase(ctx context.Context, dbName string) error
```

**Function Responsibilities:**

`getSharedPostgresContainer()`:
- Use `sync.Once` to ensure container starts only once
- Create `sharedPostgresContainer{}` and call `Start(context.Background())`
- Store result and error in package-level variables
- Return cached container or error on subsequent calls

`Start(ctx context.Context)`:
- Create PostgreSQL container using `postgres.Run()` with config matching current implementation
- Use `postgres.WithDatabase("postgres")` for the initial database (we create test databases separately)
- Extract host via `container.Host(ctx)`
- Extract port via `container.MappedPort(ctx, "5432/tcp")` then call `.Port()` on result
- Store container, host, port in struct fields
- Return wrapped error on failure

`Stop(ctx context.Context)`:
- Check if container is nil, return early if so
- Call `s.container.Terminate(ctx)`
- Log any termination errors but don't panic (cleanup is best-effort)

`CreateDatabase(ctx context.Context)`:
- Generate unique database name: `fmt.Sprintf("querator_test_%d", s.dbCounter.Add(1))`
- Build admin DSN: `fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres?sslmode=disable", s.host, s.port)`
- Connect using `pgx.Connect(ctx, adminDSN)`
- Execute `CREATE DATABASE` using `pgx.Identifier{dbName}.Sanitize()` for SQL safety
- Close connection
- Build and return DSN for new database: `fmt.Sprintf("postgres://postgres:postgres@%s:%s/%s?sslmode=disable", s.host, s.port, dbName)`

`DropDatabase(ctx context.Context, dbName string)`:
- Build admin DSN (same as CreateDatabase)
- Connect to `postgres` database (NOT the target database)
- Execute connection termination query to close any active connections to target database
- Execute `DROP DATABASE IF EXISTS` using `pgx.Identifier{dbName}.Sanitize()` for SQL safety
- Close connection
- Log errors but don't panic (cleanup is best-effort, test already completed)

**Context for implementation:**
- Container configuration matches current: `postgres:16-alpine`, user `postgres`, password `postgres`
- Wait strategy: `wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(30*time.Second)`
- Use existing `log` package-level variable for logging
- Database names: `querator_test_1`, `querator_test_2`, etc. (atomic counter ensures uniqueness)

**SQL for DropDatabase connection termination:**
```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = $1 AND pid <> pg_backend_pid()
```

#### 3. Update postgresTestSetup Struct and Methods
**File**: `service/common_test.go`
**Changes**: Replace existing `postgresTestSetup` implementation at lines 152-205

```go
type postgresTestSetup struct {
    dsn    string
    dbName string
}

func (p *postgresTestSetup) Setup(conf store.PostgresConfig) store.Config

func (p *postgresTestSetup) Teardown()
```

**Function Responsibilities:**

`Setup(conf store.PostgresConfig) store.Config`:
- Call `getSharedPostgresContainer()` to get shared container
- If error, panic with descriptive message (matches existing pattern at `common_test.go:170-172`)
- Call `container.CreateDatabase(context.Background())` to create new database
- If error, panic with descriptive message
- Store `dsn` and `dbName` in struct fields for later cleanup
- Set `conf.ConnectionString = dsn`
- Set `conf.Log = log` (package-level logger)
- Set `conf.MaxConns = 10` (matches current implementation at line 184)
- Create `store.Config` with `PostgresQueues` and `PostgresPartitionStore` (follow pattern at lines 186-195)
- Return the config

`Teardown()`:
- If `p.dbName` is empty, return early (nothing to clean up)
- Call `getSharedPostgresContainer()` to get shared container
- If error, log warning and return (don't panic, test already completed)
- Call `container.DropDatabase(context.Background(), p.dbName)`
- If error, log warning but don't panic (test already completed)

**Context for implementation:**
- Existing Setup pattern at `common_test.go:157-195`
- Existing Teardown pattern at `common_test.go:198-205`
- Panic on setup failure (prevents test from running with invalid state)
- Log on teardown failure (test result is already determined)

#### 4. Update TestMain
**File**: `service/common_test.go`
**Changes**: Modify existing `TestMain` at lines 46-63

**IMPORTANT**: `goleak.VerifyTestMain(m)` at line 62 **never returns** - it calls `os.Exit()` internally. Container cleanup must happen before this call.

```go
func TestMain(m *testing.M)
```

**Function Responsibilities:**
- Keep existing logging setup (lines 47-60)
- Use `defer` to ensure container cleanup runs before `goleak.VerifyTestMain`
- In defer: check if `sharedPostgres != nil`, if so call `sharedPostgres.Stop(context.Background())`
- Log any stop errors but don't panic
- Call `goleak.VerifyTestMain(m)` as final statement (this exits the process)

**Pattern:**
```go
func TestMain(m *testing.M) {
    // ... existing logging setup (lines 47-60) ...

    defer func() {
        if sharedPostgres != nil {
            if err := sharedPostgres.Stop(context.Background()); err != nil {
                // Use fmt since log might not be initialized in error path
                fmt.Fprintf(os.Stderr, "failed to stop shared postgres container: %v\n", err)
            }
        }
    }()

    goleak.VerifyTestMain(m)
}
```

**Context for implementation:**
- Existing TestMain at `common_test.go:46-63`
- `goleak.VerifyTestMain(m)` internally calls `m.Run()` then `os.Exit()`
- The `defer` ensures cleanup happens after tests but before process exit
- Must handle case where no PostgreSQL tests ran (container never started, `sharedPostgres` is nil)

### Validation
- [ ] Run: `go test ./service/... -run TestQueue/PostgreSQL -v 2>&1 | grep -c "Creating container for image"`
- [ ] Verify: Output is `1` (only one container created)
- [ ] Run: `go test ./service/... -v`
- [ ] Verify: All tests pass
- [ ] Verify: No goroutine leaks reported by goleak

## Phase 2: Handle Edge Cases and Cleanup

### Overview
Ensure robust handling of edge cases like database creation failures, cleanup on panic, and parallel test execution.

### Changes Required:

#### 1. Ensure DropDatabase Terminates Connections First
**File**: `service/common_test.go`
**Changes**: This is already specified in Phase 1 `DropDatabase` implementation, but emphasizing the importance

The `DropDatabase()` method must terminate active connections before dropping. This handles:
- Tests that panic leaving connections open
- Connection pools that haven't fully released connections
- Parallel subtests with lingering connections

**Implementation detail:**
```go
func (s *sharedPostgresContainer) DropDatabase(ctx context.Context, dbName string) error {
    adminDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres?sslmode=disable",
        s.host, s.port)

    conn, err := pgx.Connect(ctx, adminDSN)
    if err != nil {
        return fmt.Errorf("connect to postgres db: %w", err)
    }
    defer conn.Close(ctx)

    // Terminate all connections to target database
    _, err = conn.Exec(ctx,
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
        dbName)
    if err != nil {
        // Log but continue - some connections may have already closed
        log.Warn("failed to terminate connections", "database", dbName, "error", err)
    }

    // Drop the database using sanitized identifier
    _, err = conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", pgx.Identifier{dbName}.Sanitize()))
    if err != nil {
        return fmt.Errorf("drop database %s: %w", dbName, err)
    }

    return nil
}
```

#### 2. Add Defensive Checks in Setup/Teardown
**File**: `service/common_test.go`
**Changes**: Ensure nil checks and proper error messages

`postgresTestSetup.Setup()`:
- Check for nil container and error from `getSharedPostgresContainer()`
- Panic with context: `panic(fmt.Sprintf("failed to get shared postgres container: %v", err))`
- Check for error from `CreateDatabase()`
- Panic with context: `panic(fmt.Sprintf("failed to create test database: %v", err))`

`postgresTestSetup.Teardown()`:
- Return early if `p.dbName == ""` (nothing to clean up)
- Get container, log warning if error (don't panic)
- Call `DropDatabase`, log warning if error (don't panic)

**Error handling strategy:**
- **Setup**: Panic on any error (test cannot proceed without valid database)
- **Teardown**: Log errors but don't panic (test result already determined, Ryuk will clean up on process exit)

#### 3. Ryuk Cleanup Behavior
**No code changes needed** - documenting expected behavior:

- Testcontainers Ryuk automatically cleans up containers even if Go process crashes
- No special handling needed for SIGINT/SIGTERM
- Container will be removed even if `TestMain` cleanup doesn't run
- This provides safety net for unexpected test failures

### Validation
- [ ] Run: `go test ./service/... -v -count=2`
- [ ] Verify: Tests pass on repeated runs (databases properly cleaned up)
- [ ] Run: `go test ./service/... -v -parallel=4`
- [ ] Verify: Tests pass with parallel execution
- [ ] Verify: No "database already exists" errors
- [ ] Verify: No orphaned databases left in container after tests complete

## Summary

This implementation:
1. Reduces PostgreSQL test overhead from ~20-40+ seconds to ~3-7 seconds (single container start)
2. Maintains test isolation via separate databases per `Setup()` call
3. Requires minimal changes to existing test structure
4. Uses existing testcontainers patterns and PostgreSQL connection pooling

**Key Files Modified:**
- `service/common_test.go` - All changes contained here

**No Changes Required:**
- `internal/store/postgres.go` - Storage implementation unchanged
- Individual test files - Test logic unchanged
- Other storage backends - InMemory and BadgerDB unchanged

**Connection Pool Behavior:**
- Each database gets its own connection pool (different DSN = different pool in `globalPools`)
- This is intentional for test isolation
- Pools are cleaned up when `PostgresConfig.Close()` is called during daemon shutdown
- Memory usage scales with number of concurrent tests, but is bounded by test count
