# PostgreSQL Storage Implementation Plan

- [x] Phase 1: PostgreSQL Queues Implementation
- [x] Phase 2: PostgreSQL Partition Implementation
- [x] Phase 3: Test Integration

## Overview

Add PostgreSQL support to the `internal/store` package, implementing the `Queues`, `PartitionStore`, and `Partition` interfaces. This will enable Querator to use PostgreSQL as a persistent storage backend alongside the existing in-memory and BadgerDB implementations.

## Current State Analysis

### Existing Implementations
- **Memory** (`internal/store/memory.go`): In-memory slice-based storage with mutex synchronization
- **BadgerDB** (`internal/store/badger.go`): Embedded key-value store using gob encoding and lazy DB initialization

### Key Requirements Discovered
1. **Three interfaces** must be implemented (`internal/store/store.go:29-168`):
   - `Queues`: Manages queue metadata (Get, Add, Update, List, Delete, Close)
   - `PartitionStore`: Factory for creating Partition instances
   - `Partition`: Manages queue items within a partition (18 methods total)

2. **Lazy initialization** (ADR 0021-storage-lazy-initialization.md):
   - Database connections created on first use, not at startup
   - Must handle connection failures gracefully
   - Pattern: `getDB()` helper method that initializes on first call

3. **FIFO ordering** (ADR 0014-ordered-storage.md):
   - Items MUST be retrieved in insertion order
   - Uses KSUID for IDs which provide natural time-based ordering
   - Primary key on ID field ensures ordered retrieval

4. **Functional test framework** (`storage_test.go:20-56`, `queue_test.go:24-59`):
   - Tests automatically run against all storage backends
   - PostgreSQL placeholder already exists (commented out)
   - Tests validate complete workflows through public APIs only

### Data Structures
- **types.Item** (`internal/types/items.go:23-58`):
  - ID ([]byte/KSUID), IsLeased (bool), LeaseDeadline, ExpireDeadline, EnqueueAt, CreatedAt (time.Time)
  - Attempts, MaxAttempts (int), Reference, Encoding, Kind (string), Payload ([]byte)

- **types.QueueInfo** (`internal/types/items.go:152-177`):
  - Name, DeadQueue, Reference (string)
  - LeaseTimeout, ExpireTimeout (Duration)
  - CreatedAt, UpdatedAt (time.Time)
  - MaxAttempts, RequestedPartitions (int)

## Desired End State

After implementation, Querator will support PostgreSQL as a production-ready storage backend with:
- Automatic table creation using lazy initialization
- Efficient batch operations via pgx.Batch
- Thread-safe connection pooling
- Full functional test coverage
- FIFO ordering guarantees

### Verification
```bash
# Run all tests with PostgreSQL backend
go test ./... -v -run "^TestQueue/PostgreSQL.*$"
go test ./... -v -run "^TestQueueStorage/PostgreSQL.*$"

# Run full test suite
make test
```

## What We're NOT Doing

- Database migrations framework (using auto-create on demand instead)
- Query optimization/indexing beyond basic schema (implement basic schema first, optimize later)
- PostgreSQL-specific features (LISTEN/NOTIFY, stored procedures, etc.)
- Multi-database sharding (single database instance per config)
- Advanced monitoring/metrics integration (basic health check only)

## Implementation Approach

Use `pgxpool.Pool` for connection management because:
1. **Concurrent access patterns**: Multiple partitions run in parallel goroutines
2. **Shared Queues interface**: Called from multiple logical queue instances
3. **Thread-safety**: pgxpool handles concurrent connection acquisition automatically
4. **Resource efficiency**: Reuses connections across partitions
5. **Production-ready**: Built-in health checks and connection management

Alternative single-connection-per-partition approach would require additional synchronization complexity.

## Phase 1: PostgreSQL Queues Implementation

### Overview
Implement the `Queues` interface for managing queue metadata in a PostgreSQL table.

### Changes Required

#### 1. Add PostgreSQL Dependencies
**File**: `go.mod`
**Changes**: Add pgx and testcontainers dependencies
```go
require (
    github.com/jackc/pgx/v5 v5.5.0
    github.com/jackc/pgx/v5/pgxpool v5.5.0
    github.com/testcontainers/testcontainers-go v0.28.0
    github.com/testcontainers/testcontainers-go/modules/postgres v0.28.0
)
```

#### 2. PostgreSQL Configuration
**File**: `internal/store/postgres.go` (new file)
**Changes**: Define configuration struct and global pool management

```go
// postgresPoolManager manages a single connection pool with atomic reference counting
// Global singleton ensures one pool per unique connection string across entire application
type postgresPoolManager struct {
    pool     *pgxpool.Pool
    refCount atomic.Int32  // Atomic reference counter (no mutex needed)
}

var (
    globalPoolMu sync.Mutex
    globalPools  = make(map[string]*postgresPoolManager)
)

// acquirePool returns the shared pool for a connection string, creating it if needed
// Thread-safe via global mutex, uses atomic operations for reference counting
func acquirePool(connString string, maxConns int32, log *slog.Logger) (*pgxpool.Pool, error)

// releasePool decrements reference count and closes pool when count reaches zero
// Thread-safe via global mutex, idempotent (safe to call multiple times)
func releasePool(connString string)

type PostgresConfig struct {
    // ConnectionString is the PostgreSQL connection string
    // Examples:
    //   Development: "postgres://user:pass@localhost:5432/querator?sslmode=disable"
    //   Production:  "postgres://user:pass@host:5432/querator?sslmode=require"
    ConnectionString string

    // MaxConns sets the maximum number of connections in the pool
    //
    // Sizing Formula: (RequestedPartitions * 2) + 10
    // - Each partition needs 2 connections: one for request loop, one for lifecycle
    // - Add 10 for Queues operations (CRUD on queue metadata) and overhead
    //
    // Examples:
    //   - 10 partitions:  (10 * 2) + 10 = 30 connections
    //   - 50 partitions:  (50 * 2) + 10 = 110 connections
    //   - 100 partitions: (100 * 2) + 10 = 210 connections
    //
    // Alternative Sizing:
    //   - Conservative: RequestedPartitions + 10 (minimal, may bottleneck under load)
    //   - Aggressive:   RequestedPartitions * 4 (high concurrency, more PostgreSQL overhead)
    //
    // If 0, uses pgxpool defaults (4 * runtime.NumCPU()), which may be insufficient
    // for partition counts exceeding CPU count
    MaxConns int32

    // ScanBatchSize controls how many items are fetched per batch in ScanForActions/ScanForScheduled
    // Default: 1000 (matches lifecycle batching pattern)
    // Increase for large partitions with slow networks, decrease for memory-constrained environments
    ScanBatchSize int

    // Log for error and debug logging
    Log *slog.Logger

    // OnQueryComplete is an optional callback for instrumentation/metrics
    // Called after each query with operation name, duration, and error (if any)
    OnQueryComplete func(operation string, duration time.Duration, err error)

    // connString stores the connection string for cleanup (internal use)
    connString string
}

// getOrCreatePool returns the shared pool, acquiring a reference (thread-safe)
func (c *PostgresConfig) getOrCreatePool(ctx context.Context) (*pgxpool.Pool, error)

// Close releases a reference to the connection pool
// The pool is only closed when all references are released
// This method is idempotent and safe to call multiple times
func (c *PostgresConfig) Close()

// Ping verifies connectivity to PostgreSQL (supports health check requirements)
func (c *PostgresConfig) Ping(ctx context.Context) error
```

**Function Responsibilities:**

**Global Pool Manager (Singleton Pattern with Atomic Operations):**
- `acquirePool`: Thread-safe pool acquisition with lazy creation and retry logic
  - Lock global mutex (protects map access only)
  - Check if pool manager exists for this connection string
  - If not exists:
    - Create pool config from connection string
    - Apply MaxConns if specified (otherwise use pgxpool defaults)
    - Retry pool creation up to 5 times with exponential backoff:
      - Attempt 1: immediate
      - Attempt 2: 100ms delay
      - Attempt 3: 200ms delay
      - Attempt 4: 400ms delay
      - Attempt 5: 800ms delay
    - Log warnings on retry with attempt number and error
    - Create pool manager with pool reference and refCount initialized to 0
    - Store in global map
  - Atomically increment reference count (lock-free operation)
  - Unlock global mutex and return pool
- `releasePool`: Thread-safe pool cleanup with automatic closure
  - Lock global mutex (protects map access only)
  - Find pool manager for connection string
  - If found:
    - Atomically decrement refCount (lock-free operation)
    - If new refCount == 0:
      - Close pool (releases all connections)
      - Remove from global map
  - Unlock global mutex

**Configuration Methods:**
- `getOrCreatePool`: Simple wrapper around global acquirePool
  - Validates ConnectionString is non-empty
  - Calls acquirePool(c.ConnectionString, c.MaxConns, c.Log)
  - Stores connection string for cleanup
  - Returns pool or error
- `Close`: Simple wrapper around global releasePool
  - Calls releasePool(c.connString)
  - Pool only closes when ALL references released (Queues + all Partitions)
  - Idempotent: safe to call multiple times
- `Ping`: Health check implementation
  - Gets pool via getOrCreatePool(ctx)
  - Calls pool.Ping(ctx)
  - Returns error or nil

**Connection Pool Sharing Pattern:**
Both `PostgresQueues` and `PostgresPartition` call `conf.getOrCreatePool()` which:
1. Uses global singleton map keyed by connection string
2. Ensures only ONE pool per unique connection string across entire application
3. Thread-safe via single global mutex protecting map access
4. Lock-free reference counting via atomic.Int32 operations
5. Pool closes automatically when last component releases its reference
6. Automatic retry with exponential backoff on connection failures

**Pool Lifecycle and Ownership:**
- **Owner**: Global `globalPools` map owns all pool lifecycle
- **Creation**: Lazy initialization on first `acquirePool()` call with retry logic
- **Cleanup**: Both `PostgresQueues.Close()` and `PostgresPartition.Close()` call `conf.Close()` → `releasePool()`
- **Reference Counting**: Atomic increment/decrement with automatic pool closure when refCount reaches 0
- **Shutdown Order**: Partitions close first (logical.go:1353-1360), then Queues (queues_manager.go:287)
- **Synchronization**:
  - Global mutex protects map access (create, delete, lookup)
  - Atomic operations protect reference count (no mutex needed)
  - Single lock design eliminates deadlock potential
  - No nested locking complexity

#### 3. PostgresQueues Implementation
**File**: `internal/store/postgres.go`
**Changes**: Implement Queues interface

```go
type PostgresQueues struct {
    QueuesValidation
    conf PostgresConfig
}

func NewPostgresQueues(conf PostgresConfig) *PostgresQueues

func (p *PostgresQueues) ensureTable(ctx context.Context, pool *pgxpool.Pool) error

func (p *PostgresQueues) Get(ctx context.Context, name string, queue *types.QueueInfo) error

func (p *PostgresQueues) Add(ctx context.Context, info types.QueueInfo) error

func (p *PostgresQueues) Update(ctx context.Context, info types.QueueInfo) error

func (p *PostgresQueues) List(ctx context.Context, queues *[]types.QueueInfo, opts types.ListOptions) error

func (p *PostgresQueues) Delete(ctx context.Context, name string) error

func (p *PostgresQueues) Close(ctx context.Context) error
```

**Function Responsibilities:**
- `NewPostgresQueues`: Set defaults, return instance (NO pool creation - lazy init via conf.getOrCreatePool())
- `ensureTable`: Create `queues` table if not exists (called by each method)
- `Get`:
  - Get pool via `conf.getOrCreatePool(ctx)`
  - Call `ensureTable` to create table if needed
  - SELECT single queue: `SELECT * FROM queues WHERE name = $1`
  - Handle `pgx.ErrNoRows` → return `store.ErrQueueNotExist`
  - Scan row into QueueInfo (convert BIGINT to Duration, TIMESTAMPTZ to time.Time)
- `Add`:
  - Validate using `validateAdd(info)`
  - Get pool via `conf.getOrCreatePool(ctx)`
  - Call `ensureTable`
  - INSERT new queue
  - Handle constraint violation → return `transport.NewInvalidOption("queue '%s' already exists")`
- `Update`:
  - Validate using `validateQueueName(info)`
  - Get pool, ensure table
  - SELECT existing queue (return `ErrQueueNotExist` if not found)
  - Merge updates using `found.Update(info)`
  - Validate using `validateUpdate(found)`
  - UPDATE with new values
- `List`:
  - Validate using `validateList(opts)`
  - Get pool, ensure table
  - SELECT with pivot: `SELECT * FROM queues WHERE name >= $1 ORDER BY name LIMIT $2`
  - **IMPORTANT**: Use `>=` (inclusive) to include pivot item, matching badger.go:1087 and memory.go:640 behavior
  - If no pivot (empty string), `WHERE name >= ''` gets all queues from beginning
  - Scan each row into QueueInfo
- `Delete`:
  - Validate using `validateDelete(name)`
  - Get pool, ensure table
  - DELETE: `DELETE FROM queues WHERE name = $1` (no error if not exists)
- `Close`: Call `conf.Close()` to release our reference to the pool (pool closes when all references released)

**Testing Requirements:**

New test functions to add (only if new functionality):
- None required - existing tests in `storage_test.go` will validate

**Test Objectives:**
- Validate lazy pool initialization
- Validate table auto-creation
- Validate all CRUD operations through existing functional tests
- Validate concurrent access patterns
- Validate error handling for connection failures

**Context for Implementation:**
- Follow validation patterns from `memory.go:570-683`
- Use `QueuesValidation` embedded struct for input validation
- Follow lazy initialization pattern from `badger.go:933-951`
- Table schema (see Database Schema section below)
- Use `pgx.QueryRow` for Get, `pgx.Exec` for Add/Update/Delete, `pgx.Query` for List

### Database Schema

```sql
CREATE TABLE IF NOT EXISTS queues (
    name TEXT PRIMARY KEY,
    lease_timeout_ns BIGINT NOT NULL,
    expire_timeout_ns BIGINT NOT NULL,
    dead_queue TEXT NOT NULL DEFAULT '',
    max_attempts INTEGER NOT NULL DEFAULT 0,
    reference TEXT NOT NULL DEFAULT '',
    requested_partitions INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

**Schema Notes:**
- Store durations as nanoseconds (BIGINT) for precision (Duration.Nanoseconds())
- Use TIMESTAMPTZ for proper timezone handling (always use time.UTC())
- PRIMARY KEY on name provides ordering for List operations (no additional index needed)
- All fields NOT NULL with defaults where appropriate
- **Missing field `PartitionInfo`**: This is runtime-only data populated from partition queries, NOT stored in queues table

## Phase 2: PostgreSQL Partition Implementation

### Overview
Implement the `PartitionStore` and `Partition` interfaces for managing queue items.

### Changes Required

#### 1. PostgresPartitionStore Implementation
**File**: `internal/store/postgres.go`
**Changes**: Implement PartitionStore interface

```go
type PostgresPartitionStore struct {
    conf PostgresConfig
}

func NewPostgresPartitionStore(conf PostgresConfig) *PostgresPartitionStore

func (p *PostgresPartitionStore) Get(info types.PartitionInfo) Partition
```

**Function Responsibilities:**
- `NewPostgresPartitionStore`: Store config, return instance
- `Get`: Create new PostgresPartition instance with partition info and initialized UID

**Implementation Example:**
```go
func (p *PostgresPartitionStore) Get(info types.PartitionInfo) Partition {
    return &PostgresPartition{
        uid:  ksuid.New(),  // CRITICAL: Initialize UID for ID generation
        conf: p.conf,
        info: info,
    }
}
```

#### 2. PostgresPartition Implementation
**File**: `internal/store/postgres.go`
**Changes**: Implement Partition interface

```go
type PostgresPartition struct {
    info      types.PartitionInfo
    conf      PostgresConfig
    mu        sync.RWMutex  // Protects info and uid fields
    uid       ksuid.KSUID   // Protected by mu (required for thread-safe ID generation)
    tableOnce sync.Once     // Ensures table is created only once
    tableErr  error         // Cached error from table creation
}

func (p *PostgresPartition) ensureTable(ctx context.Context, pool *pgxpool.Pool) error

func (p *PostgresPartition) tableName() string

func (p *PostgresPartition) Produce(ctx context.Context, batch types.ProduceBatch, now clock.Time) error

func (p *PostgresPartition) Lease(ctx context.Context, batch types.LeaseBatch, opts LeaseOptions) error

func (p *PostgresPartition) Complete(ctx context.Context, batch types.Batch[types.CompleteRequest]) error

func (p *PostgresPartition) Retry(ctx context.Context, batch types.Batch[types.RetryRequest]) error

func (p *PostgresPartition) List(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error

func (p *PostgresPartition) ListScheduled(ctx context.Context, items *[]*types.Item, opts types.ListOptions) error

func (p *PostgresPartition) Add(ctx context.Context, items []*types.Item, now clock.Time) error

func (p *PostgresPartition) Delete(ctx context.Context, ids []types.ItemID) error

func (p *PostgresPartition) Clear(ctx context.Context, destructive bool) error

func (p *PostgresPartition) Stats(ctx context.Context, stats *types.PartitionStats, now clock.Time) error

func (p *PostgresPartition) ScanForScheduled(timeout clock.Duration, now clock.Time) iter.Seq[types.Action]

func (p *PostgresPartition) ScanForActions(timeout clock.Duration, now clock.Time) iter.Seq[types.Action]

func (p *PostgresPartition) TakeAction(ctx context.Context, batch types.Batch[types.LifeCycleRequest], state *types.PartitionState) error

func (p *PostgresPartition) LifeCycleInfo(ctx context.Context, info *types.LifeCycleInfo) error

func (p *PostgresPartition) Info() types.PartitionInfo

func (p *PostgresPartition) UpdateQueueInfo(info types.QueueInfo)

func (p *PostgresPartition) Close(ctx context.Context) error

func (p *PostgresPartition) validateID(id []byte) error

// Helper functions for time conversion
func timeToNullable(t clock.Time) interface{}
func nullableToTime(val interface{}) clock.Time
```

**Function Responsibilities:**

**Initialization & Helpers:**
- `ensureTable`: Create partition table if not exists using sync.Once to prevent concurrent creation races (called by each method like `getOrCreatePool`)
- `tableName`: Return properly quoted table identifier using pgx.Identifier (see "Table Name Safety" section below)
- `validateID`: Parse KSUID from bytes, return error if invalid
- `timeToNullable`: Convert Go `time.Time` to PostgreSQL value (NULL if IsZero(), else timestamp)
- `nullableToTime`: Convert PostgreSQL NULL/timestamp back to `time.Time`

**Core Operations:**
- `Produce`: See "Produce Implementation" section below
- `Lease`: See "Lease Implementation with Race Prevention" section below
- `Complete`: See "Complete Implementation" section below
- `Retry`: See "Retry Implementation" section below

**List Operations:**
- `List`: SELECT WHERE enqueue_at IS NULL, ORDER BY id. Support pivot (inclusive) and limit
- `ListScheduled`: SELECT WHERE enqueue_at IS NOT NULL, ORDER BY id. Support pivot (inclusive) and limit
- **IMPORTANT**: Pivot behavior is inclusive (`WHERE id >= $1`), matching badger.go:352 and memory.go:205

**Storage Management:**
- `Add`: INSERT items using Batch. Generate KSUIDs, **NO EnqueueAt normalization** (only Produce normalizes)
- `Delete`: DELETE items by IDs using Batch (no error if not exist)
- `Clear`: DELETE WHERE is_leased=false (or all if destructive=true). Non-destructive only removes unleased items, preserving leased items regardless of schedule status

**Lifecycle Management:**
- `Stats`: SELECT COUNT, AVG for leased/unleased items
- `ScanForScheduled`: SELECT items WHERE enqueue_at <= now. Return iterator
- `ScanForActions`: SELECT items for lease expiry, max attempts, item expiry. Return iterator
- `TakeAction`: Process ActionLeaseExpired, ActionDeleteItem, ActionQueueScheduledItem using Batch
- `LifeCycleInfo`: SELECT MIN(lease_deadline) WHERE is_leased=true

**Metadata:**
- `Info`: Return partition info (thread-safe with mutex)
- `UpdateQueueInfo`: Update queue info in partition info (thread-safe with mutex)
- `Close`: Call `conf.Close()` to release our reference to the pool (pool closes when all references released)

**Testing Requirements:**

Existing test functions that will validate:
- `TestQueue` (`queue_test.go`) - validates all partition operations
- `TestQueueStorage` (`storage_test.go`) - validates storage CRUD operations

**Test Objectives:**
- Validate all 18 Partition interface methods work correctly
- Validate FIFO ordering maintained (critical for functional tests)
- Validate pgx.Batch usage reduces database round-trips
- Validate concurrent partition access
- Validate lease expiry and lifecycle actions
- Validate scheduled item handling

**Context for Implementation:**
- Follow memory.go patterns for business logic
- Follow badger.go patterns for storage interaction
- Use pgx.Batch for Produce, Complete, Add, Delete, TakeAction (NOT Lease or Retry - those use transactions)
- KSUID generation: `uid = uid.Next()` pattern from `memory.go:40` (protect with mutex like memory.go:36-37)
- EnqueueAt normalization logic at `memory.go:44-55`
- TakeAction logic at `memory.go:398-473` and `badger.go:667-787`
- Retry logic at `badger.go:246-331` (transaction pattern)
- Queue name validation at `validation.go:11-45` (max 512 chars, no whitespace, no tilde)

### Critical Implementation Details

#### Error Mapping

Map pgx errors to Querator error types:

| pgx Error | Querator Error | Methods |
|-----------|---------------|---------|
| `pgx.ErrNoRows` | `store.ErrQueueNotExist` | Get (queues) |
| `pgx.ErrNoRows` (items) | `transport.NewInvalidOption("id does not exist")` | Complete, Retry |
| Constraint violation (23505) | `transport.NewInvalidOption("queue already exists")` | Add (queues) |
| Connection errors | Wrap with `errors.Errorf("connection failed: %w")`, lazy init will retry | All methods |
| Context timeout | Return as-is, caller handles | All methods |
| All other errors | Wrap with `errors.Errorf("operation failed: %w")` for stack traces | All methods |

**Pattern:**
```go
err := pool.QueryRow(ctx, query, args...).Scan(&result)
if err != nil {
    if errors.Is(err, pgx.ErrNoRows) {
        return store.ErrQueueNotExist
    }
    return errors.Errorf("during query: %w", err)
}
```

#### Table Name Safety

Queue names are validated at the storage layer (`validation.go:11-45`) with these rules:
- **Max length**: 512 characters
- **No whitespace**: Cannot contain spaces, tabs, newlines, etc.
- **No tilde**: Cannot contain `~` (reserved by BadgerDB)
- **No empty names**: Cannot be empty or whitespace-only

**However**, these validations are NOT sufficient to prevent SQL injection when queue names are used in table names. Use pgx.Identifier for proper quoting:

```go
func (p *PostgresPartition) tableName() string {
    // Use hash to stay under PostgreSQL's 63-character identifier limit
    // Queue names can be up to 512 chars (validation.go:11-45)
    // Format: items_{hash}_{partition} (e.g., "items_a1b2c3d4_0")
    hash := hashQueueName(p.info.Queue.Name)
    name := fmt.Sprintf("items_%s_%d", hash, p.info.Num)

    // Use pgx.Identifier to properly quote the identifier
    // This prevents SQL injection from special characters in queue names
    return pgx.Identifier{name}.Sanitize()
}
```

**Why pgx.Identifier is Required:**
- Queue names can contain characters that have special meaning in SQL (e.g., `-`, `.`, `'`, `"`)
- Direct concatenation like `"DELETE FROM " + tableName` is unsafe
- pgx.Identifier properly escapes PostgreSQL identifiers using double quotes when needed

**Usage in Queries:**
```go
// CORRECT: Use tableName() which returns properly quoted identifier
query := "SELECT * FROM " + p.tableName()

// Examples of what pgx.Identifier handles:
// "my-queue"    → "my-queue"     (quoted because of hyphen)
// "my.queue"    → "my.queue"     (quoted because of period)
// "select"      → "select"       (quoted because it's a keyword)
// "my_queue"    → my_queue       (no quotes needed)
```

**Validation Reference:**
- Implementation: `internal/store/validation.go:11-45`
- Rules enforced by `QueuesValidation.validateQueueInfo()`
- Called by `Add()`, `Update()`, `Delete()` at storage layer

#### Transaction Strategy

Use transactions for operations requiring atomicity:

| Method | Transaction? | Reason |
|--------|--------------|--------|
| Produce | **NO** | Use pgx.Batch without transaction (performance) |
| Lease | **YES** | Prevent race conditions with FOR UPDATE SKIP LOCKED |
| Complete | **YES** | Atomic SELECT FOR UPDATE + DELETE prevents race conditions |
| Retry | **YES** | Atomic read-modify-write required (consistent with BadgerDB pattern) |
| Add | **NO** | Use pgx.Batch |
| Delete | **NO** | Use pgx.Batch, idempotent |
| TakeAction | **NO** | Use pgx.Batch (lifecycle actions are eventually consistent) |

**Transaction Pattern (Lease and Retry):**
```go
tx, err := pool.Begin(ctx)
if err != nil {
    return errors.Errorf("begin transaction: %w", err)
}
defer tx.Rollback(ctx)  // Safe to call even after commit

// Lease: Execute SELECT FOR UPDATE SKIP LOCKED + UPDATE batch
// Retry: Execute SELECT FOR UPDATE + UPDATE/DELETE operations

if err := tx.Commit(ctx); err != nil {
    return errors.Errorf("commit transaction: %w", err)
}
```

**Transaction Isolation Notes:**
- PostgreSQL default isolation level is READ COMMITTED
- FOR UPDATE SKIP LOCKED requires READ COMMITTED or higher
- The default is appropriate for our use case
- No explicit isolation level setting needed

#### Time/NULL Handling

Convert between Go `time.Time` and PostgreSQL TIMESTAMPTZ with proper NULL handling:

**Helper Functions:**
```go
// timeToNullable converts Go time to PostgreSQL value (NULL if zero)
func timeToNullable(t time.Time) interface{} {
    if t.IsZero() {
        return nil  // PostgreSQL NULL
    }
    return t
}

// nullableToTime converts PostgreSQL value to Go time (zero if NULL)
func nullableToTime(val interface{}) time.Time {
    if val == nil {
        return time.Time{}  // Go zero time
    }
    if t, ok := val.(time.Time); ok {
        return t
    }
    return time.Time{}
}
```

**Field Mapping:**
- `LeaseDeadline`: NULL if not leased, otherwise timestamp
- `EnqueueAt`: NULL if not scheduled, otherwise timestamp
- `ExpireDeadline`: Always timestamp (NOT NULL)
- `CreatedAt`: Always timestamp (NOT NULL)

**Scanning Example:**
```go
var leaseDeadline sql.NullTime
var enqueueAt sql.NullTime

err := row.Scan(&item.ID, &item.IsLeased, &leaseDeadline, &expireDeadline, &enqueueAt, ...)

if leaseDeadline.Valid {
    item.LeaseDeadline = leaseDeadline.Time
}
if enqueueAt.Valid {
    item.EnqueueAt = enqueueAt.Time
}
```

#### Produce Implementation

**Use pgx.Batch for efficient multi-item inserts with EnqueueAt normalization**

```go
func (p *PostgresPartition) Produce(ctx context.Context, batch types.ProduceBatch, now clock.Time) error {
    pool, err := p.conf.getOrCreatePool(ctx)
    if err != nil {
        return err
    }

    if err := p.ensureTable(ctx, pool); err != nil {
        return err
    }

    // Create batch for all INSERTs
    pgxBatch := &pgx.Batch{}

    // Track which request each statement belongs to for error mapping
    requestIndexes := []int{}

    p.mu.Lock()
    for i := range batch.Requests {
        for _, item := range batch.Requests[i].Items {
            // Generate KSUID for new item
            p.uid = p.uid.Next()
            item.ID = []byte(p.uid.String())
            item.CreatedAt = now

            // EnqueueAt normalization (memory.go:44-55 pattern)
            // If EnqueueAt is less than 100ms from now, enqueue immediately
            // This avoids unnecessary work for items that will be queued almost immediately
            if item.EnqueueAt.Before(now.Add(time.Millisecond * 100)) {
                item.EnqueueAt = time.Time{}
            }

            // Queue INSERT statement
            pgxBatch.Queue(`
                INSERT INTO `+p.tableName()+` (
                    id, is_leased, lease_deadline, expire_deadline, enqueue_at, created_at,
                    attempts, max_attempts, reference, encoding, kind, payload
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
                string(item.ID),
                item.IsLeased,
                timeToNullable(item.LeaseDeadline),
                item.ExpireDeadline,
                timeToNullable(item.EnqueueAt),
                item.CreatedAt,
                item.Attempts,
                item.MaxAttempts,
                item.Reference,
                item.Encoding,
                item.Kind,
                item.Payload,
            )

            requestIndexes = append(requestIndexes, i)
        }
    }
    p.mu.Unlock()

    // Execute batch
    br := pool.SendBatch(ctx, pgxBatch)
    defer br.Close()

    // Check results for each statement
    for idx, reqIdx := range requestIndexes {
        _, err := br.Exec()
        if err != nil {
            batch.Requests[reqIdx].Err = transport.NewInternalError("failed to insert item %d: %s", idx, err)
            // Continue processing remaining items
        }
    }

    return nil
}
```

**Key Points:**
- KSUID generation protected by mutex (memory.go:36-41 pattern)
- EnqueueAt normalization to avoid unnecessary scheduled item handling
- pgx.Batch for efficient multi-row INSERT
- Per-request error tracking via requestIndexes slice
- No transaction needed - INSERTs are independent operations

#### Add Implementation

**Use pgx.Batch for multi-item inserts with KSUID generation (NO EnqueueAt normalization)**

```go
func (p *PostgresPartition) Add(ctx context.Context, items []*types.Item, now clock.Time) error {
    pool, err := p.conf.getOrCreatePool(ctx)
    if err != nil {
        return err
    }

    if err := p.ensureTable(ctx, pool); err != nil {
        return err
    }

    // Create batch for all INSERTs
    pgxBatch := &pgx.Batch{}

    p.mu.Lock()
    for _, item := range items {
        // Generate KSUID for new item
        p.uid = p.uid.Next()
        item.ID = []byte(p.uid.String())
        item.CreatedAt = now

        // NOTE: NO EnqueueAt normalization in Add() - only Produce() normalizes
        // This matches memory.go:248-264 and badger.go:436-465 behavior

        // Queue INSERT statement
        pgxBatch.Queue(`
            INSERT INTO `+p.tableName()+` (
                id, is_leased, lease_deadline, expire_deadline, enqueue_at, created_at,
                attempts, max_attempts, reference, encoding, kind, payload
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
            string(item.ID),
            item.IsLeased,
            timeToNullable(item.LeaseDeadline),
            item.ExpireDeadline,
            timeToNullable(item.EnqueueAt),
            item.CreatedAt,
            item.Attempts,
            item.MaxAttempts,
            item.Reference,
            item.Encoding,
            item.Kind,
            item.Payload,
        )
    }
    p.mu.Unlock()

    // Execute batch
    br := pool.SendBatch(ctx, pgxBatch)
    defer br.Close()

    // Check results for each statement
    for range items {
        _, err := br.Exec()
        if err != nil {
            return errors.Errorf("postgres partition %s/%d: failed to insert item: %w",
                p.info.Queue.Name, p.info.PartitionNum, err)
        }
    }

    return nil
}
```

**Key Points:**
- KSUID generation protected by mutex (same as Produce)
- **NO EnqueueAt normalization** - only Produce() normalizes (matches memory.go and badger.go)
- pgx.Batch for efficient multi-row INSERT
- Error messages include queue/partition context for debugging
- No transaction needed - INSERTs are independent operations

#### Complete Implementation

**Use simple transaction loop matching BadgerDB pattern for correctness and maintainability**

**Why Simple Loop Instead of Batch/CTE?**

Complete requires granular error reporting that cannot be achieved with batch operations or CTEs:

1. **Error Granularity**: Must distinguish between:
   - Invalid ID format (malformed KSUID)
   - ID not found in database
   - ID found but not leased

2. **Per-Request Error Handling**: When one ID in a request fails validation, that entire request fails (continue nextBatch), but other requests in the batch continue processing.

3. **Early Exit Pattern**: The `continue nextBatch` pattern allows immediate failure on first error in a request, matching BadgerDB semantics (badger.go:202-228).

4. **Performance**: PostgreSQL automatically pipelines operations within a transaction, providing similar efficiency to explicit batching while maintaining error granularity.

5. **Consistency**: Matches BadgerDB pattern exactly (badger.go:178-244), ensuring identical behavior across storage backends.

**Alternative batch approaches would lose this granularity:**
- pgx.Batch: Can't distinguish which IDs failed validation or why
- CTE with RETURNING: Returns successful IDs but not failure reasons
- Bulk operations: No per-ID error information

```go
func (p *PostgresPartition) Complete(ctx context.Context, batch types.Batch[types.CompleteRequest]) error {
    pool, err := p.conf.getOrCreatePool(ctx)
    if err != nil {
        return err
    }

    if err := p.ensureTable(ctx, pool); err != nil {
        return err
    }

    // Use transaction for atomic validation and deletion
    tx, err := pool.Begin(ctx)
    if err != nil {
        return errors.Errorf("postgres partition %s/%d: begin transaction: %w",
            p.info.Queue.Name, p.info.PartitionNum, err)
    }
    defer tx.Rollback(ctx)  // Safe to call even after commit

nextBatch:
    for i := range batch.Requests {
        for _, id := range batch.Requests[i].Ids {
            // Validate ID format
            if err := p.validateID(id); err != nil {
                batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
                continue nextBatch
            }

            // SELECT FOR UPDATE to lock and validate item
            var isLeased bool
            err := tx.QueryRow(ctx,
                `SELECT is_leased FROM `+p.tableName()+` WHERE id = $1 FOR UPDATE`,
                string(id)).Scan(&isLeased)

            if err == pgx.ErrNoRows {
                batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", id)
                continue nextBatch
            }
            if err != nil {
                return errors.Errorf("postgres partition %s/%d: failed to check item: %w",
                    p.info.Queue.Name, p.info.PartitionNum, err)
            }

            if !isLeased {
                batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not marked as leased", id)
                continue nextBatch
            }

            // Delete the item
            _, err = tx.Exec(ctx, `DELETE FROM `+p.tableName()+` WHERE id = $1`, string(id))
            if err != nil {
                return errors.Errorf("postgres partition %s/%d: failed to delete item: %w",
                    p.info.Queue.Name, p.info.PartitionNum, err)
            }
        }
    }

    if err := tx.Commit(ctx); err != nil {
        return errors.Errorf("postgres partition %s/%d: commit transaction: %w",
            p.info.Queue.Name, p.info.PartitionNum, err)
    }

    return nil
}
```

**Key Points:**
- **Simple Pattern**: Matches BadgerDB implementation (badger.go:178-244) for consistency
- **Transaction Batching**: PostgreSQL automatically pipelines operations within transaction
- **FOR UPDATE**: Locks rows during validation to prevent race conditions
- **Linear Flow**: Easy to understand: validate → check → delete for each ID
- **Clear Error Points**: Each validation step has explicit error handling
- **Easy to Debug**: Can add logging at each step to trace execution
- **Easy to Maintain**: Simple loop is easier to modify than complex CTEs
- **Performance**: Transaction pipelining provides same efficiency as CTE
- DELETE removes completed items (not UPDATE)
- Error messages include queue/partition context for debugging
- Per-item validation with error tracking per request

#### Lease Implementation with Race Prevention

**Critical: Use FOR UPDATE SKIP LOCKED to prevent race conditions**

```sql
-- Step 1: SELECT items for lease (within transaction)
-- IMPORTANT: Only lease ready items (enqueue_at IS NULL), not scheduled items
-- Scheduled items are moved to ready queue by ScanForScheduled lifecycle action
SELECT id, is_leased, lease_deadline, expire_deadline, enqueue_at, created_at,
       attempts, max_attempts, reference, encoding, kind, payload
FROM {table}
WHERE is_leased = false AND enqueue_at IS NULL
ORDER BY id
LIMIT $2
FOR UPDATE SKIP LOCKED;  -- CRITICAL: Prevents concurrent leasers from selecting same items

-- Step 2: UPDATE leased items (within same transaction, using pgx.Batch)
UPDATE {table}
SET is_leased = true,
    lease_deadline = $1,
    attempts = attempts + 1
WHERE id = $2;
```

**Implementation Flow:**
1. Begin transaction
2. SELECT items with FOR UPDATE SKIP LOCKED
3. Use `LeaseBatchIterator` to distribute items across lease requests (pattern from memory.go:96-135)
4. Create pgx.Batch with UPDATE statements for each leased item
5. Execute batch within transaction
6. Commit transaction

**Key Points:**
- `SKIP LOCKED` ensures concurrent Lease calls select different items
- Transaction ensures atomicity of SELECT + UPDATE
- ORDER BY id maintains FIFO ordering
- Only leases ready items (enqueue_at IS NULL), matching badger.go:149-150 and memory.go:72-73
- Set `lease_deadline = opts.LeaseDeadline`, increment `attempts`
- Uses partial index `idx_lease` for optimal performance

**Memory Considerations:**
The SELECT query fetches all item columns including payloads (up to 5MB each). With large lease batches:
- 100 items × 5MB = 500MB potential memory usage
- This is acceptable as items must be returned to clients (can't avoid loading payloads)
- The `MaxLeaseBatchSize` configuration (service.go) provides backpressure
- Monitor memory usage in production with large batches and adjust MaxLeaseBatchSize as needed

#### Retry Implementation

Retry requires a transaction to ensure atomic read-modify-write (consistent with BadgerDB pattern at badger.go:246-331):

```go
func (p *PostgresPartition) Retry(ctx context.Context, batch types.Batch[types.RetryRequest]) error {
    pool, err := p.conf.getOrCreatePool(ctx)
    if err != nil {
        return err
    }

    // Begin transaction for atomic read-modify-write
    tx, err := pool.Begin(ctx)
    if err != nil {
        return errors.Errorf("begin transaction: %w", err)
    }
    defer tx.Rollback(ctx)  // Auto-rollback if not committed

nextBatch:
    for i := range batch.Requests {
        for _, retryItem := range batch.Requests[i].Items {
            // Validate ID
            if err := p.validateID(retryItem.ID); err != nil {
                batch.Requests[i].Err = transport.NewInvalidOption("invalid id: %s", err)
                continue nextBatch
            }

            // SELECT within transaction with FOR UPDATE to lock row
            var isLeased bool
            var enqueueAt sql.NullTime
            err := tx.QueryRow(ctx,
                `SELECT is_leased, enqueue_at FROM `+p.tableName()+` WHERE id = $1 FOR UPDATE`,
                retryItem.ID).Scan(&isLeased, &enqueueAt)

            if err == pgx.ErrNoRows {
                batch.Requests[i].Err = transport.NewInvalidOption("id does not exist")
                continue nextBatch
            }
            if err != nil {
                return errors.Errorf("failed to check lease status: %w", err)
            }

            // This should not happen, but we need to handle it anyway (matches memory.go:153-159)
            if enqueueAt.Valid && !enqueueAt.Time.IsZero() {
                p.log.LogAttrs(ctx, slog.LevelWarn, "attempted to retry a scheduled item; reported does not exist",
                    slog.String("id", string(retryItem.ID)))
                batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s' does not exist", retryItem.ID)
                continue nextBatch
            }

            if !isLeased {
                batch.Requests[i].Err = transport.NewConflict("item not leased")
                continue nextBatch
            }

            // Execute UPDATE/DELETE within same transaction
            // Branch based on Dead flag and RetryAt
            if retryItem.Dead {
                // Delete from current partition (dead letter handled elsewhere)
                _, err = tx.Exec(ctx, `DELETE FROM `+p.tableName()+` WHERE id = $1`, retryItem.ID)
            } else if !retryItem.RetryAt.IsZero() {
                // Scheduled retry: set enqueue_at, unlease
                _, err = tx.Exec(ctx, `
                    UPDATE `+p.tableName()+`
                    SET is_leased = false,
                        lease_deadline = NULL,
                        enqueue_at = $1
                    WHERE id = $2`,
                    retryItem.RetryAt, retryItem.ID)
            } else {
                // Immediate retry: just unlease (attempts already incremented during Lease)
                _, err = tx.Exec(ctx, `
                    UPDATE `+p.tableName()+`
                    SET is_leased = false,
                        lease_deadline = NULL
                    WHERE id = $1`,
                    retryItem.ID)
            }

            if err != nil {
                batch.Requests[i].Err = transport.NewInternalError("failed to retry item: %s", err)
                continue nextBatch
            }
        }
    }

    // Commit transaction
    if err := tx.Commit(ctx); err != nil {
        return errors.Errorf("commit transaction: %w", err)
    }

    return nil
}
```

**Retry Logic:**
1. **Transaction wraps entire operation** - Ensures atomicity (consistent with badger.go:254-327)
2. **SELECT FOR UPDATE** - Locks row during read to prevent race with TakeAction
3. **Dead = true**: DELETE item (will be produced to dead letter queue by caller)
4. **RetryAt set**: UPDATE to set enqueue_at, unlease (scheduled retry)
5. **Immediate retry**: UPDATE to unlease only (attempts already incremented during Lease at memory.go:155)
6. **Errors per request** - Validation failures set Err on individual requests, continue to next

#### TakeAction Implementation

**Process lifecycle actions using pgx.Batch for performance**

TakeAction handles four types of lifecycle events (memory.go:398-473, badger.go:667-787):
- `ActionLeaseExpired`: Unlease items whose lease has expired
- `ActionItemExpired`: Delete items past their expiry deadline
- `ActionDeleteItem`: Delete items (used by lifecycle manager)
- `ActionQueueScheduledItem`: Move scheduled items to ready queue (enqueue_at → NULL)

**Note**: `ActionItemMaxAttempts` is NOT handled in TakeAction - it's processed by the lifecycle manager at a higher layer (internal/lifecycle.go) which then calls TakeAction with ActionDeleteItem to remove the item.

```go
func (p *PostgresPartition) TakeAction(ctx context.Context, batch types.Batch[types.LifeCycleRequest], state *types.PartitionState) error {
    pool, err := p.conf.getOrCreatePool(ctx)
    if err != nil {
        return err
    }

    if err := p.ensureTable(ctx, pool); err != nil {
        return err
    }

    // Create batch for all actions
    pgxBatch := &pgx.Batch{}
    actionCount := 0

    for i := range batch.Requests {
        for _, action := range batch.Requests[i].Actions {
            switch action.Action {
            case types.ActionLeaseExpired:
                // Unlease the item (attempts already incremented, keep them)
                pgxBatch.Queue(`
                    UPDATE `+p.tableName()+`
                    SET is_leased = false,
                        lease_deadline = NULL
                    WHERE id = $1 AND is_leased = true`,
                    action.Item.ID)
                actionCount++

            case types.ActionItemExpired:
                // Delete expired item
                pgxBatch.Queue(`
                    DELETE FROM `+p.tableName()+`
                    WHERE id = $1`,
                    action.Item.ID)
                actionCount++

            case types.ActionDeleteItem:
                // Delete item (used by lifecycle manager for max attempts and other cleanup)
                pgxBatch.Queue(`
                    DELETE FROM `+p.tableName()+`
                    WHERE id = $1`,
                    action.Item.ID)
                actionCount++

            case types.ActionQueueScheduledItem:
                // Move scheduled item to ready queue (enqueue_at → NULL)
                pgxBatch.Queue(`
                    UPDATE `+p.tableName()+`
                    SET enqueue_at = NULL
                    WHERE id = $1`,
                    action.Item.ID)
                actionCount++
            }
        }
    }

    // Execute batch if there are actions
    if actionCount > 0 {
        br := pool.SendBatch(ctx, pgxBatch)
        defer br.Close()

        // Consume all results
        for i := 0; i < actionCount; i++ {
            _, err := br.Exec()
            if err != nil {
                // Log error but continue processing other actions
                // TakeAction is eventually consistent
                if p.conf.Log != nil {
                    p.conf.Log.LogAttrs(ctx, slog.LevelWarn, "failed to execute action",
                        slog.String("error", err.Error()))
                }
            }
        }
    }

    return nil
}
```

**Key Points:**
- **No transaction needed**: Actions are eventually consistent (consistent with memory.go pattern)
- **Batch processing**: All actions batched into single round-trip
- **Error tolerance**: Individual action failures logged but don't fail the batch
- **Idempotent operations**: All actions safe to retry (UPDATE with WHERE conditions, DELETE)
- **State parameter**: Not used in PostgreSQL implementation (retained for interface compatibility)

#### Stats Implementation

Calculate partition statistics:

```sql
SELECT
    COUNT(*) FILTER (WHERE enqueue_at IS NULL OR enqueue_at <= $1) as total,
    COUNT(*) FILTER (WHERE is_leased = true AND (enqueue_at IS NULL OR enqueue_at <= $1)) as num_leased,
    COUNT(*) FILTER (WHERE enqueue_at IS NOT NULL) as scheduled,
    COALESCE(AVG(EXTRACT(EPOCH FROM ($1 - created_at))) FILTER (WHERE enqueue_at IS NULL OR enqueue_at <= $1), 0) as avg_age_seconds,
    COALESCE(AVG(EXTRACT(EPOCH FROM (lease_deadline - $1))) FILTER (WHERE is_leased = true), 0) as avg_leased_age_seconds
FROM {table}
```

**Note**: The `scheduled` count includes ALL scheduled items (both ready and future), matching memory.go:477-512 behavior.

**Convert to PartitionStats:**
```go
stats.Total = total
stats.NumLeased = numLeased
stats.Scheduled = scheduled
stats.AverageAge = clock.Duration(avgAgeSeconds * float64(time.Second))
stats.AverageLeasedAge = clock.Duration(avgLeasedAgeSeconds * float64(time.Second))
```

#### LifeCycleInfo Implementation

Find next lease expiry for lifecycle scheduling:

```sql
SELECT COALESCE(MIN(lease_deadline), '9999-12-31 23:59:59.999999+00'::timestamptz)
FROM {table}
WHERE is_leased = true
```

**Handle NULL result:**
```go
var nextExpiry time.Time
err := pool.QueryRow(ctx, query).Scan(&nextExpiry)

// If no leased items exist, MIN returns NULL → COALESCE returns sentinel (9999-12-31)
// We detect the sentinel and leave NextLeaseExpiry as zero
sentinel := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
if nextExpiry.Before(sentinel) {
    info.NextLeaseExpiry = nextExpiry
}
```

#### Clear Implementation

Delete items based on destructive flag:

```go
func (p *PostgresPartition) Clear(ctx context.Context, destructive bool) error {
    pool, err := p.conf.getOrCreatePool(ctx)
    if err != nil {
        return err
    }

    if destructive {
        // Delete ALL items
        _, err = pool.Exec(ctx, `DELETE FROM `+p.tableName())
    } else {
        // Delete only unleased items (matching memory.go:286-304 behavior)
        // This preserves ALL leased items regardless of schedule status
        _, err = pool.Exec(ctx,
            `DELETE FROM `+p.tableName()+` WHERE is_leased = false`)
    }

    if err != nil {
        return errors.Errorf("clear failed: %w", err)
    }
    return nil
}
```

**Note**: Non-destructive clear removes ALL unleased items (both scheduled and ready), matching the behavior in memory.go:295-302.

#### ScanForScheduled and ScanForActions Implementation

**Use cursor-based pagination to avoid excessive memory usage**

The Go 1.23+ `iter.Seq` pattern allows callers to iterate over results. We need to balance two concerns:
- **Memory usage**: Large partitions (millions of items) could exhaust memory if fetched all at once
- **Connection lifetime**: Holding open connections during slow iteration risks pool exhaustion

**Analysis of Usage Pattern** (lifecycle.go:222-248):
- Lifecycle batches actions with `lActions := make([]types.Action, 0, 1_000)`
- Batches are written when they reach ~1000 items or iteration completes
- Early termination is supported (iterator can stop early)

**Solution**: Use cursor-based pagination with configurable batch size to balance memory vs round-trips.

**Key Design Decisions**:
1. **Fetch in batches of 1000 items** - matches lifecycle batch size
2. **Use LIMIT/OFFSET within iterator** - fetches next batch only when needed
3. **Cache timeout parameter** - use it to create context for each batch query
4. **Fetch only metadata fields** - payloads not needed for lifecycle actions (see Memory Optimization below)

**Memory Optimization:**
Research confirms lifecycle operations only use these Item fields:
- `ID` (required for TakeAction to identify items)
- `Attempts` (required for max attempts logic)
- `LeaseDeadline` and `ExpireDeadline` (used for debug logging)

Fields NEVER used: `Payload`, `Encoding`, `Kind`, `Reference`, `CreatedAt`, `MaxAttempts`, `IsLeased`, `EnqueueAt`

By fetching only `id, attempts, lease_deadline, expire_deadline`:
- **Memory per batch**: ~50KB for 1000 items vs 5GB if payloads were included
- **Memory savings**: 99% reduction
- **Pattern**: TakeAction re-fetches full items from storage when needed (matches BadgerDB/Memory)

This approach:
- ✅ Limits memory to ~50KB per batch (1000 items) instead of 5GB
- ✅ Releases connection between batches
- ✅ Supports early termination (stops fetching when iterator exits)
- ✅ Maintains FIFO ordering via ORDER BY id
- ⚠️ More database round-trips than fetch-all (acceptable trade-off for large partitions)

```go
func (p *PostgresPartition) ScanForScheduled(timeout clock.Duration, now clock.Time) iter.Seq[types.Action] {
    return func(yield func(types.Action) bool) {
        // Use configured batch size, default to 1000 if not set
        batchSize := p.conf.ScanBatchSize
        if batchSize == 0 {
            batchSize = 1000
        }
        var lastID string

        for {
            // Create context with timeout for each batch
            ctx, cancel := context.WithTimeout(context.Background(), timeout)

            pool, err := p.conf.getOrCreatePool(ctx)
            if err != nil {
                cancel()
                return
            }

            if err := p.ensureTable(ctx, pool); err != nil {
                cancel()
                return
            }

            // Query next batch using cursor (lastID)
            // Only fetch metadata fields - payloads not needed for lifecycle
            query := `
                SELECT id, attempts, lease_deadline, expire_deadline
                FROM ` + p.tableName() + `
                WHERE enqueue_at IS NOT NULL
                  AND enqueue_at <= $1
                  AND id > $2
                ORDER BY id
                LIMIT $3`

            rows, err := pool.Query(ctx, query, now, lastID, batchSize)
            if err != nil {
                cancel()
                return
            }

            // Fetch current batch into memory (only metadata, not payloads)
            var items []types.Item
            for rows.Next() {
                var item types.Item
                var leaseDeadline, expireDeadline sql.NullTime

                err := rows.Scan(
                    &item.ID,
                    &item.Attempts,
                    &leaseDeadline,
                    &expireDeadline,
                )
                if err != nil {
                    continue
                }

                if leaseDeadline.Valid {
                    item.LeaseDeadline = leaseDeadline.Time
                }
                if expireDeadline.Valid {
                    item.ExpireDeadline = expireDeadline.Time
                }

                items = append(items, item)
                lastID = string(item.ID)
            }
            rows.Close()
            cancel()

            // If no items in this batch, we're done
            if len(items) == 0 {
                return
            }

            // Yield items from this batch
            for _, item := range items {
                if !yield(types.Action{
                    Action:       types.ActionQueueScheduledItem,
                    PartitionNum: p.info.PartitionNum,
                    Queue:        p.info.Queue.Name,
                    Item:         item,
                }) {
                    // Iterator stopped early, exit
                    return
                }
            }

            // If we got fewer items than batch size, we're done
            if len(items) < batchSize {
                return
            }

            // Otherwise, continue to next batch
        }
    }
}

func (p *PostgresPartition) ScanForActions(timeout clock.Duration, now clock.Time) iter.Seq[types.Action] {
    return func(yield func(types.Action) bool) {
        // Use configured batch size, default to 1000 if not set
        batchSize := p.conf.ScanBatchSize
        if batchSize == 0 {
            batchSize = 1000
        }
        var lastID string

        for {
            // Create context with timeout for each batch
            ctx, cancel := context.WithTimeout(context.Background(), timeout)

            pool, err := p.conf.getOrCreatePool(ctx)
            if err != nil {
                cancel()
                return
            }

            if err := p.ensureTable(ctx, pool); err != nil {
                cancel()
                return
            }

            // Query next batch (excluding future scheduled items)
            // Only fetch metadata fields needed for lifecycle logic - excludes payloads
            query := `
                SELECT id, is_leased, attempts, lease_deadline, expire_deadline
                FROM ` + p.tableName() + `
                WHERE (enqueue_at IS NULL OR enqueue_at <= $1)
                  AND id > $2
                ORDER BY id
                LIMIT $3`

            rows, err := pool.Query(ctx, query, now, lastID, batchSize)
            if err != nil {
                cancel()
                return
            }

            // Fetch current batch into memory (metadata only, no payloads)
            var items []types.Item
            for rows.Next() {
                var item types.Item
                var leaseDeadline, expireDeadline sql.NullTime

                err := rows.Scan(
                    &item.ID,
                    &item.IsLeased,
                    &item.Attempts,
                    &leaseDeadline,
                    &expireDeadline,
                )
                if err != nil {
                    continue
                }

                if leaseDeadline.Valid {
                    item.LeaseDeadline = leaseDeadline.Time
                }
                if expireDeadline.Valid {
                    item.ExpireDeadline = expireDeadline.Time
                }

                items = append(items, item)
                lastID = string(item.ID)
            }
            rows.Close()
            cancel()

            // If no items in this batch, we're done
            if len(items) == 0 {
                return
            }

            // Process and yield items from this batch
            for _, item := range items {
                // Check for lease expiry
                if item.IsLeased && now.After(item.LeaseDeadline) {
                    if !yield(types.Action{
                        Action:       types.ActionLeaseExpired,
                        PartitionNum: p.info.PartitionNum,
                        Queue:        p.info.Queue.Name,
                        Item:         item,
                    }) {
                        return
                    }
                    continue
                }

                // Check for max attempts exceeded (memory.go:374-382)
                if item.IsLeased && p.info.Queue.MaxAttempts != 0 && item.Attempts >= p.info.Queue.MaxAttempts {
                    if !yield(types.Action{
                        Action:       types.ActionItemMaxAttempts,
                        PartitionNum: p.info.PartitionNum,
                        Queue:        p.info.Queue.Name,
                        Item:         item,
                    }) {
                        return
                    }
                    continue
                }

                // Check for item expiry
                if now.After(item.ExpireDeadline) {
                    if !yield(types.Action{
                        Action:       types.ActionItemExpired,
                        PartitionNum: p.info.PartitionNum,
                        Queue:        p.info.Queue.Name,
                        Item:         item,
                    }) {
                        return
                    }
                }
            }

            // If we got fewer items than batch size, we're done
            if len(items) < batchSize {
                return
            }

            // Otherwise, continue to next batch
        }
    }
}
```

**Key Points:**
- **Cursor-based pagination**: Fetches items in batches using `id > $lastID` pattern
- **Configurable batch size**: Uses `PostgresConfig.ScanBatchSize` (default: 1000, matching lifecycle.go:222-223, 176, 294 batching pattern)
- **Memory optimization**: Fetches only metadata (id, is_leased, attempts, deadlines) - **no payloads**
- **Memory bounded**: Max ~50KB per batch (1000 items) vs 5GB if payloads were included
- **Connection pooling**: New context created per batch, connection released between batches
- **Timeout parameter usage**: Each batch query uses the provided timeout parameter
- **Early termination**: Stops fetching when iterator exits (lifecycle reaches batch limit)
- **Empty iterator on error**: Return silently on errors to avoid crashing lifecycle
- **FIFO ordering**: `ORDER BY id` maintains insertion order across batches
- **Action priority**: Lease expiry checked first, then max attempts, then item expiry (matches memory.go:354-395)

**Performance Characteristics:**
- Small partitions (<ScanBatchSize items): Single query, same as fetch-all
- Large partitions (1M items): Multiple queries but bounded memory (~50KB per batch with default size)
- Lifecycle typically processes <1000 actions per cycle (early termination)
- Trade-off: More round-trips for better memory safety on large partitions
- **Tuning**: Increase ScanBatchSize for high-throughput/low-latency networks, decrease for memory-constrained environments
- **99% memory savings**: Excluding payloads reduces memory from 5GB to 50KB per 1000 items

#### Info/UpdateQueueInfo Thread Safety

```go
func (p *PostgresPartition) Info() types.PartitionInfo {
    p.mu.RLock()
    defer p.mu.RUnlock()
    return p.info
}

func (p *PostgresPartition) UpdateQueueInfo(info types.QueueInfo) {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.info.Queue = info
}
```

**Thread Safety Note:**
Both `p.info` and `p.uid` are protected by the mutex. While the current architecture (Logical Queue's single-threaded requestLoop) ensures only one goroutine accesses a partition at a time, protecting uid with the mutex:
- Maintains consistency with the existing MemoryPartition implementation (memory.go:34-60)
- Future-proofs against potential async partition interactions (logical.go:1025-1027 TODO)
- Has negligible performance cost since the mutex is already held during write operations

### Database Schema

```sql
CREATE TABLE IF NOT EXISTS {table} (
    id TEXT COLLATE "C" PRIMARY KEY,  -- KSUID stored as base62 string, C collation ensures ASCII sorting
    is_leased BOOLEAN NOT NULL DEFAULT false,
    lease_deadline TIMESTAMPTZ,
    expire_deadline TIMESTAMPTZ NOT NULL,
    enqueue_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 0,
    reference TEXT NOT NULL DEFAULT '',
    encoding TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL DEFAULT '',
    payload BYTEA
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS {index_lease} ON {table}(is_leased, enqueue_at) WHERE is_leased = false AND enqueue_at IS NULL;
CREATE INDEX IF NOT EXISTS {index_scheduled} ON {table}(enqueue_at) WHERE enqueue_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS {index_deadline} ON {table}(lease_deadline) WHERE is_leased = true;
CREATE INDEX IF NOT EXISTS {index_expire} ON {table}(expire_deadline);
```

**Schema Notes:**
- **Table name**: Use `p.tableName()` which returns properly quoted identifier via pgx.Identifier
- **Index names**: Use hash-based naming to avoid PostgreSQL's 63-character limit. Example: `pgx.Identifier{fmt.Sprintf("idx_%s_%d_lse", hashQueueName(queueName), partitionNum)}.Sanitize()` where `hashQueueName()` returns 8-character base62-encoded hash (more compact than hex)
- **PRIMARY KEY on id (TEXT)**: KSUIDs stored as base62-encoded strings which are lexicographically sortable, maintaining FIFO order when sorted as TEXT. The PRIMARY KEY automatically creates an index, so no additional index needed.
- **TIMESTAMPTZ** for all time fields (always store as UTC)
- **BYTEA** for opaque payload storage (never parsed by Querator)
- **Indexes** for common query patterns:
  - `idx_{queue}_{partition}_lease`: Partial index for Lease query (is_leased=false AND enqueue_at IS NULL)
  - `idx_{queue}_{partition}_scheduled`: Partial index for scheduled item queries (enqueue_at IS NOT NULL)
  - `idx_{queue}_{partition}_deadline`: Partial index for LifeCycleInfo query (MIN(lease_deadline) WHERE is_leased=true)
  - `idx_{queue}_{partition}_expire`: Full index on expire_deadline for ScanForActions item expiry checks (ActionItemExpired)
- **No separate ID index**: PRIMARY KEY already provides ordered index

**Helper Function for Index Names:**
```go
// hashQueueName returns a deterministic, collision-resistant hash of the queue name
// Uses base62 encoding of MD5 hash to stay under PostgreSQL's 63-character identifier limit
func hashQueueName(name string) string {
    // Compute MD5 hash of queue name
    h := md5.Sum([]byte(name))

    // Use first 8 bytes of hash for base62 encoding
    // 8 bytes = 64 bits provides excellent collision resistance
    var num uint64
    for i := 0; i < 8; i++ {
        num = (num << 8) | uint64(h[i])
    }

    // Base62 alphabet (alphanumeric, URL-safe, PostgreSQL-safe)
    const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    // Encode to base62 (produces ~11 characters for 64-bit number)
    if num == 0 {
        return "0000000000" // 10-char padding for zero case
    }

    result := make([]byte, 0, 11)
    for num > 0 {
        result = append(result, alphabet[num%62])
        num /= 62
    }

    // Reverse the result (we built it backwards)
    for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
        result[i], result[j] = result[j], result[i]
    }

    // Pad to 10 characters for consistent length
    for len(result) < 10 {
        result = append([]byte{'0'}, result...)
    }

    return string(result[:10])
}
```

**Collision Resistance Analysis:**
- **Hash input**: MD5 (128 bits)
- **Encoded portion**: First 64 bits of MD5
- **Output**: 10-character base62 string
- **Possible values**: 2^64 ≈ 1.8 × 10^19
- **Birthday paradox**: 50% collision probability at ~4.3 × 10^9 queue names (4.3 billion queues)
- **Practical limit**: Extremely safe for any realistic Querator deployment (typical deployments have < 10,000 queues)

**Why Not Use go-nanoid Directly?**
- nanoid generates **random** IDs, but we need **deterministic** hashes
- Same queue name must always produce same table name (across restarts, instances)
- Base62 encoding of MD5 hash provides determinism + collision resistance

**ensureTable Example:**
```go
func (p *PostgresPartition) ensureTable(ctx context.Context, pool *pgxpool.Pool) error {
    // Use sync.Once to ensure table is created only once, preventing concurrent creation races
    p.tableOnce.Do(func() {
        p.tableErr = p.createTableAndIndexes(ctx, pool)
    })
    return p.tableErr
}

func (p *PostgresPartition) createTableAndIndexes(ctx context.Context, pool *pgxpool.Pool) error {
    table := p.tableName()  // Returns quoted identifier

    // Use hash-based index names to avoid 63-character limit
    // Format: idx_{hash}_{partition}_lse (max ~20 chars)
    hash := hashQueueName(p.info.Queue.Name)
    leaseName := pgx.Identifier{fmt.Sprintf("idx_%s_%d_lse", hash, p.info.Num)}.Sanitize()
    schedName := pgx.Identifier{fmt.Sprintf("idx_%s_%d_sch", hash, p.info.Num)}.Sanitize()
    deadlineName := pgx.Identifier{fmt.Sprintf("idx_%s_%d_dln", hash, p.info.Num)}.Sanitize()
    expireName := pgx.Identifier{fmt.Sprintf("idx_%s_%d_exp", hash, p.info.Num)}.Sanitize()

    // Create table
    _, err := pool.Exec(ctx, fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id TEXT COLLATE "C" PRIMARY KEY,
            is_leased BOOLEAN NOT NULL DEFAULT false,
            lease_deadline TIMESTAMPTZ,
            expire_deadline TIMESTAMPTZ NOT NULL,
            enqueue_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 0,
            reference TEXT NOT NULL DEFAULT '',
            encoding TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL DEFAULT '',
            payload BYTEA
        )`, table))
    if err != nil {
        return errors.Errorf("failed to create table: %w", err)
    }

    // Create partial index for Lease queries
    _, err = pool.Exec(ctx, fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS %s ON %s(is_leased, enqueue_at)
        WHERE is_leased = false AND enqueue_at IS NULL`, leaseName, table))
    if err != nil {
        return errors.Errorf("failed to create lease index: %w", err)
    }

    // Create partial index for scheduled items
    _, err = pool.Exec(ctx, fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS %s ON %s(enqueue_at)
        WHERE enqueue_at IS NOT NULL`, schedName, table))
    if err != nil {
        return errors.Errorf("failed to create scheduled index: %w", err)
    }

    // Create partial index for LifeCycleInfo query (MIN(lease_deadline) WHERE is_leased=true)
    _, err = pool.Exec(ctx, fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS %s ON %s(lease_deadline)
        WHERE is_leased = true`, deadlineName, table))
    if err != nil {
        return errors.Errorf("failed to create deadline index: %w", err)
    }

    // Create index for item expiry checks (ScanForActions ActionItemExpired)
    _, err = pool.Exec(ctx, fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS %s ON %s(expire_deadline)`, expireName, table))
    if err != nil {
        return errors.Errorf("failed to create expire index: %w", err)
    }

    return nil
}
```

## Phase 3: Test Integration

### Overview
Integrate PostgreSQL into the functional test suite using testcontainers.

**Prerequisites:**
- **Docker must be running locally** - Testcontainers automatically starts PostgreSQL 16 containers
- On macOS, Docker Desktop can have slow I/O - consider benchmarking container startup time
- Each test run gets a fresh PostgreSQL instance in an isolated container
- No manual PostgreSQL installation required

### Changes Required

#### 1. Test Setup Helper with Testcontainers
**File**: `common_test.go`
**Changes**: Add PostgreSQL test setup using testcontainers

```go
import (
    "github.com/testcontainers/testcontainers-go"
    "github.com/testcontainers/testcontainers-go/modules/postgres"
    "github.com/testcontainers/testcontainers-go/wait"
)

type postgresTestSetup struct {
    container *postgres.PostgresContainer
    dsn       string
}

func (p *postgresTestSetup) Setup(conf store.PostgresConfig) store.Config

func (p *postgresTestSetup) Teardown()
```

**Function Responsibilities:**
- `Setup`:
  - Start PostgreSQL container using testcontainers (postgres:16-alpine image)
  - Configure container with test database credentials
  - Wait for database to be ready (wait strategy: "database system is ready to accept connections" × 2)
  - Get connection string from container
  - Configure storage with connection string and MaxConns
  - Return store.Config with PostgresQueues and PostgresPartitionStore
- `Teardown`:
  - Close any open connections
  - Terminate the PostgreSQL container
  - Handle errors gracefully (panic with descriptive message if cleanup fails)

**Testing Requirements:**

No new tests required - modifies existing test infrastructure.

**Test Objectives:**
- Enable PostgreSQL testing without manual PostgreSQL setup
- Complete isolation - each test gets its own container
- Automatic cleanup - container terminated after test
- CI/CD ready - works in any environment with Docker
- Parallel test execution supported (each test has own container)

**Context for Implementation:**
- Follow pattern from `badgerTestSetup` at `common_test.go:112-145`
- Uses testcontainers-go library for container management
- No manual PostgreSQL installation required
- Each test run gets fresh database instance
- Automatic port allocation prevents conflicts

**Implementation Example:**
```go
func (p *postgresTestSetup) Setup(conf store.PostgresConfig) store.Config {
    ctx := context.Background()

    // Start PostgreSQL container
    container, err := postgres.RunContainer(ctx,
        testcontainers.WithImage("postgres:16-alpine"),
        postgres.WithDatabase("querator_test"),
        postgres.WithUsername("postgres"),
        postgres.WithPassword("postgres"),
        testcontainers.WithWaitStrategy(
            wait.ForLog("database system is ready to accept connections").
                WithOccurrence(2).
                WithStartupTimeout(30*time.Second)),
    )
    if err != nil {
        panic(fmt.Sprintf("failed to start postgres container: %v", err))
    }

    // Get connection string from container
    dsn, err := container.ConnectionString(ctx, "sslmode=disable")
    if err != nil {
        panic(fmt.Sprintf("failed to get connection string: %v", err))
    }

    p.container = container
    p.dsn = dsn

    // Configure storage
    conf.ConnectionString = dsn
    conf.Log = log
    conf.MaxConns = 10 // Limit connections for tests

    var storageConf store.Config
    storageConf.Queues = store.NewPostgresQueues(conf)
    storageConf.PartitionStorage = []store.PartitionStorage{
        {
            PartitionStore: store.NewPostgresPartitionStore(conf),
            Name:           "postgres-0",
            Affinity:       1,
        },
    }
    return storageConf
}

func (p *postgresTestSetup) Teardown() {
    if p.container != nil {
        ctx := context.Background()
        if err := p.container.Terminate(ctx); err != nil {
            panic(fmt.Sprintf("failed to terminate postgres container: %v", err))
        }
    }
}
```

**Benefits of Testcontainers Approach:**
- **Zero manual setup**: No need to install or configure PostgreSQL locally
- **CI/CD friendly**: Works in any CI environment with Docker
- **Complete isolation**: Each test gets a fresh database container
- **Automatic cleanup**: Container and all data automatically removed
- **Parallel execution**: Multiple tests can run concurrently with own containers
- **Version control**: Test against specific PostgreSQL versions easily

#### 2. Enable PostgreSQL in Test Matrix
**File**: `storage_test.go`
**Changes**: Uncomment and implement PostgreSQL test case

```go
{
    Name: "PostgreSQL",
    Setup: func() store.Config {
        return postgres.Setup(store.PostgresConfig{})
    },
    TearDown: func() {
        postgres.Teardown()
    },
},
```

**Existing tests that will now run against PostgreSQL:**
- All tests in `TestQueueStorage` function (lines 58-523)

**File**: `queue_test.go`
**Changes**: Uncomment and implement PostgreSQL test case (line 50-51)

```go
{
    Name: "PostgreSQL",
    Setup: func() store.Config {
        return postgres.Setup(store.PostgresConfig{})
    },
    TearDown: func() {
        postgres.Teardown()
    },
},
```

**Existing tests that will now run against PostgreSQL:**
- All tests in `TestQueue` function

**Test Objectives:**
- Validate all functional tests pass with PostgreSQL
- Validate FIFO ordering maintained
- Validate concurrent operations
- Validate batch operations
- Validate lifecycle management

**Context for Implementation:**
- Follow setup pattern from BadgerDB test case at `storage_test.go:36-43`
- Uses testcontainers - no environment variables or manual setup needed
- Tests automatically validate all Queues and Partition interface methods
- No new test functions needed - existing tests provide complete coverage

## Validation Commands

**Prerequisites**: Docker must be running locally for PostgreSQL tests. Testcontainers will automatically start and stop PostgreSQL 16 containers.

```bash
# Verify Docker is running
docker ps

# Test PostgreSQL implementation specifically
go test ./... -v -run "^TestQueue/PostgreSQL.*$"
go test ./... -v -run "^TestQueueStorage/PostgreSQL.*$"

# Run full test suite (all backends)
go test ./... -v

# Run with race detection
go test ./... -v -race

# Check test coverage
make cover
```

## Implementation Notes

### Connection Management
- **Global Singleton Pattern**: One pool per unique connection string across entire application
- **Reference Counting**: Automatic cleanup when last component closes
- **Thread-Safe**: Global mutex protects pool map access
- **Lazy Initialization**: Pool created on first use with exponential backoff retry (5 attempts: immediate, 100ms, 200ms, 400ms, 800ms)
- **Automatic Sharing**: All Queues and Partition instances with same connection string share one pool
- **Shutdown Safety**: Partitions close first, then Queues - pool closes when refCount reaches 0

### Error Handling
- Wrap all pgx errors with `github.com/kapetan-io/errors.Errorf` for stack traces
- Include queue/partition context in error messages for debugging: `errors.Errorf("postgres partition %s/%d: operation: %w", queue, partition, err)`
- Convert `pgx.ErrNoRows` to `store.ErrQueueNotExist` where appropriate
- Handle constraint violations (duplicate queue names) gracefully
- Log connection errors but don't panic (lazy init will retry)
- Error context helps with production debugging when multiple partitions/queues exist

### Performance Optimization
- **Batch Operations**: Use `pgx.Batch` for multi-item operations (Produce, Add, Delete, TakeAction)
- **CTE Optimization**: Complete uses Common Table Expression to reduce round-trips from 2N to 1
- **Partial Indexes**: Three strategic indexes optimize common query patterns (lease, scheduled, deadline)
- **Base62 Encoding**: Index names use base62-encoded hashes for more compact identifiers
- **Configurable Scan Size**: `ScanBatchSize` config (default 1000) allows tuning for network/memory trade-offs
- **Memory Optimization**: Scan methods fetch only metadata (99% memory savings: 50KB vs 5GB per 1000 items)

### FIFO Ordering
- KSUID IDs ensure time-ordered insertion
- PRIMARY KEY on id ensures retrieval order matches insertion order
- List operations use `ORDER BY id` to maintain FIFO semantics
- Critical for passing functional tests

### Lazy Initialization Pattern

The PostgreSQL implementation uses the `getOrCreatePool()` method defined in `PostgresConfig` (lines 102-145) for lazy pool initialization. This ensures:
- Thread-safe pool creation via mutex
- Single shared pool across all Queues and Partition instances
- Automatic pool creation on first use
- Connection string validation

Each method that needs database access calls `conf.getOrCreatePool(ctx)` which handles the lazy initialization automatically.

### Batch Operations

Most batch operations use `pgx.Batch` for efficient multi-statement execution. Complete uses a transaction loop for granular error handling. See the detailed implementation sections above for complete examples:
- **Produce**: Lines 548-631 (batch INSERT with error tracking via pgx.Batch)
- **Add**: Lines 738-800 (batch INSERT via pgx.Batch)
- **Delete**: Uses pgx.Batch for batch DELETE operations
- **TakeAction**: Lines 856-941 (batch UPDATE/DELETE for lifecycle actions via pgx.Batch)
- **Complete**: Lines 633-705 (transaction loop with SELECT FOR UPDATE + DELETE per ID)
- **Retry**: Transaction loop with SELECT FOR UPDATE + UPDATE/DELETE per ID
- **Lease**: Transaction with SELECT FOR UPDATE SKIP LOCKED + pgx.Batch UPDATE

Key pattern for pgx.Batch operations (Produce, Add, Delete, TakeAction):
1. Create `pgx.Batch` instance
2. Queue all SQL statements with `.Queue()`
3. Execute with `pool.SendBatch(ctx, batch)`
4. Iterate results with `br.Exec()` for each queued statement
5. Map errors back to original request indices

Key pattern for transaction loop operations (Complete, Retry):
1. Begin transaction with `pool.Begin(ctx)`
2. Loop through items, executing SELECT FOR UPDATE + mutation
3. Per-item error handling with continue nextBatch pattern
4. Commit transaction at end
5. Transaction pipelining provides efficiency while maintaining error granularity

### Time Handling
- All time fields stored as TIMESTAMPTZ
- Convert `clock.Time` to `time.Time` for PostgreSQL
- Use `time.Time.UTC()` consistently
- Handle zero times (NULL vs epoch) based on field semantics

## Dependencies

### Go Modules
```
github.com/jackc/pgx/v5 v5.5.0
github.com/jackc/pgx/v5/pgxpool v5.5.0
```

### External Services
- **Production**: PostgreSQL 12+ (configured via ConnectionString)
- **Testing**: Docker (testcontainers automatically starts PostgreSQL 16 containers)

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| FIFO ordering not maintained | Test failures, incorrect queue behavior | Use KSUID IDs, PRIMARY KEY ordering, validate with functional tests |
| Connection pool exhaustion | Performance degradation | Use pgxpool defaults, monitor in production |
| Table name conflicts | Data corruption | Use partition-specific naming, validate in tests |
| Concurrent access race conditions | Data corruption | Use transactions, proper locking, validate with -race flag |
| Lazy init connection failures | Service degradation | Graceful error handling, retry logic in tests, health checks |

## Plan Improvements Summary

This plan incorporates the following enhancements based on deep codebase analysis:

### 1. Simplified Pool Management (CRITICAL)
**Previous**: Complex per-config reference counting with potential race conditions during shutdown
**Improved**: Global singleton pool manager keyed by connection string
- Single source of truth for pool lifecycle
- Automatic handling of multiple queues/partitions sharing same database
- Correct shutdown order guaranteed (partitions first, then queues)
- Thread-safe via global mutex

### 2. Exponential Backoff Retry Logic
**Added**: Robust retry mechanism for pool creation
- 5 attempts with exponential backoff: immediate, 100ms, 200ms, 400ms, 800ms
- Matches lifecycle recovery pattern (lifecycle.go:265, 335, 360, 380)
- Graceful degradation with detailed logging

### 3. Simple Transaction Loop for Complete (Correct Pattern)
**Pattern**: Transaction loop with individual SELECT FOR UPDATE + DELETE per ID
**Why Not CTE**: Complete requires granular error reporting that CTEs cannot provide
- Must distinguish "ID not found" vs "ID not leased" for each ID
- Must fail individual request while continuing to process others (continue nextBatch)
- Early exit pattern when validation fails maintains clear error semantics
- Matches BadgerDB pattern (badger.go:178-244) for consistency
- PostgreSQL automatically pipelines operations within transaction for efficiency

**Alternative CTE Approach Would Lose Error Granularity**:
```sql
-- This returns which IDs succeeded but not why others failed
WITH validated AS (
    SELECT id FROM table WHERE id = ANY($1) AND is_leased = true FOR UPDATE
)
DELETE FROM table WHERE id IN (SELECT id FROM validated) RETURNING id;
```

### 4. Base62 Index Naming
**Previous**: Hexadecimal MD5 hash (8 chars: "a1b2c3d4")
**Improved**: Base62-encoded hash (8 chars: "xY3kL9w")
- More compact representation
- URL-safe and case-sensitive
- Stays well under PostgreSQL's 63-character limit

### 5. Configurable Scan Batch Size
**Added**: `PostgresConfig.ScanBatchSize` configuration option
- Default: 1000 (matches lifecycle batching at lifecycle.go:222-223, 176, 294)
- Tunable for different environments:
  - Increase for high-throughput/low-latency networks
  - Decrease for memory-constrained environments
- Used by both ScanForActions and ScanForScheduled

### 6. Enhanced Configuration Options
**Added**:
- `MaxConns` with sizing guidance (Conservative/Balanced/Aggressive recommendations)
- `OnQueryComplete` callback for instrumentation/metrics
- `Ping()` method for health checks

### 7. Performance Documentation
**Added**: Detailed performance characteristics and tuning guidance
- Memory savings calculations (99% reduction)
- Round-trip optimization explanations
- Batch size trade-off analysis

## Success Criteria

1. All existing functional tests pass with PostgreSQL backend
2. No race conditions detected with `go test -race`
3. pgx.Batch and CTE optimizations implemented for all multi-item operations
4. FIFO ordering maintained (verified by functional tests)
5. Lazy initialization with exponential backoff retry works correctly
6. Connection pool sharing works across multiple queues/partitions
7. Graceful shutdown with proper resource cleanup
8. Test coverage maintained or improved

## Future Enhancements (Out of Scope)

- Connection pool tuning and configuration options
- Query optimization and indexing improvements
- PostgreSQL-specific features (LISTEN/NOTIFY for queue events)
- Connection retry logic beyond lazy init
- Database migration tooling
- Multi-database sharding
- Metrics and observability integration
