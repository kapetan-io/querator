# Multi-Tenancy and Authentication Implementation Plan

## Overview

This plan implements the namespace-based multi-tenancy system for Querator as specified in `plans/namespace-spec.md` and `docs/adr/0025-multi-tenancy-and-auth.md`. The implementation follows an incremental approach across three phases:

1. **Phase 1**: Namespace Foundation + Minimal Auth
2. **Phase 2**: Users + API Keys
3. **Phase 3**: Roles + RBAC

Each phase delivers a working increment that can be tested independently.

## Current State Analysis

### Affected Components
- `transport/http.go:105-178`: HTTP handler with path-based routing, no middleware
- `service/service.go`: Central business logic, no authorization checks
- `internal/queues_manager.go`: Queue lifecycle management, no namespace isolation
- `internal/store/store.go`: Pluggable storage interfaces (InMemory, BadgerDB, PostgreSQL)
- `internal/types/items.go:160-186`: `QueueInfo` struct - needs `Namespace` field
- `client.go`: HTTP client, no auth headers

### Key Discoveries
- All storage backends implement lazy initialization (ADR 0021)
- Error handling uses `transport.NewInvalidOption()` and `transport.NewRequestFailed()`
- Validation patterns in `service/validation.go`
- Test infrastructure in `service/common_test.go` supports multi-backend testing

## Desired End State

After this plan is complete:
1. All queues belong to exactly one namespace
2. API requests are authenticated via `Authorization: Bearer <api_key>` header
3. Authorization uses cascading RBAC checks (target namespace -> `_system` namespace)
4. Anonymous users have Admin access by default (Open Door policy for dev/testing)
5. All three storage backends (InMemory, BadgerDB, PostgreSQL) support auth data

### Verification
- `go test ./...` passes
- `make ci` passes (if available)
- Functional tests verify namespace isolation, authentication, and authorization
- Creating a queue without namespace returns error
- Anonymous user can perform admin operations (Open Door)
- After removing anonymous-admin binding, anonymous requests get 403

## What We're NOT Doing

- External identity provider integration (OIDC/SSO) - deferred to future work
- Permission caching - implement if performance testing shows need
- Audit logging - separate feature
- Rate limiting - separate feature
- Namespace quotas - separate feature
- Migration of existing data - Querator is alpha, no migration path needed

## Design Decisions

### User Deletion Behavior: CASCADE
When a user is deleted, all their API keys are automatically deleted.

### Role Deletion Behavior: RESTRICT
Roles cannot be deleted if they have active bindings.

### Standard Roles: Immutable
Standard roles (Admin, NamespaceOwner, PublicViewer) cannot be modified or deleted by users.

### Anonymous User ID: String Literal
The Anonymous user uses the literal string `"anonymous"` for both ID and Username.

### UpdateLastUsed: Async Goroutine
API key last-used tracking is fire-and-forget in a goroutine.

### Auth Caching: Read-Through + Write-Throttling
API keys cached in-memory (TTL 5m), `LastUsedAt` updates throttled (max 1/minute per key).

## Implementation Approach

We use an incremental approach: each phase adds complete, testable functionality. Phase 1 establishes the foundation (namespaces), Phase 2 adds authentication (users/keys), Phase 3 adds authorization (roles/RBAC).

---

## Phase 1: Namespace Foundation + Minimal Auth

### Overview
Establish namespace isolation for queues with a minimal authentication layer that defaults to Anonymous principal with Admin access.

### Changes Required

#### 1. Domain Error Types

**File**: `internal/types/errors.go` (new file)
**Changes**: Create domain error variables

```go
package types

// Namespace errors
var (
    ErrNamespaceNotExist      error
    ErrNamespaceAlreadyExists error
    ErrNamespaceHasQueues     error
    ErrNamespaceReserved      error
)
```

**Function responsibilities:**
- Use `transport.NewRequestFailed()` for not-found errors
- Use `transport.NewInvalidOption()` for validation/conflict errors
- All errors must implement `duh.Error` interface

**Context for implementation:**
- Follow error patterns from `transport/errors.go`

---

#### 2. Auth Error Types

**File**: `transport/errors.go`
**Changes**: Add `ErrUnauthorized` and `ErrForbidden` error types

```go
type ErrUnauthorized struct {
    msg string
}

func NewUnauthorized(msg string, args ...any) *ErrUnauthorized
func (e *ErrUnauthorized) Error() string
func (e *ErrUnauthorized) Is(target error) bool
func (e *ErrUnauthorized) Code() int
func (e *ErrUnauthorized) ProtoMessage() proto.Message
func (e *ErrUnauthorized) Details() map[string]string
func (e *ErrUnauthorized) Message() string

type ErrForbidden struct {
    msg string
}

func NewForbidden(msg string, args ...any) *ErrForbidden
func (e *ErrForbidden) Error() string
func (e *ErrForbidden) Is(target error) bool
func (e *ErrForbidden) Code() int
func (e *ErrForbidden) ProtoMessage() proto.Message
func (e *ErrForbidden) Details() map[string]string
func (e *ErrForbidden) Message() string
```

**Function responsibilities:**
- `ErrUnauthorized.Code()` returns `duh.CodeUnauthorized` (401)
- `ErrForbidden.Code()` returns `duh.CodeForbidden` (403)
- Both implement `duh.Error` interface

**Context for implementation:**
- Follow patterns from existing `ErrInvalidOption` and `ErrRequestFailed` in same file

---

#### 3. Namespace Type

**File**: `internal/types/namespace.go` (new file)
**Changes**: Create Namespace type

```go
type Namespace struct {
    Name        string
    Description string
    CreatedAt   clock.Time
}

func (n *Namespace) IsReserved() bool
func (n *Namespace) ToProto() *proto.NamespaceInfo
```

**Function responsibilities:**
- `IsReserved()`: Return true if name starts with `_` prefix
- `ToProto()`: Convert to proto representation

---

#### 4. Principal and Auth Types

**File**: `internal/types/auth.go` (new file)
**Changes**: Create Principal and User types for auth foundation

```go
type Principal struct {
    User           User
    NamespaceScope *string
    IsAnonymous    bool
}

type User struct {
    ID         string
    Username   string
    ExternalID string
    Email      string
    CreatedAt  clock.Time
    UpdatedAt  clock.Time
}

var AnonymousUser = User{
    ID:       "anonymous",
    Username: "anonymous",
}

var AnonymousPrincipal = Principal{
    User:        AnonymousUser,
    IsAnonymous: true,
}
```

**Function responsibilities:**
- Define types only, no methods needed yet

---

#### 5. Context Key for Principal

**File**: `internal/context.go` (new file)
**Changes**: Create context utilities for Principal

```go
type contextKey string

const principalKey contextKey = "principal"

func PrincipalFromContext(ctx context.Context) types.Principal
func ContextWithPrincipal(ctx context.Context, p types.Principal) context.Context
```

**Function responsibilities:**
- `PrincipalFromContext()`: Return `AnonymousPrincipal` if no principal in context
- `ContextWithPrincipal()`: Store principal in context

---

#### 6. Namespace Storage Interface

**File**: `internal/store/namespace.go` (new file)
**Changes**: Define Namespaces storage interface

```go
type Namespaces interface {
    Get(ctx context.Context, name string, ns *types.Namespace) error
    Add(ctx context.Context, ns types.Namespace) error
    List(ctx context.Context, namespaces *[]types.Namespace, opts types.ListOptions) error
    Delete(ctx context.Context, name string) error
    Close(ctx context.Context) error
}
```

**Function responsibilities:**
- Follow patterns from `Queues` interface in `internal/store/store.go:29-51`
- Return `ErrNamespaceNotExist` / `ErrNamespaceAlreadyExists` errors

---

#### 7. Update Storage Config

**File**: `internal/store/store.go`
**Changes**: Add `Namespaces` field to Config

```go
type Config struct {
    Queues           Queues
    Namespaces       Namespaces  // NEW
    PartitionStorage []PartitionStorage
}
```

---

#### 8. Memory Namespace Storage

**File**: `internal/store/memory.go`
**Changes**: Add `MemoryNamespaces` implementation

```go
type MemoryNamespaces struct {
    namespaces map[string]types.Namespace
    mutex      sync.RWMutex
    log        *slog.Logger
}

func NewMemoryNamespaces(log *slog.Logger) *MemoryNamespaces
func (m *MemoryNamespaces) Get(ctx context.Context, name string, ns *types.Namespace) error
func (m *MemoryNamespaces) Add(ctx context.Context, ns types.Namespace) error
func (m *MemoryNamespaces) List(ctx context.Context, namespaces *[]types.Namespace, opts types.ListOptions) error
func (m *MemoryNamespaces) Delete(ctx context.Context, name string) error
func (m *MemoryNamespaces) Close(ctx context.Context) error
```

**Function responsibilities:**
- Thread-safe with mutex protection
- Return appropriate errors for not-found/already-exists cases
- Support pagination via `opts.Pivot` and `opts.Limit`

**Context for implementation:**
- Follow patterns from `MemoryQueues` in same file

---

#### 9. BadgerDB Namespace Storage

**File**: `internal/store/badger.go`
**Changes**: Add `BadgerNamespaces` implementation

```go
type BadgerNamespaces struct {
    db  *badger.DB
    log *slog.Logger
}

func NewBadgerNamespaces(conf BadgerConfig) *BadgerNamespaces
func (b *BadgerNamespaces) Get(ctx context.Context, name string, ns *types.Namespace) error
func (b *BadgerNamespaces) Add(ctx context.Context, ns types.Namespace) error
func (b *BadgerNamespaces) List(ctx context.Context, namespaces *[]types.Namespace, opts types.ListOptions) error
func (b *BadgerNamespaces) Delete(ctx context.Context, name string) error
func (b *BadgerNamespaces) Close(ctx context.Context) error
```

**Function responsibilities:**
- Use key prefix `ns:` to distinguish from queue data
- Implement lazy initialization per ADR 0021
- Serialize namespace to JSON for storage

**Context for implementation:**
- Follow patterns from `BadgerQueues` in same file

---

#### 10. PostgreSQL Namespace Storage

**File**: `internal/store/postgres.go`
**Changes**: Add `PostgresNamespaces` implementation

```go
type PostgresNamespaces struct {
    pool *pgxpool.Pool
    conf PostgresConfig
    log  *slog.Logger
}

func NewPostgresNamespaces(conf PostgresConfig) *PostgresNamespaces
func (p *PostgresNamespaces) Get(ctx context.Context, name string, ns *types.Namespace) error
func (p *PostgresNamespaces) Add(ctx context.Context, ns types.Namespace) error
func (p *PostgresNamespaces) List(ctx context.Context, namespaces *[]types.Namespace, opts types.ListOptions) error
func (p *PostgresNamespaces) Delete(ctx context.Context, name string) error
func (p *PostgresNamespaces) Close(ctx context.Context) error
```

**Function responsibilities:**
- Create table: `namespaces (name TEXT PRIMARY KEY, description TEXT, created_at TIMESTAMPTZ)`
- Implement lazy initialization per ADR 0021

**Context for implementation:**
- Follow patterns from `PostgresQueues` in same file

---

#### 11. Namespace Proto Definitions

**File**: `proto/namespaces.proto` (new file)
**Changes**: Create namespace proto messages

```protobuf
syntax = "proto3";
package proto;
option go_package = "github.com/kapetan-io/querator/proto";

import "google/protobuf/timestamp.proto";

message NamespaceInfo {
    string name = 1;
    string description = 2;
    google.protobuf.Timestamp created_at = 3;
}

message NamespaceCreateRequest {
    string name = 1;
    string description = 2;
}

message NamespacesListRequest {
    int32 limit = 1;
    string pivot = 2;
}

message NamespacesListResponse {
    repeated NamespaceInfo items = 1;
}

message NamespacesDeleteRequest {
    string name = 1;
}
```

**Post-creation step:**
- Run `make proto` to generate Go code

---

#### 12. Add Namespace Field to Queue Proto Messages

**File**: `proto/queue.proto`
**Changes**: Add `namespace` field to all request messages

```protobuf
message QueueProduceRequest {
    string namespace = 4 [json_name = "namespace"];
    // ... existing fields unchanged
}

message QueueLeaseRequest {
    string namespace = 5 [json_name = "namespace"];
    // ... existing fields unchanged
}

message QueueCompleteRequest {
    string namespace = 5 [json_name = "namespace"];
    // ... existing fields unchanged
}

message QueueRetryRequest {
    string namespace = 4 [json_name = "namespace"];
    // ... existing fields unchanged
}

message QueueClearRequest {
    string namespace = 6 [json_name = "namespace"];
    // ... existing fields unchanged
}

message QueueReloadRequest {
    string namespace = 2 [json_name = "namespace"];
    // ... existing fields unchanged
}

message QueueStatsRequest {
    string namespace = 2 [json_name = "namespace"];
    // ... existing fields unchanged
}
```

**Function responsibilities:**
- Field numbers assigned sequentially based on next available field ID

---

#### 13. Add Namespace Field to Queues Proto Messages

**File**: `proto/queues.proto`
**Changes**: Add `namespace` field to request messages and QueueInfo

```protobuf
message QueueInfo {
    string namespace = 11 [json_name = "namespace"];
    // ... existing fields unchanged
}

message QueuesListRequest {
    string namespace = 4 [json_name = "namespace"];
    // ... existing fields unchanged
}

message QueuesDeleteRequest {
    string namespace = 3 [json_name = "namespace"];
    // ... existing fields unchanged
}

message QueuesInfoRequest {
    string namespace = 2 [json_name = "namespace"];
    // ... existing fields unchanged
}
```

---

#### 14. Add Namespace Field to Storage Proto Messages

**File**: `proto/storage.proto`
**Changes**: Add `namespace` field to request messages

```protobuf
message StorageItemsListRequest {
    string namespace = 5 [json_name = "namespace"];
    // ... existing fields unchanged
}

message StorageItemsImportRequest {
    string namespace = 4 [json_name = "namespace"];
    // ... existing fields unchanged
}

message StorageItemsDeleteRequest {
    string namespace = 4 [json_name = "namespace"];
    // ... existing fields unchanged
}
```

---

#### 15. Add Namespace Field to QueueInfo Type

**File**: `internal/types/items.go`
**Changes**: Add `Namespace` field to `QueueInfo` struct

```go
type QueueInfo struct {
    Namespace           string         // NEW: isolation boundary
    Name                string
    LeaseTimeout        clock.Duration
    DeadQueue           string
    ExpireTimeout       clock.Duration
    CreatedAt           clock.Time
    UpdatedAt           clock.Time
    MaxAttempts         int
    Reference           string
    RequestedPartitions int
    PartitionInfo       []PartitionInfo
}
```

**Function responsibilities:**
- Update `ToProto()` to include namespace
- Update `Update()` to handle namespace (immutable after creation)

---

#### 16. Update QueuesManager for Namespace

**File**: `internal/queues_manager.go`
**Changes**: Update queue management for namespace awareness

```go
func queueKey(namespace, name string) string

func (qm *QueuesManager) Get(ctx context.Context, namespace, queueName string) (*Queue, error)
```

**Function responsibilities:**
- `queueKey()`: Return composite key `namespace/queueName`
- Update queue map key from `queueName` to `namespace/queueName`
- Update all callers to pass namespace parameter

**Context for implementation:**
- Queue names are unique per namespace, not globally

---

#### 17. Namespace Validation

**File**: `service/validation.go`
**Changes**: Add namespace validation functions

```go
func validateNamespaceName(name string) error
func validateNamespaceCreate(req *proto.NamespaceCreateRequest) error
```

**Function responsibilities:**
- Validate name is not empty
- Validate name does not start with `_` (reserved prefix)
- Validate no whitespace
- Max length 500 characters

**Context for implementation:**
- Follow patterns from existing `validateQueueOptionsProto()`

---

#### 18. Namespace Service Methods

**File**: `service/service.go`
**Changes**: Add namespace management methods

```go
func (s *Service) NamespacesCreate(ctx context.Context, req *proto.NamespaceCreateRequest) error
func (s *Service) NamespacesList(ctx context.Context, req *proto.NamespacesListRequest, resp *proto.NamespacesListResponse) error
func (s *Service) NamespacesDelete(ctx context.Context, req *proto.NamespacesDeleteRequest) error
```

**Function responsibilities:**
- `NamespacesCreate()`: Validate input, add to storage, prevent reserved name creation by users
- `NamespacesList()`: List with pagination following `QueuesList()` pattern
- `NamespacesDelete()`: Check no queues exist in namespace before deletion, prevent deletion of reserved namespaces. (Note: Phase 3 will update this to also check for Roles and RoleBindings)

---

#### 19. Update Queue Service Methods for Namespace

**File**: `service/service.go`
**Changes**: Update queue operations to require namespace

**Function responsibilities:**
- `QueuesCreate()`: Validate namespace field is present and exists before creating queue
- All queue operations: Extract namespace from request and validate it exists
- Pass namespace to `QueuesManager.Get()`

---

#### 20. Bootstrap _system Namespace

**File**: `service/service.go`
**Changes**: Add Bootstrap method and call from New()

```go
func (s *Service) Bootstrap(ctx context.Context) error
```

**Function responsibilities:**
- Create `_system` namespace if not exists
- Ignore "already exists" error (idempotent)
- Call from `New()` during service initialization

---

#### 21. Transport Service Interface

**File**: `transport/interfaces.go`
**Changes**: Add NamespaceAdmin interface

```go
type NamespaceAdmin interface {
    NamespacesCreate(ctx context.Context, req *pb.NamespaceCreateRequest) error
    NamespacesList(ctx context.Context, req *pb.NamespacesListRequest, resp *pb.NamespacesListResponse) error
    NamespacesDelete(ctx context.Context, req *pb.NamespacesDeleteRequest) error
}

type Service interface {
    QueueOps
    QueueAdmin
    StorageInspector
    NamespaceAdmin  // NEW
    Health(ctx context.Context) (*HealthResponse, error)
}
```

---

#### 22. Namespace HTTP Endpoints

**File**: `transport/http.go`
**Changes**: Add namespace endpoint handlers and routes

```go
const (
    RPCNamespacesCreate = "/v1/namespaces.create"
    RPCNamespacesList   = "/v1/namespaces.list"
    RPCNamespacesDelete = "/v1/namespaces.delete"
)

func (h *HTTPHandler) NamespacesCreate(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) NamespacesList(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) NamespacesDelete(ctx context.Context, w http.ResponseWriter, r *http.Request)
```

**Function responsibilities:**
- Add routes to `ServeHTTP()` switch statement
- Follow handler patterns from existing queue endpoints

**Context for implementation:**
- Follow patterns from `transport/http.go:242-313`

---

#### 23. HTTP Handler Principal Extraction (Phase 1)

**File**: `transport/http.go`
**Changes**: Add extractPrincipal and update ServeHTTP

```go
func (h *HTTPHandler) extractPrincipal(r *http.Request) types.Principal
```

**Function responsibilities:**
- Phase 1: Always return `AnonymousPrincipal` (no auth configured yet)
- Update `ServeHTTP()` to call `extractPrincipal()` and inject into context via `internal.ContextWithPrincipal()`

---

#### 24. Namespace Client Methods

**File**: `client.go`
**Changes**: Add namespace client methods

```go
func (c *Client) NamespacesCreate(ctx context.Context, req *pb.NamespaceCreateRequest) error
func (c *Client) NamespacesList(ctx context.Context, res *pb.NamespacesListResponse, opts *ListOptions) error
func (c *Client) NamespacesDelete(ctx context.Context, req *pb.NamespacesDeleteRequest) error
```

**Function responsibilities:**
- Follow client patterns from existing queue methods

---

#### 25. Update Test Setup

**File**: `service/common_test.go`
**Changes**: Update test setup to include namespaces storage

**Function responsibilities:**
- Update `setupMemoryStorage()` to create and include `MemoryNamespaces`
- Update `badgerTestSetup.Setup()` to create and include `BadgerNamespaces`

---

### Testing Requirements

**File**: `service/namespace_test.go` (new file)

```go
func TestNamespaces(t *testing.T)
func testNamespaces(t *testing.T, setup NewStorageFunc, tearDown func())
```

**Test objectives:**
- Namespace CRUD operations across all storage backends
- Reserved namespace (`_system`) protection
- Namespace deletion blocked when queues exist
- Queue creation requires valid namespace
- Queue operations scoped to namespace

**Key scenarios:**

*CRUD Operations:*
- `NamespaceCRUD`: Create/list/delete namespaces
- `SystemNamespaceExistsAfterBootstrap`: `_system` namespace exists after bootstrap
- `NamespaceListPagination`: Verify pagination via `pivot` and `limit` works correctly

*Namespace Validation:*
- `ReservedNamespaceProtection`: Attempt to create namespace starting with `_` (should fail)
- `ReservedNamespaceDeletion`: Attempt to delete `_system` namespace (should fail)
- `NamespaceNameUniqueness`: Creating namespace with duplicate name returns error
- `NamespaceNameEmptyRejected`: Empty namespace name returns validation error
- `NamespaceNameWhitespaceRejected`: Namespace name with whitespace returns error
- `NamespaceNameMaxLengthEnforced`: Names exceeding 500 characters rejected

*Namespace-Queue Integration:*
- `QueueRequiresNamespace`: Create queue without namespace (should fail with error containing "namespace")
- `QueueRequiresExistingNamespace`: Create queue with non-existent namespace (should fail)
- `NamespaceDeleteBlockedByQueues`: Delete namespace with queues (should fail)
- `QueueOperationsScopedToNamespace`: Same queue name in different namespaces are independent
- `QueueOpsEmptyNamespace`: Queue operation with empty namespace field returns validation error
- `SystemNamespaceQueueCreation`: Verify whether queues can be created in `_system` namespace (document behavior)

**Testing patterns:**
- Tests MUST be in `service_test` package (external test package)
- Use `require` for critical assertions that should halt test, `assert` for non-critical
- No descriptive messages in assertions (use comments instead)
- All tests interact through HTTP client, never call internal functions

**Context for implementation:**
- Follow test structure from `service/queue_test.go` multi-backend pattern

---

### Validation

- [ ] Run: `make proto` (regenerate proto files)
- [ ] Run: `go build ./...`
- [ ] Run: `go test ./... -run "TestNamespaces"` (covers all key scenarios above)
- [ ] Run: `go test ./... -run "TestQueue"` (verify existing tests still pass)
- [ ] Verify: No tests call internal/unexported functions

---

## Phase 2: Users + API Keys

### Overview
Implement user management and API key authentication. After this phase, requests can be authenticated via Bearer tokens.

### Changes Required

#### 1. User and API Key Error Types

**File**: `internal/types/errors.go`
**Changes**: Add User and API Key error variables

```go
// User errors
var (
    ErrUserNotExist         error
    ErrUserAlreadyExists    error
    ErrUsernameAlreadyTaken error
)

// API Key errors
var (
    ErrAPIKeyNotExist error
    ErrAPIKeyExpired  error
    ErrAPIKeyInvalid  error
)

// Authorization errors
var (
    ErrAuthRequired error
    ErrAccessDenied error
)
```

**Function responsibilities:**
- User errors use `transport.NewRequestFailed()` or `transport.NewInvalidOption()`
- API Key errors use `transport.NewUnauthorized()`
- `ErrAccessDenied` uses `transport.NewForbidden()`

---

#### 2. API Key Type

**File**: `internal/types/auth.go`
**Changes**: Add APIKey type

```go
type APIKey struct {
    ID             string
    UserID         string
    NamespaceScope *string
    Name           string
    KeyHash        string
    KeyPrefix      string
    ExpiresAt      *clock.Time
    LastUsedAt     *clock.Time
    CreatedAt      clock.Time
}

func (k *APIKey) ToProto() *proto.APIKeyMetadata
func (u *User) ToProto() *proto.User
```

**Function responsibilities:**
- `KeyHash`: SHA-256 hash of raw key
- `KeyPrefix`: First 8 characters for identification
- `ToProto()`: Convert to proto representation (exclude hash)

---

#### 3. Users Storage Interface

**File**: `internal/store/users.go` (new file)
**Changes**: Define Users storage interface

```go
type Users interface {
    Get(ctx context.Context, id string, user *types.User) error
    GetByUsername(ctx context.Context, username string, user *types.User) error
    Add(ctx context.Context, user types.User) error
    List(ctx context.Context, users *[]types.User, opts types.ListOptions) error
    Delete(ctx context.Context, id string) error
    Close(ctx context.Context) error
}
```

**Function responsibilities:**
- Support lookup by ID and username
- Username must be unique

---

#### 4. API Keys Storage Interface

**File**: `internal/store/apikeys.go` (new file)
**Changes**: Define APIKeys storage interface

```go
type APIKeys interface {
    Get(ctx context.Context, id string, key *types.APIKey) error
    GetByHash(ctx context.Context, hash string, key *types.APIKey) error
    Add(ctx context.Context, key types.APIKey) error
    List(ctx context.Context, keys *[]types.APIKey, opts types.ListOptions) error
    ListByUser(ctx context.Context, userID string, keys *[]types.APIKey, opts types.ListOptions) error
    Delete(ctx context.Context, id string) error
    UpdateLastUsed(ctx context.Context, id string, lastUsed clock.Time) error
    Close(ctx context.Context) error
}
```

**Function responsibilities:**
- `GetByHash()`: Lookup by SHA-256 hash for authentication
- `ListByUser()`: List keys belonging to a user
- `UpdateLastUsed()`: Update last used timestamp

---

#### 5. Update Storage Config

**File**: `internal/store/store.go`
**Changes**: Add Users and APIKeys to Config

```go
type Config struct {
    Queues           Queues
    Namespaces       Namespaces
    Users            Users       // NEW
    APIKeys          APIKeys     // NEW
    PartitionStorage []PartitionStorage
}
```

---

#### 6. Memory Users Storage

**File**: `internal/store/memory.go`
**Changes**: Add `MemoryUsers` implementation

```go
type MemoryUsers struct {
    users    map[string]types.User
    byUsername map[string]string
    mutex    sync.RWMutex
    log      *slog.Logger
}

func NewMemoryUsers(log *slog.Logger) *MemoryUsers
func (m *MemoryUsers) Get(ctx context.Context, id string, user *types.User) error
func (m *MemoryUsers) GetByUsername(ctx context.Context, username string, user *types.User) error
func (m *MemoryUsers) Add(ctx context.Context, user types.User) error
func (m *MemoryUsers) List(ctx context.Context, users *[]types.User, opts types.ListOptions) error
func (m *MemoryUsers) Delete(ctx context.Context, id string) error
func (m *MemoryUsers) Close(ctx context.Context) error
```

---

#### 7. Memory API Keys Storage

**File**: `internal/store/memory.go`
**Changes**: Add `MemoryAPIKeys` implementation

```go
type MemoryAPIKeys struct {
    keys     map[string]types.APIKey
    byHash   map[string]string
    byUser   map[string][]string
    mutex    sync.RWMutex
    log      *slog.Logger
}

func NewMemoryAPIKeys(log *slog.Logger) *MemoryAPIKeys
func (m *MemoryAPIKeys) Get(ctx context.Context, id string, key *types.APIKey) error
func (m *MemoryAPIKeys) GetByHash(ctx context.Context, hash string, key *types.APIKey) error
func (m *MemoryAPIKeys) Add(ctx context.Context, key types.APIKey) error
func (m *MemoryAPIKeys) List(ctx context.Context, keys *[]types.APIKey, opts types.ListOptions) error
func (m *MemoryAPIKeys) ListByUser(ctx context.Context, userID string, keys *[]types.APIKey, opts types.ListOptions) error
func (m *MemoryAPIKeys) Delete(ctx context.Context, id string) error
func (m *MemoryAPIKeys) UpdateLastUsed(ctx context.Context, id string, lastUsed clock.Time) error
func (m *MemoryAPIKeys) Close(ctx context.Context) error
```

---

#### 8. BadgerDB Users Storage

**File**: `internal/store/badger.go`
**Changes**: Add `BadgerUsers` implementation

```go
type BadgerUsers struct {
    db  *badger.DB
    log *slog.Logger
}

func NewBadgerUsers(conf BadgerConfig) *BadgerUsers
func (b *BadgerUsers) Get(ctx context.Context, id string, user *types.User) error
func (b *BadgerUsers) GetByUsername(ctx context.Context, username string, user *types.User) error
func (b *BadgerUsers) Add(ctx context.Context, user types.User) error
func (b *BadgerUsers) List(ctx context.Context, users *[]types.User, opts types.ListOptions) error
func (b *BadgerUsers) Delete(ctx context.Context, id string) error
func (b *BadgerUsers) Close(ctx context.Context) error
```

**Function responsibilities:**
- Use key prefix `user:` for user records
- Use key prefix `user-username:` for username -> ID index

---

#### 9. BadgerDB API Keys Storage

**File**: `internal/store/badger.go`
**Changes**: Add `BadgerAPIKeys` implementation

```go
type BadgerAPIKeys struct {
    db  *badger.DB
    log *slog.Logger
}

func NewBadgerAPIKeys(conf BadgerConfig) *BadgerAPIKeys
func (b *BadgerAPIKeys) Get(ctx context.Context, id string, key *types.APIKey) error
func (b *BadgerAPIKeys) GetByHash(ctx context.Context, hash string, key *types.APIKey) error
func (b *BadgerAPIKeys) Add(ctx context.Context, key types.APIKey) error
func (b *BadgerAPIKeys) List(ctx context.Context, keys *[]types.APIKey, opts types.ListOptions) error
func (b *BadgerAPIKeys) ListByUser(ctx context.Context, userID string, keys *[]types.APIKey, opts types.ListOptions) error
func (b *BadgerAPIKeys) Delete(ctx context.Context, id string) error
func (b *BadgerAPIKeys) UpdateLastUsed(ctx context.Context, id string, lastUsed clock.Time) error
func (b *BadgerAPIKeys) Close(ctx context.Context) error
```

**Function responsibilities:**
- Use key prefix `apikey:` for key records
- Use key prefix `apikey-hash:` for hash -> ID index
- Use key prefix `apikey-user:` for user -> key IDs index

---

#### 10. PostgreSQL Users Storage

**File**: `internal/store/postgres.go`
**Changes**: Add `PostgresUsers` implementation

```go
type PostgresUsers struct {
    pool *pgxpool.Pool
    conf PostgresConfig
    log  *slog.Logger
}

func NewPostgresUsers(conf PostgresConfig) *PostgresUsers
func (p *PostgresUsers) Get(ctx context.Context, id string, user *types.User) error
func (p *PostgresUsers) GetByUsername(ctx context.Context, username string, user *types.User) error
func (p *PostgresUsers) Add(ctx context.Context, user types.User) error
func (p *PostgresUsers) List(ctx context.Context, users *[]types.User, opts types.ListOptions) error
func (p *PostgresUsers) Delete(ctx context.Context, id string) error
func (p *PostgresUsers) Close(ctx context.Context) error
```

**Function responsibilities:**
- Create table: `users (id TEXT PRIMARY KEY, username TEXT UNIQUE, external_id TEXT, email TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)`

---

#### 11. PostgreSQL API Keys Storage

**File**: `internal/store/postgres.go`
**Changes**: Add `PostgresAPIKeys` implementation

```go
type PostgresAPIKeys struct {
    pool *pgxpool.Pool
    conf PostgresConfig
    log  *slog.Logger
}

func NewPostgresAPIKeys(conf PostgresConfig) *PostgresAPIKeys
func (p *PostgresAPIKeys) Get(ctx context.Context, id string, key *types.APIKey) error
func (p *PostgresAPIKeys) GetByHash(ctx context.Context, hash string, key *types.APIKey) error
func (p *PostgresAPIKeys) Add(ctx context.Context, key types.APIKey) error
func (p *PostgresAPIKeys) List(ctx context.Context, keys *[]types.APIKey, opts types.ListOptions) error
func (p *PostgresAPIKeys) ListByUser(ctx context.Context, userID string, keys *[]types.APIKey, opts types.ListOptions) error
func (p *PostgresAPIKeys) Delete(ctx context.Context, id string) error
func (p *PostgresAPIKeys) UpdateLastUsed(ctx context.Context, id string, lastUsed clock.Time) error
func (p *PostgresAPIKeys) Close(ctx context.Context) error
```

**Function responsibilities:**
- Create table: `api_keys (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users(id), namespace_scope TEXT, name TEXT, key_hash TEXT UNIQUE, key_prefix TEXT, expires_at TIMESTAMPTZ, last_used_at TIMESTAMPTZ, created_at TIMESTAMPTZ)`
- Index on `key_hash` for fast authentication lookups

---

#### 12. API Key Generation Utilities

**File**: `internal/auth/apikey.go` (new file)
**Changes**: Create API key utilities

```go
func GenerateAPIKey(env string) (rawKey string, hash string, prefix string)
func HashAPIKey(rawKey string) string
func ValidateAPIKeyFormat(key string) error
```

**Function responsibilities:**
- `GenerateAPIKey()`: Generate key in format `sk-[env]-[entropy]`, 32+ character Base62 entropy, default env to "live"
- `HashAPIKey()`: SHA-256 hash using `crypto/sha256`
- `ValidateAPIKeyFormat()`: Regex validation `^sk-[a-z0-9]+-[a-zA-Z0-9]{32,}$`

**Context for implementation:**
- Use `crypto/rand` for secure entropy generation

---

#### 13. Auth Cache

**File**: `internal/auth/cache.go` (new file)
**Changes**: Create authentication cache

```go
type CacheEntry struct {
    Key        types.APIKey
    LastUsedAt time.Time
    ExpiresAt  time.Time
}

type AuthCache struct {
    entries sync.Map
}

func (c *AuthCache) Get(hash string) (*types.APIKey, bool)
func (c *AuthCache) Put(hash string, key types.APIKey)
```

**Function responsibilities:**
- Cache TTL 5 minutes
- Thread-safe with sync.Map
- Used for read-through caching of API keys

---

#### 14. Users Proto Definitions

**File**: `proto/users.proto` (new file)
**Changes**: Create user proto messages

```protobuf
syntax = "proto3";
package proto;
option go_package = "github.com/kapetan-io/querator/proto";

import "google/protobuf/timestamp.proto";

message User {
    string id = 1;
    string username = 2;
    string external_id = 3;
    string email = 4;
    google.protobuf.Timestamp created_at = 5;
    google.protobuf.Timestamp updated_at = 6;
}

message UserCreateRequest {
    string username = 1;
    string email = 2;
    string external_id = 3;
}

message UserCreateResponse {
    string id = 1;
}

message UsersListRequest {
    int32 limit = 1;
    string pivot = 2;
}

message UsersListResponse {
    repeated User items = 1;
}

message UsersDeleteRequest {
    string id = 1;
}
```

---

#### 15. API Keys Proto Definitions

**File**: `proto/apikeys.proto` (new file)
**Changes**: Create API key proto messages

```protobuf
syntax = "proto3";
package proto;
option go_package = "github.com/kapetan-io/querator/proto";

import "google/protobuf/timestamp.proto";

message APIKeyMetadata {
    string id = 1;
    string user_id = 2;
    string namespace_scope = 3;
    string name = 4;
    string prefix = 5;
    google.protobuf.Timestamp expires_at = 6;
    google.protobuf.Timestamp last_used_at = 7;
    google.protobuf.Timestamp created_at = 8;
}

message APIKeyCreateRequest {
    string user_id = 1;
    string name = 2;
    string namespace_scope = 3;
    google.protobuf.Timestamp expires_at = 4;
    string env_tag = 5;
}

message APIKeyCreateResponse {
    string id = 1;
    string key = 2;
    string prefix = 3;
}

message APIKeysListRequest {
    string user_id = 1;
    int32 limit = 2;
    string pivot = 3;
}

message APIKeysListResponse {
    repeated APIKeyMetadata items = 1;
}

message APIKeysDeleteRequest {
    string id = 1;
}
```

---

#### 16. User Validation

**File**: `service/validation.go`
**Changes**: Add user validation functions

```go
func validateUserCreate(req *proto.UserCreateRequest) error
func validateAPIKeyCreate(req *proto.APIKeyCreateRequest) error
```

**Function responsibilities:**
- Validate username not empty, reasonable length
- Validate user_id exists for API key creation

---

#### 17. User Service Methods

**File**: `service/service.go`
**Changes**: Add user management methods

```go
func (s *Service) UsersCreate(ctx context.Context, req *proto.UserCreateRequest, resp *proto.UserCreateResponse) error
func (s *Service) UsersList(ctx context.Context, req *proto.UsersListRequest, resp *proto.UsersListResponse) error
func (s *Service) UsersDelete(ctx context.Context, req *proto.UsersDeleteRequest) error
```

**Function responsibilities:**
- `UsersCreate()`: Generate unique user ID, validate username uniqueness
- `UsersList()`: List with pagination
- `UsersDelete()`: Delete user and CASCADE delete all their API keys

---

#### 18. API Key Service Methods

**File**: `service/service.go`
**Changes**: Add API key management methods

```go
func (s *Service) APIKeysCreate(ctx context.Context, req *proto.APIKeyCreateRequest, resp *proto.APIKeyCreateResponse) error
func (s *Service) APIKeysList(ctx context.Context, req *proto.APIKeysListRequest, resp *proto.APIKeysListResponse) error
func (s *Service) APIKeysDelete(ctx context.Context, req *proto.APIKeysDeleteRequest) error
```

**Function responsibilities:**
- `APIKeysCreate()`: Generate key, return raw key ONCE in response, store only hash
- `APIKeysList()`: Return metadata only (no hash/raw key)
- `APIKeysDelete()`: Allow deletion of any key

---

#### 19. Bootstrap Anonymous User

**File**: `service/service.go`
**Changes**: Update Bootstrap to create Anonymous user

```go
func (s *Service) Bootstrap(ctx context.Context) error
```

**Function responsibilities:**
- Create `_system` namespace (from Phase 1)
- Create Anonymous user if not exists (ID: "anonymous", Username: "anonymous")
- Idempotent

---

#### 20. Transport Interfaces for Users and API Keys

**File**: `transport/interfaces.go`
**Changes**: Add UserAdmin and APIKeyAdmin interfaces

```go
type UserAdmin interface {
    UsersCreate(ctx context.Context, req *pb.UserCreateRequest, resp *pb.UserCreateResponse) error
    UsersList(ctx context.Context, req *pb.UsersListRequest, resp *pb.UsersListResponse) error
    UsersDelete(ctx context.Context, req *pb.UsersDeleteRequest) error
}

type APIKeyAdmin interface {
    APIKeysCreate(ctx context.Context, req *pb.APIKeyCreateRequest, resp *pb.APIKeyCreateResponse) error
    APIKeysList(ctx context.Context, req *pb.APIKeysListRequest, resp *pb.APIKeysListResponse) error
    APIKeysDelete(ctx context.Context, req *pb.APIKeysDeleteRequest) error
}

type Service interface {
    QueueOps
    QueueAdmin
    StorageInspector
    NamespaceAdmin
    UserAdmin       // NEW
    APIKeyAdmin     // NEW
    Health(ctx context.Context) (*HealthResponse, error)
}
```

---

#### 21. Users and API Keys HTTP Endpoints

**File**: `transport/http.go`
**Changes**: Add endpoint handlers

```go
const (
    RPCUsersCreate   = "/v1/users.create"
    RPCUsersList     = "/v1/users.list"
    RPCUsersDelete   = "/v1/users.delete"
    RPCAPIKeysCreate = "/v1/api-keys.create"
    RPCAPIKeysList   = "/v1/api-keys.list"
    RPCAPIKeysDelete = "/v1/api-keys.delete"
)

func (h *HTTPHandler) UsersCreate(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) UsersList(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) UsersDelete(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) APIKeysCreate(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) APIKeysList(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) APIKeysDelete(ctx context.Context, w http.ResponseWriter, r *http.Request)
```

**Function responsibilities:**
- Add routes to `ServeHTTP()` switch statement

---

#### 22. HTTP Handler Auth Backend Integration

**File**: `transport/http.go`
**Changes**: Add AuthBackend to HTTPHandler and update extractPrincipal

```go
type HTTPHandler struct {
    // ... existing fields
    auth auth.AuthBackend  // NEW
}

type HTTPHandlerConfig struct {
    Service        Service
    AuthBackend    auth.AuthBackend  // NEW
    MaxProduceSize int64
    Log            *slog.Logger
}

func (h *HTTPHandler) extractPrincipal(r *http.Request) (types.Principal, error)
```

**Function responsibilities:**
- `extractPrincipal()`: Parse `Authorization: Bearer <token>` header
- If no header, return `AnonymousPrincipal`
- If header present, call `AuthBackend.Authenticate()`
- Return error for invalid/expired keys (401 Unauthorized)
- Update `ServeHTTP()` to handle error from extractPrincipal

---

#### 23. Client Auth Header Support

**File**: `client.go`
**Changes**: Add APIKey field and auth header support

```go
type ClientConfig struct {
    Client   *http.Client
    Endpoint string
    APIKey   string  // NEW
}
```

**Function responsibilities:**
- Update all request methods to include `Authorization: Bearer <key>` header if `APIKey` is set

---

#### 24. Users and API Keys Client Methods

**File**: `client.go`
**Changes**: Add client methods

```go
func (c *Client) UsersCreate(ctx context.Context, req *pb.UserCreateRequest, resp *pb.UserCreateResponse) error
func (c *Client) UsersList(ctx context.Context, resp *pb.UsersListResponse, opts *ListOptions) error
func (c *Client) UsersDelete(ctx context.Context, req *pb.UsersDeleteRequest) error
func (c *Client) APIKeysCreate(ctx context.Context, req *pb.APIKeyCreateRequest, resp *pb.APIKeyCreateResponse) error
func (c *Client) APIKeysList(ctx context.Context, req *pb.APIKeysListRequest, resp *pb.APIKeysListResponse) error
func (c *Client) APIKeysDelete(ctx context.Context, req *pb.APIKeysDeleteRequest) error
```

---

#### 25. Update Test Setup

**File**: `service/common_test.go`
**Changes**: Update test setup to include users and API keys storage

**Function responsibilities:**
- Update storage setup functions to create Users and APIKeys storage

---

### Testing Requirements

**File**: `service/users_test.go` (new file)

```go
func TestUsers(t *testing.T)
func testUsers(t *testing.T, setup NewStorageFunc, tearDown func())
```

**Test objectives:**
- User CRUD operations
- Username uniqueness validation
- API key generation and format validation
- Authentication via Bearer token
- Key expiration handling
- Namespace-scoped key restrictions

**Key scenarios:**

*User CRUD Operations:*
- `UserCRUD`: Create/list/delete users
- `AnonymousUserExistsAfterBootstrap`: Anonymous user exists after bootstrap
- `UserListPagination`: Verify pagination via `pivot` and `limit` works correctly

*User Validation:*
- `DuplicateUsernameRejection`: Duplicate username returns error
- `UserNameEmptyRejected`: Empty username returns validation error
- `AnonymousUserDeletionBehavior`: Document whether Anonymous user can be deleted and consequences

*API Key CRUD Operations:*
- `APIKeyCRUD`: Create/list/delete API keys
- `APIKeyListByUser`: List API keys filtered by user ID
- `APIKeyListPagination`: Verify pagination via `pivot` and `limit` works correctly

*API Key Format and Generation:*
- `APIKeyFormatValidation`: Key must match `sk-[env]-[entropy]` pattern
- `APIKeyEnvTagDefault`: Key without `env_tag` defaults to `live` (`sk-live-...`)
- `APIKeyGeneratorDefaults`: Verify internal generator defaults to "live" for empty env_tag
- `APIKeyEnvTagCustomization`: Keys support custom env tags (`sk-ci-...`, `sk-test-...`)
- `APIKeyPrefixIsFirst8Chars`: Verify `KeyPrefix` in response matches first 8 chars of raw key
- `APIKeyEntropyLength`: Verify entropy portion is 32+ characters

*API Key Scoping:*
- `APIKeyScopedToNamespace`: Key with `namespace_scope` only works in that namespace, returns 403 for other namespaces
- `APIKeyInheritanceModeDefault`: Key without `namespace_scope` inherits user's full access across namespaces
- `APIKeyScopeValidation`: Key with `namespace_scope` for non-existent namespace returns error

*API Key Validation:*
- `APIKeyForNonExistentUserRejected`: Creating key for non-existent `user_id` returns error
- `APIKeyNameEmptyRejected`: Empty API key name returns validation error
- `APIKeyExpiredAtPastRejected`: Key with `expires_at` in the past is rejected at creation

*Authentication Flow:*
- `AuthenticateWithKey`: Create user, create API key, authenticate with key
- `ExpiredKeyRejection`: Expired key returns 401 Unauthorized
- `InvalidKeyRejection`: Invalid/malformed key returns 401 Unauthorized
- `MissingAuthHeaderReturnsAnonymous`: Missing auth header returns AnonymousPrincipal (not 401 error)
- `APIKeyLastUsedAtTracking`: Verify `LastUsedAt` updates after successful authentication

*Auth Header Edge Cases:*
- `MalformedAuthHeaders`: Invalid header formats return 401:
  - Missing "Bearer" prefix (`Authorization: sk-live-abc123`)
  - Empty token (`Authorization: Bearer `)
  - Extra whitespace (`Authorization: Bearer  sk-live-abc123`)
- `AuthHeaderCaseSensitivity`: Verify `authorization` vs `Authorization` handling
- `AuthHeaderVeryLongInput`: Extremely long auth header does not crash server

*User-Key Lifecycle:*
- `UserDeleteCascadesToAPIKeys`: Delete user cascades to delete all their API keys
- `SelfRevocationWorkflow`: User can delete the key they are currently using (succeeds), subsequent requests fail (401)

**Testing patterns:**
- Tests MUST be in `service_test` package (external test package)
- Use `require` for critical assertions that should halt test, `assert` for non-critical
- No descriptive messages in assertions (use comments instead)
- All tests interact through HTTP client, never call internal functions

**Context for implementation:**
- Follow test structure from `service/queue_test.go` multi-backend pattern

---

### Validation

- [ ] Run: `make proto`
- [ ] Run: `go build ./...`
- [ ] Run: `go test ./... -run "TestUsers"` (covers all key scenarios above)
- [ ] Run: `go test ./... -run "TestNamespaces"` (Phase 1 still passes)
- [ ] Verify: No tests call internal/unexported functions

---

## Phase 3: Roles + RBAC

### Overview
Implement role-based access control with cascading permission checks. After this phase, authorization is fully enforced.

### Changes Required

#### 1. Role and RoleBinding Error Types

**File**: `internal/types/errors.go`
**Changes**: Add Role error variables

```go
var (
    ErrRoleNotExist              error
    ErrRoleAlreadyExists         error
    ErrRoleBindingAlreadyExists  error
    ErrRoleHasBindings           error
)
```

---

#### 2. Role and RoleBinding Types

**File**: `internal/types/auth.go`
**Changes**: Add Role and RoleBinding types

```go
type Role struct {
    ID          string
    Namespace   string
    Name        string
    Permissions []string
    CreatedAt   clock.Time
}

type RoleBinding struct {
    ID        string
    Namespace string
    UserID    string
    RoleID    string
    CreatedAt clock.Time
}

func (r *Role) ToProto() *proto.Role
func (b *RoleBinding) ToProto() *proto.RoleBinding
```

---

#### 3. Permission Constants

**File**: `internal/auth/permissions.go` (new file)
**Changes**: Define permission constants

```go
const (
    PermNamespaceCreate = "namespace.create"
    PermNamespaceDelete = "namespace.delete"
    PermNamespaceList   = "namespace.list"

    PermQueueCreate   = "queue.create"
    PermQueueDelete   = "queue.delete"
    PermQueueUpdate   = "queue.update"
    PermQueueList     = "queue.list"
    PermQueueProduce  = "queue.produce"
    PermQueueLease    = "queue.lease"
    PermQueueComplete = "queue.complete"
    PermQueueRetry    = "queue.retry"
    PermQueueStats    = "queue.stats"
    PermQueueClear    = "queue.clear"

    PermUserCreate   = "user.create"
    PermUserDelete   = "user.delete"
    PermUserList     = "user.list"
    PermAPIKeyCreate = "apikey.create"
    PermAPIKeyDelete = "apikey.delete"
    PermAPIKeyList   = "apikey.list"

    PermRoleCreate        = "role.create"
    PermRoleUpdate        = "role.update"
    PermRoleDelete        = "role.delete"
    PermRoleList          = "role.list"
    PermRoleBindingCreate = "rolebinding.create"
    PermRoleBindingDelete = "rolebinding.delete"
    PermRoleBindingList   = "rolebinding.list"

    PermSystemHealth  = "system.health"
    PermSystemMetrics = "system.metrics"
)

var AllPermissions = []string{...}
```

---

#### 4. Roles Storage Interface

**File**: `internal/store/roles.go` (new file)
**Changes**: Define Roles and RoleBindings storage interfaces

```go
type Roles interface {
    Get(ctx context.Context, namespace, name string, role *types.Role) error
    GetByID(ctx context.Context, id string, role *types.Role) error
    Add(ctx context.Context, role types.Role) error
    Update(ctx context.Context, role types.Role) error
    List(ctx context.Context, namespace string, roles *[]types.Role, opts types.ListOptions) error
    Delete(ctx context.Context, id string) error
    Close(ctx context.Context) error
}

type RoleBindings interface {
    Get(ctx context.Context, id string, binding *types.RoleBinding) error
    Add(ctx context.Context, binding types.RoleBinding) error
    List(ctx context.Context, namespace string, bindings *[]types.RoleBinding, opts types.ListOptions) error
    ListByUser(ctx context.Context, userID string, bindings *[]types.RoleBinding) error
    ListByRole(ctx context.Context, roleID string, bindings *[]types.RoleBinding) error
    Delete(ctx context.Context, id string) error
    Close(ctx context.Context) error
}
```

---

#### 5. Update Storage Config

**File**: `internal/store/store.go`
**Changes**: Add Roles and RoleBindings to Config

```go
type Config struct {
    Queues           Queues
    Namespaces       Namespaces
    Users            Users
    APIKeys          APIKeys
    Roles            Roles            // NEW
    RoleBindings     RoleBindings     // NEW
    PartitionStorage []PartitionStorage
}
```

---

#### 6. Memory Roles Storage

**File**: `internal/store/memory.go`
**Changes**: Add `MemoryRoles` and `MemoryRoleBindings` implementations

```go
type MemoryRoles struct {
    roles map[string]types.Role
    byNamespaceName map[string]string
    mutex sync.RWMutex
    log   *slog.Logger
}

func NewMemoryRoles(log *slog.Logger) *MemoryRoles
func (m *MemoryRoles) Get(ctx context.Context, namespace, name string, role *types.Role) error
func (m *MemoryRoles) GetByID(ctx context.Context, id string, role *types.Role) error
func (m *MemoryRoles) Add(ctx context.Context, role types.Role) error
func (m *MemoryRoles) Update(ctx context.Context, role types.Role) error
func (m *MemoryRoles) List(ctx context.Context, namespace string, roles *[]types.Role, opts types.ListOptions) error
func (m *MemoryRoles) Delete(ctx context.Context, id string) error
func (m *MemoryRoles) Close(ctx context.Context) error

type MemoryRoleBindings struct {
    bindings map[string]types.RoleBinding
    byUser   map[string][]string
    byRole   map[string][]string
    mutex    sync.RWMutex
    log      *slog.Logger
}

func NewMemoryRoleBindings(log *slog.Logger) *MemoryRoleBindings
func (m *MemoryRoleBindings) Get(ctx context.Context, id string, binding *types.RoleBinding) error
func (m *MemoryRoleBindings) Add(ctx context.Context, binding types.RoleBinding) error
func (m *MemoryRoleBindings) List(ctx context.Context, namespace string, bindings *[]types.RoleBinding, opts types.ListOptions) error
func (m *MemoryRoleBindings) ListByUser(ctx context.Context, userID string, bindings *[]types.RoleBinding) error
func (m *MemoryRoleBindings) ListByRole(ctx context.Context, roleID string, bindings *[]types.RoleBinding) error
func (m *MemoryRoleBindings) Delete(ctx context.Context, id string) error
func (m *MemoryRoleBindings) Close(ctx context.Context) error
```

---

#### 7. BadgerDB Roles Storage

**File**: `internal/store/badger.go`
**Changes**: Add `BadgerRoles` and `BadgerRoleBindings` implementations

```go
type BadgerRoles struct {
    db  *badger.DB
    log *slog.Logger
}

func NewBadgerRoles(conf BadgerConfig) *BadgerRoles
// ... all interface methods

type BadgerRoleBindings struct {
    db  *badger.DB
    log *slog.Logger
}

func NewBadgerRoleBindings(conf BadgerConfig) *BadgerRoleBindings
// ... all interface methods
```

**Function responsibilities:**
- Use key prefix `role:` for role records
- Use key prefix `role-ns-name:` for namespace+name -> ID index
- Use key prefix `rolebinding:` for binding records
- Use key prefix `rolebinding-user:` for user -> binding IDs index
- Use key prefix `rolebinding-role:` for role -> binding IDs index

---

#### 8. PostgreSQL Roles Storage

**File**: `internal/store/postgres.go`
**Changes**: Add `PostgresRoles` and `PostgresRoleBindings` implementations

```go
type PostgresRoles struct {
    pool *pgxpool.Pool
    conf PostgresConfig
    log  *slog.Logger
}

func NewPostgresRoles(conf PostgresConfig) *PostgresRoles
// ... all interface methods

type PostgresRoleBindings struct {
    pool *pgxpool.Pool
    conf PostgresConfig
    log  *slog.Logger
}

func NewPostgresRoleBindings(conf PostgresConfig) *PostgresRoleBindings
// ... all interface methods
```

**Function responsibilities:**
- Create table: `roles (id TEXT PRIMARY KEY, namespace TEXT, name TEXT, permissions TEXT[], created_at TIMESTAMPTZ, UNIQUE(namespace, name))`
- Create table: `role_bindings (id TEXT PRIMARY KEY, namespace TEXT, user_id TEXT, role_id TEXT, created_at TIMESTAMPTZ, UNIQUE(namespace, user_id, role_id))`

---

#### 9. AuthBackend Interface and Implementation

**File**: `internal/auth/backend.go` (new file)
**Changes**: Create AuthBackend interface and implementation

```go
type AuthBackend interface {
    Authenticate(ctx context.Context, token string) (types.Principal, error)
    HasPermission(ctx context.Context, principal types.Principal, targetNS string, perm string) (bool, error)
}

type DefaultAuthBackend struct {
    users        store.Users
    apiKeys      store.APIKeys
    roles        store.Roles
    roleBindings store.RoleBindings
    cache        *AuthCache
    log          *slog.Logger
    writeTimeout clock.Duration
}

type AuthBackendConfig struct {
    StorageConfig store.Config
    Log           *slog.Logger
    WriteTimeout  clock.Duration
}

func NewAuthBackend(conf AuthBackendConfig) *DefaultAuthBackend
func (a *DefaultAuthBackend) Authenticate(ctx context.Context, token string) (types.Principal, error)
func (a *DefaultAuthBackend) HasPermission(ctx context.Context, principal types.Principal, targetNS string, perm string) (bool, error)
func (a *DefaultAuthBackend) checkPermissionInNamespace(ctx context.Context, userID, namespace, perm string) (bool, error)
```

**Function responsibilities:**
- `Authenticate()`: Hash token, lookup in cache then DB, check expiration, update LastUsedAt async
- `HasPermission()`: Implement cascading check with strict logic gate `(Key_Scope_Match) AND (User_Has_Permission)`:
  1. Check API key scope: If scoped and scope != target namespace, return FALSE immediately (do not check roles).
  2. Check target namespace for permission (User has role in target NS).
  3. If not found and target != `_system`, check `_system` namespace (User has role in `_system`).

---

#### 10. Service Authorization Method

**File**: `service/service.go`
**Changes**: Add authorize method

```go
func (s *Service) authorize(ctx context.Context, targetNS, perm string) error
```

**Function responsibilities:**
- Extract Principal from context via `internal.PrincipalFromContext()`
- Call `AuthBackend.HasPermission()`
- Return `types.ErrAccessDenied` (403 Forbidden) if denied
- Return nil if allowed

---

#### 11. Add Authorization Calls to ALL Service Methods

**File**: `service/service.go`
**Changes**: Add `s.authorize()` call to every service method

**Queue operations:**
```go
func (s *Service) QueueProduce(ctx context.Context, req *proto.QueueProduceRequest) error {
    if err := s.authorize(ctx, req.Namespace, auth.PermQueueProduce); err != nil {
        return err
    }
    // ... existing logic
}
```

**Methods to update:**
- `QueueProduce` -> `auth.PermQueueProduce`
- `QueueLease` -> `auth.PermQueueLease`
- `QueueComplete` -> `auth.PermQueueComplete`
- `QueueRetry` -> `auth.PermQueueRetry`
- `QueueClear` -> `auth.PermQueueClear`
- `QueueReload` -> `auth.PermQueueClear`
- `QueueStats` -> `auth.PermQueueStats`
- `QueuesCreate` -> `auth.PermQueueCreate`
- `QueuesUpdate` -> `auth.PermQueueUpdate`
- `QueuesDelete` -> `auth.PermQueueDelete`
- `QueuesList` -> `auth.PermQueueList`
- `QueuesInfo` -> `auth.PermQueueList`
- `StorageItemsList` -> `auth.PermQueueList`
- `StorageItemsImport` -> `auth.PermQueueCreate`
- `StorageItemsDelete` -> `auth.PermQueueDelete`
- `NamespacesCreate` -> `auth.PermNamespaceCreate` (target: `_system`)
- `NamespacesList` -> `auth.PermNamespaceList` (target: `_system`)
- `NamespacesDelete` -> `auth.PermNamespaceDelete` (target: `_system`)
- `UsersCreate` -> `auth.PermUserCreate` (target: `_system`)
- `UsersList` -> `auth.PermUserList` (target: `_system`)
- `UsersDelete` -> `auth.PermUserDelete` (target: `_system`)
- `APIKeysCreate` -> `auth.PermAPIKeyCreate` (target: `_system`)
- `APIKeysList` -> `auth.PermAPIKeyList` (target: `_system`)
- `APIKeysDelete` -> `auth.PermAPIKeyDelete` (target: `_system`)
- `RolesCreate` -> `auth.PermRoleCreate` (target: `req.Namespace`)
- `RolesUpdate` -> `auth.PermRoleUpdate` (target: `req.Namespace`)
- `RolesList` -> `auth.PermRoleList` (target: `req.Namespace`)
- `RolesDelete` -> `auth.PermRoleDelete` (target: `req.Namespace`)
- `RoleBindingsCreate` -> `auth.PermRoleBindingCreate` (target: `req.Namespace`)
- `RoleBindingsList` -> `auth.PermRoleBindingList` (target: `req.Namespace`)
- `RoleBindingsDelete` -> `auth.PermRoleBindingDelete` (target: `req.Namespace`)

---

#### 11a. Update Namespace Deletion Logic

**File**: `service/service.go`
**Changes**: Update `NamespacesDelete` to check auth resources

**Function responsibilities:**
- Check `Roles` storage for any roles defined in the namespace
- Check `RoleBindings` storage for any bindings defined in the namespace
- Return error if any exist (prevent deletion until cleaned up)

---

#### 12. Roles Proto Definitions

**File**: `proto/roles.proto` (new file)
**Changes**: Create role proto messages

```protobuf
syntax = "proto3";
package proto;
option go_package = "github.com/kapetan-io/querator/proto";

import "google/protobuf/timestamp.proto";

message Role {
    string id = 1;
    string namespace = 2;
    string name = 3;
    repeated string permissions = 4;
    google.protobuf.Timestamp created_at = 5;
}

message RoleCreateRequest {
    string namespace = 1;
    string name = 2;
    repeated string permissions = 3;
}

message RoleUpdateRequest {
    string namespace = 1;
    string name = 2;
    repeated string permissions = 3;
}

message RolesListRequest {
    string namespace = 1;
    int32 limit = 2;
    string pivot = 3;
}

message RolesListResponse {
    repeated Role items = 1;
}

message RolesDeleteRequest {
    string namespace = 1;
    string name = 2;
}

message RoleBinding {
    string id = 1;
    string namespace = 2;
    string user_id = 3;
    string role_id = 4;
    google.protobuf.Timestamp created_at = 5;
}

message RoleBindingCreateRequest {
    string namespace = 1;
    string role_name = 2;
    string user_id = 3;
}

message RoleBindingsListRequest {
    string namespace = 1;
    string user_id_filter = 2;
    int32 limit = 3;
    string pivot = 4;
}

message RoleBindingsListResponse {
    repeated RoleBinding items = 1;
}

message RoleBindingDeleteRequest {
    string namespace = 1;
    string role_name = 2;
    string user_id = 3;
}
```

---

#### 13. Role Validation

**File**: `service/validation.go`
**Changes**: Add role validation functions

```go
func validateRoleCreate(req *proto.RoleCreateRequest) error
func validateRoleUpdate(req *proto.RoleUpdateRequest) error
func validateRoleBindingCreate(req *proto.RoleBindingCreateRequest) error
func isStandardRole(name string) bool
```

**Function responsibilities:**
- `isStandardRole()`: Return true for "Admin", "NamespaceOwner", "PublicViewer"
- Validate role name not empty
- Validate permissions are valid

---

#### 14. Role Service Methods

**File**: `service/service.go`
**Changes**: Add role management methods

```go
func (s *Service) RolesCreate(ctx context.Context, req *proto.RoleCreateRequest) error
func (s *Service) RolesUpdate(ctx context.Context, req *proto.RoleUpdateRequest) error
func (s *Service) RolesList(ctx context.Context, req *proto.RolesListRequest, resp *proto.RolesListResponse) error
func (s *Service) RolesDelete(ctx context.Context, req *proto.RolesDeleteRequest) error
func (s *Service) RoleBindingsCreate(ctx context.Context, req *proto.RoleBindingCreateRequest) error
func (s *Service) RoleBindingsList(ctx context.Context, req *proto.RoleBindingsListRequest, resp *proto.RoleBindingsListResponse) error
func (s *Service) RoleBindingsDelete(ctx context.Context, req *proto.RoleBindingDeleteRequest) error
```

**Function responsibilities:**
- `RolesCreate()`: Block creation of standard role names
- `RolesUpdate()`: Block update of standard roles (Admin, NamespaceOwner, PublicViewer)
- `RolesDelete()`: Block deletion of standard roles, check for active bindings (RESTRICT)
- `RoleBindingsCreate()`: Validate role and user exist

---

#### 15. Bootstrap Standard Roles and Anonymous Admin Binding

**File**: `service/service.go`
**Changes**: Complete bootstrap procedure

```go
func (s *Service) Bootstrap(ctx context.Context) error
func (s *Service) bootstrapSystemNamespace(ctx context.Context) error
func (s *Service) bootstrapAnonymousUser(ctx context.Context) error
func (s *Service) bootstrapStandardRoles(ctx context.Context) error
func (s *Service) bootstrapAnonymousAdminBinding(ctx context.Context) error
```

**Function responsibilities:**
- `bootstrapStandardRoles()`: Ensure Admin, NamespaceOwner, PublicViewer roles exist in `_system` with correct permissions (HEAL if exists but incomplete)
- `bootstrapAnonymousAdminBinding()`: Bind anonymous user to Admin role in `_system` (Open Door policy)
- Log warning: "SYSTEM RUNNING IN OPEN DOOR MODE"
- All bootstrap operations are idempotent

**Standard role definitions:**
- Admin: All permissions
- NamespaceOwner: All queue and role permissions (no namespace create/delete, no user management)
- PublicViewer: `system.health`, `system.metrics` only

---

#### 16. Service AuthBackend Integration

**File**: `service/service.go`
**Changes**: Add AuthBackend to Service and bootstrap state

```go
type Service struct {
    queues         *internal.QueuesManager
    auth           auth.AuthBackend           // NEW
    conf           Config
    bootstrapState atomic.Int32               // NEW
}

type Config struct {
    // ... existing fields
    AuthBackend auth.AuthBackend  // NEW (optional, created from storage if nil)
}
```

**Function responsibilities:**
- Create DefaultAuthBackend from storage config if not provided
- Add bootstrap state machine (Idle -> Bootstrapping -> Ready)
- Call `ensureBootstrapped()` before first request

---

#### 17. Transport Interfaces for Roles

**File**: `transport/interfaces.go`
**Changes**: Add RoleAdmin and RoleBindingAdmin interfaces

```go
type RoleAdmin interface {
    RolesCreate(ctx context.Context, req *pb.RoleCreateRequest) error
    RolesUpdate(ctx context.Context, req *pb.RoleUpdateRequest) error
    RolesList(ctx context.Context, req *pb.RolesListRequest, resp *pb.RolesListResponse) error
    RolesDelete(ctx context.Context, req *pb.RolesDeleteRequest) error
}

type RoleBindingAdmin interface {
    RoleBindingsCreate(ctx context.Context, req *pb.RoleBindingCreateRequest) error
    RoleBindingsList(ctx context.Context, req *pb.RoleBindingsListRequest, resp *pb.RoleBindingsListResponse) error
    RoleBindingsDelete(ctx context.Context, req *pb.RoleBindingDeleteRequest) error
}

type Service interface {
    // ... existing interfaces
    RoleAdmin           // NEW
    RoleBindingAdmin    // NEW
}
```

---

#### 18. Roles HTTP Endpoints

**File**: `transport/http.go`
**Changes**: Add role endpoint handlers

```go
const (
    RPCRolesCreate         = "/v1/roles.create"
    RPCRolesUpdate         = "/v1/roles.update"
    RPCRolesList           = "/v1/roles.list"
    RPCRolesDelete         = "/v1/roles.delete"
    RPCRoleBindingsCreate  = "/v1/role-bindings.create"
    RPCRoleBindingsList    = "/v1/role-bindings.list"
    RPCRoleBindingsDelete  = "/v1/role-bindings.delete"
)

func (h *HTTPHandler) RolesCreate(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) RolesUpdate(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) RolesList(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) RolesDelete(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) RoleBindingsCreate(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) RoleBindingsList(ctx context.Context, w http.ResponseWriter, r *http.Request)
func (h *HTTPHandler) RoleBindingsDelete(ctx context.Context, w http.ResponseWriter, r *http.Request)
```

---

#### 19. Roles Client Methods

**File**: `client.go`
**Changes**: Add role client methods

```go
func (c *Client) RolesCreate(ctx context.Context, req *pb.RoleCreateRequest) error
func (c *Client) RolesUpdate(ctx context.Context, req *pb.RoleUpdateRequest) error
func (c *Client) RolesList(ctx context.Context, req *pb.RolesListRequest, resp *pb.RolesListResponse) error
func (c *Client) RolesDelete(ctx context.Context, req *pb.RolesDeleteRequest) error
func (c *Client) RoleBindingsCreate(ctx context.Context, req *pb.RoleBindingCreateRequest) error
func (c *Client) RoleBindingsList(ctx context.Context, req *pb.RoleBindingsListRequest, resp *pb.RoleBindingsListResponse) error
func (c *Client) RoleBindingsDelete(ctx context.Context, req *pb.RoleBindingDeleteRequest) error
```

---

#### 20. Update OpenAPI Specification

**File**: `openapi.yaml`
**Changes**: Add definitions for all new endpoints and schemas

- Namespace endpoints and schemas
- User and API Key endpoints and schemas
- Role and RoleBinding endpoints and schemas
- Update existing queue endpoints to include `namespace` parameter
- Add `Authorization` header to security schemes
- Document 401 and 403 error responses

---

#### 21. Update Documentation

**File**: `docs/authentication.md` (new file)
**Changes**: Document the authentication and authorization model

**Content:**
- API key format and generation
- Bearer token authentication
- Open Door policy explanation
- Lock-down procedure
- Standard roles and their permissions
- Cascading permission checks
- Namespace-scoped keys

---

#### 22. Update Test Setup

**File**: `service/common_test.go`
**Changes**: Update test setup to include roles storage and auth backend

**Function responsibilities:**
- Update storage setup to include Roles and RoleBindings
- Create helper to create authenticated client: `newClientWithKey(t, address, apiKey)`

---

### Testing Requirements

**File**: `service/auth_test.go` (new file)

```go
func TestAuth(t *testing.T)
func testAuth(t *testing.T, setup NewStorageFunc, tearDown func())
```

**Test objectives:**
- Role CRUD operations
- RoleBinding CRUD operations
- Cascading permission checks
- API key scope enforcement
- Anonymous default Admin access (Open Door)
- Lock-down workflow

**Key scenarios:**

*Role CRUD Operations:*
- `RoleCRUD`: Create/list/delete custom roles
- `RoleUpdate`: Update permissions of a custom role
- `RoleListPagination`: Verify pagination via `pivot` and `limit` works correctly
- `RoleListByNamespace`: List roles filtered by namespace

*Role Validation:*
- `DuplicateRoleNameRejected`: Same role name in same namespace returns error
- `RoleNameEmptyRejected`: Empty role name returns validation error
- `RoleInvalidPermissionsRejected`: Role with unknown permission strings rejected
- `RoleEmptyPermissionsAllowed`: Document whether role with empty permissions array is valid
- `RoleNamespaceRequired`: Role creation without namespace returns error

*Standard Role Protection:*
- `StandardRolesExistAfterBootstrap`: Admin, NamespaceOwner, PublicViewer roles exist in `_system`
- `StandardRoleImmutable`: Cannot delete Admin, NamespaceOwner, PublicViewer roles
- `RoleUpdateStandardRoleBlocked`: Attempt to update Admin/NamespaceOwner/PublicViewer fails
- `StandardRoleModificationBlocked`: Cannot update/modify standard roles via API
- `StandardRoleNameCreationBlocked`: Creating custom role named "Admin"/"NamespaceOwner"/"PublicViewer" rejected
- `StandardRoleAutoHeal`: Restarting service restores missing permissions to standard roles
- `RoleDeleteRestricted`: Cannot delete role with active bindings

*RoleBinding CRUD Operations:*
- `RoleBindingCRUD`: Create/list/delete role bindings
- `RoleBindingListPagination`: Verify pagination via `pivot` and `limit` works correctly
- `RoleBindingListByUser`: List bindings filtered by `user_id_filter`
- `RoleBindingListByNamespace`: List bindings filtered by namespace

*RoleBinding Validation:*
- `RoleBindingNonExistentUser`: Binding to non-existent `user_id` returns error
- `RoleBindingNonExistentRole`: Binding to non-existent `role_name` returns error
- `DuplicateRoleBindingRejected`: Same user+role+namespace binding twice returns error
- `RoleBindingNamespaceRequired`: RoleBinding creation without namespace returns error

*Bootstrap State:*
- `AnonymousAdminBindingExistsAfterBootstrap`: Anonymous user is bound to Admin role in `_system`
- `AnonymousHasAdminByDefault`: Anonymous can create namespaces and queues (Open Door)

*Cascading Authorization (AUTH-004):*
- `CascadeCheckStepByStep`: Explicit test of 3-step cascade order:
  1. Key Scope Check (if scoped and mismatch → 403 immediately)
  2. Target Namespace Check (user has role in target NS)
  3. System Namespace Check (user has role in `_system`)
- `CascadingPermissions`: User with Admin in `_system` can access any namespace
- `NamespaceScopedPermissions`: User with NamespaceOwner in ns-A can only access ns-A, gets 403 for ns-B
- `UserWithNoBindingsGets403`: User exists but has no role bindings → 403 Forbidden on any operation

*API Key Scope Enforcement (Section 9.7):*
- `ScopedKeyRestriction`: Key scoped to ns-A cannot access ns-B even if user has Admin in `_system`
- `ScopeImmutability`: User creates key scoped to ns-A, later granted ns-B access, key still cannot access ns-B
- `ScopedKeyNamespaceDeleted`: API Key scoped to deleted namespace returns 401 on subsequent requests
- `UnscopedKeyInheritsAllAccess`: Unscoped key inherits user's full permissions across all namespaces

*Security & Scoping Edge Cases (Critical):*
- `ScopedKeyPrivilegeEscalation`: Admin-owned key scoped to `production` namespace cannot perform system-level operations (UsersCreate, NamespacesCreate, RolesCreate), even if User is Admin
- `SystemScopedKeyIsolation`: Admin-owned key scoped to `_system` namespace cannot access resources in other namespaces (e.g., QueueProduce in `production`), even if User is Admin
- `NamespaceScopedRoleVisibility`: Verify RoleBinding in namespace A does not grant list/view access to Roles or Bindings in namespace B

*Lock-Down Workflow (Section 9.5):*
- `LockDownWorkflow`: After removing anonymous-admin binding, anonymous gets 403
- `LockDownWithPublicViewer`: After lock-down, bind Anonymous to PublicViewer, verify limited access (health only)
- `AnonymousAfterLockdownHealthOnly`: Anonymous with PublicViewer can call health but not queue operations

*Permission Enforcement:*
- `PermissionEnforcement`: User without required permission gets 403 Forbidden
- `PublicViewerCapabilities`: PublicViewer role allows health/metrics but denies queue operations
- `MetricsEndpointPublicViewer`: Verify metrics endpoint accessible with PublicViewer
- `HealthEndpointNoAuth`: Verify health endpoint behavior for anonymous users (based on bindings)

*Namespace Deletion with Auth Resources:*
- `NamespaceDeleteBlockedByRoles`: Cannot delete namespace if it contains custom Roles
- `NamespaceDeleteBlockedByRoleBindings`: Cannot delete namespace if it contains RoleBindings
- `NamespaceDeleteAfterAuthCleanup`: Can delete namespace after removing all Roles and RoleBindings

*Cross-Cutting Authorization Scenarios:*
- `QueueProduceRequiresPermission`: User needs `queue.produce` permission
- `QueueLeaseRequiresPermission`: User needs `queue.lease` permission
- `QueueCompleteRequiresPermission`: User needs `queue.complete` permission
- `NamespaceCreateRequiresSystemPermission`: Namespace creation requires permission in `_system`
- `UserCreateRequiresSystemPermission`: User creation requires permission in `_system`

**Testing patterns:**
- Tests MUST be in `service_test` package (external test package)
- Use `require` for critical assertions that should halt test, `assert` for non-critical
- No descriptive messages in assertions (use comments instead)
- All tests interact through HTTP client, never call internal functions

**Context for implementation:**
- Follow existing `testQueues()` structure in `service/queue_test.go`
- Use `newClientWithKey()` helper for authenticated requests
- Test scenarios should exercise full authorization flow through public API

---

### Validation

- [ ] Run: `make proto`
- [ ] Run: `go build ./...`
- [ ] Run: `go test ./...` (all tests pass, covers all key scenarios above)
- [ ] Verify: No tests call internal/unexported functions
- [ ] Verify: OpenAPI spec is valid (use online validator or `swagger-cli validate openapi.yaml`)

---

## Summary of Files Changed/Created

### New Files
- `internal/types/namespace.go`
- `internal/types/auth.go`
- `internal/types/errors.go`
- `internal/context.go`
- `internal/store/namespace.go`
- `internal/store/users.go`
- `internal/store/apikeys.go`
- `internal/store/roles.go`
- `internal/auth/apikey.go`
- `internal/auth/cache.go`
- `internal/auth/permissions.go`
- `internal/auth/backend.go`
- `proto/namespaces.proto`
- `proto/users.proto`
- `proto/apikeys.proto`
- `proto/roles.proto`
- `service/namespace_test.go`
- `service/users_test.go`
- `service/auth_test.go`
- `docs/authentication.md`

### Modified Files
- `internal/types/items.go` - Add Namespace to QueueInfo
- `internal/store/store.go` - Add interfaces to Config
- `internal/store/memory.go` - Add all implementations
- `internal/store/badger.go` - Add all implementations
- `internal/store/postgres.go` - Add all implementations
- `internal/queues_manager.go` - Update for namespace-aware queue keys
- `proto/queue.proto` - Add namespace field to all request messages
- `proto/queues.proto` - Add namespace field to request messages and QueueInfo
- `proto/storage.proto` - Add namespace field to request messages
- `transport/errors.go` - Add ErrUnauthorized and ErrForbidden
- `transport/interfaces.go` - Add admin interfaces
- `transport/http.go` - Add middleware, endpoints, and auth integration
- `service/service.go` - Add methods, authorization, and bootstrap
- `service/validation.go` - Add validation functions
- `service/common_test.go` - Update test setup
- `client.go` - Add client methods and auth header support
- `openapi.yaml` - Add all new endpoints and schemas
