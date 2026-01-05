# Multi-Tenancy and Authentication Implementation Plan v2

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

```go
func (s *Service) UsersDelete(ctx context.Context, req *proto.UsersDeleteRequest) error {
    // Delete all API keys for this user first
    var keys []types.APIKey
    if err := s.conf.StorageConfig.APIKeys.ListByUser(ctx, req.Id, &keys, types.ListOptions{}); err != nil {
        return err
    }
    for _, key := range keys {
        if err := s.conf.StorageConfig.APIKeys.Delete(ctx, key.ID); err != nil {
            return err
        }
    }
    // Then delete the user
    return s.conf.StorageConfig.Users.Delete(ctx, req.Id)
}
```

### Role Deletion Behavior: RESTRICT
Roles cannot be deleted if they have active bindings.

### Standard Roles: Immutable
Standard roles (Admin, NamespaceOwner, PublicViewer) cannot be modified or deleted by users.

### Anonymous User ID: String Literal
The Anonymous user uses the literal string `"anonymous"` for both ID and Username.

### UpdateLastUsed: Async Goroutine
API key last-used tracking is fire-and-forget in a goroutine. Errors are logged at WARN level.

```go
func (a *DefaultAuthBackend) Authenticate(ctx context.Context, token string) (types.Principal, error) {
    // ... authentication logic ...

    // Update last used time asynchronously
    go func() {
        ctx, cancel := context.WithTimeout(context.Background(), a.writeTimeout)
        defer cancel()

        if err := a.apiKeys.UpdateLastUsed(ctx, key.ID, time.Now().UTC()); err != nil {
            a.log.Warn("failed to update API key last used time",
                "key_id", key.ID,
                "error", err)
        }
    }()

    return principal, nil
}
```

### Migration: Not Required
Querator is alpha-quality software with no deployed instances.

### Auth Caching: Read-Through + Write-Throttling
API keys are cached in-memory (TTL 5m). LastUsedAt updates are throttled (max 1/minute per key).

---

## Data Flow Architecture

### Request Flow (After Full Implementation)

```
HTTP Request
    │
    ▼
┌─────────────────────────────────────────┐
│ transport/http.go ServeHTTP()           │
│ 1. extractPrincipal(r) → Principal      │
│ 2. ctx = ContextWithPrincipal(ctx, p)   │
│ 3. Route to handler                     │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│ service/service.go MethodName()         │
│ 1. Extract namespace from request       │
│ 2. s.authorize(ctx, namespace, perm)    │
│ 3. Business logic                       │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│ internal/queues_manager.go              │
│ Queue operations are namespace-scoped   │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│ internal/store/*.go                     │
│ Namespace prefix in storage keys        │
└─────────────────────────────────────────┘
```

### Queue Name Uniqueness
Queue names are unique **per namespace** (not globally). This allows different namespaces to have queues with the same name.

---

## HTTP Status Code Mapping

| Error Type | HTTP Status | When to Use |
|------------|-------------|-------------|
| `transport.ErrInvalidOption` | 400 Bad Request | Invalid input, validation failure |
| `transport.ErrRequestFailed` | 400 Bad Request | Operation failed (resource not found, etc.) |
| `transport.ErrUnauthorized` | 401 Unauthorized | Missing/invalid/expired API key |
| `transport.ErrForbidden` | 403 Forbidden | Valid auth but insufficient permissions |
| `transport.ErrConflict` | 400 Bad Request | Conflict (currently maps to 400) |
| `transport.ErrRetryRequest` | 454 Retry Request | Temporary failure, retry |

---

## store.Config Evolution Across Phases

```go
// Phase 1 additions
type Config struct {
    Queues           Queues
    Namespaces       Namespaces       // NEW Phase 1
    PartitionStorage []PartitionStorage
}

// Phase 2 additions
type Config struct {
    Queues           Queues
    Namespaces       Namespaces
    Users            Users            // NEW Phase 2
    APIKeys          APIKeys          // NEW Phase 2
    PartitionStorage []PartitionStorage
}

// Phase 3 additions (final)
type Config struct {
    Queues           Queues
    Namespaces       Namespaces
    Users            Users
    APIKeys          APIKeys
    Roles            Roles            // NEW Phase 3
    RoleBindings     RoleBindings     // NEW Phase 3
    PartitionStorage []PartitionStorage
}
```

---

## Complete Permission Mapping by Endpoint

| Endpoint | Permission | Target Namespace |
|----------|------------|------------------|
| `/v1/queue.produce` | `queue.produce` | `req.Namespace` |
| `/v1/queue.lease` | `queue.lease` | `req.Namespace` |
| `/v1/queue.complete` | `queue.complete` | `req.Namespace` |
| `/v1/queue.retry` | `queue.retry` | `req.Namespace` |
| `/v1/queue.clear` | `queue.clear` | `req.Namespace` |
| `/v1/queue.reload` | `queue.clear` | `req.Namespace` |
| `/v1/queue.stats` | `queue.stats` | `req.Namespace` |
| `/v1/queues.create` | `queue.create` | `req.Namespace` |
| `/v1/queues.update` | `queue.update` | `req.Namespace` |
| `/v1/queues.delete` | `queue.delete` | `req.Namespace` |
| `/v1/queues.list` | `queue.list` | `req.Namespace` (or `_system` if empty) |
| `/v1/queues.info` | `queue.list` | `req.Namespace` |
| `/v1/storage.items.list` | `queue.list` | `req.Namespace` |
| `/v1/storage.items.import` | `queue.create` | `req.Namespace` |
| `/v1/storage.items.delete` | `queue.delete` | `req.Namespace` |
| `/v1/storage.scheduled.list` | `queue.list` | `req.Namespace` |
| `/v1/namespaces.create` | `namespace.create` | `_system` |
| `/v1/namespaces.delete` | `namespace.delete` | `_system` |
| `/v1/namespaces.list` | `namespace.list` | `_system` |
| `/v1/users.create` | `user.create` | `_system` |
| `/v1/users.delete` | `user.delete` | `_system` |
| `/v1/users.list` | `user.list` | `_system` |
| `/v1/api-keys.create` | `apikey.create` | `_system` |
| `/v1/api-keys.delete` | `apikey.delete` | `_system` |
| `/v1/api-keys.list` | `apikey.list` | `_system` |
| `/v1/roles.create` | `role.create` | `req.Namespace` |
| `/v1/roles.delete` | `role.delete` | `req.Namespace` |
| `/v1/roles.list` | `role.list` | `req.Namespace` |
| `/v1/role-bindings.create` | `rolebinding.create` | `req.Namespace` |
| `/v1/role-bindings.delete` | `rolebinding.delete` | `req.Namespace` |
| `/v1/role-bindings.list` | `rolebinding.list` | `req.Namespace` |
| `/health` | `system.health` | `_system` (or no auth) |
| `/metrics` | `system.metrics` | `_system` (or no auth) |

**Note**: `/health` and `/metrics` bypass normal routing. Recommendation: Keep unauthenticated by default.

---

# Phase 1: Namespace Foundation + Minimal Auth

## Overview
Establish namespace isolation for queues. After this phase:
- Namespaces can be created, listed, and deleted
- All queue operations require a namespace
- Queues are isolated by namespace
- `_system` namespace is bootstrapped automatically
- Anonymous principal is injected into context (no auth enforcement yet)

## Task Checklist

### 1. Error Types

#### 1.1 Transport Error Types
- [ ] Add `ErrUnauthorized` type to `transport/errors.go` (implements `duh.Error`, returns 401)
- [ ] Add `ErrForbidden` type to `transport/errors.go` (implements `duh.Error`, returns 403)
- [ ] Add `NewUnauthorized(msg string, args ...any)` constructor
- [ ] Add `NewForbidden(msg string, args ...any)` constructor

**Implementation Pattern** (both error types follow this pattern):
```go
type ErrUnauthorized struct {
    msg string
}

func NewUnauthorized(msg string, args ...any) *ErrUnauthorized {
    return &ErrUnauthorized{msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrUnauthorized) Error() string { return e.msg }

func (e *ErrUnauthorized) Is(target error) bool {
    var err *ErrUnauthorized
    return errors.As(target, &err)
}

func (e *ErrUnauthorized) Code() int { return duh.CodeUnauthorized }

func (e *ErrUnauthorized) ProtoMessage() proto.Message {
    return &v1.Reply{
        Message:  e.msg,
        CodeText: duh.CodeText(duh.CodeUnauthorized),
        Code:     int32(duh.CodeUnauthorized),
        Details:  nil,
    }
}

func (e *ErrUnauthorized) Details() map[string]string { return nil }
func (e *ErrUnauthorized) Message() string { return e.msg }

var _ duh.Error = &ErrUnauthorized{}
```
Use `duh.CodeForbidden` for `ErrForbidden`.

#### 1.2 Domain Error Variables
- [ ] Create `internal/types/errors.go`
- [ ] Add `ErrNamespaceNotExist = transport.NewRequestFailed("namespace does not exist")`
- [ ] Add `ErrNamespaceAlreadyExists = transport.NewInvalidOption("namespace already exists")`
- [ ] Add `ErrNamespaceHasQueues = transport.NewInvalidOption("cannot delete namespace: contains queues")`
- [ ] Add `ErrNamespaceReserved = transport.NewInvalidOption("namespace name is reserved")`
- [ ] Add `ErrAuthRequired = transport.NewUnauthorized("authentication required")`
- [ ] Add `ErrAccessDenied = transport.NewForbidden("access denied: insufficient permissions")`

### 2. Types

#### 2.1 Namespace Type
- [ ] Create `internal/types/namespace.go`
- [ ] Add `Namespace` struct with fields: `Name`, `Description`, `CreatedAt`
- [ ] Add `IsReserved() bool` method (returns true if name starts with `_`)
- [ ] Add `ToProto() *proto.NamespaceInfo` method

#### 2.2 Auth Types (Foundation for Phase 2-3)
- [ ] Create `internal/types/auth.go`
- [ ] Add `User` struct with fields: `ID`, `Username`, `ExternalID`, `Email`, `CreatedAt`, `UpdatedAt`
- [ ] Add `Principal` struct with fields: `User`, `NamespaceScope *string`, `IsAnonymous bool`
- [ ] Add `AnonymousUser` variable: `User{ID: "anonymous", Username: "anonymous"}`
- [ ] Add `AnonymousPrincipal` variable: `Principal{User: AnonymousUser, IsAnonymous: true}`

#### 2.3 Context Key
- [ ] Create `internal/context.go`
- [ ] Add `principalKey` context key type
- [ ] Add `PrincipalFromContext(ctx) Principal` (returns `AnonymousPrincipal` if not set)
- [ ] Add `ContextWithPrincipal(ctx, Principal) context.Context`

### 3. Proto Definitions

#### 3.1 Namespace Proto
- [ ] Create `proto/namespaces.proto`
- [ ] Add `NamespaceInfo` message with fields: `name`, `description`, `created_at`
- [ ] Add `NamespaceCreateRequest` message with fields: `name`, `description`
- [ ] Add `NamespacesListRequest` message with fields: `limit`, `pivot`
- [ ] Add `NamespacesListResponse` message with field: `items`
- [ ] Add `NamespacesDeleteRequest` message with field: `name`
- [ ] Run `make proto` to generate Go code

#### 3.2 Add Namespace to Queue Proto Messages
- [ ] Add `string namespace = 4` to `QueueProduceRequest` in `proto/queue.proto`
- [ ] Add `string namespace = 5` to `QueueLeaseRequest` in `proto/queue.proto`
- [ ] Add `string namespace = 5` to `QueueCompleteRequest` in `proto/queue.proto`
- [ ] Add `string namespace = 4` to `QueueRetryRequest` in `proto/queue.proto`
- [ ] Add `string namespace = 6` to `QueueClearRequest` in `proto/queue.proto`
- [ ] Add `string namespace = 2` to `QueueReloadRequest` in `proto/queue.proto`
- [ ] Add `string namespace = 2` to `QueueStatsRequest` in `proto/queue.proto`
- [ ] Add `string namespace = 11` to `QueueInfo` in `proto/queue.proto`

#### 3.3 Add Namespace to Queues Proto Messages
- [ ] Add `string namespace = 4` to `QueuesListRequest` in `proto/queues.proto`
- [ ] Add `string namespace = 3` to `QueuesDeleteRequest` in `proto/queues.proto`
- [ ] Add `string namespace = 2` to `QueuesInfoRequest` in `proto/queues.proto`

#### 3.4 Add Namespace to Storage Proto Messages
- [ ] Add `string namespace = 5` to `StorageItemsListRequest` in `proto/storage.proto`
- [ ] Add `string namespace = 4` to `StorageItemsImportRequest` in `proto/storage.proto`
- [ ] Add `string namespace = 4` to `StorageItemsDeleteRequest` in `proto/storage.proto`
- [ ] Run `make proto` to regenerate all Go code

### 4. Storage Layer

#### 4.1 Namespace Storage Interface
- [ ] Create `internal/store/namespace.go`
- [ ] Add `Namespaces` interface with methods: `Get`, `Add`, `List`, `Delete`, `Close`

#### 4.2 Update Store Config
- [ ] Add `Namespaces Namespaces` field to `Config` struct in `internal/store/store.go`

#### 4.3 InMemory Namespace Storage
- [ ] Add `MemoryNamespaces` struct to `internal/store/memory.go`
- [ ] Implement `NewMemoryNamespaces(log *slog.Logger) *MemoryNamespaces`
- [ ] Implement `Get(ctx, name, *Namespace) error`
- [ ] Implement `Add(ctx, Namespace) error`
- [ ] Implement `List(ctx, *[]Namespace, ListOptions) error`
- [ ] Implement `Delete(ctx, name) error`
- [ ] Implement `Close(ctx) error`

#### 4.4 BadgerDB Namespace Storage
- [ ] Add `BadgerNamespaces` struct to `internal/store/badger.go`
- [ ] Implement `NewBadgerNamespaces(conf BadgerConfig) *BadgerNamespaces`
- [ ] Implement all interface methods (use key prefix `ns:`)

#### 4.5 PostgreSQL Namespace Storage
- [ ] Add `PostgresNamespaces` struct to `internal/store/postgres.go`
- [ ] Implement `NewPostgresNamespaces(conf PostgresConfig) *PostgresNamespaces`
- [ ] Implement all interface methods
- [ ] Create table: `namespaces (name TEXT PRIMARY KEY, description TEXT, created_at TIMESTAMPTZ)`

### 5. QueueInfo Namespace Field

#### 5.1 Update QueueInfo Type
- [ ] Add `Namespace string` field to `QueueInfo` struct in `internal/types/items.go`
- [ ] Update `ToProto()` method to include `Namespace`
- [ ] Update `FromProto()` or equivalent to extract `Namespace`

#### 5.2 Update QueuesManager
- [ ] Update `QueuesManager.Get()` signature to accept `namespace, queueName` in `internal/queues_manager.go`
- [ ] Update queue map key from `queueName` to `namespace/queueName`
- [ ] Add helper function `queueKey(namespace, name string) string`
- [ ] Update `QueuesManager.Create()` to validate namespace is set
- [ ] Update all callers of `QueuesManager.Get()` to pass namespace

**Queue Key Implementation:**
```go
// internal/queues_manager.go
type QueuesManager struct {
    queues map[string]*Queue  // Key changes from "queueName" to "namespace/queueName"
    // ...
}

func queueKey(namespace, name string) string {
    return namespace + "/" + name
}

func (qm *QueuesManager) Get(ctx context.Context, namespace, queueName string) (*Queue, error) {
    key := queueKey(namespace, queueName)
    q, ok := qm.queues[key]
    if !ok {
        // Load from storage...
    }
    return q, nil
}
```

**Storage Key Format per Backend:**

| Backend | Queue Key Format |
|---------|------------------|
| InMemory | Map key: `namespace/queueName` |
| BadgerDB | Key prefix: `queue:{namespace}:{queueName}` |
| PostgreSQL | WHERE clause: `namespace = $1 AND name = $2` |

### 6. Service Layer

#### 6.1 Validation Functions
- [ ] Add `validateNamespaceName(name string) error` to `service/validation.go`
- [ ] Add `validateNamespaceCreate(req *proto.NamespaceCreateRequest) error` to `service/validation.go`

**Namespace Validation Rules:**
- Name is not empty
- Name is not reserved (starts with `_`) unless creating `_system`
- Name contains no whitespace
- Max length 500 characters

#### 6.2 Namespace Service Methods
- [ ] Add `NamespacesCreate(ctx, *proto.NamespaceCreateRequest) error` to `service/service.go`
- [ ] Add `NamespacesList(ctx, *proto.NamespacesListRequest, *proto.NamespacesListResponse) error` to `service/service.go`
- [ ] Add `NamespacesDelete(ctx, *proto.NamespacesDeleteRequest) error` to `service/service.go`

**NamespacesDelete Implementation:**
```go
func (s *Service) NamespacesDelete(ctx context.Context, req *proto.NamespacesDeleteRequest) error {
    // Prevent deletion of reserved namespaces
    if strings.HasPrefix(req.Name, "_") {
        return types.ErrNamespaceReserved
    }

    // Check for queues in this namespace
    var queues []types.QueueInfo
    if err := s.conf.StorageConfig.Queues.List(ctx, &queues, types.ListOptions{}); err != nil {
        return err
    }

    for _, q := range queues {
        if q.Namespace == req.Name {
            return transport.NewInvalidOption("cannot delete namespace; namespace '%s' contains queue '%s'", req.Name, q.Name)
        }
    }

    // Delete the namespace
    return s.conf.StorageConfig.Namespaces.Delete(ctx, req.Name)
}
```

#### 6.3 Bootstrap Function
- [ ] Add `bootstrapState atomic.Int32` field to `Service` struct
- [ ] Add bootstrap state constants: `stateIdle = 0`, `stateBootstrapping = 1`, `stateReady = 2`
- [ ] Add `Bootstrap(ctx) error` method to `Service` in `service/service.go`
- [ ] Add `ensureBootstrapped(ctx) error` method using atomic state machine
- [ ] Implement `bootstrapSystemNamespace(ctx) error` helper
- [ ] Bootstrap creates `_system` namespace if not exists
- [ ] Bootstrap is idempotent (ignores "already exists" errors)
- [ ] Start bootstrap in background goroutine during `Service.New()`

**Bootstrap State Machine Implementation:**
```go
type Service struct {
    queues         *internal.QueuesManager
    auth           auth.AuthBackend
    conf           Config
    bootstrapState atomic.Int32  // 0=Idle, 1=Bootstrapping, 2=Ready
}

const (
    stateIdle          = 0
    stateBootstrapping = 1
    stateReady         = 2
)

// ensureBootstrapped ensures the system is initialized using an atomic state machine.
func (s *Service) ensureBootstrapped(ctx context.Context) error {
    // Fast path: check if already ready
    if s.bootstrapState.Load() == stateReady {
        return nil
    }

    // Attempt to transition from Idle -> Bootstrapping
    if s.bootstrapState.CompareAndSwap(stateIdle, stateBootstrapping) {
        // We won the race, perform bootstrap
        if err := s.Bootstrap(ctx); err != nil {
            // Reset state to Idle on failure so we can retry
            s.bootstrapState.Store(stateIdle)
            return err
        }
        // Success
        s.bootstrapState.Store(stateReady)
        return nil
    }

    // If we get here, we are either:
    // 1. In stateBootstrapping (another goroutine is doing it)
    // 2. In stateReady (finished just after our first check)

    if s.bootstrapState.Load() == stateReady {
        return nil
    }

    // Still bootstrapping - tell client to retry
    return transport.NewRetryRequest("system is starting up")
}
```

#### 6.4 Update Queue Service Methods
- [ ] Update `QueuesCreate()` to validate namespace exists before creating queue
- [ ] Update `QueuesCreate()` to require non-empty `Namespace` field
- [ ] Update `QueueProduce()` to extract namespace from request
- [ ] Update `QueueLease()` to extract namespace from request
- [ ] Update `QueueComplete()` to extract namespace from request
- [ ] Update `QueueRetry()` to extract namespace from request
- [ ] Update `QueueClear()` to extract namespace from request
- [ ] Update `QueueReload()` to extract namespace from request
- [ ] Update `QueueStats()` to extract namespace from request
- [ ] Update `QueuesList()` to filter by namespace
- [ ] Update `QueuesDelete()` to extract namespace from request
- [ ] Update `QueuesInfo()` to extract namespace from request
- [ ] Update `StorageItemsList()` to extract namespace from request
- [ ] Update `StorageItemsImport()` to extract namespace from request
- [ ] Update `StorageItemsDelete()` to extract namespace from request

### 7. Transport Layer

#### 7.1 Transport Interfaces
- [ ] Add `NamespaceAdmin` interface to `transport/interfaces.go`
- [ ] Add `NamespacesCreate`, `NamespacesList`, `NamespacesDelete` to interface
- [ ] Update `Service` interface to embed `NamespaceAdmin`

#### 7.2 HTTP Handler - Principal Extraction
- [ ] Add `extractPrincipal(r *http.Request) types.Principal` to `transport/http.go`
- [ ] Phase 1 implementation: always return `types.AnonymousPrincipal`
- [ ] Update `ServeHTTP()` to call `extractPrincipal()` and inject into context

#### 7.3 HTTP Endpoints
- [ ] Add route constant `RPCNamespacesCreate = "/v1/namespaces.create"`
- [ ] Add route constant `RPCNamespacesList = "/v1/namespaces.list"`
- [ ] Add route constant `RPCNamespacesDelete = "/v1/namespaces.delete"`
- [ ] Add `NamespacesCreate(ctx, w, r)` handler method
- [ ] Add `NamespacesList(ctx, w, r)` handler method
- [ ] Add `NamespacesDelete(ctx, w, r)` handler method
- [ ] Add cases to `ServeHTTP()` switch for namespace routes

### 8. Client

#### 8.1 Namespace Client Methods
- [ ] Add `NamespacesCreate(ctx, *proto.NamespaceCreateRequest) error` to `client.go`
- [ ] Add `NamespacesList(ctx, *proto.NamespacesListResponse, *ListOptions) error` to `client.go`
- [ ] Add `NamespacesDelete(ctx, *proto.NamespacesDeleteRequest) error` to `client.go`

### 9. Test Setup Updates

#### 9.1 Update Test Infrastructure
- [ ] Update `setupMemoryStorage()` in `service/common_test.go` to include `Namespaces`
- [ ] Update `badgerTestSetup.Setup()` to include `Namespaces`
- [ ] Update `postgresTestSetup.Setup()` to include `Namespaces`

### 10. Integration

#### 10.1 Wire Bootstrap
- [ ] Call `s.Bootstrap(ctx)` during `Service.New()` initialization (in background goroutine)

#### 10.2 Wire Principal to Context
- [ ] Ensure `ServeHTTP()` calls `internal.ContextWithPrincipal(ctx, principal)` before routing

## Tests

### 10.1 Namespace Tests
- [ ] Create `service/namespace_test.go`
- [ ] Test: `CreateNamespace` - create namespace, verify in list
- [ ] Test: `CreateQueueRequiresNamespace` - queue creation requires valid namespace
- [ ] Test: `SystemNamespaceBootstrapped` - `_system` exists after daemon start
- [ ] Test Error: `ReservedPrefix` - creating `_reserved` returns error
- [ ] Test Error: `DuplicateName` - creating same namespace twice returns error
- [ ] Test Error: `HasQueues` - deleting namespace with queues returns error
- [ ] Test Error: `NamespaceNotExist` - creating queue in nonexistent namespace returns error

## Verification Checklist

- [ ] `go build ./...` passes
- [ ] `go vet ./...` passes
- [ ] `go test ./service/... -run "TestNamespaces"` passes for InMemory
- [ ] `go test ./service/... -run "TestNamespaces"` passes for BadgerDB
- [ ] `go test ./service/... -run "TestQueue"` passes (existing tests still work)
- [ ] Creating a queue without namespace field returns error containing "namespace"
- [ ] Creating a queue with non-existent namespace returns "namespace does not exist"
- [ ] `_system` namespace exists after server startup
- [ ] Cannot create namespace starting with `_` (reserved)
- [ ] Cannot delete namespace that contains queues

---

# Phase 2: Users + API Keys

## Overview
Implement user management and API key authentication. After this phase:
- Users can be created, listed, and deleted
- API keys can be generated for users
- Requests with `Authorization: Bearer <key>` are authenticated
- Anonymous requests still work (return AnonymousPrincipal)
- Anonymous user is bootstrapped automatically

## Task Checklist

### 1. Error Types

#### 1.1 Domain Error Variables
- [ ] Add to `internal/types/errors.go`:
  - [ ] `ErrUserNotExist = transport.NewRequestFailed("user does not exist")`
  - [ ] `ErrUserAlreadyExists = transport.NewInvalidOption("user already exists")`
  - [ ] `ErrUsernameAlreadyTaken = transport.NewInvalidOption("username already taken")`
  - [ ] `ErrAPIKeyNotExist = transport.NewUnauthorized("API key does not exist")`
  - [ ] `ErrAPIKeyExpired = transport.NewUnauthorized("API key has expired")`
  - [ ] `ErrAPIKeyInvalid = transport.NewUnauthorized("invalid API key")`

### 2. Types

#### 2.1 API Key Type
- [ ] Add `APIKey` struct to `internal/types/auth.go` with fields:
  - [ ] `ID string`
  - [ ] `UserID string`
  - [ ] `NamespaceScope *string`
  - [ ] `Name string`
  - [ ] `KeyHash string` (SHA-256)
  - [ ] `KeyPrefix string` (first 8 chars)
  - [ ] `ExpiresAt *clock.Time`
  - [ ] `LastUsedAt *clock.Time`
  - [ ] `CreatedAt clock.Time`
- [ ] Add `ToProto() *proto.APIKeyMetadata` method to `APIKey`
- [ ] Add `ToProto() *proto.User` method to `User`

### 3. Proto Definitions

#### 3.1 Users Proto
- [ ] Create `proto/users.proto`
- [ ] Add `User` message
- [ ] Add `UserCreateRequest` message
- [ ] Add `UserCreateResponse` message (contains generated ID)
- [ ] Add `UsersListRequest` message
- [ ] Add `UsersListResponse` message
- [ ] Add `UsersDeleteRequest` message

#### 3.2 API Keys Proto
- [ ] Create `proto/apikeys.proto`
- [ ] Add `APIKeyMetadata` message (no hash/raw key)
- [ ] Add `APIKeyCreateRequest` message with `env_tag` field (defaults to "live")
- [ ] Add `APIKeyCreateResponse` message (contains raw key - shown only once)
- [ ] Add `APIKeysListRequest` message
- [ ] Add `APIKeysListResponse` message
- [ ] Add `APIKeysDeleteRequest` message
- [ ] Run `make proto` to generate Go code

**APIKeyCreateRequest Proto:**
```protobuf
message APIKeyCreateRequest {
    string user_id = 1;
    string name = 2;
    string namespace_scope = 3;
    google.protobuf.Timestamp expires_at = 4;
    string env_tag = 5;  // defaults to "live"
}
```

### 4. Storage Layer

#### 4.1 Users Storage Interface
- [ ] Create `internal/store/users.go`
- [ ] Add `Users` interface with methods: `Get`, `GetByUsername`, `Add`, `List`, `Delete`, `Close`

#### 4.2 API Keys Storage Interface
- [ ] Create `internal/store/apikeys.go`
- [ ] Add `APIKeys` interface with methods: `Get`, `GetByHash`, `Add`, `List`, `ListByUser`, `Delete`, `UpdateLastUsed`, `Close`

#### 4.3 Update Store Config
- [ ] Add `Users Users` field to `Config` struct in `internal/store/store.go`
- [ ] Add `APIKeys APIKeys` field to `Config` struct in `internal/store/store.go`

#### 4.4 InMemory Users Storage
- [ ] Add `MemoryUsers` struct to `internal/store/memory.go`
- [ ] Implement all `Users` interface methods

#### 4.5 InMemory API Keys Storage
- [ ] Add `MemoryAPIKeys` struct to `internal/store/memory.go`
- [ ] Implement all `APIKeys` interface methods

#### 4.6 BadgerDB Users Storage
- [ ] Add `BadgerUsers` struct to `internal/store/badger.go`
- [ ] Implement all `Users` interface methods (use key prefix `user:`)

#### 4.7 BadgerDB API Keys Storage
- [ ] Add `BadgerAPIKeys` struct to `internal/store/badger.go`
- [ ] Implement all `APIKeys` interface methods (use key prefix `apikey:`)

#### 4.8 PostgreSQL Users Storage
- [ ] Add `PostgresUsers` struct to `internal/store/postgres.go`
- [ ] Implement all `Users` interface methods
- [ ] Create table: `users (id TEXT PRIMARY KEY, username TEXT UNIQUE, external_id TEXT, email TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)`

#### 4.9 PostgreSQL API Keys Storage
- [ ] Add `PostgresAPIKeys` struct to `internal/store/postgres.go`
- [ ] Implement all `APIKeys` interface methods
- [ ] Create table: `api_keys (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users(id), namespace_scope TEXT, name TEXT, key_hash TEXT UNIQUE, key_prefix TEXT, expires_at TIMESTAMPTZ, last_used_at TIMESTAMPTZ, created_at TIMESTAMPTZ)`
- [ ] Add index on `key_hash` for fast authentication lookups

### 5. Auth Package

#### 5.1 API Key Utilities
- [ ] Create `internal/auth/apikey.go`
- [ ] Implement `GenerateAPIKey(env string) (rawKey, hash, prefix string)`
- [ ] Implement `HashAPIKey(rawKey string) string` (SHA-256)
- [ ] Implement `ValidateAPIKeyFormat(key string) error` (regex: `^sk-[a-z0-9]+-[a-zA-Z0-9]{32,}$`)
- [ ] Implement `ExtractKeyPrefix(rawKey string) string` (first 8 chars after `sk-env-`)

#### 5.2 Auth Cache
- [ ] Create `internal/auth/cache.go`
- [ ] Add `CacheEntry` struct with fields: `Key`, `LastUsedAt`, `ExpiresAt`
- [ ] Add `AuthCache` struct with `sync.Map`
- [ ] Implement `Get(hash string) (*types.APIKey, bool)`
- [ ] Implement `Put(hash string, key types.APIKey)`
- [ ] Implement cache TTL of 5 minutes

**Auth Cache with Write-Throttling:**
```go
type CacheEntry struct {
    Key        types.APIKey
    LastUsedAt time.Time // Local tracker for throttling
    ExpiresAt  time.Time // Cache TTL
}

type AuthCache struct {
    entries sync.Map // map[string]*CacheEntry
}

func (c *AuthCache) Get(hash string) (*types.APIKey, bool)
func (c *AuthCache) Put(hash string, key types.APIKey)
```

**Authenticate Logic with Write-Throttling:**
1. **Hash Token**: `hash := HashAPIKey(token)`
2. **Read-Through**: Check cache. If miss, load from DB and cache it.
3. **Write-Throttling**: Check `LastUsedAt`. If `time.Since(lastUpdated) > 1*time.Minute`:
   - Fire async `apiKeys.UpdateLastUsed()` goroutine
   - Update cache entry's `LastUsedAt`

### 6. Service Layer

#### 6.1 Validation Functions
- [ ] Add `validateUserCreate(req *proto.UserCreateRequest) error` to `service/validation.go`
- [ ] Add `validateAPIKeyCreate(req *proto.APIKeyCreateRequest) error` to `service/validation.go`

#### 6.2 User Service Methods
- [ ] Add `UsersCreate(ctx, *proto.UserCreateRequest, *proto.UserCreateResponse) error` to `service/service.go`
- [ ] Add `UsersList(ctx, *proto.UsersListRequest, *proto.UsersListResponse) error` to `service/service.go`
- [ ] Add `UsersDelete(ctx, *proto.UsersDeleteRequest) error` to `service/service.go`
- [ ] `UsersDelete` must cascade delete all API keys for that user

#### 6.3 API Key Service Methods
- [ ] Add `APIKeysCreate(ctx, *proto.APIKeyCreateRequest, *proto.APIKeyCreateResponse) error` to `service/service.go`
- [ ] Add `APIKeysList(ctx, *proto.APIKeysListRequest, *proto.APIKeysListResponse) error` to `service/service.go`
- [ ] Add `APIKeysDelete(ctx, *proto.APIKeysDeleteRequest) error` to `service/service.go`
- [ ] `APIKeysCreate` generates key, stores hash, returns raw key once

#### 6.4 Bootstrap Updates
- [ ] Add `bootstrapAnonymousUser(ctx) error` helper to `service/service.go`
- [ ] Update `Bootstrap()` to call `bootstrapAnonymousUser()` after `bootstrapSystemNamespace()`
- [ ] Anonymous user: `User{ID: "anonymous", Username: "anonymous"}`

### 7. Transport Layer

#### 7.1 Transport Interfaces
- [ ] Add `UserAdmin` interface to `transport/interfaces.go`
- [ ] Add `APIKeyAdmin` interface to `transport/interfaces.go`
- [ ] Update `Service` interface to embed `UserAdmin`, `APIKeyAdmin`

#### 7.2 HTTP Handler - Auth Field and extractPrincipal
- [ ] Add `auth auth.AuthBackend` field to `HTTPHandler` struct
- [ ] Add `AuthBackend auth.AuthBackend` field to `HTTPHandlerConfig` struct
- [ ] Update `extractPrincipal()` signature to return `(types.Principal, error)`
- [ ] Parse `Authorization: Bearer <token>` header
- [ ] If no header, return `AnonymousPrincipal, nil`
- [ ] If header present but malformed, return error
- [ ] If header present and valid format, call `AuthBackend.Authenticate()`
- [ ] Update `ServeHTTP()` to handle error from `extractPrincipal()`

**HTTPHandler Struct Update:**
```go
type HTTPHandler struct {
    duration       *prometheus.SummaryVec
    log            *slog.Logger
    metrics        http.Handler
    service        Service
    auth           auth.AuthBackend  // NEW Phase 2
    maxProduceSize int64
}

type HTTPHandlerConfig struct {
    Service        Service
    AuthBackend    auth.AuthBackend  // NEW Phase 2
    MaxProduceSize int64
    Log            *slog.Logger
}
```

**Phased extractPrincipal Evolution:**

Phase 1 (simple):
```go
func (h *HTTPHandler) extractPrincipal(r *http.Request) types.Principal {
    return types.AnonymousPrincipal
}
```

Phase 2-3 (with authentication):
```go
func (h *HTTPHandler) extractPrincipal(r *http.Request) (types.Principal, error) {
    authHeader := r.Header.Get("Authorization")
    if authHeader == "" {
        return types.AnonymousPrincipal, nil
    }

    if !strings.HasPrefix(authHeader, "Bearer ") {
        return types.Principal{}, transport.NewUnauthorized("invalid authorization header format")
    }
    token := strings.TrimPrefix(authHeader, "Bearer ")

    if h.auth == nil {
        return types.AnonymousPrincipal, nil
    }

    principal, err := h.auth.Authenticate(r.Context(), token)
    if err != nil {
        return types.Principal{}, err
    }

    return principal, nil
}
```

**ServeHTTP Signature Change (Phase 2):**
```go
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    defer prometheus.NewTimer(h.duration.WithLabelValues(r.URL.Path)).ObserveDuration()

    // Extract principal (with error handling in Phase 2+)
    principal, err := h.extractPrincipal(r)
    if err != nil {
        h.ReplyError(w, r, err)
        return
    }

    ctx := internal.ContextWithPrincipal(r.Context(), principal)

    // ... rest of routing
}
```

#### 7.3 HTTP Endpoints - Users
- [ ] Add route constant `RPCUsersCreate = "/v1/users.create"`
- [ ] Add route constant `RPCUsersList = "/v1/users.list"`
- [ ] Add route constant `RPCUsersDelete = "/v1/users.delete"`
- [ ] Add `UsersCreate(ctx, w, r)` handler method
- [ ] Add `UsersList(ctx, w, r)` handler method
- [ ] Add `UsersDelete(ctx, w, r)` handler method
- [ ] Add cases to `ServeHTTP()` switch for user routes

#### 7.4 HTTP Endpoints - API Keys
- [ ] Add route constant `RPCAPIKeysCreate = "/v1/api-keys.create"`
- [ ] Add route constant `RPCAPIKeysList = "/v1/api-keys.list"`
- [ ] Add route constant `RPCAPIKeysDelete = "/v1/api-keys.delete"`
- [ ] Add `APIKeysCreate(ctx, w, r)` handler method
- [ ] Add `APIKeysList(ctx, w, r)` handler method
- [ ] Add `APIKeysDelete(ctx, w, r)` handler method
- [ ] Add cases to `ServeHTTP()` switch for API key routes

### 8. Client

#### 8.1 Client Config Update
- [ ] Add `APIKey string` field to `ClientConfig` in `client.go`

#### 8.2 Request Auth Header
- [ ] Update `do()` or equivalent to add `Authorization: Bearer <key>` header if `APIKey` is set

#### 8.3 User Client Methods
- [ ] Add `UsersCreate(ctx, *proto.UserCreateRequest, *proto.UserCreateResponse) error`
- [ ] Add `UsersList(ctx, *proto.UsersListResponse, *ListOptions) error`
- [ ] Add `UsersDelete(ctx, *proto.UsersDeleteRequest) error`

#### 8.4 API Key Client Methods
- [ ] Add `APIKeysCreate(ctx, *proto.APIKeyCreateRequest, *proto.APIKeyCreateResponse) error`
- [ ] Add `APIKeysList(ctx, *proto.APIKeysListRequest, *proto.APIKeysListResponse) error`
- [ ] Add `APIKeysDelete(ctx, *proto.APIKeysDeleteRequest) error`

### 9. Test Setup Updates

#### 9.1 Update Test Infrastructure
- [ ] Update `setupMemoryStorage()` to include `Users`, `APIKeys`
- [ ] Update `badgerTestSetup.Setup()` to include `Users`, `APIKeys`
- [ ] Update `postgresTestSetup.Setup()` to include `Users`, `APIKeys`
- [ ] Add helper `newClientWithKey(t, address, apiKey string) *Client`

### 10. Integration

#### 10.1 Wire Auth to HTTP Handler
- [ ] Add `AuthBackend` field to `HTTPHandler` struct
- [ ] Add `AuthBackend` field to `HTTPHandlerConfig` struct
- [ ] Pass `AuthBackend` in handler initialization
- [ ] Call `AuthBackend.Authenticate()` from `extractPrincipal()`

## Tests

### 10.1 User and API Key Tests
- [ ] Create `service/users_test.go`
- [ ] Test: `CreateUser` - create user, verify in list
- [ ] Test: `CreateAPIKey` - create key, verify format `sk-[env]-[entropy]`
- [ ] Test: `AuthenticateWithKey` - create user, create key, make authenticated request
- [ ] Test: `AnonymousStillWorks` - request without auth header succeeds
- [ ] Test: `DeleteUserCascadesKeys` - delete user, verify keys are gone
- [ ] Test Error: `DuplicateUsername` - same username twice returns error
- [ ] Test Error: `InvalidKeyFormat` - malformed auth header returns 401
- [ ] Test Error: `ExpiredKey` - expired key returns 401
- [ ] Test Error: `NonexistentKey` - random key returns 401

## Verification Checklist

- [ ] `go build ./...` passes
- [ ] `go vet ./...` passes
- [ ] `go test ./service/... -run "TestUsers"` passes for InMemory
- [ ] `go test ./service/... -run "TestUsers"` passes for BadgerDB
- [ ] `go test ./service/... -run "TestNamespaces"` still passes (Phase 1 tests)
- [ ] Request without `Authorization` header works (returns AnonymousPrincipal)
- [ ] Request with valid `Authorization: Bearer <key>` authenticates correctly
- [ ] Request with invalid key returns 401 Unauthorized
- [ ] Request with expired key returns 401 Unauthorized
- [ ] API key creation returns raw key only once
- [ ] API key list does NOT include raw key or hash
- [ ] Anonymous user exists after server startup

---

# Phase 3: Roles + RBAC

## Overview
Implement role-based access control. After this phase:
- Roles can be created, listed, and deleted
- Role bindings associate users with roles in namespaces
- All service methods check authorization
- Anonymous user has Admin role in `_system` (Open Door policy)
- Cascading permission check: target namespace -> `_system`

## Task Checklist

### 1. Error Types

#### 1.1 Domain Error Variables
- [ ] Add to `internal/types/errors.go`:
  - [ ] `ErrRoleNotExist = transport.NewRequestFailed("role does not exist")`
  - [ ] `ErrRoleAlreadyExists = transport.NewInvalidOption("role already exists")`
  - [ ] `ErrRoleBindingAlreadyExists = transport.NewInvalidOption("role binding already exists")`
  - [ ] `ErrRoleHasBindings = transport.NewInvalidOption("cannot delete role: has active bindings")`
  - [ ] `ErrAccessDenied = transport.NewForbidden("access denied: insufficient permissions")`

### 2. Types

#### 2.1 Role Type
- [ ] Add `Role` struct to `internal/types/auth.go` with fields:
  - [ ] `ID string`
  - [ ] `Namespace string`
  - [ ] `Name string`
  - [ ] `Permissions []string`
  - [ ] `CreatedAt clock.Time`
- [ ] Add `ToProto() *proto.Role` method

#### 2.2 RoleBinding Type
- [ ] Add `RoleBinding` struct to `internal/types/auth.go` with fields:
  - [ ] `ID string`
  - [ ] `Namespace string`
  - [ ] `UserID string`
  - [ ] `RoleID string`
  - [ ] `CreatedAt clock.Time`
- [ ] Add `ToProto() *proto.RoleBinding` method

### 3. Permission Constants

#### 3.1 Create Permissions File
- [ ] Create `internal/auth/permissions.go`
- [ ] Add constants:
  - [ ] `PermNamespaceCreate = "namespace.create"`
  - [ ] `PermNamespaceDelete = "namespace.delete"`
  - [ ] `PermNamespaceList = "namespace.list"`
  - [ ] `PermQueueCreate = "queue.create"`
  - [ ] `PermQueueDelete = "queue.delete"`
  - [ ] `PermQueueUpdate = "queue.update"`
  - [ ] `PermQueueList = "queue.list"`
  - [ ] `PermQueueProduce = "queue.produce"`
  - [ ] `PermQueueLease = "queue.lease"`
  - [ ] `PermQueueComplete = "queue.complete"`
  - [ ] `PermQueueRetry = "queue.retry"`
  - [ ] `PermQueueStats = "queue.stats"`
  - [ ] `PermQueueClear = "queue.clear"`
  - [ ] `PermUserCreate = "user.create"`
  - [ ] `PermUserDelete = "user.delete"`
  - [ ] `PermUserList = "user.list"`
  - [ ] `PermAPIKeyCreate = "apikey.create"`
  - [ ] `PermAPIKeyDelete = "apikey.delete"`
  - [ ] `PermAPIKeyList = "apikey.list"`
  - [ ] `PermRoleCreate = "role.create"`
  - [ ] `PermRoleDelete = "role.delete"`
  - [ ] `PermRoleList = "role.list"`
  - [ ] `PermRoleBindingCreate = "rolebinding.create"`
  - [ ] `PermRoleBindingDelete = "rolebinding.delete"`
  - [ ] `PermRoleBindingList = "rolebinding.list"`
  - [ ] `PermSystemHealth = "system.health"`
  - [ ] `PermSystemMetrics = "system.metrics"`

#### 3.2 Standard Roles Definition
- [ ] Add `StandardRoles` map to `internal/auth/permissions.go`:
  - [ ] `Admin` - all permissions
  - [ ] `NamespaceOwner` - namespace-scoped permissions (no namespace create/delete)
  - [ ] `PublicViewer` - only `system.health`, `system.metrics`

**StandardRoles Map Implementation:**
```go
var StandardRoles = map[string]Role{
    "Admin": {
        Name: "Admin",
        Permissions: []string{
            PermNamespaceCreate, PermNamespaceDelete, PermNamespaceList,
            PermQueueCreate, PermQueueDelete, PermQueueUpdate, PermQueueList,
            PermQueueProduce, PermQueueLease, PermQueueComplete, PermQueueRetry,
            PermQueueStats, PermQueueClear,
            PermUserCreate, PermUserDelete, PermUserList,
            PermAPIKeyCreate, PermAPIKeyDelete, PermAPIKeyList,
            PermRoleCreate, PermRoleDelete, PermRoleList,
            PermRoleBindingCreate, PermRoleBindingDelete, PermRoleBindingList,
            PermSystemHealth, PermSystemMetrics,
        },
    },
    "NamespaceOwner": {
        Name: "NamespaceOwner",
        Permissions: []string{
            // Namespace-scoped full access (no namespace create/delete)
            PermQueueCreate, PermQueueDelete, PermQueueUpdate, PermQueueList,
            PermQueueProduce, PermQueueLease, PermQueueComplete, PermQueueRetry,
            PermQueueStats, PermQueueClear,
            PermRoleCreate, PermRoleDelete, PermRoleList,
            PermRoleBindingCreate, PermRoleBindingDelete, PermRoleBindingList,
        },
    },
    "PublicViewer": {
        Name: "PublicViewer",
        Permissions: []string{
            PermSystemHealth,
            PermSystemMetrics,
        },
    },
}
```

### 4. Proto Definitions

#### 4.1 Roles Proto
- [ ] Create `proto/roles.proto`
- [ ] Add `Role` message
- [ ] Add `RoleCreateRequest` message
- [ ] Add `RolesListRequest` message
- [ ] Add `RolesListResponse` message
- [ ] Add `RolesDeleteRequest` message
- [ ] Add `RoleBinding` message
- [ ] Add `RoleBindingCreateRequest` message
- [ ] Add `RoleBindingsListRequest` message with `user_id_filter` field
- [ ] Add `RoleBindingsListResponse` message
- [ ] Add `RoleBindingDeleteRequest` message with `namespace`, `role_name`, `user_id` fields
- [ ] Run `make proto` to generate Go code

**Role Proto Messages:**
```protobuf
message RoleBindingsListRequest {
    string namespace = 1;
    string user_id_filter = 2;  // Optional: filter by user
    int32 limit = 3;
    string pivot = 4;
}

message RoleBindingDeleteRequest {
    string namespace = 1;
    string role_name = 2;
    string user_id = 3;
}
```

### 5. Storage Layer

#### 5.1 Roles Storage Interface
- [ ] Create `internal/store/roles.go`
- [ ] Add `Roles` interface with methods: `Get`, `GetByID`, `Add`, `List`, `Delete`, `Close`
- [ ] Add `RoleBindings` interface with methods: `Get`, `Add`, `List`, `ListByUser`, `ListByRole`, `Delete`, `Close`

**RoleBindings Interface:**
```go
type RoleBindings interface {
    Get(ctx context.Context, id string, binding *types.RoleBinding) error
    Add(ctx context.Context, binding types.RoleBinding) error
    List(ctx context.Context, namespace string, bindings *[]types.RoleBinding, opts types.ListOptions) error
    ListByUser(ctx context.Context, userID string, bindings *[]types.RoleBinding) error
    ListByRole(ctx context.Context, roleID string, bindings *[]types.RoleBinding) error  // For role deletion check
    Delete(ctx context.Context, id string) error
    Close(ctx context.Context) error
}
```

#### 5.2 Update Store Config
- [ ] Add `Roles Roles` field to `Config` struct in `internal/store/store.go`
- [ ] Add `RoleBindings RoleBindings` field to `Config` struct

#### 5.3 InMemory Roles Storage
- [ ] Add `MemoryRoles` struct to `internal/store/memory.go`
- [ ] Implement all `Roles` interface methods
- [ ] Add `MemoryRoleBindings` struct
- [ ] Implement all `RoleBindings` interface methods

#### 5.4 BadgerDB Roles Storage
- [ ] Add `BadgerRoles` struct to `internal/store/badger.go`
- [ ] Implement all `Roles` interface methods (use key prefix `role:`)
- [ ] Add `BadgerRoleBindings` struct
- [ ] Implement all `RoleBindings` interface methods (use key prefix `rolebinding:`)

#### 5.5 PostgreSQL Roles Storage
- [ ] Add `PostgresRoles` struct to `internal/store/postgres.go`
- [ ] Implement all `Roles` interface methods
- [ ] Create table: `roles (id TEXT PRIMARY KEY, namespace TEXT, name TEXT, permissions TEXT[], created_at TIMESTAMPTZ, UNIQUE(namespace, name))`
- [ ] Add `PostgresRoleBindings` struct
- [ ] Implement all `RoleBindings` interface methods
- [ ] Create table: `role_bindings (id TEXT PRIMARY KEY, namespace TEXT, user_id TEXT, role_id TEXT, created_at TIMESTAMPTZ, UNIQUE(namespace, user_id, role_id))`

### 6. Auth Backend

#### 6.1 AuthBackend Interface
- [ ] Create `internal/auth/backend.go`
- [ ] Add `AuthBackend` interface with methods: `Authenticate(ctx, token) (Principal, error)`, `HasPermission(ctx, principal, targetNS, perm) (bool, error)`

#### 6.2 DefaultAuthBackend Implementation
- [ ] Add `DefaultAuthBackend` struct with fields: `users`, `apiKeys`, `roles`, `roleBindings`, `log`, `cache`, `writeTimeout`
- [ ] Implement `Authenticate(ctx, token)`:
  - [ ] Hash token
  - [ ] Check cache, if miss load from storage
  - [ ] Validate expiration
  - [ ] Async update `LastUsedAt` (throttled to 1/minute)
  - [ ] Return Principal
- [ ] Implement `HasPermission(ctx, principal, targetNS, perm)`:
  - [ ] Step 1: If `principal.NamespaceScope != nil && *NamespaceScope != targetNS`, return false
  - [ ] Step 2: Check role bindings in `targetNS`
  - [ ] Step 3: If not found and `targetNS != "_system"`, check role bindings in `_system`
  - [ ] Return true if permission found, false otherwise
- [ ] Add helper `checkPermissionInNamespace(ctx, userID, namespace, perm) (bool, error)`

**Complete HasPermission Implementation:**
```go
// HasPermission implements the cascading authorization check per spec Section 8.2
func (a *DefaultAuthBackend) HasPermission(ctx context.Context, principal types.Principal, targetNS string, perm string) (bool, error) {
    // Step 1: API Key Scope Check (The Logic Gate)
    // If key is scoped, it MUST match the target namespace - this is a hard filter
    if principal.NamespaceScope != nil && *principal.NamespaceScope != targetNS {
        // Scoped key cannot access other namespaces, regardless of user permissions
        return false, nil
    }

    // Step 2: Check Target Namespace
    if hasPermInNamespace, err := a.checkPermissionInNamespace(ctx, principal.User.ID, targetNS, perm); err != nil {
        return false, err
    } else if hasPermInNamespace {
        return true, nil
    }

    // Step 3: Check _system Namespace (Admin Cascade)
    if targetNS != "_system" {
        if hasPermInSystem, err := a.checkPermissionInNamespace(ctx, principal.User.ID, "_system", perm); err != nil {
            return false, err
        } else if hasPermInSystem {
            return true, nil
        }
    }

    // No permission found
    return false, nil
}

// checkPermissionInNamespace checks if user has permission via role bindings in a specific namespace
func (a *DefaultAuthBackend) checkPermissionInNamespace(ctx context.Context, userID, namespace, perm string) (bool, error) {
    // Get all role bindings for this user
    var bindings []types.RoleBinding
    if err := a.roleBindings.ListByUser(ctx, userID, &bindings); err != nil {
        return false, err
    }

    // Check each binding in the target namespace
    for _, binding := range bindings {
        if binding.Namespace != namespace {
            continue
        }

        // Get the role
        var role types.Role
        if err := a.roles.GetByID(ctx, binding.RoleID, &role); err != nil {
            a.log.Warn("role not found for binding", "role_id", binding.RoleID, "binding_id", binding.ID)
            continue
        }

        // Check if role has the required permission
        for _, p := range role.Permissions {
            if p == perm {
                return true, nil
            }
        }
    }

    return false, nil
}
```

**Authorization Flow Diagram:**
```
HasPermission(principal, targetNS="production", perm="queue.produce")
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ Step 1: API Key Scope Check                         │
│ if principal.NamespaceScope != nil                  │
│    AND *NamespaceScope != "production"              │
│ → return FALSE (hard filter)                        │
└─────────────────────────────────────────────────────┘
    │ scope matches or unscoped
    ▼
┌─────────────────────────────────────────────────────┐
│ Step 2: Check Target Namespace ("production")       │
│ Find RoleBindings where:                            │
│   - user_id = principal.User.ID                     │
│   - namespace = "production"                        │
│ For each binding, check if role has "queue.produce" │
│ → return TRUE if found                              │
└─────────────────────────────────────────────────────┘
    │ not found in target
    ▼
┌─────────────────────────────────────────────────────┐
│ Step 3: Check _system Namespace (Cascade)           │
│ Find RoleBindings where:                            │
│   - user_id = principal.User.ID                     │
│   - namespace = "_system"                           │
│ For each binding, check if role has "queue.produce" │
│ → return TRUE if found, FALSE otherwise             │
└─────────────────────────────────────────────────────┘
```

### 7. Service Layer

#### 7.1 Authorization Method
- [ ] Add `authorize(ctx, targetNS, perm string) error` to `service/service.go`
- [ ] Extract Principal from context
- [ ] Call `AuthBackend.HasPermission()`
- [ ] Return `types.ErrAccessDenied` if denied

#### 7.2 Add Authorization to ALL Service Methods
- [ ] Add `s.authorize(ctx, "_system", auth.PermNamespaceCreate)` to `NamespacesCreate()`
- [ ] Add `s.authorize(ctx, "_system", auth.PermNamespaceList)` to `NamespacesList()`
- [ ] Add `s.authorize(ctx, "_system", auth.PermNamespaceDelete)` to `NamespacesDelete()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueCreate)` to `QueuesCreate()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueUpdate)` to `QueuesUpdate()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueDelete)` to `QueuesDelete()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueList)` to `QueuesList()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueList)` to `QueuesInfo()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueProduce)` to `QueueProduce()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueLease)` to `QueueLease()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueComplete)` to `QueueComplete()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueRetry)` to `QueueRetry()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueClear)` to `QueueClear()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueClear)` to `QueueReload()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueStats)` to `QueueStats()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueList)` to `StorageItemsList()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueCreate)` to `StorageItemsImport()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermQueueDelete)` to `StorageItemsDelete()`
- [ ] Add `s.authorize(ctx, "_system", auth.PermUserCreate)` to `UsersCreate()`
- [ ] Add `s.authorize(ctx, "_system", auth.PermUserList)` to `UsersList()`
- [ ] Add `s.authorize(ctx, "_system", auth.PermUserDelete)` to `UsersDelete()`
- [ ] Add `s.authorize(ctx, "_system", auth.PermAPIKeyCreate)` to `APIKeysCreate()`
- [ ] Add `s.authorize(ctx, "_system", auth.PermAPIKeyList)` to `APIKeysList()`
- [ ] Add `s.authorize(ctx, "_system", auth.PermAPIKeyDelete)` to `APIKeysDelete()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermRoleCreate)` to `RolesCreate()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermRoleList)` to `RolesList()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermRoleDelete)` to `RolesDelete()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermRoleBindingCreate)` to `RoleBindingsCreate()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermRoleBindingList)` to `RoleBindingsList()`
- [ ] Add `s.authorize(ctx, req.Namespace, auth.PermRoleBindingDelete)` to `RoleBindingsDelete()`

#### 7.3 Validation Functions
- [ ] Add `validateRoleCreate(req *proto.RoleCreateRequest) error` to `service/validation.go`
- [ ] Add `validateRoleBindingCreate(req *proto.RoleBindingCreateRequest) error` to `service/validation.go`

#### 7.4 Role Service Methods
- [ ] Add `RolesCreate(ctx, *proto.RoleCreateRequest) error` to `service/service.go`
- [ ] Add `RolesList(ctx, *proto.RolesListRequest, *proto.RolesListResponse) error` to `service/service.go`
- [ ] Add `RolesDelete(ctx, *proto.RolesDeleteRequest) error` to `service/service.go`
- [ ] `RolesDelete` must check for active bindings (RESTRICT)
- [ ] `RolesDelete` must block deletion of standard roles
- [ ] Add `isStandardRole(name string) bool` helper

**RolesDelete Implementation:**
```go
func (s *Service) RolesDelete(ctx context.Context, req *proto.RolesDeleteRequest) error {
    // Block deletion of standard roles
    if isStandardRole(req.Name) {
        return transport.NewInvalidOption("cannot delete standard role; role '%s' is immutable", req.Name)
    }

    // Get the role
    var role types.Role
    if err := s.conf.StorageConfig.Roles.Get(ctx, req.Namespace, req.Name, &role); err != nil {
        return err
    }

    // Check for active bindings (RESTRICT)
    var bindings []types.RoleBinding
    if err := s.conf.StorageConfig.RoleBindings.ListByRole(ctx, role.ID, &bindings); err != nil {
        return err
    }
    if len(bindings) > 0 {
        return types.ErrRoleHasBindings
    }

    return s.conf.StorageConfig.Roles.Delete(ctx, role.ID)
}

func isStandardRole(name string) bool {
    switch name {
    case "Admin", "NamespaceOwner", "PublicViewer":
        return true
    }
    return false
}
```

#### 7.5 RoleBinding Service Methods
- [ ] Add `RoleBindingsCreate(ctx, *proto.RoleBindingCreateRequest) error` to `service/service.go`
- [ ] Add `RoleBindingsList(ctx, *proto.RoleBindingsListRequest, *proto.RoleBindingsListResponse) error` to `service/service.go`
- [ ] Add `RoleBindingsDelete(ctx, *proto.RoleBindingDeleteRequest) error` to `service/service.go`

#### 7.6 Bootstrap Updates
- [ ] Add `bootstrapStandardRoles(ctx) error` helper
- [ ] Add `bootstrapAnonymousAdminBinding(ctx) error` helper
- [ ] Update `Bootstrap()` to call `bootstrapStandardRoles()` after `bootstrapAnonymousUser()`
- [ ] Update `Bootstrap()` to call `bootstrapAnonymousAdminBinding()` after `bootstrapStandardRoles()`
- [ ] Log warning: "SYSTEM RUNNING IN OPEN DOOR MODE" with lock-down instructions

**Bootstrap Warning Log:**
```go
func (s *Service) bootstrapAnonymousAdminBinding(ctx context.Context) error {
    binding := types.RoleBinding{
        ID:        "anonymous-admin",
        Namespace: "_system",
        UserID:    "anonymous",
        RoleID:    "admin",
        CreatedAt: s.conf.Clock.Now().UTC(),
    }

    if err := s.conf.StorageConfig.RoleBindings.Add(ctx, binding); err != nil {
        if !errors.Is(err, types.ErrRoleBindingAlreadyExists) {
            return err
        }
    }

    // Log the Open Door status to help admins find it
    s.conf.Log.Warn("SYSTEM RUNNING IN OPEN DOOR MODE",
        "details", "Anonymous users have ADMIN access. To lock down, create an admin user and revoke the 'anonymous-admin' binding.")

    return nil
}
```

**Lock-Down Procedure Examples:**
```go
// Example: Lock down the cluster by removing Anonymous Admin access
func lockDownCluster(ctx context.Context, client *querator.Client) error {
    // Remove the Open Door binding
    return client.RoleBindingsDelete(ctx, &pb.RoleBindingDeleteRequest{
        Namespace: "_system",
        UserID:    "anonymous",
        RoleName:  "Admin",
    })
}

// Optionally, allow public health/metrics access
func allowPublicMonitoring(ctx context.Context, client *querator.Client) error {
    return client.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
        RoleName:  "PublicViewer",
        Namespace: "_system",
        UserID:    "anonymous",
    })
}
```

### 8. Transport Layer

#### 8.1 Transport Interfaces
- [ ] Add `RoleAdmin` interface to `transport/interfaces.go`
- [ ] Add `RoleBindingAdmin` interface to `transport/interfaces.go`
- [ ] Update `Service` interface to embed `RoleAdmin`, `RoleBindingAdmin`

#### 8.2 HTTP Endpoints - Roles
- [ ] Add route constant `RPCRolesCreate = "/v1/roles.create"`
- [ ] Add route constant `RPCRolesList = "/v1/roles.list"`
- [ ] Add route constant `RPCRolesDelete = "/v1/roles.delete"`
- [ ] Add `RolesCreate(ctx, w, r)` handler method
- [ ] Add `RolesList(ctx, w, r)` handler method
- [ ] Add `RolesDelete(ctx, w, r)` handler method
- [ ] Add cases to `ServeHTTP()` switch for role routes

#### 8.3 HTTP Endpoints - RoleBindings
- [ ] Add route constant `RPCRoleBindingsCreate = "/v1/role-bindings.create"`
- [ ] Add route constant `RPCRoleBindingsList = "/v1/role-bindings.list"`
- [ ] Add route constant `RPCRoleBindingsDelete = "/v1/role-bindings.delete"`
- [ ] Add `RoleBindingsCreate(ctx, w, r)` handler method
- [ ] Add `RoleBindingsList(ctx, w, r)` handler method
- [ ] Add `RoleBindingsDelete(ctx, w, r)` handler method
- [ ] Add cases to `ServeHTTP()` switch for role binding routes

### 9. Client

#### 9.1 Role Client Methods
- [ ] Add `RolesCreate(ctx, *proto.RoleCreateRequest) error` to `client.go`
- [ ] Add `RolesList(ctx, *proto.RolesListRequest, *proto.RolesListResponse) error` to `client.go`
- [ ] Add `RolesDelete(ctx, *proto.RolesDeleteRequest) error` to `client.go`

#### 9.2 RoleBinding Client Methods
- [ ] Add `RoleBindingsCreate(ctx, *proto.RoleBindingCreateRequest) error` to `client.go`
- [ ] Add `RoleBindingsList(ctx, *proto.RoleBindingsListRequest, *proto.RoleBindingsListResponse) error` to `client.go`
- [ ] Add `RoleBindingsDelete(ctx, *proto.RoleBindingDeleteRequest) error` to `client.go`

### 10. Test Setup Updates

#### 10.1 Update Test Infrastructure
- [ ] Update `setupMemoryStorage()` to include `Roles`, `RoleBindings`
- [ ] Update `badgerTestSetup.Setup()` to include `Roles`, `RoleBindings`
- [ ] Update `postgresTestSetup.Setup()` to include `Roles`, `RoleBindings`

### 11. Integration

#### 11.1 Wire AuthBackend to Service
- [ ] Add `AuthBackend auth.AuthBackend` field to `Service` struct
- [ ] Add `AuthBackend auth.AuthBackend` field to `Service.Config`
- [ ] Create `DefaultAuthBackend` in `Service.New()` if not provided
- [ ] Pass storage interfaces to `DefaultAuthBackend`

## Tests

### 11.1 Authorization Tests
- [ ] Create `service/auth_test.go`
- [ ] Test: `AnonymousHasAdminByDefault` - anonymous can create namespaces
- [ ] Test: `CascadingPermissions` - user with Admin in `_system` can access any namespace
- [ ] Test: `ScopedKeyRestriction` - key scoped to ns-A cannot access ns-B
- [ ] Test: `LockDownWorkflow` - remove anonymous-admin binding, anonymous gets 403
- [ ] Test Error: `DuplicateRole` - same role name in namespace returns error
- [ ] Test Error: `RoleNotExist` - binding to nonexistent role returns error
- [ ] Test Error: `DeleteStandardRole` - cannot delete Admin/NamespaceOwner/PublicViewer
- [ ] Test Error: `DeleteRoleWithBindings` - cannot delete role with active bindings

### 12. Health Endpoint Bootstrap Check

#### 12.1 Update Health Method
- [ ] Check `bootstrapState` in Health method
- [ ] Report "fail" status if not bootstrapped

**Health Endpoint Bootstrap Check:**
```go
func (s *Service) Health(ctx context.Context) (*transport.HealthResponse, error) {
    // ... existing setup ...

    // Check bootstrap status
    if s.bootstrapState.Load() != stateReady {
        response.Status = transport.HealthStatusFail
        response.Checks["system:bootstrap"] = []transport.Check{{
            ComponentType: "system",
            Status:        transport.HealthStatusFail,
            Output:        "system not bootstrapped",
            Time:          time.Now().UTC().Format(time.RFC3339),
        }}
    }

    // ... existing checks ...
    return response, nil
}
```

## Verification Checklist

- [ ] `go build ./...` passes
- [ ] `go vet ./...` passes
- [ ] `go test ./service/... -run "TestAuth"` passes for InMemory
- [ ] `go test ./service/... -run "TestAuth"` passes for BadgerDB
- [ ] `go test ./service/... -run "TestUsers"` still passes (Phase 2 tests)
- [ ] `go test ./service/... -run "TestNamespaces"` still passes (Phase 1 tests)
- [ ] `go test ./service/... -run "TestQueue"` still passes (existing tests)
- [ ] Anonymous user can create namespace (Open Door policy)
- [ ] After removing anonymous-admin binding, anonymous gets 403 Forbidden
- [ ] User with Admin role in `_system` can access any namespace
- [ ] Key scoped to namespace A cannot access namespace B
- [ ] Standard roles (Admin, NamespaceOwner, PublicViewer) cannot be deleted
- [ ] Role with active bindings cannot be deleted
- [ ] Health endpoint reports "fail" if system not bootstrapped

---

# Phase 4: Documentation + OpenAPI

## Overview
Update documentation and OpenAPI specification to reflect all changes.

## Task Checklist

### 1. OpenAPI Specification

#### 1.1 Namespace Endpoints
- [ ] Add `/v1/namespaces.create` endpoint definition to `openapi.yaml`
- [ ] Add `/v1/namespaces.list` endpoint definition to `openapi.yaml`
- [ ] Add `/v1/namespaces.delete` endpoint definition to `openapi.yaml`
- [ ] Add `NamespaceInfo` schema
- [ ] Add `NamespaceCreateRequest` schema
- [ ] Add `NamespacesListRequest` schema
- [ ] Add `NamespacesListResponse` schema
- [ ] Add `NamespacesDeleteRequest` schema

#### 1.2 User Endpoints
- [ ] Add `/v1/users.create` endpoint definition
- [ ] Add `/v1/users.list` endpoint definition
- [ ] Add `/v1/users.delete` endpoint definition
- [ ] Add `User` schema
- [ ] Add `UserCreateRequest` schema
- [ ] Add `UserCreateResponse` schema
- [ ] Add `UsersListRequest` schema
- [ ] Add `UsersListResponse` schema
- [ ] Add `UsersDeleteRequest` schema

#### 1.3 API Key Endpoints
- [ ] Add `/v1/api-keys.create` endpoint definition
- [ ] Add `/v1/api-keys.list` endpoint definition
- [ ] Add `/v1/api-keys.delete` endpoint definition
- [ ] Add `APIKeyMetadata` schema
- [ ] Add `APIKeyCreateRequest` schema
- [ ] Add `APIKeyCreateResponse` schema
- [ ] Add `APIKeysListRequest` schema
- [ ] Add `APIKeysListResponse` schema
- [ ] Add `APIKeysDeleteRequest` schema

#### 1.4 Role Endpoints
- [ ] Add `/v1/roles.create` endpoint definition
- [ ] Add `/v1/roles.list` endpoint definition
- [ ] Add `/v1/roles.delete` endpoint definition
- [ ] Add `Role` schema
- [ ] Add `RoleCreateRequest` schema
- [ ] Add `RolesListRequest` schema
- [ ] Add `RolesListResponse` schema
- [ ] Add `RolesDeleteRequest` schema

#### 1.5 RoleBinding Endpoints
- [ ] Add `/v1/role-bindings.create` endpoint definition
- [ ] Add `/v1/role-bindings.list` endpoint definition
- [ ] Add `/v1/role-bindings.delete` endpoint definition
- [ ] Add `RoleBinding` schema
- [ ] Add `RoleBindingCreateRequest` schema
- [ ] Add `RoleBindingsListRequest` schema
- [ ] Add `RoleBindingsListResponse` schema
- [ ] Add `RoleBindingDeleteRequest` schema

#### 1.6 Update Existing Endpoints
- [ ] Add `namespace` parameter to all queue endpoints
- [ ] Add `namespace` parameter to all storage endpoints
- [ ] Add `Authorization` header documentation to security section

#### 1.7 Error Responses
- [ ] Add 401 Unauthorized response schema
- [ ] Add 403 Forbidden response schema
- [ ] Document when each error is returned

### 2. Documentation

#### 2.1 Authentication Guide
- [ ] Create `docs/authentication.md`
- [ ] Document API key format (`sk-[env]-[entropy]`)
- [ ] Document `Authorization: Bearer <key>` header usage
- [ ] Document anonymous access behavior

#### 2.2 Authorization Guide
- [ ] Create `docs/authorization.md`
- [ ] Document RBAC model
- [ ] Document permission list with descriptions
- [ ] Document cascading check (namespace -> `_system`)
- [ ] Document namespace-scoped keys

#### 2.3 Open Door Policy Guide
- [ ] Create `docs/open-door-policy.md`
- [ ] Document default behavior (anonymous has Admin)
- [ ] Document lock-down procedure step-by-step
- [ ] Provide code examples for lock-down

#### 2.4 Standard Roles Reference
- [ ] Create `docs/standard-roles.md`
- [ ] Document Admin role and permissions
- [ ] Document NamespaceOwner role and permissions
- [ ] Document PublicViewer role and permissions

## Verification Checklist

- [ ] OpenAPI spec validates without errors
- [ ] All new endpoints are documented in OpenAPI
- [ ] All new request/response schemas are defined
- [ ] Authentication documentation is complete
- [ ] Authorization documentation is complete
- [ ] Lock-down procedure is documented

---

# Summary of Files Changed/Created

## New Files
| Phase | File | Description |
|-------|------|-------------|
| 1 | `internal/types/namespace.go` | Namespace type |
| 1 | `internal/types/auth.go` | Principal, User, APIKey, Role, RoleBinding types |
| 1 | `internal/types/errors.go` | Domain error variables |
| 1 | `internal/context.go` | Principal context functions |
| 1 | `internal/store/namespace.go` | Namespaces storage interface |
| 1 | `proto/namespaces.proto` | Namespace proto messages |
| 1 | `service/namespace_test.go` | Namespace tests |
| 2 | `internal/store/users.go` | Users storage interface |
| 2 | `internal/store/apikeys.go` | APIKeys storage interface |
| 2 | `internal/auth/apikey.go` | API key generation utilities |
| 2 | `internal/auth/cache.go` | Auth cache |
| 2 | `proto/users.proto` | User proto messages |
| 2 | `proto/apikeys.proto` | API key proto messages |
| 2 | `service/users_test.go` | User and API key tests |
| 3 | `internal/store/roles.go` | Roles and RoleBindings storage interfaces |
| 3 | `internal/auth/permissions.go` | Permission constants |
| 3 | `internal/auth/backend.go` | AuthBackend interface and implementation |
| 3 | `proto/roles.proto` | Role proto messages |
| 3 | `service/auth_test.go` | Authorization tests |
| 4 | `docs/authentication.md` | Authentication guide |
| 4 | `docs/authorization.md` | Authorization guide |
| 4 | `docs/open-door-policy.md` | Open Door policy guide |
| 4 | `docs/standard-roles.md` | Standard roles reference |

## Modified Files
| Phase | File | Changes |
|-------|------|---------|
| 1 | `transport/errors.go` | Add ErrUnauthorized, ErrForbidden |
| 1 | `internal/types/items.go` | Add Namespace to QueueInfo |
| 1 | `internal/store/store.go` | Add Namespaces to Config |
| 1 | `internal/store/memory.go` | Add MemoryNamespaces |
| 1 | `internal/store/badger.go` | Add BadgerNamespaces |
| 1 | `internal/store/postgres.go` | Add PostgresNamespaces |
| 1 | `internal/queues_manager.go` | Namespace-aware queue keys |
| 1 | `proto/queue.proto` | Add namespace to request messages |
| 1 | `proto/queues.proto` | Add namespace to request messages |
| 1 | `proto/storage.proto` | Add namespace to request messages |
| 1 | `service/validation.go` | Add namespace validation |
| 1 | `service/service.go` | Add namespace methods, Bootstrap |
| 1 | `service/common_test.go` | Update test setup |
| 1 | `transport/interfaces.go` | Add NamespaceAdmin |
| 1 | `transport/http.go` | Add namespace endpoints, extractPrincipal |
| 1 | `client.go` | Add namespace client methods |
| 2 | `internal/store/store.go` | Add Users, APIKeys to Config |
| 2 | `internal/store/memory.go` | Add MemoryUsers, MemoryAPIKeys |
| 2 | `internal/store/badger.go` | Add BadgerUsers, BadgerAPIKeys |
| 2 | `internal/store/postgres.go` | Add PostgresUsers, PostgresAPIKeys |
| 2 | `service/validation.go` | Add user/key validation |
| 2 | `service/service.go` | Add user/key methods, update Bootstrap |
| 2 | `transport/interfaces.go` | Add UserAdmin, APIKeyAdmin |
| 2 | `transport/http.go` | Add user/key endpoints, update extractPrincipal |
| 2 | `client.go` | Add user/key methods, auth header |
| 3 | `internal/store/store.go` | Add Roles, RoleBindings to Config |
| 3 | `internal/store/memory.go` | Add MemoryRoles, MemoryRoleBindings |
| 3 | `internal/store/badger.go` | Add BadgerRoles, BadgerRoleBindings |
| 3 | `internal/store/postgres.go` | Add PostgresRoles, PostgresRoleBindings |
| 3 | `service/validation.go` | Add role validation |
| 3 | `service/service.go` | Add role methods, authorize, update Bootstrap |
| 3 | `transport/interfaces.go` | Add RoleAdmin, RoleBindingAdmin |
| 3 | `transport/http.go` | Add role endpoints |
| 3 | `client.go` | Add role client methods |
| 4 | `openapi.yaml` | Add all new endpoints and schemas |
