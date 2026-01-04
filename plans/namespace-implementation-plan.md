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

## What We're NOT Doing

- External identity provider integration (OIDC/SSO) - deferred to future work
- Permission caching - implement if performance testing shows need
- Audit logging - separate feature
- Rate limiting - separate feature
- Namespace quotas - separate feature
- Migration of existing data - Querator is alpha, no migration path needed

## Design Decisions

The following decisions were made during plan review:

### User Deletion Behavior: CASCADE
When a user is deleted, all their API keys are automatically deleted. This prevents orphaned data and is the cleanest approach.

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
Roles cannot be deleted if they have active bindings. This prevents accidental revocation of access.

```go
func (s *Service) RolesDelete(ctx context.Context, req *proto.RolesDeleteRequest) error {
    // Check for active bindings
    var bindings []types.RoleBinding
    if err := s.conf.StorageConfig.RoleBindings.List(ctx, req.Namespace, &bindings, types.ListOptions{}); err != nil {
        return err
    }

    // Find the role ID
    var role types.Role
    if err := s.conf.StorageConfig.Roles.Get(ctx, req.Namespace, req.Name, &role); err != nil {
        return err
    }

    for _, binding := range bindings {
        if binding.RoleID == role.ID {
            return types.ErrRoleHasBindings
        }
    }

    return s.conf.StorageConfig.Roles.Delete(ctx, role.ID)
}
```

### Standard Roles: Immutable
Standard roles (Admin, NamespaceOwner, PublicViewer) cannot be modified or deleted by users. They are synced from code on every startup.

```go
func (s *Service) RolesDelete(ctx context.Context, req *proto.RolesDeleteRequest) error {
    // Block deletion of standard roles
    if isStandardRole(req.Name) {
        return transport.NewInvalidOption("cannot delete standard role; role '%s' is immutable", req.Name)
    }
    // ... rest of deletion logic
}

func isStandardRole(name string) bool {
    switch name {
    case "Admin", "NamespaceOwner", "PublicViewer":
        return true
    }
    return false
}
```

### Anonymous User ID: String Literal
The Anonymous user uses the literal string `"anonymous"` for both ID and Username. This is simple, predictable, and easy to reference in role bindings.

```go
var AnonymousUser = User{
    ID:       "anonymous",
    Username: "anonymous",
}
```

### UpdateLastUsed: Async Goroutine
API key last-used tracking is fire-and-forget in a goroutine. Errors are logged at WARN level but don't affect request processing.

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
Querator is alpha-quality software with no deployed instances. All queue operations require a namespace field. There is no migration path for data without namespaces.

### Auth Caching: Read-Through + Write-Throttling
To prevent performance degradation:
1.  **Reads**: API keys are cached in-memory (TTL 5m) to avoid DB lookups on every request.
2.  **Writes**: `LastUsedAt` updates are throttled (max 1 update per minute per key) to prevent write amplification.

## Implementation Approach

We use an incremental approach: each phase adds complete, testable functionality. Phase 1 establishes the foundation (namespaces), Phase 2 adds authentication (users/keys), Phase 3 adds authorization (roles/RBAC).

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

### Namespace Extraction from Queue Requests

Every queue operation request contains a `namespace` field. The namespace is extracted in the service layer and used for:
1. Authorization checks (does principal have permission in this namespace?)
2. Queue lookup (queues are stored with namespace prefix)

**Proto changes - COMPLETE LIST of messages needing `namespace` field:**

> **Note**: Field numbers are assigned sequentially based on the next available field ID for each message.

```protobuf
// proto/queue.proto - Add namespace field to ALL request messages

message QueueProduceRequest {
    string namespace = 4 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}

message QueueLeaseRequest {
    string namespace = 5 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}

message QueueCompleteRequest {
    string namespace = 5 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}

message QueueRetryRequest {
    string namespace = 4 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}

message QueueClearRequest {
    string namespace = 6 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}

message QueueReloadRequest {
    string namespace = 2 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}

message QueueStatsRequest {
    string namespace = 2 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}

message QueueInfo {
    string namespace = 11 [json_name = "namespace"];  // NEW: owning namespace
    // ... existing fields unchanged
}
```

```protobuf
// proto/queues.proto - Add namespace field

message QueuesListRequest {
    string namespace = 4 [json_name = "namespace"];  // NEW: filter by namespace (empty = all)
    // ... existing fields unchanged
}

message QueuesDeleteRequest {
    string namespace = 3 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}

message QueuesInfoRequest {
    string namespace = 2 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}
```

```protobuf
// proto/storage.proto - Add namespace field

message StorageItemsListRequest {
    string namespace = 5 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}

message StorageItemsImportRequest {
    string namespace = 4 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}

message StorageItemsDeleteRequest {
    string namespace = 4 [json_name = "namespace"];  // NEW: target namespace
    // ... existing fields unchanged
}
```

**Summary of proto changes:**
| File | Messages Updated |
|------|------------------|
| `proto/queue.proto` | `QueueProduceRequest`, `QueueLeaseRequest`, `QueueCompleteRequest`, `QueueRetryRequest`, `QueueClearRequest`, `QueueReloadRequest`, `QueueStatsRequest`, `QueueInfo` |
| `proto/queues.proto` | `QueuesListRequest`, `QueuesDeleteRequest`, `QueuesInfoRequest` |
| `proto/storage.proto` | `StorageItemsListRequest`, `StorageItemsImportRequest`, `StorageItemsDeleteRequest` |

### Queue Name Uniqueness

Queue names are unique **per namespace** (not globally). This allows:
- Different namespaces to have queues with the same name
- Storage keys to be prefixed: `{namespace}/{queue_name}`

---

## Error Types and HTTP Status Codes

### New Auth Error Types in transport/errors.go

**File**: `transport/errors.go`
**Changes**: Add `ErrUnauthorized` and `ErrForbidden` error types following existing patterns

```go
// -------------------------------------------------
// ErrUnauthorized is returned when authentication fails (invalid/expired/missing credentials)
// Maps to HTTP 401 Unauthorized
type ErrUnauthorized struct {
    msg string
}

func NewUnauthorized(msg string, args ...any) *ErrUnauthorized {
    return &ErrUnauthorized{msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrUnauthorized) Error() string {
    return e.msg
}

func (e *ErrUnauthorized) Is(target error) bool {
    var err *ErrUnauthorized
    return errors.As(target, &err)
}

func (e *ErrUnauthorized) Code() int {
    return duh.CodeUnauthorized
}

func (e *ErrUnauthorized) ProtoMessage() proto.Message {
    return &v1.Reply{
        Message:  e.msg,
        CodeText: duh.CodeText(duh.CodeUnauthorized),
        Code:     int32(duh.CodeUnauthorized),
        Details:  nil,
    }
}

func (e *ErrUnauthorized) Details() map[string]string {
    return nil
}

func (e *ErrUnauthorized) Message() string {
    return e.msg
}

var _ duh.Error = &ErrUnauthorized{}

// -------------------------------------------------
// ErrForbidden is returned when the user is authenticated but lacks permission
// Maps to HTTP 403 Forbidden
type ErrForbidden struct {
    msg string
}

func NewForbidden(msg string, args ...any) *ErrForbidden {
    return &ErrForbidden{msg: fmt.Sprintf(msg, args...)}
}

func (e *ErrForbidden) Error() string {
    return e.msg
}

func (e *ErrForbidden) Is(target error) bool {
    var err *ErrForbidden
    return errors.As(target, &err)
}

func (e *ErrForbidden) Code() int {
    return duh.CodeForbidden
}

func (e *ErrForbidden) ProtoMessage() proto.Message {
    return &v1.Reply{
        Message:  e.msg,
        CodeText: duh.CodeText(duh.CodeForbidden),
        Code:     int32(duh.CodeForbidden),
        Details:  nil,
    }
}

func (e *ErrForbidden) Details() map[string]string {
    return nil
}

func (e *ErrForbidden) Message() string {
    return e.msg
}

var _ duh.Error = &ErrForbidden{}
```

### Domain Error Variables

**File**: `internal/types/errors.go` (new file)

```go
package types

import "github.com/kapetan-io/querator/transport"

// Namespace errors
var (
    ErrNamespaceNotExist      = transport.NewRequestFailed("namespace does not exist")
    ErrNamespaceAlreadyExists = transport.NewInvalidOption("namespace already exists")
    ErrNamespaceHasQueues     = transport.NewInvalidOption("cannot delete namespace: contains queues")
    ErrNamespaceReserved      = transport.NewInvalidOption("namespace name is reserved")
)

// User errors
var (
    ErrUserNotExist         = transport.NewRequestFailed("user does not exist")
    ErrUserAlreadyExists    = transport.NewInvalidOption("user already exists")
    ErrUsernameAlreadyTaken = transport.NewInvalidOption("username already taken")
)

// API Key errors
var (
    ErrAPIKeyNotExist = transport.NewUnauthorized("API key does not exist")
    ErrAPIKeyExpired  = transport.NewUnauthorized("API key has expired")
    ErrAPIKeyInvalid  = transport.NewUnauthorized("invalid API key")
)

// Role errors
var (
    ErrRoleNotExist              = transport.NewRequestFailed("role does not exist")
    ErrRoleAlreadyExists         = transport.NewInvalidOption("role already exists")
    ErrRoleBindingAlreadyExists  = transport.NewInvalidOption("role binding already exists")
    ErrRoleHasBindings           = transport.NewInvalidOption("cannot delete role: has active bindings")
)

// Authorization errors (use these in service layer)
var (
    ErrAuthRequired = transport.NewUnauthorized("authentication required")
    ErrAccessDenied = transport.NewForbidden("access denied: insufficient permissions")
)
```

### HTTP Status Code Mapping

| Error Type | HTTP Status | When to Use |
|------------|-------------|-------------|
| `transport.ErrInvalidOption` | 400 Bad Request | Invalid input, validation failure |
| `transport.ErrRequestFailed` | 400 Bad Request | Operation failed (resource not found, etc.) |
| `transport.ErrUnauthorized` | 401 Unauthorized | Missing/invalid/expired API key |
| `transport.ErrForbidden` | 403 Forbidden | Valid auth but insufficient permissions |
| `transport.ErrConflict` | 400 Bad Request | Conflict (currently maps to 400) |
| `transport.ErrRetryRequest` | 454 Retry Request | Temporary failure, retry |

### Error Comparison Pattern

All error types implement `Is(target error) bool` which supports `errors.Is()` comparison:

```go
// Correct usage
if errors.Is(err, types.ErrNamespaceNotExist) {
    // handle namespace not found
}

// Also works with error type checking
var authErr *transport.ErrUnauthorized
if errors.As(err, &authErr) {
    // handle auth error
}
```

---

## store.Config Evolution Across Phases

The `store.Config` struct grows incrementally with each phase:

**File**: `internal/store/store.go`

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

## Service and AuthBackend Wiring

### Phase 1-2: Service Config Updates

**File**: `service/service.go`

```go
// Phase 1: No auth enforcement, just namespace storage
type Config struct {
    Log                   *slog.Logger
    StorageConfig         store.Config
    InstanceID            string
    WriteTimeout          clock.Duration
    ReadTimeout           clock.Duration
    MaxLeaseBatchSize     int
    MaxProduceBatchSize   int
    MaxCompleteBatchSize  int
    MaxRequestsPerQueue   int
    MaxConcurrentRequests int
    Clock                 *clock.Provider
}

// Phase 2-3: Add AuthBackend
type Config struct {
    Log                   *slog.Logger
    StorageConfig         store.Config
    AuthBackend           auth.AuthBackend  // NEW Phase 2
    InstanceID            string
    // ... rest unchanged
}
```

### Phase 3: AuthBackend Integration

**File**: `service/service.go`

```go
type Service struct {
    queues         *internal.QueuesManager
    auth           auth.AuthBackend           // NEW Phase 3
    conf           Config
    bootstrapState atomic.Int32               // NEW: 0=Idle, 1=Bootstrapping, 2=Ready
}

const (
    stateIdle          = 0
    stateBootstrapping = 1
    stateReady         = 2
)

func New(conf Config) (*Service, error) {
    // ... existing setup ...

    // Create auth backend if storage is configured
    var authBackend auth.AuthBackend
    if conf.AuthBackend != nil {
        authBackend = conf.AuthBackend
    } else if conf.StorageConfig.Users != nil {
        // Create default auth backend from storage config
        authBackend = auth.NewDefaultAuthBackend(conf.StorageConfig, conf.Log)
    }

    svc := &Service{
        conf:   conf,
        queues: qm,
        auth:   authBackend,
    }

    // Attempt to bootstrap in background to satisfy health checks and log access info
    // This deviates slightly from strict lazy-init (ADR 21) for auth, but provides better UX
    go func() {
        // Use WriteTimeout as this involves writes (creating namespaces, users, roles)
        ctx, cancel := context.WithTimeout(context.Background(), conf.WriteTimeout)
        defer cancel()

        if err := svc.ensureBootstrapped(ctx); err != nil {
            conf.Log.Error("background bootstrap failed", "error", err)
        }
    }()

    return svc, nil
}

// ensureBootstrapped ensures the system is initialized using an atomic state machine.
// It returns an error if bootstrapping fails or is currently in progress.
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

// authorize checks if the current principal has permission in the target namespace
func (s *Service) authorize(ctx context.Context, targetNS, perm string) error {
    // Lazily bootstrap
    if err := s.ensureBootstrapped(ctx); err != nil {
        s.conf.Log.Error("bootstrap failed/pending during authorization", "error", err)
        return err // Pass through the specific error (e.g. "system is starting up")
    }

    if s.auth == nil {
        // Auth not configured - allow all (Phase 1 behavior)
        return nil
    }

    // ... rest of authorize ...
}

// Updated Health method in service/service.go
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
        // We still return the response so we can see other checks
    }

    // ... existing checks ...
    return response, nil
}
```

### HTTPHandler Auth Access

**File**: `transport/http.go`

The HTTPHandler needs access to the AuthBackend for `extractPrincipal()`:

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

func NewHTTPHandler(conf HTTPHandlerConfig) *HTTPHandler {
    return &HTTPHandler{
        service:        conf.Service,
        auth:           conf.AuthBackend,
        log:            conf.Log,
        maxProduceSize: conf.MaxProduceSize,
        // ... prometheus setup
    }
}
```

### Phased extractPrincipal Evolution

**Phase 1**: Always returns AnonymousPrincipal (no auth configured)

```go
func (h *HTTPHandler) extractPrincipal(r *http.Request) types.Principal {
    // Phase 1: No auth - always anonymous
    return types.AnonymousPrincipal
}
```

**Phase 2-3**: Authenticates via API key

```go
func (h *HTTPHandler) extractPrincipal(r *http.Request) (types.Principal, error) {
    // Check for Authorization header
    authHeader := r.Header.Get("Authorization")
    if authHeader == "" {
        return types.AnonymousPrincipal, nil
    }

    // Parse "Bearer <token>"
    if !strings.HasPrefix(authHeader, "Bearer ") {
        return types.Principal{}, transport.NewUnauthorized("invalid authorization header format")
    }
    token := strings.TrimPrefix(authHeader, "Bearer ")

    // Auth not configured - allow anonymous
    if h.auth == nil {
        return types.AnonymousPrincipal, nil
    }

    // Authenticate via AuthBackend
    principal, err := h.auth.Authenticate(r.Context(), token)
    if err != nil {
        return types.Principal{}, err  // Returns 401 error
    }

    return principal, nil
}
```

**ServeHTTP signature change (Phase 2)**:

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

---

## Complete Permission Mapping by Endpoint

This table shows which permission is required for each endpoint:

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

**Note**: `/health` and `/metrics` bypass normal routing in current code. For Phase 3:
- Option A: Keep them unauthenticated (current behavior)
- Option B: Add auth check before returning health/metrics
- Recommendation: Keep unauthenticated by default, optionally check `system.health`/`system.metrics` if auth is configured

---

## Phase 1: Namespace Foundation + Minimal Auth

### Overview
Establish namespace isolation for queues with a minimal authentication layer that defaults to Anonymous principal with Admin access.

### Changes Required

#### 1. Define Namespace Type and Storage Interface

**File**: `internal/types/namespace.go` (new file)

```go
// Namespace represents a logical isolation boundary
type Namespace struct {
    Name        string
    Description string
    CreatedAt   clock.Time
}

// IsReserved returns true if this is a system-reserved namespace
func (n *Namespace) IsReserved() bool
```

**Function responsibilities:**
- `IsReserved()`: Check if name starts with `_` prefix

**File**: `internal/store/namespace.go` (new file)

```go
// Namespaces is storage for namespace metadata
type Namespaces interface {
    Get(ctx context.Context, name string, ns *types.Namespace) error
    Add(ctx context.Context, ns types.Namespace) error
    List(ctx context.Context, namespaces *[]types.Namespace, opts types.ListOptions) error
    Delete(ctx context.Context, name string) error
    Close(ctx context.Context) error
}
```

**Function responsibilities:**
- Follow patterns from existing `Queues` interface in `internal/store/store.go:29-51`
- Implement lazy initialization per ADR 0021
- Return `ErrNamespaceNotExist` / `ErrNamespaceAlreadyExists` errors

#### 2. Add Namespace Field to QueueInfo

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

**File**: `proto/queues.proto`
**Changes**: Add `namespace` field to `QueueInfo` message

```protobuf
message QueueInfo {
    string namespace = 10;       // NEW
    string queue_name = 1;
    // ... existing fields
}
```

#### 3. Implement Namespace Storage Backends

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
- Follow patterns from `MemoryQueues` in `internal/store/memory.go`
- Thread-safe with mutex protection
- Return appropriate errors for not-found/already-exists cases

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
- Follow patterns from `BadgerQueues` in `internal/store/badger.go`
- Use key prefix `ns:` to distinguish from queue data
- Implement lazy initialization

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
- Follow patterns from `PostgresQueues` in `internal/store/postgres.go`
- Create table: `namespaces (name TEXT PRIMARY KEY, description TEXT, created_at TIMESTAMPTZ)`
- Implement lazy initialization

#### 4. Update Storage Config

**File**: `internal/store/store.go`
**Changes**: Add `Namespaces` field to `Config`

```go
type Config struct {
    Queues           Queues
    Namespaces       Namespaces  // NEW
    PartitionStorage []PartitionStorage
}
```

#### 5. Define Principal and Auth Types

**File**: `internal/types/auth.go` (new file)

```go
// Principal represents the security identity for a request
type Principal struct {
    User           User
    NamespaceScope *string // If set, restricts access to this namespace only
    IsAnonymous    bool
}

// User represents a user in the system
type User struct {
    ID         string
    Username   string
    ExternalID string
    Email      string
    CreatedAt  clock.Time
    UpdatedAt  clock.Time
}

// AnonymousUser is the reserved user for unauthenticated requests
var AnonymousUser = User{
    ID:       "anonymous",
    Username: "anonymous",
}

// AnonymousPrincipal is the default principal for unauthenticated requests
var AnonymousPrincipal = Principal{
    User:        AnonymousUser,
    IsAnonymous: true,
}
```

#### 6. Add Context Key for Principal

**File**: `internal/context.go` (new file)

```go
type contextKey string

const principalKey contextKey = "principal"

// PrincipalFromContext extracts the Principal from context
func PrincipalFromContext(ctx context.Context) Principal

// ContextWithPrincipal adds Principal to context
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context
```

**Function responsibilities:**
- Return `AnonymousPrincipal` if no principal in context
- Thread-safe context value handling

#### 7. Add Auth Middleware to HTTP Handler

**File**: `transport/http.go`
**Changes**: Add authentication middleware

```go
// extractPrincipal extracts the Principal from the request
func (h *HTTPHandler) extractPrincipal(r *http.Request) Principal

// requireNamespace extracts the target namespace from request
func (h *HTTPHandler) requireNamespace(r *http.Request, req interface{}) (string, error)
```

**Function responsibilities:**
- `extractPrincipal()`: Parse `Authorization: Bearer <token>` header, return `AnonymousPrincipal` if missing
- `requireNamespace()`: Extract namespace from request body or header
- Update `ServeHTTP()` to inject principal into context before routing

**Changes to `ServeHTTP()`:**
```go
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    defer prometheus.NewTimer(h.duration.WithLabelValues(r.URL.Path)).ObserveDuration()

    // NEW: Extract and inject principal
    principal := h.extractPrincipal(r)
    ctx := internal.ContextWithPrincipal(r.Context(), principal)

    // ... rest of routing
}
```

#### 8. Add Namespace Validation to Service Layer

**File**: `service/validation.go`
**Changes**: Add namespace validation functions

```go
func validateNamespaceName(name string) error
func validateNamespaceCreate(req *proto.NamespaceCreateRequest) error
```

**Function responsibilities:**
- Validate name is not empty, not reserved (unless creating `_system`), no whitespace
- Max length 500 characters
- Follow validation patterns from existing `validateQueueOptionsProto()`

#### 9. Add Namespace Service Methods

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
- `NamespacesDelete()`: Check no queues exist in namespace before deletion

#### 10. Add Namespace HTTP Endpoints

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
- Follow handler patterns from existing queue endpoints
- Add routes to `ServeHTTP()` switch statement

#### 11. Add Proto Definitions for Namespaces

**File**: `proto/namespaces.proto` (new file)

```protobuf
syntax = "proto3";
package proto;
option go_package = "github.com/kapetan-io/querator/proto";

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

#### 12. Update Queue Operations for Namespace

**File**: `service/service.go`
**Changes**: Update queue operations to require namespace

```go
func (s *Service) QueuesCreate(ctx context.Context, req *proto.QueueInfo) error
```

**Function responsibilities:**
- Validate namespace field is present and exists
- Ensure namespace exists before creating queue
- Update queue storage to include namespace

**File**: `internal/queues_manager.go`
**Changes**: Update queue management for namespace awareness

```go
// Update signature to accept namespace explicitly
func (qm *QueuesManager) Get(ctx context.Context, namespace, queueName string) (*Queue, error)

// Create already uses QueueInfo which has Namespace field
func (qm *QueuesManager) Create(ctx context.Context, info types.QueueInfo) (*Queue, error)
```

**Function responsibilities:**
- Validate namespace exists before queue creation
- Store queue with namespace association
- Update queue lookup to use composite key: `{namespace}/{queueName}`

**Queue Key Design:**

```go
// internal/queues_manager.go - Update queue map key
type QueuesManager struct {
    queues map[string]*Queue  // Key changes from "queueName" to "namespace/queueName"
    // ...
}

// Helper to build queue key
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

**Storage Key Design:**

| Backend | Queue Key Format |
|---------|------------------|
| InMemory | Map key: `namespace/queueName` |
| BadgerDB | Key prefix: `queue:{namespace}:{queueName}` |
| PostgreSQL | WHERE clause: `namespace = $1 AND name = $2` |

#### 13. Update Transport Service Interface

**File**: `transport/interfaces.go`
**Changes**: Add NamespaceAdmin interface

```go
// NamespaceAdmin provides namespace management operations
type NamespaceAdmin interface {
    NamespacesCreate(ctx context.Context, req *pb.NamespaceCreateRequest) error
    NamespacesList(ctx context.Context, req *pb.NamespacesListRequest, resp *pb.NamespacesListResponse) error
    NamespacesDelete(ctx context.Context, req *pb.NamespacesDeleteRequest) error
}

// Update main Service interface
type Service interface {
    QueueOps
    QueueAdmin
    StorageInspector
    NamespaceAdmin  // NEW
    Health(ctx context.Context) (*HealthResponse, error)
}
```

#### 14. Add Client Methods for Namespaces

**File**: `client.go`
**Changes**: Add namespace client methods

```go
func (c *Client) NamespacesCreate(ctx context.Context, req *pb.NamespaceCreateRequest) error
func (c *Client) NamespacesList(ctx context.Context, res *pb.NamespacesListResponse, opts *ListOptions) error
func (c *Client) NamespacesDelete(ctx context.Context, req *pb.NamespacesDeleteRequest) error
```

**Function responsibilities:**
- Follow client patterns from existing queue methods

#### 16. Namespace Deletion Validation

**File**: `service/service.go`
**Changes**: Validate namespace is empty before deletion

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

#### 17. Bootstrap _system Namespace

**File**: `service/service.go` or `internal/bootstrap.go` (new file)
**Changes**: Ensure `_system` namespace exists on startup

```go
func (s *Service) Bootstrap(ctx context.Context) error {
    // Create _system namespace if not exists
    ns := types.Namespace{
        Name:        "_system",
        Description: "System administration namespace",
        CreatedAt:   s.conf.Clock.Now().UTC(),
    }

    if err := s.conf.StorageConfig.Namespaces.Add(ctx, ns); err != nil {
        // Ignore "already exists" error - bootstrap is idempotent
        if !errors.Is(err, types.ErrNamespaceAlreadyExists) {
            return err
        }
    }

    return nil
}
```

**Function responsibilities:**
- Create `_system` namespace if not exists
- Called during service initialization in `New()`
- Idempotent operation - safe to call on every startup

### Testing Requirements

**File**: `service/namespace_test.go` (new file)

```go
package service_test

func TestNamespaces(t *testing.T) {
    badgerdb := badgerTestSetup{Dir: t.TempDir()}
    postgres := postgresTestSetup{}

    for _, tc := range []struct {
        Setup    NewStorageFunc
        TearDown func()
        Name     string
    }{
        {Name: "InMemory", Setup: func() store.Config { return setupMemoryStorage(store.Config{}) }, TearDown: func() {}},
        {Name: "BadgerDB", Setup: func() store.Config { return badgerdb.Setup(store.BadgerConfig{}) }, TearDown: badgerdb.Teardown},
        {Name: "PostgreSQL", Setup: func() store.Config { return postgres.Setup(store.PostgresConfig{}) }, TearDown: postgres.Teardown},
    } {
        t.Run(tc.Name, func(t *testing.T) {
            testNamespaces(t, tc.Setup, tc.TearDown)
        })
    }
}

func testNamespaces(t *testing.T, setup NewStorageFunc, tearDown func()) {
    defer goleak.VerifyNone(t, goleakOptions...)

    t.Run("CreateNamespace", func(t *testing.T) {
        d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
        defer func() {
            d.Shutdown(t)
            tearDown()
        }()

        nsName := random.String("ns-", 10)
        err := c.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{
            Name:        nsName,
            Description: "Test namespace",
        })
        require.NoError(t, err)

        // Verify namespace exists in list
        var resp pb.NamespacesListResponse
        err = c.NamespacesList(ctx, &resp, nil)
        require.NoError(t, err)

        found := false
        for _, ns := range resp.Items {
            if ns.Name == nsName {
                found = true
                break
            }
        }
        assert.True(t, found)
    })

    t.Run("CreateQueueRequiresNamespace", func(t *testing.T) {
        d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
        defer func() {
            d.Shutdown(t)
            tearDown()
        }()

        // Create namespace first
        nsName := random.String("ns-", 10)
        require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: nsName}))

        // Create queue with valid namespace
        queueName := random.String("queue-", 10)
        err := c.QueuesCreate(ctx, &pb.QueueInfo{
            RequestedPartitions: 1,
            ExpireTimeout:       "24h",
            LeaseTimeout:        "1m",
            QueueName:           queueName,
            Namespace:           nsName,
        })
        require.NoError(t, err)
    })

    t.Run("Errors", func(t *testing.T) {
        t.Run("NamespacesCreate", func(t *testing.T) {
            t.Run("ReservedPrefix", func(t *testing.T) {
                d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
                defer func() {
                    d.Shutdown(t)
                    tearDown()
                }()

                err := c.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: "_reserved"})
                var e duh.Error
                require.True(t, errors.As(err, &e))
                assert.Equal(t, duh.CodeBadRequest, e.Code())
            })

            t.Run("DuplicateName", func(t *testing.T) {
                d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
                defer func() {
                    d.Shutdown(t)
                    tearDown()
                }()

                nsName := random.String("ns-", 10)
                require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: nsName}))

                // Second create should fail
                err := c.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: nsName})
                var e duh.Error
                require.True(t, errors.As(err, &e))
                assert.Equal(t, duh.CodeBadRequest, e.Code())
            })
        })

        t.Run("NamespacesDelete", func(t *testing.T) {
            t.Run("HasQueues", func(t *testing.T) {
                d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
                defer func() {
                    d.Shutdown(t)
                    tearDown()
                }()

                // Create namespace with queue
                nsName := random.String("ns-", 10)
                require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: nsName}))
                require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
                    RequestedPartitions: 1,
                    ExpireTimeout:       "24h",
                    LeaseTimeout:        "1m",
                    QueueName:           "test-queue",
                    Namespace:           nsName,
                }))

                // Delete should fail
                err := c.NamespacesDelete(ctx, &pb.NamespacesDeleteRequest{Name: nsName})
                require.ErrorContains(t, err, "contains queue")
            })
        })

        t.Run("QueuesCreate", func(t *testing.T) {
            t.Run("NamespaceNotExist", func(t *testing.T) {
                d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
                defer func() {
                    d.Shutdown(t)
                    tearDown()
                }()

                err := c.QueuesCreate(ctx, &pb.QueueInfo{
                    RequestedPartitions: 1,
                    ExpireTimeout:       "24h",
                    LeaseTimeout:        "1m",
                    Namespace:           "nonexistent",
                    QueueName:           "test-queue",
                })
                require.ErrorContains(t, err, "namespace does not exist")
            })
        })
    })
}
```

**Test objectives:**
- Namespace CRUD operations across all storage backends
- Reserved namespace (`_system`) protection
- Namespace deletion blocked when queues exist
- Queue creation requires valid namespace
- Queue operations scoped to namespace

**Key scenarios:**
- Create/list/delete namespaces
- Attempt to create namespace starting with `_` (should fail except `_system`)
- Create queue without namespace (should fail)
- Create queue with non-existent namespace (should fail)
- Delete namespace with queues (should fail)

### Validation
- [ ] Run: `go test ./... -run "TestNamespaces"`
- [ ] Run: `go test ./... -run "TestQueue"` (verify existing tests still pass with namespace)
- [ ] Run: `make proto` (regenerate proto files)
- [ ] Verify: All three backends (InMemory, BadgerDB, PostgreSQL) pass tests

### Context for implementation
- Namespace storage follows patterns from `internal/store/store.go:29-51` (Queues interface)
- Validation follows patterns from `service/validation.go`
- HTTP handlers follow patterns from `transport/http.go:242-313`
- Test structure follows `service/queue_test.go` multi-backend pattern

---

## Phase 2: Users + API Keys

### Overview
Implement user management and API key authentication. After this phase, requests can be authenticated via Bearer tokens.

### Changes Required

#### 1. Define User Storage Interface

**File**: `internal/store/users.go` (new file)

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
- Follow patterns from `Namespaces` interface
- Support lookup by ID and username
- Username must be unique

#### 2. Define API Key Types and Storage

**File**: `internal/types/auth.go`
**Changes**: Add APIKey type

```go
type APIKey struct {
    ID             string
    UserID         string
    NamespaceScope *string
    Name           string
    KeyHash        string     // SHA-256 hash
    KeyPrefix      string     // first 8 chars for identification
    ExpiresAt      *clock.Time
    LastUsedAt     *clock.Time
    CreatedAt      clock.Time
}
```

**File**: `internal/store/apikeys.go` (new file)

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
- `UpdateLastUsed()`: Update last used timestamp (fire-and-forget, non-blocking)

#### 3. Implement User Storage Backends

**File**: `internal/store/memory.go`
**Changes**: Add `MemoryUsers` implementation

```go
func NewMemoryUsers(log *slog.Logger) *MemoryUsers
```

**File**: `internal/store/badger.go`
**Changes**: Add `BadgerUsers` implementation

```go
func NewBadgerUsers(conf BadgerConfig) *BadgerUsers
```

**File**: `internal/store/postgres.go`
**Changes**: Add `PostgresUsers` implementation

```go
func NewPostgresUsers(conf PostgresConfig) *PostgresUsers
```

**Function responsibilities:**
- PostgreSQL table: `users (id TEXT PRIMARY KEY, username TEXT UNIQUE, external_id TEXT, email TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)`
- Follow patterns established in Phase 1 for namespaces

#### 4. Implement API Key Storage Backends

**File**: `internal/store/memory.go`
**Changes**: Add `MemoryAPIKeys` implementation

```go
func NewMemoryAPIKeys(log *slog.Logger) *MemoryAPIKeys
```

**File**: `internal/store/badger.go`
**Changes**: Add `BadgerAPIKeys` implementation

```go
func NewBadgerAPIKeys(conf BadgerConfig) *BadgerAPIKeys
```

**File**: `internal/store/postgres.go`
**Changes**: Add `PostgresAPIKeys` implementation

```go
func NewPostgresAPIKeys(conf PostgresConfig) *PostgresAPIKeys
```

**Function responsibilities:**
- PostgreSQL table: `api_keys (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users(id), namespace_scope TEXT, name TEXT, key_hash TEXT UNIQUE, key_prefix TEXT, expires_at TIMESTAMPTZ, last_used_at TIMESTAMPTZ, created_at TIMESTAMPTZ)`
- Index on `key_hash` for fast authentication lookups

#### 5. API Key Generation Utilities

**File**: `internal/auth/apikey.go` (new file)

```go
// GenerateAPIKey creates a new API key in format sk-[env]-[entropy]
func GenerateAPIKey(env string) (rawKey string, hash string, prefix string)

// HashAPIKey computes SHA-256 hash of a raw key
func HashAPIKey(rawKey string) string

// ValidateAPIKeyFormat checks if key matches sk-[env]-[entropy] format
func ValidateAPIKeyFormat(key string) error
```

**Function responsibilities:**
- `GenerateAPIKey()`: Generate 32+ character Base62 entropy, default env to "live"
- `HashAPIKey()`: SHA-256 hash
- `ValidateAPIKeyFormat()`: Regex validation `^sk-[a-z0-9]+-[a-zA-Z0-9]{32,}$`

#### 6. Update Storage Config

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

#### 7. Implement Authentication Middleware

**File**: `transport/http.go`
**Changes**: Update `extractPrincipal()` to authenticate via API key

```go
func (h *HTTPHandler) extractPrincipal(r *http.Request) (Principal, error)
```

**Function responsibilities:**
- Parse `Authorization: Bearer <key>` header
- Delegate validation to `AuthBackend.Authenticate()` (which handles caching/throttling)
- Return `AnonymousPrincipal` if no header present
- Return error for invalid/expired keys (401 Unauthorized)

#### 8. Add User Service Methods

**File**: `service/service.go`
**Changes**: Add user management methods

```go
func (s *Service) UsersCreate(ctx context.Context, req *proto.UserCreateRequest, resp *proto.UserCreateResponse) error
func (s *Service) UsersList(ctx context.Context, req *proto.UsersListRequest, resp *proto.UsersListResponse) error
func (s *Service) UsersDelete(ctx context.Context, req *proto.UsersDeleteRequest) error
```

**Function responsibilities:**
- Generate unique user ID
- Validate username uniqueness
- Delete should check for associated API keys (optional: cascade delete keys)

#### 9. Add API Key Service Methods

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
- `APIKeysDelete()`: Allow deletion of any key (self-lockout permitted per spec)

#### 10. Add HTTP Endpoints for Users and API Keys

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

#### 11. Add Proto Definitions

**File**: `proto/auth.proto` (new file)

```protobuf
syntax = "proto3";
package proto;
option go_package = "github.com/kapetan-io/querator/proto";

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
    string env_tag = 5;  // defaults to "live"
}

message APIKeyCreateResponse {
    string id = 1;
    string key = 2;      // Raw key, shown ONLY here
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

#### 12. Add Client Methods

**File**: `client.go`
**Changes**: Add client methods and auth header support

```go
type ClientConfig struct {
    Client   *http.Client
    Endpoint string
    APIKey   string  // NEW: Optional API key for authentication
}

func (c *Client) UsersCreate(ctx context.Context, req *pb.UserCreateRequest, resp *pb.UserCreateResponse) error
func (c *Client) UsersList(ctx context.Context, resp *pb.UsersListResponse, opts *ListOptions) error
func (c *Client) UsersDelete(ctx context.Context, req *pb.UsersDeleteRequest) error
func (c *Client) APIKeysCreate(ctx context.Context, req *pb.APIKeyCreateRequest, resp *pb.APIKeyCreateResponse) error
func (c *Client) APIKeysList(ctx context.Context, req *pb.APIKeysListRequest, resp *pb.APIKeysListResponse) error
func (c *Client) APIKeysDelete(ctx context.Context, req *pb.APIKeysDeleteRequest) error
```

**Function responsibilities:**
- Update all request methods to include `Authorization: Bearer <key>` header if `APIKey` is set
- Follow existing client patterns

#### 13. Bootstrap Anonymous User

**File**: `service/service.go` or `internal/bootstrap.go`
**Changes**: Update `Bootstrap()` to create Anonymous user

```go
func (s *Service) Bootstrap(ctx context.Context) error
```

**Function responsibilities:**
- Create `_system` namespace (from Phase 1)
- Create Anonymous user if not exists (ID: "anonymous", Username: "anonymous")
- Idempotent

#### 14. High-Performance Auth Backend (Caching)

**File**: `internal/auth/cache.go` (new file)

To prevent database read bottlenecks and write amplification on `LastUsedAt` updates, we implement an in-memory cache.

```go
type CacheEntry struct {
    Key        types.APIKey
    LastUsedAt time.Time // Local tracker for throttling
    ExpiresAt  time.Time // Cache TTL
}

// Simple thread-safe cache
type AuthCache struct {
    entries sync.Map // map[string]*CacheEntry
}

func (c *AuthCache) Get(hash string) (*types.APIKey, bool)
func (c *AuthCache) Put(hash string, key types.APIKey)
```

**Updated `Authenticate` Logic:**
1. **Hash Token**: `hash := HashAPIKey(token)`
2. **Read-Through**: Check cache. If miss, load from DB and cache it.
3. **Write-Throttling**: Check `LastUsedAt`. If `time.Since(lastUpdated) > 1 * time.Minute`, fire async update `apiKeys.UpdateLastUsed()` AND update cache entry.

### Testing Requirements

**File**: `service/users_test.go` (new file)

```go
func TestUsers(t *testing.T)
```

**Test objectives:**
- User CRUD operations
- Username uniqueness validation
- API key generation and format validation
- Authentication via Bearer token
- Key expiration handling
- Namespace-scoped key restrictions

**Key scenarios:**
- Create user, create API key, authenticate with key
- Key format validation (sk-[env]-[entropy])
- Expired key rejection
- Namespace-scoped key cannot access other namespaces
- Self-lockout (delete own key) permitted

### Validation
- [ ] Run: `go test ./... -run "TestUsers"`
- [ ] Run: `go test ./... -run "TestNamespaces"` (Phase 1 still passes)
- [ ] Run: `make proto`
- [ ] Verify: Authentication works with valid API key
- [ ] Verify: Anonymous requests still work (return AnonymousPrincipal)

### Context for implementation
- API key hashing: Use `crypto/sha256` standard library
- Key generation: Use `crypto/rand` for secure entropy
- Authentication middleware follows `transport/http.go:105-127` ServeHTTP pattern
- User/APIKey storage follows patterns from Phase 1 namespace storage

---

## Phase 3: Roles + RBAC

### Overview
Implement role-based access control with cascading permission checks. After this phase, authorization is fully enforced.

### Changes Required

#### 1. Define Role and RoleBinding Types

**File**: `internal/types/auth.go`
**Changes**: Add Role and RoleBinding types

```go
type Role struct {
    ID          string
    Namespace   string      // owning namespace
    Name        string
    Permissions []string
    CreatedAt   clock.Time
}

type RoleBinding struct {
    ID        string
    Namespace string      // scope
    UserID    string
    RoleID    string
    CreatedAt clock.Time
}
```

#### 2. Define Permission Constants

**File**: `internal/auth/permissions.go` (new file)

```go
const (
    // Namespace operations
    PermNamespaceCreate = "namespace.create"
    PermNamespaceDelete = "namespace.delete"
    PermNamespaceList   = "namespace.list"

    // Queue operations
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

    // User/Key operations
    PermUserCreate   = "user.create"
    PermUserDelete   = "user.delete"
    PermUserList     = "user.list"
    PermAPIKeyCreate = "apikey.create"
    PermAPIKeyDelete = "apikey.delete"
    PermAPIKeyList   = "apikey.list"

    // Role operations
    PermRoleCreate        = "role.create"
    PermRoleDelete        = "role.delete"
    PermRoleList          = "role.list"
    PermRoleBindingCreate = "rolebinding.create"
    PermRoleBindingDelete = "rolebinding.delete"
    PermRoleBindingList   = "rolebinding.list"

    // System operations
    PermSystemHealth  = "system.health"
    PermSystemMetrics = "system.metrics"
)

// StandardRoles defines code-first roles synchronized at startup
var StandardRoles = map[string]Role{
    "Admin": {
        Name:        "Admin",
        Permissions: []string{/* all permissions */},
    },
    "NamespaceOwner": {
        Name:        "NamespaceOwner",
        Permissions: []string{/* namespace-scoped permissions */},
    },
    "PublicViewer": {
        Name:        "PublicViewer",
        Permissions: []string{PermSystemHealth, PermSystemMetrics},
    },
}
```

#### 3. Define Role Storage Interface

**File**: `internal/store/roles.go` (new file)

```go
type Roles interface {
    Get(ctx context.Context, namespace, name string, role *types.Role) error
    GetByID(ctx context.Context, id string, role *types.Role) error
    Add(ctx context.Context, role types.Role) error
    List(ctx context.Context, namespace string, roles *[]types.Role, opts types.ListOptions) error
    Delete(ctx context.Context, id string) error
    Close(ctx context.Context) error
}

type RoleBindings interface {
    Get(ctx context.Context, id string, binding *types.RoleBinding) error
    Add(ctx context.Context, binding types.RoleBinding) error
    List(ctx context.Context, namespace string, bindings *[]types.RoleBinding, opts types.ListOptions) error
    ListByUser(ctx context.Context, userID string, bindings *[]types.RoleBinding) error
    Delete(ctx context.Context, id string) error
    Close(ctx context.Context) error
}
```

#### 4. Implement Role Storage Backends

**File**: `internal/store/memory.go`, `internal/store/badger.go`, `internal/store/postgres.go`
**Changes**: Add Role and RoleBinding implementations for all backends

**Function responsibilities:**
- PostgreSQL tables:
  - `roles (id TEXT PRIMARY KEY, namespace TEXT, name TEXT, permissions TEXT[], created_at TIMESTAMPTZ, UNIQUE(namespace, name))`
  - `role_bindings (id TEXT PRIMARY KEY, namespace TEXT, user_id TEXT, role_id TEXT, created_at TIMESTAMPTZ, UNIQUE(namespace, user_id, role_id))`

#### 5. Implement AuthBackend Interface

**File**: `internal/auth/backend.go` (new file)

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
    log          *slog.Logger
    writeTimeout clock.Duration
}

type AuthBackendConfig struct {
    StorageConfig store.Config
    Log           *slog.Logger
    WriteTimeout  clock.Duration
}

func NewAuthBackend(conf AuthBackendConfig) *DefaultAuthBackend {
    return &DefaultAuthBackend{
        users:        conf.StorageConfig.Users,
        apiKeys:      conf.StorageConfig.APIKeys,
        roles:        conf.StorageConfig.Roles,
        roleBindings: conf.StorageConfig.RoleBindings,
        log:          conf.Log,
        writeTimeout: conf.WriteTimeout,
    }
}

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
    // Look for role bindings in the target namespace
    if hasPermInNamespace, err := a.checkPermissionInNamespace(ctx, principal.User.ID, targetNS, perm); err != nil {
        return false, err
    } else if hasPermInNamespace {
        return true, nil
    }

    // Step 3: Check _system Namespace (Admin Cascade)
    // Permissions in _system cascade to all namespaces
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
    // Get all role bindings for this user in this namespace
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

#### 6. Add Authorization to Service Layer

**File**: `service/service.go`
**Changes**: Add authorization checks to all methods

```go
func (s *Service) authorize(ctx context.Context, targetNS, perm string) error
```

**Function responsibilities:**
- Extract Principal from context
- Call `HasPermission()`
- Return 403 Forbidden error if denied

**Update all service methods:**
```go
func (s *Service) QueueProduce(ctx context.Context, req *proto.QueueProduceRequest) error {
    // NEW: Authorization check
    if err := s.authorize(ctx, req.Namespace, PermQueueProduce); err != nil {
        return err
    }
    // ... existing logic
}
```

#### 7. Add Role Service Methods

**File**: `service/service.go`
**Changes**: Add role management methods

```go
func (s *Service) RolesCreate(ctx context.Context, req *proto.RoleCreateRequest) error
func (s *Service) RolesList(ctx context.Context, req *proto.RolesListRequest, resp *proto.RolesListResponse) error
func (s *Service) RolesDelete(ctx context.Context, req *proto.RolesDeleteRequest) error
func (s *Service) RoleBindingsCreate(ctx context.Context, req *proto.RoleBindingCreateRequest) error
func (s *Service) RoleBindingsList(ctx context.Context, req *proto.RoleBindingsListRequest, resp *proto.RoleBindingsListResponse) error
func (s *Service) RoleBindingsDelete(ctx context.Context, req *proto.RoleBindingDeleteRequest) error
```

#### 8. Add HTTP Endpoints for Roles

**File**: `transport/http.go`
**Changes**: Add role endpoint handlers

```go
const (
    RPCRolesCreate         = "/v1/roles.create"
    RPCRolesList           = "/v1/roles.list"
    RPCRolesDelete         = "/v1/roles.delete"
    RPCRoleBindingsCreate  = "/v1/role-bindings.create"
    RPCRoleBindingsList    = "/v1/role-bindings.list"
    RPCRoleBindingsDelete  = "/v1/role-bindings.delete"
)
```

#### 9. Add Proto Definitions for Roles

**File**: `proto/auth.proto`
**Changes**: Add role messages

```protobuf
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

#### 10. Update Bootstrap for Standard Roles

**File**: `service/service.go` or `internal/bootstrap.go`
**Changes**: Complete bootstrap procedure

**Complete Bootstrap Implementation:**

```go
// Bootstrap initializes the system with required data structures.
// This is idempotent and safe to call on every startup.
func (s *Service) Bootstrap(ctx context.Context) error {
    // Step 1: Create _system namespace (from Phase 1)
    if err := s.bootstrapSystemNamespace(ctx); err != nil {
        return fmt.Errorf("failed to bootstrap _system namespace: %w", err)
    }

    // Step 2: Create Anonymous user (from Phase 2)
    if err := s.bootstrapAnonymousUser(ctx); err != nil {
        return fmt.Errorf("failed to bootstrap anonymous user: %w", err)
    }

    // Step 3: Sync standard roles to _system namespace
    if err := s.bootstrapStandardRoles(ctx); err != nil {
        return fmt.Errorf("failed to bootstrap standard roles: %w", err)
    }

    // Step 4: Create Anonymous -> Admin binding (Open Door policy)
    if err := s.bootstrapAnonymousAdminBinding(ctx); err != nil {
        return fmt.Errorf("failed to bootstrap anonymous admin binding: %w", err)
    }

    return nil
}

func (s *Service) bootstrapSystemNamespace(ctx context.Context) error {
    ns := types.Namespace{
        Name:        "_system",
        Description: "System administration namespace",
        CreatedAt:   s.conf.Clock.Now().UTC(),
    }

    if err := s.conf.StorageConfig.Namespaces.Add(ctx, ns); err != nil {
        if !errors.Is(err, types.ErrNamespaceAlreadyExists) {
            return err
        }
    }
    return nil
}

func (s *Service) bootstrapAnonymousUser(ctx context.Context) error {
    user := types.User{
        ID:        "anonymous",
        Username:  "anonymous",
        CreatedAt: s.conf.Clock.Now().UTC(),
        UpdatedAt: s.conf.Clock.Now().UTC(),
    }

    if err := s.conf.StorageConfig.Users.Add(ctx, user); err != nil {
        if !errors.Is(err, types.ErrUserAlreadyExists) {
            return err
        }
    }
    return nil
}

func (s *Service) bootstrapStandardRoles(ctx context.Context) error {
    standardRoles := []types.Role{
        {
            ID:        "admin",
            Namespace: "_system",
            Name:      "Admin",
            Permissions: []string{
                // All permissions - Admin has full access
                auth.PermNamespaceCreate, auth.PermNamespaceDelete, auth.PermNamespaceList,
                auth.PermQueueCreate, auth.PermQueueDelete, auth.PermQueueUpdate, auth.PermQueueList,
                auth.PermQueueProduce, auth.PermQueueLease, auth.PermQueueComplete, auth.PermQueueRetry,
                auth.PermQueueStats, auth.PermQueueClear,
                auth.PermUserCreate, auth.PermUserDelete, auth.PermUserList,
                auth.PermAPIKeyCreate, auth.PermAPIKeyDelete, auth.PermAPIKeyList,
                auth.PermRoleCreate, auth.PermRoleDelete, auth.PermRoleList,
                auth.PermRoleBindingCreate, auth.PermRoleBindingDelete, auth.PermRoleBindingList,
                auth.PermSystemHealth, auth.PermSystemMetrics,
            },
            CreatedAt: s.conf.Clock.Now().UTC(),
        },
        {
            ID:        "namespace-owner",
            Namespace: "_system",
            Name:      "NamespaceOwner",
            Permissions: []string{
                // Namespace-scoped full access (no namespace create/delete)
                auth.PermQueueCreate, auth.PermQueueDelete, auth.PermQueueUpdate, auth.PermQueueList,
                auth.PermQueueProduce, auth.PermQueueLease, auth.PermQueueComplete, auth.PermQueueRetry,
                auth.PermQueueStats, auth.PermQueueClear,
                auth.PermRoleCreate, auth.PermRoleDelete, auth.PermRoleList,
                auth.PermRoleBindingCreate, auth.PermRoleBindingDelete, auth.PermRoleBindingList,
            },
            CreatedAt: s.conf.Clock.Now().UTC(),
        },
        {
            ID:        "public-viewer",
            Namespace: "_system",
            Name:      "PublicViewer",
            Permissions: []string{
                auth.PermSystemHealth,
                auth.PermSystemMetrics,
            },
            CreatedAt: s.conf.Clock.Now().UTC(),
        },
    }

    for _, role := range standardRoles {
        if err := s.conf.StorageConfig.Roles.Add(ctx, role); err != nil {
            if !errors.Is(err, types.ErrRoleAlreadyExists) {
                return err
            }
        }
    }
    return nil
}

func (s *Service) bootstrapAnonymousAdminBinding(ctx context.Context) error {
    // Create the "Open Door" binding: Anonymous user has Admin role in _system
    // This gives anonymous access to everything by default (cascades to all namespaces)
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
        // If binding exists, we are likely in Open Door mode (unless it's a different binding with same ID?)
        // We log the warning anyway to be safe.
    }
    
    // Log the Open Door status to help admins find it
    s.conf.Log.Warn("SYSTEM RUNNING IN OPEN DOOR MODE", 
        "details", "Anonymous users have ADMIN access. To lock down, create an admin user and revoke the 'anonymous-admin' binding.")

    return nil
}
```

**Lock-Down Procedure:**

To secure a production deployment, administrators should:

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

**Bootstrap Sequence Diagram:**

```
Service.New()
    │
    ▼
Bootstrap(ctx)
    │
    ├─► bootstrapSystemNamespace()
    │       └─► Create "_system" namespace (idempotent)
    │
    ├─► bootstrapAnonymousUser()
    │       └─► Create "anonymous" user (idempotent)
    │
    ├─► bootstrapStandardRoles()
    │       ├─► Create "Admin" role in _system
    │       ├─► Create "NamespaceOwner" role in _system
    │       └─► Create "PublicViewer" role in _system
    │
    └─► bootstrapAnonymousAdminBinding()
            └─► Bind anonymous → Admin in _system
                (Open Door policy - delete to lock down)
```

#### 11. Add Client Methods for Roles

**File**: `client.go`
**Changes**: Add role client methods

```go
func (c *Client) RolesCreate(ctx context.Context, req *pb.RoleCreateRequest) error
func (c *Client) RolesList(ctx context.Context, req *pb.RolesListRequest, resp *pb.RolesListResponse) error
func (c *Client) RolesDelete(ctx context.Context, req *pb.RolesDeleteRequest) error
func (c *Client) RoleBindingsCreate(ctx context.Context, req *pb.RoleBindingCreateRequest) error
func (c *Client) RoleBindingsList(ctx context.Context, req *pb.RoleBindingsListRequest, resp *pb.RoleBindingsListResponse) error
func (c *Client) RoleBindingsDelete(ctx context.Context, req *pb.RoleBindingDeleteRequest) error
```

#### 12. Update OpenAPI Specification and Documentation

**File**: `openapi.yaml`
**Changes**: Add definitions for all new endpoints and schemas introduced in Phases 1, 2, and 3.
- Namespace endpoints and schemas
- User and API Key endpoints and schemas
- Role and RoleBinding endpoints and schemas
- Update existing queue endpoints to include `namespace` parameter

**File**: `docs/`
**Changes**:
- Update API documentation to describe the new authentication and authorization model.
- Document the "Open Door" policy and how to lock down the cluster.
- Document standard roles and permissions.

### Testing Requirements

**File**: `service/auth_test.go` (new file)

```go
package service_test

func TestAuth(t *testing.T) {
    badgerdb := badgerTestSetup{Dir: t.TempDir()}
    postgres := postgresTestSetup{}

    for _, tc := range []struct {
        Setup    NewStorageFunc
        TearDown func()
        Name     string
    }{
        {Name: "InMemory", Setup: func() store.Config { return setupMemoryStorage(store.Config{}) }, TearDown: func() {}},
        {Name: "BadgerDB", Setup: func() store.Config { return badgerdb.Setup(store.BadgerConfig{}) }, TearDown: badgerdb.Teardown},
        {Name: "PostgreSQL", Setup: func() store.Config { return postgres.Setup(store.PostgresConfig{}) }, TearDown: postgres.Teardown},
    } {
        t.Run(tc.Name, func(t *testing.T) {
            testAuth(t, tc.Setup, tc.TearDown)
        })
    }
}

func testAuth(t *testing.T, setup NewStorageFunc, tearDown func()) {
    defer goleak.VerifyNone(t, goleakOptions...)

    t.Run("AnonymousHasAdminByDefault", func(t *testing.T) {
        d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
        defer func() {
            d.Shutdown(t)
            tearDown()
        }()

        // Anonymous should be able to create namespaces (Admin permission)
        nsName := random.String("ns-", 10)
        err := c.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: nsName})
        require.NoError(t, err)

        // And create queues
        err = c.QueuesCreate(ctx, &pb.QueueInfo{
            RequestedPartitions: 1,
            ExpireTimeout:       "24h",
            LeaseTimeout:        "1m",
            QueueName:           "test-queue",
            Namespace:           nsName,
        })
        require.NoError(t, err)
    })

    t.Run("CascadingPermissions", func(t *testing.T) {
        d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
        defer func() {
            d.Shutdown(t)
            tearDown()
        }()

        // Create a user and give them Admin in _system
        var userResp pb.UserCreateResponse
        require.NoError(t, c.UsersCreate(ctx, &pb.UserCreateRequest{Username: "sysadmin"}, &userResp))

        require.NoError(t, c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
            UserID:    userResp.Id,
            Namespace: "_system",
            RoleName:  "Admin",
        }))

        // Create API key for this user
        var keyResp pb.APIKeyCreateResponse
        require.NoError(t, c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
            UserID: userResp.Id,
            Name:   "sysadmin-key",
        }, &keyResp))

        // Create client with this key
        authClient := newClientWithKey(t, d.Address(), keyResp.Key)

        // Create a new namespace
        nsName := random.String("ns-", 10)
        require.NoError(t, authClient.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: nsName}))

        // User should be able to create queue in ANY namespace (cascading from _system)
        err := authClient.QueuesCreate(ctx, &pb.QueueInfo{
            RequestedPartitions: 1,
            ExpireTimeout:       "24h",
            QueueName:           "cascade-queue",
            LeaseTimeout:        "1m",
            Namespace:           nsName,
        })
        require.NoError(t, err)
    })

    t.Run("ScopedKeyRestriction", func(t *testing.T) {
        d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
        defer func() {
            d.Shutdown(t)
            tearDown()
        }()

        // Create two namespaces
        nsA := random.String("ns-a-", 10)
        nsB := random.String("ns-b-", 10)
        require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: nsA}))
        require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: nsB}))

        // Create user with Admin in _system
        var userResp pb.UserCreateResponse
        require.NoError(t, c.UsersCreate(ctx, &pb.UserCreateRequest{Username: "admin-user"}, &userResp))
        require.NoError(t, c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
            UserID:    userResp.Id,
            Namespace: "_system",
            RoleName:  "Admin",
        }))

        // Create SCOPED key for ns-A only
        var keyResp pb.APIKeyCreateResponse
        require.NoError(t, c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
            Name:           "scoped-to-ns-a",
            NamespaceScope: nsA,
            UserID:         userResp.Id,
        }, &keyResp))

        scopedClient := newClientWithKey(t, d.Address(), keyResp.Key)

        // Should succeed in ns-A
        err := scopedClient.QueuesCreate(ctx, &pb.QueueInfo{
            RequestedPartitions: 1,
            ExpireTimeout:       "24h",
            QueueName:           "queue-in-ns-a",
            LeaseTimeout:        "1m",
            Namespace:           nsA,
        })
        require.NoError(t, err)

        // Should FAIL in ns-B even though user is Admin (key scope restriction)
        err = scopedClient.QueuesCreate(ctx, &pb.QueueInfo{
            RequestedPartitions: 1,
            ExpireTimeout:       "24h",
            QueueName:           "queue-in-ns-b",
            LeaseTimeout:        "1m",
            Namespace:           nsB,
        })
        var e duh.Error
        require.True(t, errors.As(err, &e))
        assert.Equal(t, duh.CodeForbidden, e.Code())
    })

    t.Run("LockDownWorkflow", func(t *testing.T) {
        d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
        defer func() {
            d.Shutdown(t)
            tearDown()
        }()

        // First, create a real admin user before locking down
        var userResp pb.UserCreateResponse
        require.NoError(t, c.UsersCreate(ctx, &pb.UserCreateRequest{Username: "real-admin"}, &userResp))
        require.NoError(t, c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
            UserID:    userResp.Id,
            Namespace: "_system",
            RoleName:  "Admin",
        }))

        var keyResp pb.APIKeyCreateResponse
        require.NoError(t, c.APIKeysCreate(ctx, &pb.APIKeyCreateRequest{
            UserID: userResp.Id,
            Name:   "admin-key",
        }, &keyResp))

        // Remove Anonymous Admin binding (lock down)
        require.NoError(t, c.RoleBindingsDelete(ctx, &pb.RoleBindingDeleteRequest{
            Namespace: "_system",
            UserID:    "anonymous",
            RoleName:  "Admin",
        }))

        // Anonymous client should now be forbidden
        anonClient := newClient(t, d.Address())  // No API key
        err := anonClient.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: "should-fail"})
        var e duh.Error
        require.True(t, errors.As(err, &e))
        assert.Equal(t, duh.CodeForbidden, e.Code())

        // Authenticated client should still work
        authClient := newClientWithKey(t, d.Address(), keyResp.Key)
        err = authClient.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: "should-succeed"})
        require.NoError(t, err)
    })

    t.Run("Errors", func(t *testing.T) {
        t.Run("RolesCreate", func(t *testing.T) {
            t.Run("DuplicateRole", func(t *testing.T) {
                d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
                defer func() {
                    d.Shutdown(t)
                    tearDown()
                }()

                nsName := random.String("ns-", 10)
                require.NoError(t, c.NamespacesCreate(ctx, &pb.NamespaceCreateRequest{Name: nsName}))

                require.NoError(t, c.RolesCreate(ctx, &pb.RoleCreateRequest{
                    Permissions: []string{auth.PermQueueProduce},
                    Name:        "custom-role",
                    Namespace:   nsName,
                }))

                err := c.RolesCreate(ctx, &pb.RoleCreateRequest{
                    Permissions: []string{auth.PermQueueLease},
                    Name:        "custom-role",
                    Namespace:   nsName,
                })
                var e duh.Error
                require.True(t, errors.As(err, &e))
                assert.Equal(t, duh.CodeBadRequest, e.Code())
            })
        })

        t.Run("RoleBindingsCreate", func(t *testing.T) {
            t.Run("RoleNotExist", func(t *testing.T) {
                d, c, ctx := newDaemon(t, 10*clock.Second, svc.Config{StorageConfig: setup()})
                defer func() {
                    d.Shutdown(t)
                    tearDown()
                }()

                var userResp pb.UserCreateResponse
                require.NoError(t, c.UsersCreate(ctx, &pb.UserCreateRequest{Username: "test-user"}, &userResp))

                err := c.RoleBindingsCreate(ctx, &pb.RoleBindingCreateRequest{
                    RoleName:  "nonexistent-role",
                    Namespace: "_system",
                    UserID:    userResp.Id,
                })
                require.ErrorContains(t, err, "role does not exist")
            })
        })
    })
}
```

**Test objectives:**
- Role CRUD operations
- RoleBinding CRUD operations
- Cascading permission checks
- API key scope enforcement
- Anonymous default Admin access
- Lock-down workflow (remove Anonymous Admin binding)

**Key scenarios per spec Testing Strategy:**
- **Scope Testing**: API key scoped to ns-A cannot access ns-B even if user is Admin
- **Cascading Testing**: User with `queue.delete` in `_system` can delete queue in ns-production
- **Anonymous Testing**: Accessing /metrics works without header, /v1/queue.create works (Anonymous is Admin)
- **Lock-down Testing**: After removing Anonymous Admin binding, /v1/queue.create fails without auth

### Validation
- [ ] Run: `go test ./...` (all tests pass)
- [ ] Run: `make proto`
- [ ] Verify: Anonymous has Admin access by default
- [ ] Verify: Authenticated users have correct permissions
- [ ] Verify: Scoped keys cannot access other namespaces
- [ ] Verify: Cascading from `_system` works correctly

### Context for implementation
- HasPermission logic follows spec Section 8.2 pseudocode
- Bootstrap follows spec Section 9.5 (Open Door policy)
- Standard roles follow spec Section 9.4 (Code-First Role Management)
- Authorization errors should return HTTP 403 Forbidden per AUTH-005

---

## Summary of Files Changed/Created

### New Files
- `internal/types/namespace.go`
- `internal/types/auth.go`
- `internal/store/namespace.go`
- `internal/store/users.go`
- `internal/store/apikeys.go`
- `internal/store/roles.go`
- `internal/context.go`
- `internal/auth/apikey.go`
- `internal/auth/permissions.go`
- `internal/auth/backend.go`
- `internal/bootstrap.go` (or add to service.go)
- `proto/namespaces.proto`
- `proto/auth.proto`
- `service/namespace_test.go`
- `service/users_test.go`
- `service/auth_test.go`

### Modified Files
- `internal/types/items.go` - Add Namespace to QueueInfo
- `internal/store/store.go` - Add interfaces to Config
- `internal/store/memory.go` - Add implementations
- `internal/store/badger.go` - Add implementations
- `internal/store/postgres.go` - Add implementations
- `proto/queues.proto` - Add namespace field
- `transport/http.go` - Add middleware and endpoints
- `service/service.go` - Add methods and authorization
- `service/validation.go` - Add validation functions
- `client.go` - Add client methods and auth header

---

## Implementation Notes

### TDD Approach
Each phase should be implemented using Test-Driven Development:
1. Write failing tests for the new functionality
2. Implement the minimum code to pass tests
3. Refactor while keeping tests green

### Storage Backend Order
Implement storage backends in this order for each new entity:
1. InMemory (fastest iteration)
2. BadgerDB (embedded persistence)
3. PostgreSQL (production database)

### Error Handling
- 401 Unauthorized: Invalid/expired/missing API key (when required)
- 403 Forbidden: Valid auth but insufficient permissions
- 400 Bad Request: Invalid input (namespace name, etc.)

### Performance Considerations
- API key lookups must be indexed by hash
- Consider caching HasPermission results if <1ms requirement not met
- Role bindings should be indexed by user_id for fast lookup
