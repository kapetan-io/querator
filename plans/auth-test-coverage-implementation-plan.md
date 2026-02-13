# Auth Test Coverage & Bug Fixes Implementation Plan

## Context

A test coverage analysis revealed 7 issues in the auth system: missing CRUD tests for Roles/RoleBindings, a cascade delete bug, duplicate permission constants, dead code, missing test scenarios, and a memory growth issue. This plan addresses all 7 gaps through 3 phases.

## Current State Analysis

### What's tested well:
- Auth defaults-open, authentication happy/error paths, authorization basics, namespace scoping, `_system` cascade
- Namespaces CRUD + error paths
- Users & API Keys CRUD, cascade delete (user deletes their keys), listing by user

### What's broken or missing:
1. **Zero Roles/RoleBindings CRUD tests** - 7 client API endpoints with no dedicated tests
2. **Orphaned role bindings on user deletion** - `service.go:719-729` cascades to `APIKeys.DeleteByUser` but NOT `RoleBindings.DeleteByUser` (functional bug)
3. **Duplicate permission constants** - `internal/auth/permissions.go:5-44` and `transport/http.go:87-119` define identical constants
4. **ParseAPIKey is dead code** - `apikey.go:60-66` exported but never called
5. **No expired API key test** - `cache.go:91-93` has expiry check but no test exercises it
6. **MalformedAuthHeader test doesn't test malformed headers** - `auth_test.go:313-334` sends empty header (duplicate of MissingAPIKey test)
7. **sync.Map memory growth** - `daemon/auth_adapter.go:34` stores principals forever with no eviction

### Key Discoveries:
- `internal/types` imports `transport`, so `transport` cannot import `internal/auth` (cycle). However, a sub-package `transport/auth` with zero dependencies can be imported by both `transport/http.go` and `internal/auth/` without cycles
- Permission constants are part of the public API — SDK users must pass permission strings to `RolesCreate` (`Permissions: []string{...}`). These belong in `transport/auth` as the public auth API surface
- `RoleBindings.DeleteByUser` exists in both memory (`memory.go:1670`) and badger (`badger.go:2935`) stores
- The `AuthBackendAdapter.principals` sync.Map is only used to pass `types.Principal` between `Authenticate` and `HasPermission` - can be replaced by reconstructing from `transport.Principal`

## Desired End State

All 7 identified issues are resolved:
- Roles and RoleBindings have full CRUD + error test coverage through the public client API
- User deletion cascades to role bindings (bug fixed + test)
- Permission constants defined once with compile-time verification
- Dead code removed
- Expired API key path tested
- MalformedAuthHeader test actually sends malformed header
- sync.Map removed from AuthBackendAdapter

Verify via: `go test ./... -count=1`

## What We're NOT Doing

- Adding eviction/TTL to the auth `Cache` (it already has cleanup via `cleanupLoop`)
- Changing the RBAC model or adding new permissions

## Implementation Approach

3 phases ordered by dependency: bug fixes and code cleanup first, then test additions. Complete all Phase 1 changes atomically as a single commit since the `transport/auth` package migration requires updating multiple files simultaneously.

---

## Phase 1: Bug Fixes & Code Cleanup

### Overview
Fix the cascade delete bug, remove dead code, eliminate duplicate constants, and remove the sync.Map memory leak.

### Changes Required:

#### 1. Fix orphaned role bindings on user deletion
**File**: `service/service.go`
**Changes**: Add `RoleBindings.DeleteByUser` call before user deletion

Add cascade delete for role bindings alongside the existing API key cascade at `service.go:719-729`:

```go
func (s *Service) UsersDelete(ctx context.Context, req *proto.UsersDeleteRequest) error
```

**Function Responsibilities:**
- Call `RoleBindings.DeleteByUser(ctx, req.Id)` BEFORE `APIKeys.DeleteByUser`
- Preserve existing API key cascade and user deletion logic
- Follow the same error handling pattern as the existing `APIKeys.DeleteByUser` call

---

#### 2. Remove sync.Map from AuthBackendAdapter
**File**: `daemon/auth_adapter.go`
**Changes**: Remove the `principals sync.Map` field entirely. Reconstruct `types.Principal` from `transport.Principal` in `HasPermission` instead.

```go
type AuthBackendAdapter struct {
	backend auth.AuthBackend
}

func (a *AuthBackendAdapter) Authenticate(ctx context.Context, token string) (transport.Principal, error)
func (a *AuthBackendAdapter) HasPermission(ctx context.Context, principal transport.Principal, targetNS string, perm string) (bool, error)
```

**Function Responsibilities:**
- `Authenticate`: Delegate to `backend.Authenticate`, convert `types.Principal` to `transport.Principal`, do NOT cache in sync.Map
- `HasPermission`: Reconstruct `types.Principal` from `transport.Principal` fields, determining anonymous status from identity fields (reference: existing fallback logic at `auth_adapter.go:58`). Delegate to `backend.HasPermission`
- Keep `Close()` as it delegates to `backend.Close()` which closes the auth cache
- Remove `sync` import

---

#### 3. Remove ParseAPIKey dead code
**File**: `internal/auth/apikey.go`
**Changes**: Delete the `ParseAPIKey` function at lines 59-66

---

#### 4. Eliminate duplicate permission constants
Permission constants are part of the public API — SDK users must pass permission strings when calling `RolesCreate` (`Permissions: []string{...}`). These belong in `transport/auth`, the public auth API surface, rather than buried in `internal/auth`.

**New File**: `transport/auth/auth.go`
**Package**: `package auth`
**Contents**: Move ALL permission constants, role constants, variables, and functions from `internal/auth/permissions.go` to this new sub-package:

```go
package auth

const (
    NamespaceCreate = "namespace.create"
    NamespaceDelete = "namespace.delete"
    NamespaceList   = "namespace.list"
    QueueCreate     = "queue.create"
    // ... all permission constants (drop the "Perm" prefix since the package name provides context)
    SystemNamespace = "_system"
    RoleAdmin          = "Admin"
    RoleNamespaceOwner = "NamespaceOwner"
    RolePublicViewer   = "PublicViewer"
)

var AllPermissions = []string{ ... }
var AdminPermissions = AllPermissions
var NamespaceOwnerPermissions = []string{ ... }
var PublicViewerPermissions = []string{ ... }

func IsValidPermission(perm string) bool
func IsStandardRole(name string) bool
```

**Approach**: Delete `internal/auth/permissions.go` entirely and update ALL callers to use `transport/auth` directly. Do NOT create a thin re-export wrapper.

**Files to update** (find all with `grep -r "auth\.Perm\|auth\.System\|auth\.IsStandard\|auth\.IsValid\|auth\.AllPerm\|auth\.Admin\|auth\.Namespace\|auth\.RoleAdmin\|auth\.RoleNamespace\|auth\.PublicViewer\|transport\.Perm\|transport\.System" --include="*.go" .`):

- `transport/http.go`: Remove duplicate constants (lines 87-119), import `transport/auth` (aliased as `tauth` or similar to avoid collision with `internal/auth` in files that import both), reference `tauth.NamespaceCreate` etc.
- `internal/auth/permissions.go`: DELETE this file entirely
- `internal/auth/backend.go`: Import `transport/auth`, change `SystemNamespace` reference
- `service/service.go`: Import `transport/auth`, change `auth.IsStandardRole` and `auth.IsValidPermission` references
- `service/auth_test.go`: Import `transport/auth`, change `auth.PermQueueList` -> `auth.QueueList`, `auth.SystemNamespace` -> `auth.SystemNamespace`, `auth.AllPermissions` -> `auth.AllPermissions`, etc.

**Context for implementation:**
- `transport/auth` is a new sub-package under `transport/` (currently flat with no sub-packages)
- `transport/auth` has zero dependencies, so no import cycles are possible
- `transport/http.go` can import its own sub-package `transport/auth`
- `internal/auth/` can import `transport/auth` — no cycle because `transport/auth` imports nothing
- `service/` already imports both `internal/auth` and `transport` — files that need both will use an import alias (e.g., `tauth "github.com/kapetan-io/querator/transport/auth"` vs `auth "github.com/kapetan-io/querator/internal/auth"`)
- The `Perm` prefix on constants is dropped since the package name `auth` already provides context (e.g., `auth.QueueCreate` instead of `auth.PermQueueCreate`)
- SDK users benefit from `auth.QueueCreate` being importable outside `internal/`

### Testing Requirements

Existing tests must pass after these changes. No new test functions needed for this phase.

### Validation
- [ ] Run: `go build ./...` (verify no import cycles or compilation errors)
- [ ] Run: `go test ./... -count=1` (all existing tests pass)
- [ ] Verify: `ParseAPIKey` no longer exists in codebase
- [ ] Verify: No `sync.Map` in `daemon/auth_adapter.go`
- [ ] Verify: No `PermQueueCreate` or `PermNamespaceCreate` constants in `transport/http.go` (all moved to `transport/auth/`)
- [ ] Verify: `internal/auth/permissions.go` no longer exists
- [ ] Verify: No references to `auth.Perm`, `auth.SystemNamespace`, `transport.Perm`, or `transport.SystemNamespace` remain in `internal/` or `service/`

---

## Phase 2: Fix Existing Test Issues

### Overview
Fix the MalformedAuthHeader test so it actually tests malformed headers, and add an expired API key test.

### Changes Required:

#### 1. Fix MalformedAuthHeader test
**File**: `service/auth_test.go`
**Changes**: Replace the existing `MalformedAuthHeader` test at line 313 with a test that sends a non-Bearer authorization header

The current test sends an empty API key (no header), which duplicates `MissingAPIKeyWithAuthEnabled`. The `querator.Client` always sends `"Bearer " + apiKey` via `setAuthHeader` (`client.go:76-80`), so the client cannot trigger the malformed-header path.

**Test Objectives:**
- Verify that a non-Bearer authorization header (e.g., `"Basic ..."`) returns 401 unauthorized
- Verify error message contains "invalid authorization header format"
- Exercise the code path at `http.go:239` that was previously uncovered

**Context for implementation:**
- Must use `net/http` directly since the client always prepends `"Bearer "`
- Follow `duh.Error` response assertion pattern from existing auth error tests in `auth_test.go`
- Use `transport.RPCQueuesList` for the request path
- Use `duh.ContentTypeProtoBuf` for the Content-Type header

---

#### 2. Add expired API key test
**File**: `service/auth_test.go`
**Changes**: Add a new test under `Authentication` that creates an API key with an `ExpiresAt` in the past and verifies it's rejected

The cache automatically misses on first request for a new key hash, so creating a fresh expired key in storage and immediately using it will exercise the expiry check at `cache.go:91-93`.

**Test Objectives:**
- Create a user and API key with `ExpiresAt` set in the past
- Authenticate with the expired key and verify 401 unauthorized response
- Exercise the expiry check at `cache.go:91-93`

**Context for implementation:**
- `ExpiresAt` is `*clock.Time` on `types.APIKey` (`internal/types/auth.go:56`)
- Seed user and expired key directly in storage since the client API doesn't support setting `ExpiresAt`
- Follow `createAdminUser` pattern for direct storage seeding (`auth_test.go:415-468`)
- Use `newClientWithAPIKey` to authenticate with the expired key
- Use `auth.GenerateAPIKey` to produce a valid key/hash pair

### Validation
- [ ] Run: `go test ./service/ -run "TestAuth/.*/Errors/MalformedAuthHeader" -v` (test passes, exercises correct path)
- [ ] Run: `go test ./service/ -run "TestAuth/.*/Authentication/ExpiredAPIKey" -v` (test passes)
- [ ] Run: `go test ./... -count=1` (all tests pass)

---

## Phase 3: Roles & RoleBindings CRUD Tests

### Overview
Add comprehensive CRUD tests for Roles and RoleBindings through the public client API, following the same patterns as `users_test.go`.

**IMPORTANT**: Phase 3 depends on Phase 1 completion (the `transport/auth` package and the cascade delete fix must be in place).

### Changes Required:

#### 1. Create Roles and RoleBindings test file
**New File**: `service/roles_test.go`
**Package**: `package service_test`
**Changes**: Add full CRUD + error test coverage for Roles and RoleBindings

```go
func TestRoles(t *testing.T)
```

Follow the `TestUsers` pattern at `users_test.go:17-43` for multi-backend test setup (InMemory + BadgerDB).

```go
func testRoles(t *testing.T, setup NewStorageFunc)
```

**Happy Path Tests:**

```go
t.Run("RoleCreateAndList", func(t *testing.T) {
    // Create namespace via NamespacesCreate
    // Create role in namespace via RolesCreate with permissions like []string{auth.QueueCreate, auth.QueueList}
    // List roles via RolesList for that namespace
    // Verify role appears with correct name, namespace, permissions
})

t.Run("RoleUpdate", func(t *testing.T) {
    // Create namespace, create role with [auth.QueueCreate]
    // Update role via RolesUpdate with [auth.QueueCreate, auth.QueueDelete]
    // List roles and verify permissions were updated
})

t.Run("RoleDelete", func(t *testing.T) {
    // Create namespace, create role
    // Delete role via RolesDelete
    // List roles and verify empty
})

t.Run("RoleBindingCreateAndList", func(t *testing.T) {
    // Create namespace, create user via UsersCreate, create role via RolesCreate
    // Create binding via RoleBindingsCreate (namespace, userId, roleName)
    // List bindings via RoleBindingsList for that namespace
    // Verify binding appears with correct userId and roleId
})

t.Run("RoleBindingDelete", func(t *testing.T) {
    // Create namespace, user, role, binding
    // Delete binding via RoleBindingsDelete (namespace, userId, roleName)
    // List bindings and verify empty
})

t.Run("UserDeleteCascadeRoleBindings", func(t *testing.T) {
    // Create namespace, user, role, binding
    // Delete user via UsersDelete
    // List role bindings for that namespace - verify binding is gone
    // This tests the Phase 1 bug fix (issue #2)
})
```

**Error Tests:**

```go
t.Run("Errors", func(t *testing.T) {
    t.Run("RolesCreate", func(t *testing.T) {
        t.Run("EmptyNamespace", func(t *testing.T) {
            // RolesCreate with Namespace: "" -> error contains "namespace is invalid"
        })
        t.Run("EmptyName", func(t *testing.T) {
            // RolesCreate with Name: "" -> error contains "name is invalid"
        })
        t.Run("StandardRoleName", func(t *testing.T) {
            // RolesCreate with Name: "Admin" (auth.RoleAdmin)
            // -> error contains "cannot modify or delete standard role"
            // This exercises IsStandardRole in the create path at service.go:824
        })
        t.Run("InvalidPermission", func(t *testing.T) {
            // RolesCreate with Permissions: []string{"invalid.perm"}
            // -> error contains "permission" and "is invalid"
        })
        t.Run("NamespaceNotExist", func(t *testing.T) {
            // RolesCreate with Namespace: "non-existent-ns"
            // -> error (namespace does not exist)
        })
    })

    t.Run("RolesUpdate", func(t *testing.T) {
        t.Run("StandardRole", func(t *testing.T) {
            // Seed storage directly: storageConf.Roles.Add(ctx, types.Role{Name: "Admin", Namespace: ns, ...})
            // RolesUpdate with Name: "Admin" -> error contains "cannot modify or delete standard role"
            // Note: Must seed via storage since RolesCreate rejects standard names
        })
        t.Run("InvalidPermission", func(t *testing.T) {
            // Create role normally, then RolesUpdate with Permissions: []string{"bogus.perm"}
            // -> error contains "permission" and "is invalid"
        })
    })

    t.Run("RolesDelete", func(t *testing.T) {
        t.Run("StandardRole", func(t *testing.T) {
            // Seed storage directly with standard role
            // RolesDelete with Name: "Admin" -> error contains "cannot modify or delete standard role"
        })
        t.Run("RoleHasBindings", func(t *testing.T) {
            // Create namespace, user, role, binding
            // RolesDelete for the role -> error contains "role has active bindings"
        })
        t.Run("NotExist", func(t *testing.T) {
            // RolesDelete for non-existent role -> error contains "role does not exist"
        })
    })

    t.Run("RoleBindingsCreate", func(t *testing.T) {
        t.Run("EmptyNamespace", func(t *testing.T) {
            // -> error contains "namespace is invalid"
        })
        t.Run("EmptyRoleName", func(t *testing.T) {
            // -> error contains "role_name is invalid"
        })
        t.Run("EmptyUserID", func(t *testing.T) {
            // -> error contains "user_id is invalid"
        })
        t.Run("RoleNotExist", func(t *testing.T) {
            // Namespace exists, user exists, but role doesn't -> error contains "role does not exist"
        })
        t.Run("UserNotExist", func(t *testing.T) {
            // Namespace exists, role exists, but user doesn't -> error contains "user does not exist"
        })
    })

    t.Run("RoleBindingsDelete", func(t *testing.T) {
        t.Run("NotExist", func(t *testing.T) {
            // Create namespace and role but no binding
            // RoleBindingsDelete for non-existent binding -> error contains "role binding does not exist"
        })
    })
})
```

**Context for implementation:**
- Follow `users_test.go` patterns exactly: `newDaemon`, `defer d.Shutdown(t)`, client-based testing
- These tests run WITHOUT auth enabled (NoOp backend) since they test CRUD functionality, not authorization
- Need to create namespace first via `c.NamespacesCreate` since roles are namespace-scoped
- Error assertions follow the pattern: `require.Error` -> `require.ErrorAs(t, err, &duhErr)` -> `assert.Contains(t, duhErr.Message(), ...)`
- Daemon creation: `d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: setup()})`
- For StandardRole update/delete tests: The API rejects creating standard roles, so seed storage directly. This requires passing `setup()` result to a variable to access `storageConf.Roles.Add()`. Use a pattern like:
  ```go
  storageConf := setup()
  d, c, ctx := newDaemon(t, clock.Minute*10, svc.Config{StorageConfig: storageConf})
  // Seed standard role directly
  err := storageConf.Roles.Add(ctx, types.Role{
      ID:        random.String("role-", 10),
      Name:      auth.RoleAdmin,
      Namespace: ns,
  })
  ```
- For RoleBindingDelete test: `RoleBindingsDelete` takes `Namespace`, `UserId`, and `RoleName` fields (see `proto/roles.proto` and `client.go:827`)
- Use `random.String` for unique names to avoid collision across test runs
- Use permission subsets like `[]string{auth.QueueCreate, auth.QueueList}` in test data (not AllPermissions)
- Error test groups with uniform setup (e.g., `RolesCreate` validation errors, `RoleBindingsCreate` validation errors) should use `for _, test := range []struct` table-driven style per CLAUDE.md; error tests requiring unique setup should remain as individual `t.Run` cases
- Struct literals should follow visual tapering (longest lines first, shortest last)

### Validation
- [ ] Run: `go test ./service/ -run "TestRoles" -v` (all new tests pass for both InMemory and BadgerDB)
- [ ] Run: `go test ./... -count=1` (all tests pass including existing)

