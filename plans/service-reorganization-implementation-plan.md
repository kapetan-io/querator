# Service Package Reorganization Implementation Plan

## Overview

Move service-based code and testing from the repository root into a `service/` package to reduce clutter in the repo root. The `Client` will remain in the root for easy external imports, while the `Service` and all related code will be in the `service/` subpackage. This is a non-breaking change since no external users currently depend on this project.

## Current State Analysis

### Files in repo root to be moved:

**Source files:**
- `service.go` - Contains `Service` struct and all queue operation methods
- `validation.go` - Contains private validation methods for proto conversion

**Test files:**
- `common_test.go` - Shared test utilities and setup
- `benchmark_test.go` - Benchmark tests
- `queue_test.go` - Queue operation tests
- `queues_test.go` - Queues management tests
- `partition_test.go` - Partition-level tests
- `retry_test.go` - Retry mechanism tests
- `storage_test.go` - Storage backend tests
- `fuzz_test.go` - Fuzzing tests

**Files to remain in root:**
- `client.go` - HTTP client for external users to import
- All non-test files currently in other packages (daemon/, transport/, internal/, etc.)

### Key discoveries:
- All test files are already in `querator_test` package (separate from production code)
- Tests use functional testing approach, importing service as external package
- `Service` struct is instantiated by `daemon/daemon.go` and used throughout tests
- No circular dependencies identified
- Current imports: `daemon/daemon.go` imports `github.com/kapetan-io/querator` (root package)

## Desired End State

After this reorganization:
1. Root package will contain only `client.go` for external library users
2. Service code (`service.go`, `validation.go`) moves to `service/` package
3. All service test files move to `service/` package (keeping original file names and organization)
4. Import paths change from `querator.NewService()` to `service.NewService()`
5. Repo root becomes cleaner with only client-related files
6. No functional behavior changes - purely structural

### How to verify completion:
- `ls -la /path/to/querator/` shows only client.go (+ config files, docs, etc.)
- `service/service.go` exists with `Service` struct
- `service/` contains all test files: `*_test.go` files
- `go test ./service/...` runs all tests successfully
- `go build ./...` builds without errors

## What We're NOT Doing

- Refactoring Service internals or methods
- Changing functional behavior
- Moving daemon, transport, internal, or config packages
- Changing test approach or structure (tests remain functional, testing public API)
- Moving client.go to a subdirectory
- Moving the Version variable (it stays in root for build flag compatibility)

## Implementation Approach

This is primarily a file reorganization task with import path updates:

1. **Create directory structure**: Create `service/` directory
2. **Move source files**: Move `service.go` and `validation.go` to `service/`
3. **Move test files**: Move all `*_test.go` files to `service/` keeping original names
4. **Update package declarations**: Change test package from `querator_test` to `service_test` in all test files
5. **Update imports**: Update all import paths in test files and production code
6. **Update module references**: Ensure `daemon/daemon.go` and other code imports from new location
7. **Run validation**: Verify builds and tests pass

---

## Phase 1: Directory Structure and File Movement

### Overview
Create the service package directory and move source files into it. This phase establishes the new package structure while maintaining all functionality.

### Changes Required:

#### 1. Create service/ directory and move source and test files
**Files to move:**
- `service.go` → `service/service.go` (change package declaration to `package service`)
- `validation.go` → `service/validation.go` (change package declaration to `package service`)
- `common_test.go` → `service/common_test.go` (change package declaration to `package service_test`)
- `queue_test.go` → `service/queue_test.go` (change package declaration to `package service_test`)
- `queues_test.go` → `service/queues_test.go` (change package declaration to `package service_test`)
- `partition_test.go` → `service/partition_test.go` (change package declaration to `package service_test`)
- `retry_test.go` → `service/retry_test.go` (change package declaration to `package service_test`)
- `storage_test.go` → `service/storage_test.go` (change package declaration to `package service_test`)
- `benchmark_test.go` → `service/benchmark_test.go` (change package declaration to `package service_test`)
- `fuzz_test.go` → `service/fuzz_test.go` (change package declaration to `package service_test`)

**Package declarations in source files:**
```go
package service
```

**Package declarations in test files:**
```go
package service_test
```

**Update import aliases in all test files:**
In each test file, update imports:
- Old: `import que "github.com/kapetan-io/querator"`
- New: `import svc "github.com/kapetan-io/querator/service"`
- All test code: change `que.NewService()` → `svc.NewService()`, `que.Service` → `svc.Service`, etc.

**Context for implementation:**
- Root service.go:17 declares `package querator` - change to `package service`
- Root validation.go:1 declares `package querator` - change to `package service`
- Root test files declare `package querator_test` - change to `package service_test`
- Root test files import `que "github.com/kapetan-io/querator"` - update to `svc "github.com/kapetan-io/querator/service"`
- All public API exports remain unchanged (Service struct, all public methods)
- service.go internal imports (internal/, proto/, transport/) are unchanged
- Keep test files separate to maintain organization and readability

#### 2. Verify service/ files have correct imports
**File**: `service/service.go` and `service/validation.go`

These files should NOT import the root `github.com/kapetan-io/querator` package. Verify they only import:
```go
import (
    "context"
    "log/slog"
    "time"

    "github.com/kapetan-io/querator/internal"
    "github.com/kapetan-io/querator/internal/store"
    "github.com/kapetan-io/querator/internal/types"
    "github.com/kapetan-io/querator/proto"
    "github.com/kapetan-io/querator/transport"
    "github.com/kapetan-io/tackle/clock"
    "github.com/kapetan-io/tackle/set"
    "google.golang.org/protobuf/types/known/timestamppb"
)
```

**Context for implementation:**
- service.go:19-31 shows current imports - no root package imports, only internal packages
- validation.go:3-7 shows current imports - only types and proto packages
- These import statements do NOT change when moving files
- No circular dependencies possible since service/ doesn't import root

### Validation
- [ ] Run: `mkdir -p service && ls service/`
- [ ] Verify: `service/` directory exists
- [ ] Run: `go build ./service/...`
- [ ] Verify: No build errors

---

## Phase 2: Update Client and Root Package

### Overview
Ensure root package remains viable with only client.go and update client if needed to reference the new service package location.

### Changes Required:

#### 1. Clean up root package
**Files to delete from root:**
- `service.go`
- `validation.go`
- `common_test.go`
- `queue_test.go`
- `queues_test.go`
- `partition_test.go`
- `retry_test.go`
- `storage_test.go`
- `benchmark_test.go`
- `fuzz_test.go`

**Context for implementation:**
- After moving files, these should no longer exist in root
- git rm will handle this automatically during implementation

### Validation
- [ ] Run: `ls -la /path/to/querator/*.go | grep -v client.go` returns empty (only client.go remains)
- [ ] Verify: No *_test.go files in root
- [ ] Verify: No service.go or validation.go in root

---

## Phase 3: Update all imports throughout codebase

### Overview
Update all import paths that reference the service to use the new `service/` package location.

### Changes Required:

#### 1. Update daemon package imports
**File**: `daemon/daemon.go`

Update import to reference service package:
```go
// Old (line 24)
import (
    "github.com/kapetan-io/querator"
)

// New
import (
    "github.com/kapetan-io/querator/service"
)
```

Update struct field type:
```go
// Old (line 38)
service  *querator.Service

// New (keep field name unchanged)
service  *service.Service
```

Update NewService() call:
```go
// Old - locate in NewDaemon function
svc, err := querator.NewService(conf.Service)

// New
svc, err := service.NewService(conf.Service)
```

Update ServiceConfig type reference:
```go
// In config field (line 43)
// Old
Service querator.ServiceConfig

// New
Service service.ServiceConfig
```

**Context for implementation:**
- daemon.go:24 - Add import: `"github.com/kapetan-io/querator/service"`
- daemon.go:38 - Change `*querator.Service` to `*service.Service`
- daemon.go:43 - Change `querator.ServiceConfig` to `service.ServiceConfig`
- Find and replace `querator.NewService` with `service.NewService` in NewDaemon function
- daemon.go is the only file that needs this type of update in daemon/ package

#### 2. Update cmd/querator files - Version reference only
**Files to update:**
- `cmd/querator/main.go` - Check for `querator.Version` reference
- `cmd/querator/server.go` - Check for `querator.Version` reference

These files reference Version variable, which STAYS in root package, so:
```go
// Import stays the same
import "github.com/kapetan-io/querator"

// Usage stays the same (NO CHANGE)
// querator.Version remains valid
```

**Context for implementation:**
- Version variable does NOT move (stays in root)
- cmd files that reference `querator.Version` need NO changes
- Files should only have been importing querator package for Version, not Service

#### 3. Verify other packages don't reference Service
**Files to check:**
- `cmd/quickstart/main.go` - Verify it only imports querator for Client, not Service
- `cmd/querator-doc/main.go` - Verify it only imports querator for specific exports, not Service
- `daemon/config.go` - Already covered in step 1

Use grep to verify:
```bash
grep -r "querator.Service\|querator.NewService" . --include="*.go" | grep -v "service/"
```

Should return empty (all Service references should be in service/ package or updated)

**Context for implementation:**
- After daemon.go updates, no other production code should reference querator.Service
- External packages (cmd/, config, etc.) only reference querator for Client or Version
- The grep command ensures no stragglers

### Validation
- [ ] Run: `grep -r "querator.Service\|querator.NewService" . --include="*.go" | grep -v "service/"` returns empty
- [ ] Run: `go build ./...`
- [ ] Verify: No build errors or import errors
- [ ] Verify: daemon.go still builds: `go build ./daemon/...`
- [ ] Verify: cmd files still build: `go build ./cmd/...`

---

## Phase 4: Run tests and verify

### Overview
Execute tests to ensure all functionality works correctly after reorganization.

### Changes Required:

#### 1. Run service tests
**Command**: `go test ./service/...`

Verify all tests pass:
- Common tests setup/teardown still works
- Queue tests pass
- Benchmark tests pass
- All storage backend tests pass
- Fuzz tests pass

**Context for implementation:**
- Tests are comprehensive functional tests that exercise the entire service
- They test public API surface area only
- All tests should work identically to before, just in new location

#### 2. Run full test suite
**Command**: `go test ./...`

Verify all tests across all packages pass.

**Context for implementation:**
- Ensures no other packages were broken by reorganization
- May include daemon tests, transport tests, etc.

#### 3. Run build validation
**Command**: `go build ./...`

Verify everything builds without errors.

**Context for implementation:**
- Confirms no import cycles or missing imports
- Verifies package structure is correct

### Validation
- [ ] Run: `go test ./service/... -v`
- [ ] Verify: All tests pass with output like "ok github.com/kapetan-io/querator/service X.XXXs"
- [ ] Run: `go test ./... -v`
- [ ] Verify: All tests pass across entire project
- [ ] Run: `go build ./...`
- [ ] Verify: No errors, all packages build successfully
- [ ] Run: `go build ./cmd/querator`
- [ ] Verify: CLI binary builds successfully
- [ ] Run: `go build ./cmd/quickstart`
- [ ] Verify: Quickstart binary builds successfully
- [ ] Run: `make cover` (if applicable)
- [ ] Verify: Coverage reports work correctly

---

## Phase 5: Verify repo structure

### Overview
Final verification that the repository root is cleaner and all files are in correct locations.

### Changes Required:

#### 1. Verify root directory cleanliness
**Files that should remain in root:**
- `client.go` - HTTP client library
- `Makefile`, `go.mod`, `go.sum` - Build files
- `README.md`, `CLAUDE.md`, `CONTRIBUTING.md` - Documentation
- `Dockerfile`, `docker-compose.yml` - Container files
- `.gitignore`, `.golangci.yml` - Config files
- `proto/`, `internal/`, `transport/`, `daemon/`, `cmd/`, `config/`, `testdata/`, `benchmarks/`, `docs/` - Packages/directories
- `openapi.yaml`, `example.yaml`, `buf.gen.yaml`, `buf.lock`, `buf.yaml` - API/config files

**Files that should NOT be in root:**
- ~~`service.go`~~ ✓
- ~~`validation.go`~~ ✓
- ~~`*_test.go`~~ ✓

#### 2. Verify service/ directory structure
**Files in service/ should be:**
- `service/service.go`
- `service/validation.go`
- `service/common_test.go`
- `service/queue_test.go`
- `service/queues_test.go`
- `service/partition_test.go`
- `service/retry_test.go`
- `service/storage_test.go`
- `service/benchmark_test.go`
- `service/fuzz_test.go`

#### 3. Verify no orphaned files
Ensure no duplicate files or broken references.

### Validation
- [ ] Run: `ls -la service/`
- [ ] Verify: Shows service.go, service_test.go, validation.go
- [ ] Run: `find . -name "service.go" -o -name "validation.go" | grep -v ".git"`
- [ ] Verify: Only results are in service/ directory
- [ ] Run: `ls -la *.go`
- [ ] Verify: Only client.go (and possibly types like Version)
- [ ] Run: `git status`
- [ ] Verify: All old root files show as deleted, new service/ files show as added

---

## Implementation Notes

### Import Changes Summary

**daemon/daemon.go:**
```go
// Before
import "github.com/kapetan-io/querator"
service  *querator.Service
Service querator.ServiceConfig
svc, err := querator.NewService(...)

// After
import "github.com/kapetan-io/querator/service"
service  *service.Service
Service service.ServiceConfig
svc, err := service.NewService(...)
```

**Test files (in service/service_test.go):**
```go
// Before
import que "github.com/kapetan-io/querator"
in tests: que.NewService(...), que.Service, etc.

// After
import svc "github.com/kapetan-io/querator/service"
in tests: svc.NewService(...), svc.Service, etc.
```

**Version references (NO CHANGE):**
```go
// cmd/querator/main.go, cmd/querator/server.go
// Before AND After (unchanged)
import "github.com/kapetan-io/querator"
querator.Version  // stays the same, Version never moves
```

### What STAYS in Root Package
- `client.go` - HTTP client library
- Version variable - referenced by build flags and cmd files
- No service-related code after this reorganization

### What MOVES to service/ Package
- `service.go` → `service/service.go` (package service)
- `validation.go` → `service/validation.go` (package service)
- All root *_test.go → `service/service_test.go` (package service_test)

### What Doesn't Change
- Internal packages (`internal/`, `proto/`, `transport/`, `daemon/`) - structure unchanged, only imports updated
- External packages (`cmd/`, `config/`) - only daemon.go imports change
- Test approach - remains functional testing through public APIs
- All public API signatures of Service - unchanged, just in different package
- Makefile, Dockerfile, build scripts - unchanged (Version stays in root)

### Test File Organization

Test files are kept separate in the service/ package to maintain organization:

**common_test.go** - TestMain, helper types, constants, and helper functions:
```go
package service_test

func TestMain(m *testing.M) { ... }
type NewStorageFunc func() store.Config
const ExpireTimeout = "24h0m0s"
func setupDaemon(...) *daemon.Daemon { ... }
func tearDown(...) { ... }
```

**Individual test files** - Each maintains its original name and focus:
- `queue_test.go` - Queue operation tests (Produce, Lease, Complete, Retry)
- `queues_test.go` - Queue management tests (Create, List, Update, Delete, Info)
- `partition_test.go` - Partition-level tests
- `retry_test.go` - Retry mechanism tests
- `storage_test.go` - Storage backend tests
- `benchmark_test.go` - Benchmark tests
- `fuzz_test.go` - Fuzzing tests

All test files import the service package the same way:
```go
import svc "github.com/kapetan-io/querator/service"
```

This organization keeps files at a manageable size while making it clear which tests cover which functionality.
