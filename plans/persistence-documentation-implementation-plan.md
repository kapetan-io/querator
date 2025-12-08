# Persistence Documentation Implementation Plan

## Overview

Create comprehensive documentation for getting persistence working in Querator, including a quick start guide in README.md, separate storage backend documentation, a new `/health` endpoint following RFC Health Check Response Format, and a Golang validation script.

## Current State Analysis

**What exists:**
- `Dockerfile` - Multi-stage build with distroless base, exposes port 2319
- `docker-compose.yml` - Mounts `./data:/data`, uses `ghcr.io/kapetan-io/querator:latest`
- `example.yaml` - Uses `/tmp/` paths (needs to change to `/data`)
- `/metrics` endpoint for Prometheus metrics (GET)
- Three storage backends implemented:
  - InMemory (`internal/store/memory.go`)
  - BadgerDB (`internal/store/badger.go`) - embedded key-value store
  - PostgreSQL (`internal/store/postgres.go`) - full implementation ready for use
- No `/health` endpoint exists

**Key Discoveries:**
- PostgreSQL implementation is complete at `internal/store/postgres.go:109-1688`
- Storage configuration happens via `config/config.go:140-163` for partitions and `config/config.go:165-200` for queues
- HTTP routing at `transport/http.go:103-169` - health endpoint needs to be added here
- Default listen address is `localhost:2319` per `daemon/config.go:58`

## Desired End State

After this plan is complete:
1. README.md contains a Quick Start section with Docker commands and curl health check
2. Storage backend documentation exists in `docs/storage/` directory
3. `/health` endpoint returns RFC-compliant health check response with storage status
4. `example.yaml` uses `/data` paths for Docker compatibility
5. `docker-compose.yml` mounts to `/tmp` for ephemeral quick-start usage
6. Golang validation script at repo root verifies the quick start works

**Verification:**
- Run `docker compose up -d` and curl health endpoint returns healthy status
- Run validation script and it passes all checks

## What We're NOT Doing

- Not implementing new storage backends
- Not changing the core storage interfaces
- Not modifying the existing API endpoints (except adding `/health`)
- Not adding authentication to health endpoint
- Not creating automated CI/CD tests for Docker (manual validation script only)

## Implementation Approach

1. Add `/health` endpoint following RFC Health Check Response Format with storage backend status
2. Update configuration files (`example.yaml`, `docker-compose.yml`)
3. Create storage documentation in `docs/storage/`
4. Add Quick Start section to README.md
5. Create validation script at repo root

---

## Phase 1: Add Health Endpoint

### Overview
Implement a `/health` endpoint following the RFC Health Check Response Format (draft-inadarei-api-health-check-06) that returns service status including storage backend health.

### Changes Required:

#### 1. Health Types
**File**: `transport/health.go` (new file)
**Purpose**: Define health response structures per RFC

```go
// HealthStatus represents the overall health status
type HealthStatus string

const (
    HealthStatusPass HealthStatus = "pass"
    HealthStatusWarn HealthStatus = "warn"
    HealthStatusFail HealthStatus = "fail"
)

// HealthResponse represents the RFC Health Check response format
type HealthResponse struct {
    Status      HealthStatus           `json:"status"`
    Version     string                 `json:"version,omitempty"`
    ReleaseID   string                 `json:"releaseId,omitempty"`
    Notes       []string               `json:"notes,omitempty"`
    Output      string                 `json:"output,omitempty"`
    Checks      map[string][]Check     `json:"checks,omitempty"`
    Links       map[string]string      `json:"links,omitempty"`
    ServiceID   string                 `json:"serviceId,omitempty"`
    Description string                 `json:"description,omitempty"`
}

// Check represents a component health check
type Check struct {
    ComponentID   string       `json:"componentId,omitempty"`
    ComponentType string       `json:"componentType,omitempty"`
    Status        HealthStatus `json:"status"`
    Time          string       `json:"time,omitempty"`
    Output        string       `json:"output,omitempty"`
}
```

**Function responsibilities:**
- Define RFC-compliant health response structures
- Support storage backend status checks
- Include version information

#### 2. Service Health Method
**File**: `service.go`
**Changes**: Add `Health` method to Service

```go
func (s *Service) Health(ctx context.Context, version string) (*transport.HealthResponse, error)
```

**Function responsibilities:**
- Accept version string as parameter (passed from daemon which has access to main.Version)
- Check queue storage connectivity by calling `s.queues.List(ctx, limit=1)`
- Create health check entries for queue storage backend
- Set overall status to "pass" if queue storage responds successfully
- Set overall status to "fail" if any storage operation fails or times out
- Include version in response
- Add 5-second timeout to health check operations

**Health check implementation details:**
1. Create context with 5-second timeout for health operations
2. Call `s.queues.List(ctx, &queues, types.ListOptions{Limit: 1})`
3. If successful: status="pass", add check entry with componentType="datastore"
4. If error: status="fail", include error message in output field
5. Record check time in ISO8601 format

**Version source:**
- Version is defined at `cmd/querator/main.go:10` as `var Version = "dev-build"`
- Set via ldflags at build time
- Pass version from daemon layer to service.Health()

#### 3. Transport Interface Update
**File**: `transport/interfaces.go`
**Changes**: Add Health method to Service interface

```go
Health(ctx context.Context, version string) (*HealthResponse, error)
```

#### 4. HTTP Handler Update
**File**: `transport/http.go`
**Changes**: Add health endpoint handling in ServeHTTP and add version field to HTTPHandler

```go
func (h *HTTPHandler) Health(ctx context.Context, w http.ResponseWriter, r *http.Request)
```

**Changes to NewHTTPHandler:**
- Add `version string` parameter to `NewHTTPHandler()` function
- Store version in HTTPHandler struct
- Pass version to service.Health() when called

**Function responsibilities:**
- Handle GET requests to `/health`
- Call `service.Health(ctx, h.version)`
- Return JSON with `application/health+json` content type
- Return HTTP 200 for "pass"/"warn", HTTP 503 for "fail"
- Follow pattern from `/metrics` endpoint at `transport/http.go:107-109`
- Use `encoding/json` for response marshaling (not protobuf)

**HTTP Status Code Mapping:**
- 200 OK: status="pass" (all backends healthy)
- 200 OK: status="warn" (degraded but operational)
- 503 Service Unavailable: status="fail" (not operational)

**Context for implementation:**
- Health endpoint should be GET, similar to `/metrics` at line 107
- Response format follows: https://datatracker.ietf.org/doc/html/draft-inadarei-api-health-check-06
- Content-Type: `application/health+json`
- Must use `encoding/json` marshaling, NOT protobuf

#### 5. Daemon Update
**File**: `daemon/daemon.go`
**Changes**: Pass version to NewHTTPHandler

Update `NewDaemon` to accept version and pass it to transport layer:
- Add `Version string` field to `daemon.Config`
- Pass `conf.Version` to `transport.NewHTTPHandler()`

### Testing Requirements:

**File**: `daemon/health_test.go` (new file)
**Package**: `daemon_test`

```go
func TestHealthEndpoint(t *testing.T)
func TestHealthEndpointFailure(t *testing.T)
```

**Test Pattern (following CLAUDE.md guidelines):**
- Use `require` for critical assertions (error checks, nil checks)
- Use `assert` for non-critical assertions (value comparisons)
- No descriptive messages in assertions
- Follow test setup pattern from `daemon/listener_test.go:71-115`

**Test implementation outline:**
```go
func TestHealthEndpoint(t *testing.T) {
    ctx := context.Background()

    d, err := daemon.NewDaemon(ctx, daemon.Config{
        InMemoryListener: true,
        Version:          "test-version",
    })
    require.NoError(t, err)
    defer func() { _ = d.Shutdown(ctx) }()

    // Make HTTP GET request to /health
    resp, err := http.Get(d.Listener.Addr().String() + "/health")
    require.NoError(t, err)
    defer resp.Body.Close()

    // Verify HTTP status
    assert.Equal(t, http.StatusOK, resp.StatusCode)

    // Verify content type
    assert.Equal(t, "application/health+json", resp.Header.Get("Content-Type"))

    // Parse and verify response
    var health transport.HealthResponse
    err = json.NewDecoder(resp.Body).Decode(&health)
    require.NoError(t, err)

    assert.Equal(t, transport.HealthStatusPass, health.Status)
    assert.Equal(t, "test-version", health.Version)
    assert.NotEmpty(t, health.Checks)
}
```

**Test Objectives:**
- Verify `/health` returns 200 with healthy storage
- Verify response content-type is `application/health+json`
- Verify response contains required `status` field
- Verify response contains version field
- Verify storage backend checks are included in `checks` map

### Validation
- [ ] Run: `go test ./... -run TestHealth`
- [ ] Verify: Health endpoint returns RFC-compliant JSON
- [ ] Verify: `curl http://localhost:2319/health` returns valid JSON

---

## Phase 2: Update Configuration Files

### Overview
Update `example.yaml` to use `/data` paths for Docker compatibility, and update `docker-compose.yml` to mount volumes to `/tmp` for ephemeral quick-start usage.

### Changes Required:

#### 1. Update example.yaml
**File**: `example.yaml`
**Changes**: Change storage paths from `/tmp/` to `/data/`

**Current** (lines 17-27):
```yaml
queue-storage:
  driver: badger
  config:
    storage-dir: /tmp/badger-queue.db

partition-storage:
  - name: badger-01
    driver: badger
    affinity: 1
    config:
      storage-dir: /tmp/badger-01.db
```

**New**:
```yaml
queue-storage:
  driver: badger
  config:
    storage-dir: /data/queues

partition-storage:
  - name: badger-01
    driver: badger
    affinity: 1
    config:
      storage-dir: /data/partitions
```

#### 2. Update docker-compose.yml
**File**: `docker-compose.yml`
**Changes**: Mount to `/tmp` instead of `./data` for ephemeral usage

**Current** (line 8):
```yaml
volumes:
  - ./data:/data
```

**New**:
```yaml
volumes:
  - /tmp/querator:/data
```

#### 3. Remove pre-configured queue from example.yaml
**File**: `example.yaml`
**Changes**: Remove or comment out the `queues:` section (lines 59-76) to start with a clean slate for quick start

**Function responsibilities:**
- Users will create queues via API in quick start
- Prevents confusion about pre-existing queue configuration

### Validation
- [ ] Run: `docker compose up -d`
- [ ] Verify: Container starts without errors
- [ ] Verify: `/tmp/querator` directory created on host

---

## Phase 3: Create Storage Documentation

### Overview
Create documentation for each storage backend in `docs/storage/` directory.

### Changes Required:

#### 1. BadgerDB Documentation
**File**: `docs/storage/badger.md` (new file)
**Purpose**: Document BadgerDB configuration and usage

**Content structure:**
- Overview of BadgerDB (embedded key-value store)
- Use cases (single-node, embedded, development)
- Configuration options (`storage-dir`)
- Example YAML configuration
- Directory structure created by Querator
- Performance considerations
- Reference: `internal/store/badger.go:23-28` for config structure

#### 2. PostgreSQL Documentation
**File**: `docs/storage/postgres.md` (new file)
**Purpose**: Document PostgreSQL configuration and usage

**Content structure:**
- Overview of PostgreSQL backend
- Use cases (production, distributed, high availability)
- Configuration options:
  - `connection-string`: PostgreSQL connection string (required)
  - `max-conns`: Maximum connections in pool (optional, default: pgxpool default)
- Example YAML configuration
- Required PostgreSQL version: 12+ (uses TIMESTAMPTZ, partial indexes)
- Required permissions: CREATE TABLE, SELECT, INSERT, UPDATE, DELETE
- Schema auto-creation: Yes, tables created automatically on first use
- Table naming: `items_<hash>_<partition>` where hash is MD5 of queue name
- Connection pooling: Uses pgxpool with global pool manager (shared across partitions)
- Reference: `internal/store/postgres.go:109-117` for config structure
- Reference: `internal/store/postgres.go:540-611` for table schema
- Reference: `example.yaml:29-41` for commented example

**PostgreSQL YAML example to include:**
```yaml
queue-storage:
  driver: postgres
  config:
    connection-string: "postgres://user:pass@localhost:5432/querator?sslmode=disable"

partition-storage:
  - name: postgres-01
    driver: postgres
    affinity: 1
    config:
      connection-string: "postgres://user:pass@localhost:5432/querator?sslmode=disable"
      max-conns: 10
```

#### 3. InMemory Documentation
**File**: `docs/storage/memory.md` (new file)
**Purpose**: Document InMemory storage for testing

**Content structure:**
- Overview (RAM-only, no persistence)
- Use cases (testing, development, ephemeral workloads)
- Configuration (no options required)
- Example YAML configuration
- Warning about data loss on restart

#### 4. Storage Overview
**File**: `docs/storage/README.md` (new file)
**Purpose**: Overview and comparison of storage backends

**Content structure:**
- Available backends table (features comparison)
- Choosing a storage backend
- Common configuration patterns
- Links to individual backend docs

**Comparison table to include:**

| Feature | InMemory | BadgerDB | PostgreSQL |
|---------|----------|----------|------------|
| Persistence | No | Yes | Yes |
| Distributed | No | No | Yes |
| Transactions | No | Yes | Yes |
| High Availability | No | No | Yes (with replication) |
| Suitable for Production | No | Single-node only | Yes |
| Resource Usage | Low | Medium | Medium-High |
| Setup Complexity | None | Low | Medium |
| Use Case | Testing, Development | Embedded, Edge | Production, Cloud |

### Validation
- [ ] Verify: All documentation files created
- [ ] Verify: YAML examples are valid syntax
- [ ] Verify: Links between docs work

---

## Phase 4: Update README.md Quick Start

### Overview
Add a Quick Start section to README.md with Docker instructions and curl examples.

### Changes Required:

#### 1. Update README.md
**File**: `README.md`
**Changes**: Add Quick Start section after the badges, before "Almost Exactly Once Delivery" section

**Content structure:**
- Quick Start header
- Prerequisites (Docker)
- Start Querator with docker compose
- Check health with curl (JSON response)
- Note about protobuf API and Go client
- Link to validation script for complete workflow example
- Stop Querator
- Link to full storage documentation

**Important protocol clarification:**
- The `/health` endpoint uses JSON (per RFC Health Check Response Format)
- All other API endpoints (`/v1/queue.*`, `/v1/queues.*`) use protobuf
- curl examples for protobuf endpoints are impractical for documentation
- Quick start should focus on:
  1. `curl http://localhost:2319/health` (JSON - easy to show)
  2. Reference the Go client or validation script for queue operations
  3. Link to API documentation for programmatic access

**Example health curl command:**
```bash
curl -s http://localhost:2319/health | jq .
```

**Expected output:**
```json
{
  "status": "pass",
  "version": "1.0.0",
  "checks": {
    "queues:storage": [{"status": "pass", "componentType": "datastore"}]
  }
}
```

**Context for implementation:**
- Health endpoint is GET with JSON response (not protobuf)
- Reference: `transport/http.go:37-74` for API endpoint paths
- Do NOT try to show curl examples for protobuf endpoints
- Instead, point users to: Go client, validation script, or API docs at querator.io

#### 2. Update README.md Backend Status
**File**: `README.md`
**Changes**: Move PostgreSQL from "Planned Backends" to main backends list

**Current** (lines 116-120):
```markdown
##### Planned Backends
- PostgreSQL
- MySQL
- SurrealDB
- FoundationDB
```

**New**:
```markdown
##### PostgreSQL
This backend uses PostgreSQL for production deployments requiring high availability and horizontal scaling.
See [PostgreSQL Storage Documentation](docs/storage/postgres.md) for configuration details.

##### Planned Backends
- MySQL
- SurrealDB
- FoundationDB
```

### Validation
- [ ] Verify: Quick start commands work as documented
- [ ] Verify: curl commands return expected responses
- [ ] Verify: Links to storage docs work

---

## Phase 5: Create Validation Script

### Overview
Create a Golang script at repo root that starts docker-compose, verifies health endpoint, creates a queue, produces/leases/completes items, and cleans up.

### Changes Required:

#### 1. Validation Script
**File**: `quickstart.go` (new file at repo root)
**Purpose**: Automated validation of quick start documentation

```go
// Command quickstart validates the Querator quick start documentation
// by starting docker-compose, checking health, and performing basic queue operations.
//
// Usage:
//   go run quickstart.go [flags]
//
// Flags:
//   --endpoint string   Querator endpoint (default "http://localhost:2319")
//   --skip-docker       Skip docker compose up/down (use existing instance)
//   --cleanup           Run docker compose down after tests
//   --verbose           Print detailed output
package main

func main()
```

**Function responsibilities:**
- Parse command-line flags using standard `flag` package
- Check prerequisites (docker command exists, port 2319 available)
- Start `docker compose up -d` (unless --skip-docker)
- Wait for health endpoint to return "pass" status (30s timeout, 1s retry interval)
- Create a test queue named "quickstart-test"
- Produce 3 items to the queue
- Lease 3 items from the queue
- Complete all leased items
- Print success/failure summary with checkmarks
- Run `docker compose down` if --cleanup flag set

**Key functions to implement:**
```go
func checkPrerequisites() error
func startDocker() error
func stopDocker() error
func waitForHealth(endpoint string, timeout time.Duration) error
func runQueueWorkflow(client *querator.Client) error
```

**Implementation approach:**
1. Use `os/exec` package to run `docker compose up -d`
2. Import `github.com/kapetan-io/querator` as the client library
3. Create client with: `querator.NewClient(querator.ClientConfig{Endpoint: endpoint})`
4. Use `net/http` for health check (JSON endpoint)
5. Use querator client for protobuf API calls (queue operations)

**Output format example:**
```
Querator Quick Start Validation
================================
[✓] Prerequisites check passed
[✓] Docker compose started
[✓] Health check passed (status: pass)
[✓] Queue "quickstart-test" created
[✓] Produced 3 items
[✓] Leased 3 items
[✓] Completed 3 items

All checks passed!
```

**Error handling:**
- If any step fails, print error and exit with code 1
- If --cleanup flag set, always attempt cleanup even on failure
- Include helpful error messages pointing to documentation

**Context for implementation:**
- Reference client creation from `client.go:38-67`
- Reference queue produce from `client.go:69-84`
- Reference queue lease from `client.go:86-107`
- Reference queue complete from `client.go:109-126`
- Reference test patterns from `common_test.go` for queue creation
- Health endpoint is simple HTTP GET returning JSON (not protobuf)

### Validation
- [ ] Run: `go run quickstart.go`
- [ ] Verify: Script completes successfully with all checkmarks
- [ ] Run: `go run quickstart.go --skip-docker` (with running container)
- [ ] Verify: Script works against existing instance
- [ ] Run: `go run quickstart.go --cleanup`
- [ ] Verify: Docker container cleaned up after run

---

## Summary

| Phase | Deliverable | Key Files |
|-------|-------------|-----------|
| 1 | Health Endpoint | `transport/health.go` (new), `transport/http.go`, `transport/interfaces.go`, `service.go`, `daemon/daemon.go`, `daemon/config.go`, `daemon/health_test.go` (new) |
| 2 | Config Updates | `example.yaml`, `docker-compose.yml` |
| 3 | Storage Docs | `docs/storage/README.md` (new), `docs/storage/badger.md` (new), `docs/storage/postgres.md` (new), `docs/storage/memory.md` (new) |
| 4 | README Quick Start | `README.md` |
| 5 | Validation Script | `quickstart.go` (new) |

**New files:** 8
- `transport/health.go`
- `daemon/health_test.go`
- `docs/storage/README.md`
- `docs/storage/badger.md`
- `docs/storage/postgres.md`
- `docs/storage/memory.md`
- `quickstart.go`

**Modified files:** 7
- `transport/http.go`
- `transport/interfaces.go`
- `service.go`
- `daemon/daemon.go`
- `daemon/config.go`
- `example.yaml`
- `docker-compose.yml`
- `README.md`
