# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Core Development Tasks
- `make cover` - Generate test coverage report and open in browser
- `make proto` - Generate protobuf files using buf
- `make ci` - Run the full test suite and linter. Before running, stage the files you intend to commit (`git add`), then run `make ci`. Commit only after everything passes.
- `make tiger` - Run the Tiger static analyzer over every package. Install it with `go install github.com/kapetan-io/tiger/cmd/tiger@latest`. Any `make` run turns on `.githooks/pre-push`, which runs Tiger before each push. `git push --no-verify` skips it.

### Functional Testing Strategy

This codebase uses a **functional testing approach** where tests exercise complete workflows through public APIs only.

#### Test Structure
- **Single Entry Point**: `TestXXXX()` automatically runs all tests against multiple storage backends
- **Nested Organization**: `t.Run("Category", func() { t.Run("TestName", func() { ... }) })`
- **Path Pattern**: `TestQueue/<Backend>/Category/TestName` (e.g., `TestQueue/InMemory/Errors/QueueRetry/InvalidId`)

#### Adding New Tests
1. **Location**: Add tests inside the `testXXXX()` function in test files
2. **Backend Coverage**: Tests automatically run against every storage backend - no extra work needed
3. **Organization**:
    - Happy path tests: Add as top-level `t.Run("TestName", ...)`
    - Error tests: Add under `t.Run("Errors", func() { t.Run("QueueMethodName", ...) })`

#### Test Patterns
- **Setup**: Each test creates its own daemon and uses random queue names for isolation
- **API Testing**: Use client (`c`) to make requests, never test internal methods directly
- **Error Validation**: Use `duh.Error` type to validate error codes and messages
- **Cleanup**: Each test handles its own `defer d.Shutdown(t)` and `defer tearDown()`

#### Example Structure for New Endpoint Tests:
  ```go
  // Happy path test
  t.Run("QueueRetryWorkflow", func(t *testing.T) {
      // setup, produce, lease, retry, verify
  })

  // Error tests
  t.Run("Errors", func(t *testing.T) {
      t.Run("QueueRetry", func(t *testing.T) {
          t.Run("InvalidId", func(t *testing.T) { ... })
          t.Run("NotLeased", func(t *testing.T) { ... })
          t.Run("DeadLetter", func(t *testing.T) { ... })
      })
  })
```

This structure ensures **every test automatically validates every storage backend** and maintains **complete functional
coverage** through public API testing only.

### Running Individual Tests
Use nested test paths from `go test ./... -v` output:
```bash
# Run specific test
go test ./... -run "^TestQueue/<Backend>/Errors/QueueLease/DuplicateClientId$"

Where <Backend> is the name of a storage backend

# Run test suite for specific storage backend
go test ./... -v -run "^TestQueue/InMemory.*$"
```

The test suite is designed to run ALL the tests using multiple backends. As such,
the tests are all nested under a single test function.

## Architecture Overview

### Core Components

**Service Layer**
- Main entry point implementing the Querator API
- Key limits: `MaxLeaseBatchSize`, `MaxProduceBatchSize`

**QueuesManager (`internal/queues_manager.go`)**
- Manages multiple queues and their lifecycle
- Handles logical queue distribution across partitions
- Coordinates between storage backends and queue operations

**Storage Backends (`internal/store/`)**
- Pluggable storage system with `Queues` and `Storage` interfaces

**Queue Structure**
- Each queue can have multiple partitions for horizontal scaling
- Partitions enable parallel processing and load distribution
- Logical queues automatically balance consumers across partitions

### Key Data Types

**Items vs Messages**
- Use "item" terminology throughout codebase, not "message"
- `types.Item` represents queue entries with lease semantics
- Items support scheduled delivery via `EnqueueAt`

**Time Conventions**
- Deadlines: `time.Time` (specific point in time)
- Timeouts: `time.Duration` (duration of time)
- Examples: `LeaseDeadline`, `LeaseTimeout`

### Lease-Based Processing
- Items are leased to consumers with deadlines
- Consumers must complete or retry items before lease expires
- Supports scheduled item delivery and retry mechanisms

## Code Style Guidelines

### Testing Patterns
- Use camelCase for test names
- Functional tests preferred over unit tests (90% target)
- Nested test organization for isolation and dependencies
- Table-driven tests for validation scenarios
- You MUST use `github.com/stretchr/testify/assert` and `github.com/stretchr/testify/require` instead of `if` for assertions
- All tests MUST be in the `XXXX_test` package and NOT in the same package which is under test

### Documentation
- A change to the implementation needs no documentation edit. `docs/operating-limits.md` holds only limits an operator sees or must work around, and changes only when what an operator sees or must do changes. `docs/CONTEXT.md` is the glossary. If your change makes a sentence in any of these false, delete the sentence and say so in the pull request.
- A rule or trap that must stay true goes in a test that fails when the code breaks it. A reason why something is built the way it is goes in an ADR under `docs/adr/`, written as the decision as it stands, and no longer in a package comment (`doc.go`). Tiger (the static checker, `GOTOOLCHAIN=go1.26.10 tiger check ./...`) allows no comments below the `package` line, so a reason cannot sit next to the code it explains.

### Naming Conventions
- Follow `docs/NAMING.md` guidelines
- Use `const` for unchanging values
- Prefer short, single-word variable names
- Options vs Args vs Config distinction matters

### Required Practices
- Always use `clock.Now().UTC()` instead of `time.Now()`
- Use inline structs instead of single-use variables
- Avoid abbreviations in variable names
- Single letter variables only for small scopes (loop indices)

## Storage Interface Requirements
- Support for ordered primary keys (minimum requirement)
- Lazy initialization of storage backends
- Transactions and secondary indexes optional but improve performance
- Each partition backed by single table/collection/bucket