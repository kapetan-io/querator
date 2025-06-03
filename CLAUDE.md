# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Core Development Tasks
- `make cover` - Generate test coverage report and open in browser
- `make proto` - Generate protobuf files using buf
- You MUST NOT call `make ci` to run the test suite

### Running Individual Tests
Use nested test paths from `go test ./... -v` output:
```bash
# Run specific test
go test ./... -run "^TestQueue/InMemory/Errors/QueueLease/DuplicateClientId$"

# Run test suite for specific storage backend
go test ./... -v -run "^TestQueue/InMemory.*$"
```

## Architecture Overview

### Core Components

**Service Layer (`service.go`)**
- Main entry point implementing the Querator API
- Manages configuration via `ServiceConfig`
- Key limits: `MaxLeaseBatchSize`, `MaxProduceBatchSize`

**QueuesManager (`internal/queues_manager.go`)**
- Manages multiple queues and their lifecycle
- Handles logical queue distribution across partitions
- Coordinates between storage backends and queue operations

**Storage Backends (`internal/store/`)**
- Pluggable storage system with `Queues` and `Storage` interfaces
- Current implementations: InMemory, BadgerDB
- Planned: PostgreSQL, MySQL, SurrealDB, FoundationDB

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
- All timestamps in UTC using `time.Now().UTC()`
- Examples: `LeaseDeadline`, `LeaseTimeout`

### Lease-Based Processing
- Items are leased to consumers with deadlines
- Consumers must complete or retry items before lease expires
- Enables exactly-once processing semantics
- Supports scheduled item delivery and retry mechanisms

## Code Style Guidelines

### Testing Patterns
- Use camelCase for test names
- Functional tests preferred over unit tests (90% target)
- Nested test organization for isolation and dependencies
- Table-driven tests for validation scenarios

### Naming Conventions
- Follow `doc/NAMING.md` guidelines
- Use `const` for unchanging values
- Prefer short, single-word variable names
- Options vs Args vs Config distinction matters

### Required Practices
- Always use `time.Now().UTC()` instead of `time.Now()`
- Use inline structs instead of single-use variables
- Avoid abbreviations in variable names
- Single letter variables only for small scopes (loop indices)

## Storage Interface Requirements
- Support for ordered primary keys (minimum requirement)
- Lazy initialization of storage backends
- Transactions and secondary indexes optional but improve performance
- Each partition backed by single table/collection/bucket
