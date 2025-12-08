# InMemory Storage Backend

InMemory is a RAM-based storage backend that stores all queue data in memory. It provides the fastest performance but offers no persistence - all data is lost when Querator stops or restarts.

## Overview

InMemory is ideal for:
- **Testing**: Unit tests, integration tests, CI/CD pipelines
- **Development**: Local development without external dependencies
- **Ephemeral Workloads**: Temporary processing where persistence is not needed
- **Benchmarking**: Performance testing without disk I/O overhead
- **Prototyping**: Rapid application development

## Configuration

### Basic Configuration

```yaml
queue-storage:
  driver: memory

partition-storage:
  - name: memory-01
    driver: memory
    affinity: 1
```

### Configuration Options

InMemory storage has **no configuration options**. Simply specify `driver: memory` and it works.

### Example: Testing Setup

```yaml
queue-storage:
  driver: memory

partition-storage:
  - name: memory-01
    driver: memory
    affinity: 1
```

This is the simplest possible configuration - perfect for tests and local development.

### Example: Mixed Storage (Not Recommended)

While possible, mixing InMemory with persistent storage is not recommended:

```yaml
# NOT RECOMMENDED: Queue metadata persisted, but items are ephemeral
queue-storage:
  driver: badger
  config:
    storage-dir: /data/queues

partition-storage:
  - name: memory-01
    driver: memory
    affinity: 1
```

In this configuration, queue metadata survives restarts, but all queue items are lost. This creates confusing behavior and should be avoided.

### Memory Usage

Memory usage scales with the number of items:
- Each item consumes ~200-500 bytes of RAM (depending on payload size)
- 1 million items ≈ 200-500 MB RAM
- No disk space required

Monitor memory usage to avoid OOM (out of memory) conditions:

```bash
# Linux: Check memory usage
ps aux | grep querator

# macOS: Check memory usage
ps -o rss,command | grep querator
```

### CI/CD Pipelines

InMemory storage is perfect for CI/CD:
- No external dependencies
- Fast startup
- No cleanup required (data is ephemeral)
- Consistent test environment

## Summary

InMemory storage is perfect for:
- Testing and development
- Scenarios where data loss is acceptable: 
  - cached data 
  - stream or batch processing

For production use, choose:
- [BadgerDB](badger.md) for single-node persistent storage
- [PostgreSQL](postgres.md) for distributed, production-grade deployments

## Additional Resources

- [example.yaml](../../example.yaml) - Complete configuration examples
- [Storage Backend Overview](README.md) - Compare all storage backends
- [Testing Documentation](../../CLAUDE.md#testing-patterns) - Best practices for testing
