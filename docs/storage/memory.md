# InMemory Storage Backend

InMemory is a RAM-based storage backend that stores all queue data in memory. It provides the fastest performance but offers no persistence - all data is lost when Querator stops or restarts.

## Overview

InMemory provides:
- **Fastest Performance**: No disk I/O, all operations in RAM
- **Zero Configuration**: No setup required, works out of the box
- **No Persistence**: Data is lost on restart (by design)
- **Simplicity**: Perfect for development and testing
- **Low Resource Usage**: Minimal overhead

## Use Cases

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

## Performance Characteristics

### Speed

InMemory is the fastest storage backend:
- **No Disk I/O**: All operations happen in RAM
- **No Serialization**: No encoding/decoding overhead
- **No Network**: No remote calls

Expect:
- **Produce**: 100,000+ items/sec
- **Lease**: 50,000+ items/sec
- **Complete**: 50,000+ items/sec

Actual performance depends on CPU and memory speed.

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

## Limitations

InMemory storage has significant limitations:

- **No Persistence**: All data is lost on restart
- **No Durability**: Power loss or crash loses all items
- **Memory Constraints**: Limited by available RAM
- **Single Process Only**: Cannot be shared across Querator instances
- **Not for Production**: Should never be used in production environments

## Data Loss Scenarios

Data is lost when:
- Querator process stops or restarts
- Server reboots
- Power failure
- Out of memory (OOM) kill
- Process crash or panic

**There is no recovery mechanism.** All items are permanently lost.

## When NOT to Use InMemory

Avoid InMemory storage when:
- Running production workloads
- Persistence is required
- Data loss is unacceptable
- Sharing state across multiple instances
- Processing important transactions

For production use, choose [BadgerDB](badger.md) or [PostgreSQL](postgres.md).

## Testing Best Practices

InMemory storage is perfect for tests:

### Go Tests

```go
func TestQueueOperations(t *testing.T) {
    // Create daemon with InMemory storage
    d, err := daemon.NewDaemon(ctx, daemon.Config{
        InMemoryListener: true,  // Also uses in-memory listener
    })
    require.NoError(t, err)
    defer d.Shutdown(ctx)

    // Test queue operations...
}
```

### Integration Tests

```yaml
# test-config.yaml
queue-storage:
  driver: memory

partition-storage:
  - name: memory-01
    driver: memory
    affinity: 1
```

```bash
querator --config test-config.yaml &
QUERATOR_PID=$!

# Run tests...
curl http://localhost:2319/health

# Cleanup
kill $QUERATOR_PID
```

### CI/CD Pipelines

InMemory storage is perfect for CI/CD:
- No external dependencies
- Fast startup
- No cleanup required (data is ephemeral)
- Consistent test environment

## Migration

### From InMemory to Persistent Storage

To move from InMemory to persistent storage:

1. Update configuration to use BadgerDB or PostgreSQL
2. Restart Querator
3. Recreate queues (InMemory data is lost)
4. Start producing items to new persistent storage

There is no migration path because InMemory data is not persisted.

### From Persistent Storage to InMemory

Not recommended, but if needed:
1. Update configuration to use InMemory
2. Restart Querator
3. All queue items from persistent storage are lost

This is typically only done when switching from production back to development/testing.

## Troubleshooting

### Out of Memory Errors

If Querator crashes with OOM:

```
fatal error: out of memory
```

**Solutions:**
1. Reduce the number of items in queues
2. Process items faster (increase consumers)
3. Switch to persistent storage (BadgerDB or PostgreSQL)
4. Increase available RAM

### Performance Degradation

If InMemory performance degrades:
- Check for memory swapping (use `vmstat` or `top`)
- Ensure sufficient RAM is available
- Monitor garbage collection pressure (use `GODEBUG=gctrace=1`)

### Unexpected Data Loss

If data disappears unexpectedly:
- Check if Querator restarted (check logs)
- Verify no OOM kills occurred (check system logs)
- Ensure you're using the same Querator instance (not a new one)

Remember: InMemory storage is **designed** to lose data on restart. This is expected behavior.

## Comparison with Other Backends

| Feature | InMemory | BadgerDB | PostgreSQL |
|---------|----------|----------|------------|
| **Speed** | Fastest | Fast | Moderate |
| **Persistence** | None | Yes | Yes |
| **Memory Usage** | High (all in RAM) | Low (disk-backed) | Low (disk-backed) |
| **Setup Complexity** | None | Low | Medium |
| **Production Ready** | No | Single-node only | Yes |

## Summary

InMemory storage is perfect for:
- Testing and development
- Scenarios where data loss is acceptable
- Maximum performance without persistence

Avoid InMemory for:
- Production deployments
- Any scenario requiring data durability
- Sharing state across multiple instances

For production use, choose:
- [BadgerDB](badger.md) for single-node persistent storage
- [PostgreSQL](postgres.md) for distributed, production-grade deployments

## Additional Resources

- [example.yaml](../../example.yaml) - Complete configuration examples
- [Storage Backend Overview](README.md) - Compare all storage backends
- [Testing Documentation](../../CLAUDE.md#testing-patterns) - Best practices for testing
