# Storage Backends

Querator supports multiple storage backends for both queue metadata and partition data. This flexibility allows you to choose the right storage solution for your deployment requirements.

## Available Backends

Querator currently supports three storage backends:

- **[InMemory](memory.md)** - RAM-only storage for testing and development
- **[BadgerDB](badger.md)** - Embedded key-value store for single-node deployments
- **[PostgreSQL](postgres.md)** - Full-featured database for production deployments

## Quick Comparison

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

## Choosing a Storage Backend

### InMemory
Use InMemory storage when:
- Running tests or local development
- You don't need persistence
- You want the fastest possible performance
- Data loss on restart is acceptable

**Not recommended for:** Production environments, any scenario requiring data persistence.

### BadgerDB
Use BadgerDB when:
- You need persistence but want embedded storage
- Running a single-node deployment
- Deploying to edge devices or environments without external databases
- You want minimal operational complexity

**Not recommended for:** Distributed deployments, high-availability requirements.

### PostgreSQL
Use PostgreSQL when:
- Running production workloads
- You need horizontal scaling across multiple Querator instances
- High availability is required
- You have existing PostgreSQL infrastructure
- You need advanced database features (replication, backups, monitoring)

**Not recommended for:** Single-node embedded deployments, edge devices.

## Configuration Patterns

### Queue Storage and Partition Storage

Querator uses two types of storage:

1. **Queue Storage** - Stores metadata about queues (names, configuration, settings)
2. **Partition Storage** - Stores the actual queue items and their state

You can mix storage backends. For example, you could use BadgerDB for queue metadata and PostgreSQL for partition data:

```yaml
queue-storage:
  driver: badger
  config:
    storage-dir: /data/queues

partition-storage:
  - name: postgres-01
    driver: postgres
    affinity: 1
    config:
      connection-string: "postgres://user:pass@localhost:5432/querator"
```

### Multiple Partition Stores

You can configure multiple partition stores with different affinities to distribute queues across storage backends:

```yaml
partition-storage:
  - name: badger-01
    driver: badger
    affinity: 3
    config:
      storage-dir: /data/partitions-badger

  - name: postgres-01
    driver: postgres
    affinity: 7
    config:
      connection-string: "postgres://user:pass@localhost:5432/querator"
```

This configuration would place 30% of partitions on BadgerDB and 70% on PostgreSQL.

## Storage Directory Structure

### BadgerDB

When using BadgerDB, Querator creates a directory structure like this:

```
/data/
├── queues/
│   └── ~queue-storage-partition/    # Queue metadata
│       ├── 000000.vlog
│       ├── 000001.sst
│       └── MANIFEST
└── partitions/
    ├── queue-name-000000-partition/ # Partition 0 for queue-name
    ├── queue-name-000001-partition/ # Partition 1 for queue-name
    └── queue-name-000002-partition/ # Partition 2 for queue-name
```

### PostgreSQL

When using PostgreSQL, Querator automatically creates tables with the naming pattern:

```
items_<hash>_<partition>
```

For example:
- `items_5d41402abc4b_000000` - Partition 0 for a queue
- `items_5d41402abc4b_000001` - Partition 1 for the same queue

The hash is derived from the queue name to ensure unique table names across queues.

## Next Steps

- Review backend-specific documentation for detailed configuration options
- See [example.yaml](../../example.yaml) for complete configuration examples
- Check the [Quick Start](../../README.md#quick-start) guide for getting started with Docker
