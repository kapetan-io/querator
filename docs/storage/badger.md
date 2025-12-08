# BadgerDB Storage Backend

BadgerDB is an embedded key-value database written in Go. Querator uses BadgerDB for persistent storage in single-node deployments where you need data persistence without the complexity of an external database.

## Overview

BadgerDB is ideal for:
- **Single-Node Deployments**: When you don't need to scale horizontally
- **Edge Computing**: Running Querator on edge devices with local storage
- **Development**: Local testing with persistence
- **Embedded Systems**: Applications with Querator embedded as a library
- **Simple Production**: Small to medium workloads on a single server

## Configuration

### Basic Configuration

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

### Configuration Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `storage-dir` | string | Yes | Directory path where BadgerDB will store its data files |

### Example: Docker Deployment

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

Mount a volume to `/data` to persist data:

```bash
docker run -v /path/on/host:/data ghcr.io/kapetan-io/querator:latest
```

### Example: Multiple Storage Locations

You can configure multiple BadgerDB partition stores with different storage directories:

```yaml
partition-storage:
  - name: badger-ssd
    driver: badger
    affinity: 7
    config:
      storage-dir: /mnt/ssd/querator

  - name: badger-hdd
    driver: badger
    affinity: 3
    config:
      storage-dir: /mnt/hdd/querator
```

This places 70% of partitions on SSD storage and 30% on HDD storage.

## Directory Structure

BadgerDB creates a directory structure like this:

```
/data/
├── queues/
│   └── ~queue-storage-partition/    # Queue metadata
│       ├── 000000.vlog               # Value log
│       ├── 000001.sst                # SSTable files
│       ├── 000002.sst
│       ├── MANIFEST                  # Metadata
│       └── LOCK                      # Lock file
└── partitions/
    ├── my-queue-000000-partition/   # Partition 0 of "my-queue"
    │   ├── 000000.vlog
    │   ├── 000001.sst
    │   └── MANIFEST
    ├── my-queue-000001-partition/   # Partition 1 of "my-queue"
    └── my-queue-000002-partition/   # Partition 2 of "my-queue"
```

Each queue partition gets its own BadgerDB database directory, allowing for:
- Isolated storage per partition
- Independent backup/restore of individual partitions
- Per-partition storage management

### Storage Requirements

BadgerDB uses disk space efficiently:
- **LSM Tree Design**: Writes are buffered in memory then flushed to disk
- **Compression**: Data is compressed by default
- **Compaction**: Background processes merge and compact data files

Typical storage overhead: 1.2-1.5x the size of your queue items (including metadata and compression).

### Memory Usage

BadgerDB keeps some data in memory for performance:
- **Value Log Cache**: Recently accessed values
- **Block Cache**: Index blocks for fast lookups
- **Bloom Filters**: Reduce disk reads for non-existent keys

Expect ~50-200MB RAM per partition depending on workload.

### Disk I/O

BadgerDB is optimized for SSDs but works on HDDs:
- **SSD**: Best performance, low latency for leases and completions
- **HDD**: Works well but expect higher latency for operations

## Troubleshooting

### Corruption or Lock Errors

If BadgerDB fails to open due to corruption:

```
Error: while opening db '/data/partitions': Cannot acquire directory lock
```

**Solutions:**
1. Check if another Querator process is running
2. Remove the `LOCK` file if no process is running
3. If corrupted, restore from backup

### Disk Space Issues

BadgerDB requires free space for compaction:
- Ensure 2-3x the database size is available for compaction
- Monitor disk space usage
- Consider cleaning up old data or increasing storage

### Performance Degradation

If performance degrades over time:
- BadgerDB runs automatic compaction, but check if it's keeping up
- Monitor disk I/O and ensure it's not saturated
- Consider moving to faster storage (SSD)

## Additional Resources

- [BadgerDB Documentation](https://dgraph.io/docs/badger/)
- [example.yaml](../../example.yaml) - Complete configuration examples
- [Storage Backend Overview](README.md) - Compare all storage backends
