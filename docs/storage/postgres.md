# PostgreSQL Storage Backend

PostgreSQL is a powerful, open-source relational database. Querator uses PostgreSQL for production deployments requiring high availability, horizontal scaling, and advanced database features.

## Overview

PostgreSQL provides:
- **Persistence**: Reliable, ACID-compliant data storage
- **Distributed**: Multiple Querator instances can share the same database
- **High Availability**: Use PostgreSQL replication for HA setups
- **Connection Pooling**: Efficient connection management with pgxpool
- **Automatic Schema**: Tables and indexes created automatically
- **Advanced Features**: Partial indexes, JSONB support, full-text search (future)

## Use Cases

PostgreSQL is ideal for:
- **Production Deployments**: Mission-critical workloads requiring reliability
- **Horizontal Scaling**: Multiple Querator instances sharing partition storage
- **High Availability**: PostgreSQL replication for zero-downtime deployments
- **Cloud Environments**: Managed PostgreSQL services (RDS, Cloud SQL, Azure Database)
- **Large Scale**: High throughput, millions of items
- **Compliance**: Audit trails, backups, point-in-time recovery

## Configuration

### Basic Configuration

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

### Configuration Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `connection-string` | string | Yes | - | PostgreSQL connection string (see format below) |
| `max-conns` | integer | No | pgxpool default | Maximum connections in the pool per storage backend |

### Connection String Format

The connection string follows the PostgreSQL URI format:

```
postgres://username:password@host:port/database?options
```

**Examples:**

```yaml
# Basic connection
connection-string: "postgres://querator:secret@localhost:5432/querator"

# With SSL/TLS
connection-string: "postgres://querator:secret@db.example.com:5432/querator?sslmode=require"

# Disable SSL (development only)
connection-string: "postgres://querator:secret@localhost:5432/querator?sslmode=disable"

# With connection timeout
connection-string: "postgres://querator:secret@localhost:5432/querator?connect_timeout=10"
```

**Common SSL Modes:**
- `disable` - No SSL (not recommended for production)
- `require` - Require SSL but don't verify certificate
- `verify-ca` - Require SSL and verify certificate authority
- `verify-full` - Require SSL and verify hostname

### Example: Production Setup

```yaml
queue-storage:
  driver: postgres
  config:
    connection-string: "postgres://querator_user:secure_password@db-primary.internal:5432/querator?sslmode=verify-full&pool_max_conns=20"

partition-storage:
  - name: postgres-01
    driver: postgres
    affinity: 1
    config:
      connection-string: "postgres://querator_user:secure_password@db-primary.internal:5432/querator?sslmode=verify-full"
      max-conns: 50
```

### Example: Multiple Database Servers

You can distribute partitions across multiple PostgreSQL servers:

```yaml
partition-storage:
  - name: postgres-us-east
    driver: postgres
    affinity: 5
    config:
      connection-string: "postgres://user:pass@db-us-east.internal:5432/querator"
      max-conns: 50

  - name: postgres-us-west
    driver: postgres
    affinity: 5
    config:
      connection-string: "postgres://user:pass@db-us-west.internal:5432/querator"
      max-conns: 50
```

This distributes partitions evenly across two regional databases.

## Database Requirements

### PostgreSQL Version

**Minimum Version:** PostgreSQL 12+

PostgreSQL 12 introduced features that Querator relies on:
- `TIMESTAMPTZ` precision improvements
- Better partial index support
- Improved query planner

**Recommended Version:** PostgreSQL 14+ for best performance.

### Required Permissions

The PostgreSQL user needs the following permissions:

```sql
GRANT CREATE ON DATABASE querator TO querator_user;
GRANT CREATE ON SCHEMA public TO querator_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO querator_user;
```

Or simply grant full access to a dedicated database:

```sql
CREATE USER querator_user WITH PASSWORD 'secure_password';
CREATE DATABASE querator OWNER querator_user;
```

### Schema Auto-Creation

Querator automatically creates tables and indexes on first use. No manual schema setup is required.

When a queue partition is first accessed, Querator creates:
1. A table named `items_<hash>_<partition>`
2. Indexes for efficient querying
3. Unique constraints for deduplication

## Table Schema

For each partition, Querator creates a table with this schema:

```sql
CREATE TABLE items_<hash>_<partition> (
    id TEXT COLLATE "C" PRIMARY KEY,
    source_id TEXT,
    is_leased BOOLEAN NOT NULL DEFAULT false,
    lease_deadline TIMESTAMPTZ,
    expire_deadline TIMESTAMPTZ NOT NULL,
    enqueue_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 0,
    reference TEXT NOT NULL DEFAULT '',
    encoding TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL DEFAULT '',
    payload BYTEA
);
```

**Indexes Created:**

```sql
-- Unique index for source_id deduplication
CREATE UNIQUE INDEX idx_<hash>_<partition>_src ON items_<hash>_<partition> (source_id)
WHERE source_id IS NOT NULL;

-- Index for leasing items
CREATE INDEX idx_<hash>_<partition>_lse ON items_<hash>_<partition> (is_leased, enqueue_at)
WHERE is_leased = false AND enqueue_at IS NULL;

-- Index for scheduled items
CREATE INDEX idx_<hash>_<partition>_sch ON items_<hash>_<partition> (enqueue_at)
WHERE enqueue_at IS NOT NULL;

-- Index for lease expiry scanning
CREATE INDEX idx_<hash>_<partition>_dln ON items_<hash>_<partition> (lease_deadline)
WHERE is_leased = true;

-- Index for item expiry scanning
CREATE INDEX idx_<hash>_<partition>_exp ON items_<hash>_<partition> (expire_deadline);
```

### Table Naming Convention

Table names use the pattern: `items_<hash>_<partition>`

- `<hash>` - MD5 hash of the queue name (first 12 characters)
- `<partition>` - Zero-padded partition number (e.g., `000000`, `000001`)

**Examples:**
- Queue `my-queue` partition 0: `items_c5a38fb13ab1_000000`
- Queue `my-queue` partition 1: `items_c5a38fb13ab1_000001`
- Queue `orders` partition 0: `items_d79c8788647f_000000`

The hash ensures unique table names even if queue names are similar.

## Connection Pooling

Querator uses `pgxpool` for efficient connection management:

- **Global Pool Manager**: Connections are shared across partitions with the same connection string
- **Automatic Pooling**: Pool is created on first use
- **Connection Reuse**: Connections are reused across requests
- **Health Checks**: Connections are validated before use

### Pool Configuration

The `max-conns` setting controls the maximum number of connections per storage backend:

```yaml
partition-storage:
  - name: postgres-01
    driver: postgres
    config:
      connection-string: "postgres://user:pass@localhost:5432/querator"
      max-conns: 50  # Maximum 50 connections to this database
```

**Guidelines:**
- Start with 10-20 connections per Querator instance
- Increase if you see connection pool exhaustion
- Monitor PostgreSQL connection count: `SELECT count(*) FROM pg_stat_activity;`
- Don't exceed PostgreSQL's `max_connections` setting

## Performance Considerations

### Indexing Strategy

Querator uses partial indexes to optimize common queries:
- **Leasing**: Index on `(is_leased, enqueue_at)` for fast item selection
- **Scheduled Items**: Index on `enqueue_at` for scheduled delivery
- **Lease Expiry**: Index on `lease_deadline` for expired lease detection
- **Item Expiry**: Index on `expire_deadline` for TTL enforcement

These indexes keep leasing fast even with millions of items.

### Write Performance

PostgreSQL handles high write throughput:
- **Batching**: Querator batches produce operations for efficiency
- **Transactions**: ACID guarantees for item operations
- **WAL**: Write-ahead logging for durability

Expect:
- **Produce**: 5,000-20,000 items/sec per Querator instance
- **Lease**: 2,000-10,000 items/sec
- **Complete**: 2,000-10,000 items/sec

Actual performance depends on PostgreSQL hardware, network latency, and item size.

### Query Optimization

For best performance:
- Ensure indexes are used (check with `EXPLAIN ANALYZE`)
- Keep item payloads reasonably sized (<1MB)
- Use connection pooling
- Consider read replicas for scaling reads

### Scaling Strategies

**Vertical Scaling:**
- Increase PostgreSQL server CPU/RAM/disk
- Use faster storage (NVMe SSDs)
- Tune PostgreSQL settings (`shared_buffers`, `work_mem`, etc.)

**Horizontal Scaling:**
- Use PostgreSQL replication (read replicas)
- Distribute partitions across multiple Querator instances
- Consider sharding across multiple databases

## High Availability

### PostgreSQL Replication

Use PostgreSQL replication for HA:

```
Primary (Read/Write) --> Standby (Read-Only)
```

Configure Querator to connect to the primary:

```yaml
partition-storage:
  - name: postgres-01
    driver: postgres
    config:
      connection-string: "postgres://user:pass@primary.db:5432/querator"
```

Use a connection pooler (PgBouncer, pgpool-II) or load balancer to handle failover.

### Managed Services

Cloud providers offer managed PostgreSQL with HA:
- **AWS RDS**: Multi-AZ deployments
- **Google Cloud SQL**: Regional HA with automatic failover
- **Azure Database**: Zone-redundant HA
- **DigitalOcean Managed Databases**: Standby nodes

Configure Querator with the managed service endpoint:

```yaml
partition-storage:
  - name: postgres-prod
    driver: postgres
    config:
      connection-string: "postgres://user:pass@managed-postgres.cloud:5432/querator?sslmode=require"
```

## Backup and Recovery

### Backup Strategies

**Option 1: PostgreSQL pg_dump**

```bash
pg_dump -h localhost -U querator_user -d querator -F c -f querator_backup.dump
```

**Option 2: Continuous Archiving (WAL)**

Configure PostgreSQL for point-in-time recovery (PITR):
- Enable WAL archiving
- Use `pg_basebackup` for base backups
- Archive WAL files to S3/GCS

**Option 3: Managed Service Backups**

Use your cloud provider's automated backup features:
- AWS RDS: Automated backups and snapshots
- Google Cloud SQL: Automated backups with retention
- Azure Database: Automated backups with PITR

### Recovery

**From pg_dump:**

```bash
pg_restore -h localhost -U querator_user -d querator querator_backup.dump
```

**From PITR:**

Follow PostgreSQL's point-in-time recovery process:
1. Restore base backup
2. Replay WAL files to desired point in time
3. Promote to primary

## Monitoring

### Key Metrics

Monitor these PostgreSQL metrics:
- **Connection Count**: Ensure pool is not exhausted
- **Query Latency**: P95/P99 latency for lease/produce operations
- **Table Size**: Growth rate of `items_*` tables
- **Index Usage**: Verify indexes are used efficiently
- **WAL Size**: Monitor write-ahead log disk usage

### Useful Queries

**Check connection count:**

```sql
SELECT count(*) FROM pg_stat_activity WHERE datname = 'querator';
```

**Check table sizes:**

```sql
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename LIKE 'items_%'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

**Check index usage:**

```sql
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan
FROM pg_stat_user_indexes
WHERE tablename LIKE 'items_%'
ORDER BY idx_scan DESC;
```

## Troubleshooting

### Connection Pool Exhausted

```
Error: failed to acquire connection: all connections in pool are busy
```

**Solutions:**
1. Increase `max-conns` in configuration
2. Increase PostgreSQL's `max_connections` setting
3. Scale horizontally with more Querator instances

### Slow Queries

If operations are slow:
1. Check query plans with `EXPLAIN ANALYZE`
2. Ensure indexes are being used
3. Increase PostgreSQL resources (CPU, RAM)
4. Check network latency between Querator and PostgreSQL

### Table Size Growth

If tables grow too large:
1. Implement expiry on items (`expire_timeout` in queue config)
2. Use dead letter queues to remove failed items
3. Archive old items to cold storage
4. Increase `VACUUM` frequency

## Migration

### From InMemory to PostgreSQL

Change the configuration and restart Querator. Existing queues will be lost (InMemory has no persistence).

### From BadgerDB to PostgreSQL

Currently, there is no automated migration tool. To migrate:
1. Set up PostgreSQL backend
2. Create queues in new setup
3. Drain old queues (process all items)
4. Switch to PostgreSQL configuration
5. Start producing to new queues

## Additional Resources

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [pgxpool Documentation](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool)
- [example.yaml](../../example.yaml) - Complete configuration examples
- [Storage Backend Overview](README.md) - Compare all storage backends
