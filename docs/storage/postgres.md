# PostgreSQL Storage Backend

PostgreSQL is a powerful, open-source relational database. Querator uses PostgreSQL for production deployments requiring high availability, horizontal scaling, and advanced database features.

## Overview

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
    # sslmode=disable for local development only; use sslmode=require or higher in production
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

## Additional Resources

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [pgxpool Documentation](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool)
- [example.yaml](../../example.yaml) - Complete configuration examples
- [Storage Backend Overview](README.md) - Compare all storage backends
