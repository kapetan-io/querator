# MongoDB Storage Backend

MongoDB is a widely deployed document database and a natural fit for Querator, which needs only ordered
primary keys from its storage. Querator uses MongoDB as a partition and queue-metadata backend for
production deployments.

This backend is **non-transactional and standalone-compatible**: it uses no multi-document transactions,
requires no replica set, and runs against a single `mongod`. It upholds querator's correctness contracts
through single-document atomic operations, insert-before-delete ordering, and the
single-writer-per-partition guarantee.

## Overview

MongoDB is ideal for:
- **Production workloads**: Horizontal scaling across multiple Querator instances
- **Existing MongoDB infrastructure**: Reuse an operational MongoDB deployment
- **Managed clusters**: Atlas and other managed MongoDB topologies
- **Standalone / development**: Works against a single-node `mongod` — no replica set required

> **Scope:** Auth stores (namespaces, users, API keys, roles, role bindings) are not yet implemented on
> MongoDB — configure memory or badger for those, exactly as the PostgreSQL backend does.

## Configuration

### Basic Configuration

```yaml
queue-storage:
  driver: mongo
  config:
    connection-string: "mongodb://user:pass@localhost:27017"
    database: querator

partition-storage:
  - name: mongo-01
    driver: mongo
    affinity: 1
    config:
      connection-string: "mongodb://user:pass@localhost:27017"
      database: querator
      max-pool-size: "50"
```

### Configuration Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `connection-string` | string | Yes | - | MongoDB connection URI (see format below) |
| `database` | string | No | `querator` | Logical database name within the MongoDB server |
| `max-pool-size` | string (integer) | No | driver default | Maximum connections in the pool per client (`0` = driver default) |

### Connection String Format

The connection string follows the standard MongoDB URI format:

```
mongodb://username:password@host:port
```

**Examples:**

```yaml
# Standalone, no auth (development only)
connection-string: "mongodb://localhost:27017"

# With authentication
connection-string: "mongodb://querator:secret@db.example.com:27017"

# With TLS and options
connection-string: "mongodb://querator:secret@db.example.com:27017/?tls=true"

# Managed cluster (mongodb+srv)
connection-string: "mongodb+srv://querator:secret@cluster0.example.mongodb.net"
```

A **replica set is not required.** The backend is designed and tested against a standalone `mongod`,
which is the practical proof that the non-transactional design holds.

## Database Requirements

### MongoDB Version

**Minimum Version:** MongoDB 4.4+ (for `partialFilterExpression` and `$type` aggregation support).
**Recommended Version:** MongoDB 6.0+.

### Required Permissions

The MongoDB user needs read/write access to the configured database plus the ability to create
collections and indexes:

```javascript
db.createUser({
  user: "querator",
  pwd: "secure_password",
  roles: [ { role: "readWrite", db: "querator" } ]
})
```

### Schema Auto-Creation

Querator creates collections and indexes lazily on first use — no manual setup is required. When a
partition is first written, the collection (and its indexes) are created implicitly.

## Connection Pooling

A `mongo.Client` is itself a connection pool. Querator shares one client per connection string across all
partitions and queues pointed at the same server, ref-counted so the client disconnects only when the
last user closes. The `max-pool-size` setting caps connections per client.

```yaml
partition-storage:
  - name: mongo-01
    driver: mongo
    config:
      connection-string: "mongodb://user:pass@localhost:27017"
      database: querator
      max-pool-size: "50"
```

## Performance Considerations

- **Indexes** keep lease selection, scheduled scans, and expiry scans index-backed.
- **`Clear`** with a destructive whole-queue request drops the collection (O(1)) instead of deleting
  documents one by one.
- **Collection-per-partition**: many queues × partitions ⇒ many collections. MongoDB/WiredTiger handles
  large collection counts, but each collection carries namespace and index overhead. This is an
  operational consideration (not a correctness limit) when running thousands of partitions on one server.

## Operational Notes

- A mid-operation crash during a tail-move leaves the original item in place, to be re-found and
  re-queued on the next lifecycle scan — at worst a duplicate delivery, never a lost item.
- There is no database-level backstop against a duplicate `source_id` or a double claim; correctness
  rests on the single-writer-per-partition invariant (ADR-0003/0009).

## Additional Resources

- [MongoDB Documentation](https://www.mongodb.com/docs/)
- [Go Driver Documentation](https://pkg.go.dev/go.mongodb.org/mongo-driver/mongo)
- [example.yaml](../../example.yaml) - Complete configuration examples
- [Storage Backend Overview](README.md) - Compare all storage backends
