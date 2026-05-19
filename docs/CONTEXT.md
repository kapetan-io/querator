# Querator Domain Context

Querator is a lease-based FIFO queue system providing Almost Exactly Once Delivery (AEOD). This context defines the domain language used throughout the codebase, API, and documentation.

## Language

### Queue Operations

**Item**:
A single entry in a queue — the unit of work that is produced, leased, and completed.
_Avoid_: Message, event, task, job

**Produce**:
The act of adding one or more items to a queue.
_Avoid_: Publish, send, enqueue, push

**Lease**:
An exclusive, time-bounded grant to a consumer giving them the right to process an item.
_Avoid_: Reserve, lock, claim, consume

**Complete**:
The act of marking a leased item as successfully processed, removing it from the queue.
_Avoid_: Acknowledge, ack, finish, delete

**Retry**:
The act of releasing a lease and returning an item to the queue for reprocessing, either immediately or at a scheduled time.
_Avoid_: Nack, reject, requeue

**Dead Letter**:
A destination queue for items that have exceeded their maximum retry attempts or dead deadline.
_Avoid_: DLQ (in prose), poison queue

### Architecture

**Partition**:
A single storage-backed segment of a queue. Each partition maps to one table/collection/bucket in the backing store.
_Avoid_: Shard, segment, bucket (when referring to the logical concept)

**Logical Queue**:
A single-threaded synchronization point that manages a subset of partitions and their assigned consumers. Multiple logical queues can compose a single queue for horizontal scaling.
_Avoid_: Worker, coordinator, handler

**Queue**:
A named resource composed of one or more partitions. Users interact with queues; partitions are an implementation detail exposed for operational control.
_Avoid_: Topic, channel, stream

**Namespace**:
An isolation boundary for resources (queues, roles, bindings). A queue belongs to exactly one namespace.
_Avoid_: Tenant, organization, workspace

**QueuesManager**:
The top-level coordinator that manages the lifecycle of all queues, their logical queue instances, and partition assignments.
_Avoid_: Orchestrator, registry

**Lifecycle**:
A per-partition goroutine that scans for items needing action (expired leases, exceeded attempts, scheduled enqueue times) and delegates writes to the logical queue.
_Avoid_: GC, janitor, sweeper

### Time Semantics

**Deadline** (`time.Time`):
A specific point in time by which an operation must complete. Example: `LeaseDeadline`.
_Avoid_: Using "timeout" for a `time.Time` value

**Timeout** (`time.Duration`):
A maximum duration allowed for an operation. Example: `LeaseTimeout`.
_Avoid_: Using "deadline" for a `time.Duration` value

**EnqueueAt** (`time.Time`):
A future timestamp at which a produced item becomes available for leasing (scheduled delivery).
_Avoid_: Delay, defer, schedule (as nouns)

### Auth & Multi-Tenancy

**API Key**:
The sole credential type, formatted as `qtr-[tag]-[entropy]`. Stored as SHA-256 hashes; raw value returned only at creation.
_Avoid_: Token, secret, password

**Role**:
A named collection of permissions (e.g., `NamespaceOwner`, `QueueProducer`). Standard roles are code-defined and synced at startup.
_Avoid_: Policy, group

**Role Binding**:
An association of a user to a role within a specific namespace, granting that user the role's permissions in that namespace.
_Avoid_: Assignment, grant, membership

**Principal**:
The authenticated identity making a request — resolved from an API Key to a User.
_Avoid_: Actor, caller, subject

**_system Namespace**:
A reserved namespace for cluster-wide administration. Permissions granted here cascade to all namespaces.
_Avoid_: Global, root, admin (as namespace names)

### API Design

**Request Timeout**:
A client-specified duration indicating how long the server should attempt to fulfill a produce or lease request before returning an error.
_Avoid_: Wait time, poll duration

**Client ID**:
A unique identifier provided by consumers on lease requests. Encourages efficient batching and prevents duplicate concurrent leases from the same client.
_Avoid_: Consumer ID, subscriber ID

### Storage

**Storage Backend**:
A pluggable implementation of the storage interface (InMemory, BadgerDB, PostgreSQL). Each partition is backed by exactly one backend instance.
_Avoid_: Driver, adapter, plugin

## Relationships

- A **Namespace** contains zero or more **Queues**
- A **Queue** consists of one or more **Partitions**
- A **Logical Queue** manages a subset of **Partitions** within a **Queue**
- A **Partition** is backed by exactly one **Storage Backend**
- An **Item** lives in exactly one **Partition** for its entire lifecycle
- A **Lease** grants one consumer exclusive access to one **Item** until its **Deadline**
- A **Lifecycle** goroutine monitors exactly one **Partition**
- A **User** authenticates via one or more **API Keys**
- A **Role Binding** associates one **User** with one **Role** in one **Namespace**
- The **_system Namespace** cascades permissions to all other **Namespaces**

## Example Dialogue

> **Dev:** "When a **Producer** calls produce with an **EnqueueAt** in the future, does the **Item** go into the **Partition** immediately?"
> **Domain expert:** "Yes — the **Item** is written to the **Partition's** scheduled storage immediately. But it won't appear in the main queue until the **Lifecycle** detects that its **EnqueueAt** has passed and moves it."

> **Dev:** "If a **Lease** expires, does the **Item** stay at its original position in the queue?"
> **Domain expert:** "No — to avoid head-of-line blocking, the **Lifecycle** assigns a new ID and places expired-lease **Items** at the *tail* of the FIFO in that **Partition**. This ensures items already waiting in the queue take precedence over expired items."

> **Dev:** "Can a scoped **API Key** access queues in a different **Namespace** if the **User** has a **Role Binding** there?"
> **Domain expert:** "Never. A scoped key's namespace acts as a hard boolean filter — it's mathematically impossible regardless of the user's permissions elsewhere."

## Flagged Ambiguities

- "queue" was used historically to mean both the user-facing resource and the underlying storage unit — resolved: the user-facing resource is **Queue**, the storage unit is **Partition** (see ADR 0016).
- "group" appeared in early designs (ADR 0010) as "Queue Group" — resolved: superseded by the **Queue** + **Partition** model.
- "message" vs "item" — resolved: always use **Item** in code and API; "message" is acceptable only in informal external communication.
- "reserve" appeared in early code — resolved: the canonical term is **Lease** (both noun and verb).
