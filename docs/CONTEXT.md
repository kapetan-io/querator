# Querator Domain Context

> **No implementation detail in this file.** This page holds only what the code cannot say: purpose,
> domain terms, why a choice was made, the rules that must stay true, and traps a reader would not
> see from one function. If reading the code would tell you this, it does not belong here. Step-by-step flows,
> lists of functions, fields or methods, timings, constants and per-backend mechanics stay in the
> code. A reason with no place on this page goes in an ADR under `docs/adr/` or in the package
> comment (`doc.go`).

This page states what Querator is for and defines the domain language used throughout the codebase,
API, and documentation. A term is defined by what it means to a user of Querator, not by how the
code builds it.

## Purpose

Querator is a lease-based FIFO queue system providing Almost Exactly Once Delivery (AEOD), for
developers building event-driven, resilient, distributed applications. When a consumer takes an
item it gets a lease, an exclusive claim on that item. Completing the item removes it. If the
consumer crashes, the lease runs out and another consumer gets the item. An item may be processed
twice after a failure, but a crash never loses it. An item that expires or runs out of attempts goes
to the dead letter queue, or is deleted when the queue has none. Querator coordinates producers and consumers
itself, and keeps its queues in any database that can keep rows in primary key order.

## Use cases

- **Work queue with proof of processing.** A consumer holds an item until it has finished the work,
  then completes it, so the producer knows the item was delivered and processed.
- **Saga pattern.** Coordinate the steps of a distributed transaction through leased items.
- **Ordered delivery.** A queue with one partition delivers items in strict first-in-first-out
  order. More partitions trade that order for throughput.
- **Limited lockable resources.** Items in a queue stand for a limited set of resources, and a lease
  is the lock on one of them.
- **Scheduled delivery.** An item, or a retry, stays hidden until a chosen time.
- **Isolating bad items.** A failed item goes to the back of the queue, and one that keeps failing
  moves to a dead letter queue, so a poison item cannot block the items behind it.
- **Many teams on one deployment.** Namespaces and roles keep each team's or tenant's queues apart.
- **Library or service.** Embed Querator in a Go program, or run it as a server speaking HTTP.

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
A destination queue for items that reached their maximum attempts or expired.
_Avoid_: DLQ (in prose), poison queue

### Architecture

**Partition**:
A single storage-backed segment of a queue, kept in one storage backend. Items in one partition are delivered in order.
_Avoid_: Shard, segment, bucket (when referring to the logical concept)

**Logical Queue**:
A single-threaded synchronization point that manages a subset of partitions and their assigned consumers. Each queue has one logical queue today. The design lets several compose a single queue for horizontal scaling (ADR 0016).
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
The work a queue does on its own as time passes: it returns items whose lease expired to the queue, moves items that expired or reached their maximum attempts to the dead letter queue (or deletes them when the queue has none), and enqueues scheduled items when their time arrives. The logical queue that owns a partition runs that partition's lifecycle.
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
The sole credential type. Its raw value is shown only once, when the key is created.
_Avoid_: Token, secret, password

**Role**:
A named collection of permissions (e.g., `NamespaceOwner`, `QueueProducer`). Querator ships a set of standard roles.
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
A database Querator can keep partitions in (InMemory, BadgerDB, PostgreSQL, MongoDB). Each partition is kept in exactly one backend.
_Avoid_: Driver, adapter, plugin

## Relationships

- A **Namespace** contains zero or more **Queues**
- A **Queue** consists of one or more **Partitions**
- A **Logical Queue** manages a subset of **Partitions** within a **Queue**
- A **Partition** is backed by exactly one **Storage Backend**
- An **Item** lives in exactly one **Partition** for its entire lifecycle
- A **Lease** grants one consumer exclusive access to one **Item** until its **Deadline**
- The **Logical Queue** runs the **Lifecycle** of each **Partition** it manages
- A **User** authenticates via one or more **API Keys**
- A **Role Binding** associates one **User** with one **Role** in one **Namespace**
- The **_system Namespace** cascades permissions to all other **Namespaces**

## Example Dialogue

> **Dev:** "When a **Producer** calls produce with an **EnqueueAt** in the future, does the **Item** go into the **Partition** immediately?"
> **Domain expert:** "Yes — the **Item** is stored in the **Partition** immediately, but no consumer can **Lease** it until the **Lifecycle** enqueues it once its **EnqueueAt** has arrived."

> **Dev:** "If a **Lease** expires, does the **Item** stay at its original position in the queue?"
> **Domain expert:** "No — to avoid head-of-line blocking, the **Lifecycle** puts an **Item** whose **Lease** expired at the *tail* of its **Partition**. Items already waiting in the queue go first."

> **Dev:** "Can a scoped **API Key** access queues in a different **Namespace** if the **User** has a **Role Binding** there?"
> **Domain expert:** "No. A scoped key reaches only its own **Namespace**, whatever the user's permissions elsewhere. The one exception is the **_system Namespace**, where it has only the permissions the user holds through **_system** role bindings."

## Flagged Ambiguities

- "queue" was used historically to mean both the user-facing resource and the underlying storage unit — resolved: the user-facing resource is **Queue**, the storage unit is **Partition** (see ADR 0016).
- "group" appeared in early designs (ADR 0010) as "Queue Group" — resolved: superseded by the **Queue** + **Partition** model.
- "message" vs "item" — resolved: always use **Item** in code and API; "message" is acceptable only in informal external communication.
- "reserve" appeared in early code — resolved: the canonical term is **Lease** (both noun and verb).
