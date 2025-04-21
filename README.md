> NOTE: Querator is currently still in early development

## Querator
Querator is a **Distributed Durable Execution System** built on top of an **Almost Exactly Once Delivery**
(AEOD) Queue.

Querator addresses both **Durable Execution** and **Exactly Once Delivery Queues**, which together form a
symbiotic relationship that enables developers to build event-driven, highly resilient, distributed, 
high-performance applications.

### Durable Execution With Querator
TODO: Details on this are still evolving.

### Exactly Once Delivery With Querator
At the heart of Querator is the Almost Exactly Once Delivery FIFO queue, backed by a database of your choice.

We say **“Almost” Exactly Once** because [Exactly Once Delivery (EOD) is theoretically impossible](https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/).
However, in practice, you can achieve AEOD—or “Almost Exactly Once Delivery”—which is functionally equivalent to EOD, 
with the understanding that occasional duplicate deliveries may occur due to system failures.

From our extensive experience operating EOD systems at scale, the likelihood of duplicate delivery and processing 
is about the same as your system’s overall failure rate. In other words, message delivery is only as reliable as 
the system it runs on. Remember, [Distributed systems are all
about trade-offs](https://www.infoq.com/articles/cap-twelve-years-later-how-the-rules-have-changed/).

Anyone claiming their system provides, precise EOD is being disingenuous at best.

#### Exactly Once with Lease
Unlike streaming or other queue systems, Querator is designed from the ground up to provide high throughput
access to items in the queue via a [Lease](https://en.wikipedia.org/wiki/Lease_(computer_science)). When a consumer 
pulls an item off the queue there is an implicit **lease** created between the consumer and Querator. The 
consumer gains exclusive right to process that item, until such time as they mark the item as **complete** 
or tell Querator to **retry** the item by releasing the **lease** and giving the item to another consumer 
immediately, or at some future date and time.

The concept of a **lease** provides users of Querator with proof the item was delivered and provides assurances
that the item was processed by the consumer! In this way, you can sort of think of a **lease** as a locking 
primitive for processing items. A Consumer can hold on to the lease until the agreed upon timeout, it can hold 
off marking the lease as complete until it has processed the item that it consumed. As a result, you can use the
locking primitive the lease provides to solve several distributed problems.

- Implement multi-step, durable execution functions.
- Implement the Saga Pattern for distributed transactions
- Use it as a FIFO queue with ordered delivery of messages
- Use it as a limit locking system, where items in the queue represent a limited lockable resource

#### Scheduled Delivery
Querator also supports scheduled delivery of the item queued. Given a specific `enqueue_at` querator
will defer enqueue of that item until the specified time. Retries can also specify a future **retry** time such
that the retried item will not be enqueued to be processed until the specified time is reached.

### Does Querator scale?
YES! We scaled a closed source version of the Querator queue service at https://mailgun.com to multi-billions of
messages a day in a very efficient and cost-effective manner, Meta (aka FaceBook) 
[built a similar system](https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/) which
scaled to billions of messages. The closed source version of Querator we ran at mailgun was such a success I'm was 
surprised to find no similar projects available in the open source community, so I'm building one!

### Architecture Overview
Querator enables API users to interact with queues created by the user, where each queue can consist of one or more
partitions. Each partition is backed by a single table, collection, or bucket provided by the chosen backend storage.

Partitions are a fundamental component of Querator, enabling the Querator to scale horizontally and handle high volumes
of data efficiently. Partitions provide the following benefits.

##### Scalability
Partitions allow a single queue to be distributed across multiple Querator instances in a cluster. This distribution
prevents any single instance from being overwhelmed by data volume, enabling the system to manage larger data volumes
than a single server could handle. By dividing a queue into multiple partitions, Querator can scale beyond the limits
of a single machine, making it suitable for large-scale distributed systems.

##### Parallel Distribution
Partitions serve as units of parallelism, allowing multiple consumer instances to process different partitions
concurrently. This significantly increases overall processing throughput. Unlike some legacy streaming and queuing
platforms, clients are not assigned specific partitions. Instead, clients interact with Querator endpoints, which 
automatically distribute producers and consumers requests across partitions as needed. This automation eliminates the
need for operators to manage the number of consumers per partition to achieve even work distribution.

For example, if there are 100 partitions and only one consumer, the single consumer will receive items from all 100
partitions in a round-robin fashion as they become available. As the number of consumers increases, Querator
automatically adjusts and rebalances the consumers to partitions to ensure even distribution, even if the number
of consumers exceeds the total number of partitions. This is achieved through "Logical Queues," which dynamically
grow and shrink based on the number of consumers and available partitions.
See [ADR 0016 Queue Partitions](doc/adr/0016-queue-partitions.md) for details

![](doc/Querator%20Logical%20Queue%20Diagram.png)

##### Disaggregated Storage Backends
Each partition is supported by a user-selected data store backend. This separation of storage from processing gives
operators the flexibility to choose the desired level of fault tolerance for their specific deployment, and allows
them to use a data store they are already familiar with in terms of operation and scaling. Each partition is backed
by a single table, collection, or bucket (depending on the backend), making it easy to add or remove partitions as
throughput requirements change or as items are drained from the partition backends.

Unlike similar queue systems, Querator manages the complexity of partitioning and balancing on the server side,
simplifying the client API. This design choice makes it easy to integrate Querator with third-party systems, 
frameworks, and languages, fostering a rich open-source ecosystem.

The only requirement for databases is support for ordered primary keys. With just this requirement, Querator can
work with any database that implements the storage backend interface. Support for transactions and secondary
indexes is not required, but can be used to improve Querator’s reliability and performance.

##### InMemory
This backend stores all queues and items into RAM memory. This is useful when testing and creating a baseline for 
benchmarking Querator operation. It can also be used in ephemeral environments where speed is valued over durability.

##### BadgerDB
This backend uses [BadgerDB from DGraph](https://github.com/dgraph-io/badger) and is intended to be used for embedded
or in limited resource environments where High Availability is not a concern. 

##### Planned Backends
- PostgreSQL
- MySQL
- SurrealDB
- FoundationDB

### Preserving FIFO Order
Although a queue is implemented as a First-In-First-Out (FIFO) structure, the system's order cannot be maintained
if there is more than one consumer accessing the queue.

Consider a scenario with two consumers accessing a FIFO queue:

- Consumer 1 retrieves an item.
- Consumer 2 retrieves an item.
- Consumer 2 finishes processing their item.
- Consumer 1 finishes processing their item.

In this situation, the system -— which includes the entire setup of client producers, Querator, and
client consumers -— cannot reliably maintain the order of item processing when multiple consumers are
involved. To ensure ordered processing, items which require preservation of order should be placed
in the same queue, and that queue must have only one partition and one consumer.

Even though Querator queues are designed to deliver items in the order they were produced (FIFO), this order is
disrupted if multiple consumers process items out of sequence. If a user desires a strictly ordered and processed
FIFO queue, they must create a queue with only one partition and ensure that only one consumer processes that queue.

### Embedded Querator
Querator is designed as a library which exposes all API functionality via `Service` method calls. Users can use
the `daemon` package or invoke `querator.NewService()` directly to get a new instance of `Service` to interact with.

### HTTP API
See [Querator API Reference](https://querator.io/api) for and idea of what the API looks like.

### Design
See our [Architecture Decision Docs](doc/adr) for details on our current implementation design.

### Contributing 
- See the [Querator Trello Board](https://trello.com/b/cey2cB3i/querator) for work status and progress and things to do
- Join our [Discord](https://discord.gg/XwfBdN9wdg)
- Checkout [querator.io](https://querator.io/api) for the HTML OpenAPI docs

### Similar Projects
* https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/
* https://temporal.io/
* https://www.resonatehq.io/
