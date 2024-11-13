Querator is currently still in early development

## Querator
A Reservation based FIFO queue backed by a Database with Almost Exactly Once Delivery semantics. (AEOD)

The interesting thing about a reservation queue is that you can use the reservation to not only ensure
a message was delivered, but ensure the message was processed by the consumer. In this way, you can
sort of think of it as a locking primitive for processing items. Because the consumer can hold on to the 
reservation until the timeout, it can hold off marking the reservation as complete until it has processed the 
message that was consumed. As a result, you can use the locking primitive the reservation queue provides to
solve several distributed problems.

- Implement Multistep, retry-able workflows.
- Implement the Saga Pattern for distributed transactions
- Use it as a FIFO queue with ordered delivery of messages
- Run async background tasks that can retry if failed (like webhook delivery)
- Schedule jobs to run at a specific time in the future and retry if failed
- Use it as a limit locking system, where items in the queue represent a limited lockable resource

### Does it scale?
YES! We scaled a closed source version of a reservation queue service at https://mailgun.com to multi-billions of
messages a day in a very efficient and cost-effective manner, Meta (aka FaceBook) 
[built a similar system](https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/) which
scaled to billions of messages. The closed source version of Querator we ran at mailgun was such a success I'm was 
surprised to find no similar projects available in the open source community, so I'm building one!

### What is Almost Exactly Once?
The reservation pattern is used to implement an “Almost Exactly Once Delivery” style queue by ensuring that 
each item is processed “almost exactly” once and in the order it was received. I say, “almost” because 
[Exactly Once Delivery (EOD) is theoretically impossible](https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/).
HOWEVER, In practice you can achieve AEOD or “Almost Exactly Once Delivery” which is just EOD with the
understanding that you have the occasional duplicate delivery due to some failure of the system. 
Anyone trying to tell you their system provides you with EOD is being a bit disingenuous.

However... In our experience, the duplicate delivery & processing rate is very low indeed. When I say “very low” I 
mean, it has about the same failure rate of whatever your current uptime is. That is to say, message delivery is 
about as reliable as the system it runs on. If you need additional protection against duplication, you can ensure
the messages consumed are idempotent. Remember, [Distributed systems are all 
about trade-offs](https://www.infoq.com/articles/cap-twelve-years-later-how-the-rules-have-changed/)

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

##### Disaggregated Storage Backends
Each partition is supported by a user-chosen data store backend. This separation of storage from processing provides
operators with the flexibility to select the desired level of fault tolerance for their specific deployment and 
allows them to use a data store with which they are familiar in terms of operation and scaling. Each partition is 
backed by a single table, collection, or bucket (depending on the backend), enabling partitions to be added or 
dropped as throughput capacity changes and items are drained from the partition backends.

Unlike similar queue systems, Querator handles the complexity of partitioning and balancing on the server side, 
simplifying the client API. This simplicity facilitates easy integration with third-party systems, frameworks, 
and languages, fostering a rich open-source ecosystem.

### Preserving FIFO Order
Although a queue is implemented as a First-In-First-Out (FIFO) structure, the system's
order cannot be maintained if there is more than one consumer accessing the queue.

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

### Storage Backends
- [ ] TODO - Storage backend benchmarks

##### InMemory
This backend stores all queues and items into RAM memory. This is useful when testing and creating a baseline for 
benchmarking Querator operation. It can also be used in ephemeral environments where speed is valued over durability.

##### BoltDB
This backend uses the ETCD flavor of BoltDB and is intended to be used for embedded or in limited resource environments
where High Availability is not a concern, and where networking is limited or access to a remote storage system is not
possible. 

##### BadgerDB
This backend uses [BadgerDB from DGraph](https://github.com/dgraph-io/badger) and is intended to be used for embedded
or in limited resource environments where High Availability is not a concern. Badger's LSM design out performs BoltDB
but the LSM design may result in more writes to disk.

##### Planned Backends
- PostgreSQL
- FoundationDB
- SurrealDB

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

