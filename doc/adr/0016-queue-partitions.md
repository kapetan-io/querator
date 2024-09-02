# 16. Queue Partitions

Date: 2024-08-15

## Status

Proposed

Supercedes [13. pausing a queue](0013-pausing-a-queue.md)

Supercedes [10. Queue Groups](0010-queue-groups.md)

## Context

This ADR attempts to address several issues with pause functionality and Queue Groups.

#### Pause
There is an operational need to preform no downtime maintenance of the underlying storage system a 
queue uses. `/pause` was intended to temporarily pause a queue so that storage maintenance could 
occur. However, the implementation proved to be more complex than assumed, and maintenance windows
could be much longer than clients are willing to wait.

#### Queue Groups
I intended to introduce a feature that would allow almost unlimited read/write scaling of queues
by organizing individual queues into a logical entity known as a "Queue Group." The initial plan
involved creating a separate yet similar API to the existing `/queue` API, named `/groups`, which
would interact these queue groups. However, having multiple API with similar functionality 
could potentially confuse users. Furthermore, we would need a mechanism to prevent users from 
accessing the underlying queues through the `/queue` API if they were included in a Queue Group.

## Decision

After a late-night discussion with Maxim, he suggested renaming `Queue` to `Partition` and 
`Queue Groups` to Queue. This change allows us to expose only the Queue API to users. If a user 
requires a FIFO queue, they can create a Queue with a single Partition. If a single partition
becomes a throughput bottleneck and FIFO is not necessary, users can add more partitions, accepting
the trade-off of losing FIFO order.

This decision also facilitates the migration from one partition to another by marking a partition
within a queue as 'read-only' while creating a new partition potentially on a different data store.
This implementation eliminates the need for a `/pause` function. 

With partitions, we can configure Querator to utilize multiple data storage backends, enabling the 
distribution of partitions across various storage clusters or instances, thereby enhancing 
read/write throughput.

### Logical Queues
I propose creating an in-code `Logical Queue`, which is assigned a specific number of partitions.
The `Logical Queue` acts as a single-threaded synchronization point for all the partitions it manages,
thus allowing many of the optimizations and benefits described in [3. RW Sync Point](0003-rw-sync-point.md)
The logical ensures that a `Queue` with hundreds of partitions is not constrained by the single-threaded
nature of a `Queue`.

Multiple `Logical Queues` can constitute a single `Queue`. These Logical Queues function as 
coordinators for subsets of partitions. This setup allows a queue with numerous clients
to distribute those clients across multiple Logical Queues, potentially running on multiple
Querator instances.

The Logical Queue is designed to avoid the issue seen in Kafka, where one consumer is required per
partition. For example, if a Queue has 100 partitions and only one consumer, Querator will create a
single Logical Queue that round-robins across all 100 partitions. As the number of consumers
increases and the limits of a single-threaded Logical Queue are reached, Querator can automatically
or via configuration increase the number of Logical Queue instances. Adding more Logical Queue
instances allows Querator to distribute the available partitions and clients across many instances,
thus avoiding synchronization contention and greater scale.

Querator will manage the distribution of partitions across Logical Queue instances. Distribution of
partitions to Logical Queues should happen automatically and should react to current operational
conditions

See [Querator Logical Queue Diagram](../Querator%20Logical%20Queue%20Diagram.png)

### Notes
The introduction of a Logical Queue creates a synchronization point that must exist on a Querator
instance, which may be running in a querator cluster. This necessitates coordinating both partitions 
and clients with their respective Logical Queues across Querator instances. We will address the 
assignment of clients to a Logical Queue in a future ADR. Briefly, there are two options for assigning
clients to a Logical Queue:

- Redirect clients to the instance handling the Logical Queue.
- Proxy requests to the instance handling the Logical Queue.
 
Logical partitions make calls to each partition, attempting to fulfill reserve requests until
either full, or wait for next produce. Since logical partitions are the point of sync, we can
still preform some optimizations on producing and consuming as the LQ knows how many items are
in each partition.

### Partition Backend Storage Assignment
Separate from partition assignment to Logical Queues is the assignment of partitions to backend data
storage systems. To handle the management and distribution of partitions across backend data storage,
I propose adding a `/queue.rebalance` API. This API would allow users to increase or decrease the
number of partitions and re-balance them across all configured backend data storage instances.

## Scenarios
The implementation of the proposal in this ADR allows the possible operational scenarios

### Adding Partitions
Call to `/queue.rebalance` with the number of partitions requested
- Creates new partitions, distributing them across all available data storage
- Distributes Logical Queues across Querator instances, and re-balances clients

### Removing Partitions
Call to `/queue.rebalance` with a number of partitions requested which is less than the current
number of partitions.
- Sets partitions no longer needed to read only, re-balancing partitions across storage backends
- Distributes Logical Queues across Querator instances, and re-balances clients
- Monitor the read only partitions until they are empty, then removes the partitions

### Adding a new data storage backend and re-balance partitions
The operator will change the static Querator config to add a new data storage backend, then
restart all Querator instances. Then calls `/queue.rebalance` to spread partitions for a queue 
across the new data storage backend, which will...

- Mark partitions to be moved from the existing storage backend as read only
- Create new partitions on the new storage backend
- New produced items are written to the partitions moved to the new storage backend
- Existing item in the partitions marked as read only are consumed until the partition is empty
- Once the read only partitions on the existing storage backend are empty, remove the partition
- Querying the `/queues.info` endpoint will return the current status of the partitions,
storage backend and logical group assignments
- Users are not allowed to make further changes via `/queue.rebalance` until the existing re-balance
is complete. This includes the deletion of all read only queues, and any outstanding defer operations.

### Removal of a data storage backend and re-balance partitions
This is the opposite to adding a new data storage backend. We must allow the static configuration
to set an affinity value for a storage backend. This should influence where new partitions are placed
when a re-balance occurs. For instance, if we want to vacate a storage backend for decommission, we 
would set the affinity value to zero then call re-balance on the queue which will move all partitions
for that queue off of that storage backend.

### Notes
Notes about re-balance
- Cannot be called multiple times, the first re-balance must be completed before another can start.
- Must be preformed by a single Queue coordinator, perhaps the cluster leader?
- Status of the re-balance is stored in metadata storage, such that instance restarts can resume 
  monitoring the re-balance
- The status of the re-balance must be available via `/queues.info`
- An audit of each action must be made available via an endpoint.
- audit log includes request time, new queues, deleted queues, errors, retries.
- Imagine a UI which shows the status of the Queue partitions and the actions taken to reach the
current status.

Notes about storage backends
- Calling re-balance on every queue in querator might be tedious for querator operators
- Consider allowing Querator to hot reload it's static config.
- `partition_affinity=0.0` - No Partitions should be created here (during re-balance)
- `partition_affinity=1.0` - Partitions should be created here
- `partition_affinity=0.5` - Partitions should be created here half of the time

## Consequences

This proposal both complicates and in some ways simplifies the design. This design is very similar
to what we had at Mailgun, except that migration of partitions was a manual labor-intensive process.
Having Querator handle this process makes the code more complex, but reduces operational burden.

### Preserving Item Order
Although a queue is typically implemented as a First-In-First-Out (FIFO) structure, the system's 
order cannot be maintained if there is more than one consumer accessing the queue.

Consider a scenario with two consumers accessing a FIFO queue:

- Consumer 1 retrieves an item.
- Consumer 2 retrieves an item.
- Consumer 2 finishes processing their item.
- Consumer 1 finishes processing their item.

In this situation, the system -— which includes the entire setup of producers, Querator, and
consumers -— cannot reliably maintain the order of item processing when multiple consumers are
involved. To ensure ordered processing, items which require preservation of order should be placed
in the same queue, and that queue must have only one partition.

Even though Querator queues are designed to deliver items in the order they were produced (FIFO), 
this order is disrupted if multiple consumers process items out of sequence. This potential
confusion for users should be clearly documented.

The introduction of partitions helps users familiar with streaming systems like Kafka understand
this design more easily. In Kafka, to achieve a FIFO stream, a user can create a topic with a
single partition. Similarly, in Querator, if a user desires a FIFO queue, they can create a queue
with only one partition and ensure that only one consumer accesses it.
