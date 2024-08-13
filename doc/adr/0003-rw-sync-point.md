# 9. R/W sync point

Date: 2024-07-04

## Status

Accepted

## Context

Because efficiency is a goal of the project, understanding where the synchronization points for read and writes will
have an impact on efficiency and scalability, in addition to what storage engines Querator can make use of.


## Decision

We want to avoid placing the R/W synchronization burden on the underlying storage system querator uses. Instead, 
the synchronization point for all queue operations will be handled by the `Queue` interface. 

The `Queue` interface handles processing for a single queue, it handles processing of both producing and reserving
clients. It is the synchronization point where reads and writes to the queue are handled. Because reads and writes 
to the queue are handled by a single go routine (single threaded). We can preform optimizations where items produced 
to the queue can be marked as reserved before they are written to the data store. Similar in design to a single 
threaded redis server.

Single threaded design may also allow batching reads and writes in the same transaction. Most databases
benefit from batching as it reduces IOPs, transaction overheard, network overhead, results in fewer locks,
and logging. In addition, by preforming the R/W synchronization here in code, we avoid pushing that synchronization
on to the datastore, which reduces datastore costs.

The design goal here is to have all R/W occur inside a single go routine which when, woken up by the go scheduler
can do as much work as possible without giving the scheduler an excuse to steal away our CPU time. In other words,
the code path to get an item into or out of the queue should result in as little blocking or mutex lock
contention as possible. In addition, this design allows us the opportunity to optimize the code to maximize
the L3 cache when the CPU is engaged by avoiding HEAP objects that the synchronization go routine interacts with.

While we can't avoid IO to the data store in the main loop, we can have producers and consumers
'queue' as much work as possible so when the main loop is active, we get as much done as quickly as possible.

If the single thread per queue becomes a bottleneck for throughput, users should create more queues, and thus utilize more threads.

##### The Write Path
The best cast scenario is that each item will have 3 writes.
 * 1st Write - Add item to the Queue
 * 2nd Write - Fetch Reservable items from the queue and mark them as Reserved
 * 3rd Write - Write the item in the queue as complete (delete the item)

In addition, our single threaded synchronized design may allow for several optimizations and probably some I'm
not thinking of.
* Pre-fetching reservable items from the queue into memory, if we know items are in high demand.
* Reserving items as soon as they are produced, if we see that there are waiting reservation requests and if 
  the storage queue is currently empty -- thus avoiding the second write by writing items to the queue as 
  already reserved.

## Consequences

Possible downsides is that the initial version of this design will create an I/O bottleneck as none of the
optimizations will have been implemented. We know this design works well in practice as we've had good experience
with this sort of design, and other high performance servers such as [Redis](https://redis.io/) have greatly 
benefited from a low contention single threaded I/O design.
