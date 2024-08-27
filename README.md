## Querator
A Reservation based FIFO queue backed by a Database with Almost Exactly Once Delivery semantics. (AEOD)

The interesting thing about a reservation queue is that you can use the reservation to not only ensure
a message was delivered, but ensure the message was processed by the consumer. In this way, you can
sort of think of it as a locking primitive for processing items. Because the consumer can hold on to the 
reservation until the timeout, it can hold off marking the reservation as complete until it has processed the 
message that was consumed. As a result, you can use the locking primitive the reservation queue provides to
solve several distributed problems.

* Implement Multistep, retry-able workflows.
* Implement the Saga Pattern for distributed transactions
* Use it as a lock to gain exclusive access to an item of work
* Use it as a FIFO queue with ordered delivery of messages
* Run async background tasks that can retry if failed
* Schedule cron style jobs to run at a specific time in the future and retry if failed
* Retryable and reliable webhook delivery with external systems

### Does it scale?
YES! We scaled a closed source version of a reservation queue service at https://mailgun.com to multi-billions of messages a day in a very efficient and cost effective manner. It was a key component of our micro-service (domain based) system. The implementation was such a success I'm surprised to find no similar projects available in the open source community, so I'm building one!

### What is the Reservation Pattern?
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

### API
See [Querator OSS API Reference](https://thrawn01-llc.stoplight.io/docs/querator-oss/924788fc33955-querator-oss-api) for and idea of what the API looks like.

### Design
See our [Architecture Decision Docs](doc/adr) for details on our current implementation design.

### TODOs
* Implementate PostgreSQL backend with SKIP LOCK, Partitions, Truncate.
* Implementate Partitioning as a core function. (See The ADR).
* Experiment with [Badger](https://github.com/dgraph-io/badger) as a replacement for boltDB. Bolt turned out to be much
  slower than I expected due to the lack of an LSM.
* Finish Testing existing functionality
* Implement Scheduled and Defer
* Consider allowing a produced item to specify the ReserveTimeout

### Similar Projects
* https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/
