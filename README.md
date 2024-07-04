## Querator
A Reservation based FIFO queue backed by a Database with Almost Exactly Once Delivery semantics. (AEOD)

The interesting thing about a reservation queue is that you can use the reservation to not only ensure
a message was delivered, but ensure the message was processed by the consumer. In this way, you can
sort of think of it as a distributed transaction. Because the consumer can hold on to the reservation
until the timeout, it can hold off marking the reservation as complete until it has processed the 
message that was consumed. As a result, you can use the transactional primitive the reservation queue 
provides to solve several distributed problems.

* Implement Multistep, retry-able workflows.
* Implement the Saga Pattern for distributed transactions
* Use it as a FIFO queue with ordered delivery of messages.
* Run async background tasks that can retry if failed.
* Schedule cron style jobs to run at a specific time in the future and retry if failed.
* Retryable and reliable Webhook delivery with external systems.

### What is the Reservation Pattern?
The reservation pattern is used to implement an “Almost Exactly Once Delivery” queue by ensuring that 
each message is processed “almost” once and in the order it was received. I say, “almost” because 
[Exactly Once Delivery (EOD) is theoretically impossible](https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/).
HOWEVER, In practice you can achieve AEOD or “Almost Exactly Once Delivery” which is just EOD with the
understanding that you have the occasional duplicate delivery due to some failure of the system. 
Anyone trying to tell you their system provides you with EOD is being disingenuous.

However... In our experience, the duplicate delivery rate is very low indeed. When I say “very low” I 
mean, it has about the same delivery failure rate of whatever your current uptime is. That is to say, 
message delivery is about as reliable as the system it runs on. If you need additional protection against
duplication, you can ensure the messages consumed are idempotent. Remember, [Distributed systems are all 
about trade-offs](https://www.infoq.com/articles/cap-twelve-years-later-how-the-rules-have-changed/)

### API
See [Querator OSS API Reference](https://thrawn01-llc.stoplight.io/docs/querator-oss/924788fc33955-querator-oss-api) for and idea of what the API looks like.

### Design
See our [Architecture Decision Docs](doc/adr) for details on our current implementation design.