# 7. encourage simple and efficient clients

Date: 2024-06-28

## Status

Accepted

## Context

It is a stated goal of the project to make clients simple, which encourages integration by third parties. To 
simplify client implementations and facilitate easy adoption of Querator, we need to make deliberate API design
choices which encourage simple and efficient behavior from third party client implementations. In addition, there
are design choices which may encourage third party client developers to make efficient design choices. The goal is
to encourage client implementations and users to [Fall into the pit of success](https://blog.codinghorror.com/falling-into-the-pit-of-success/)


## Decision

##### Queue Name Required
We require clients to provide the name of the queue for the `/queue.produce`, `/queue.complete`, and
`/queue.retry` APIs. This decision is made despite the fact that the item `id` retrieved during `/queue.reserve`
may include information about the item's storage location, likely including the name of the queue.

##### Unique Clients
The `/queue.reserve` API will require clients to provide a client id which uniquely identifies a client to Querator.

##### Avoid Large Payloads
Querator will have a maximum payload requirement which is enforced via the HTTP API since the current protobuf 
library we are using does not support decoding streams. TODO(thrawn01): This is not yet implemented

### Rational

##### Queue Name Required
By requiring the queue name, we discourage clients from implementing complex batching optimizations that can lead
to increased complexity and potential errors. For example, if the `/queue.complete` endpoint did not require a
queue name, clients might be tempted to combine multiple 'complete' operations for different queues into a single
request. This would force the server implementation to separate the IDs by queue.

This has several implications:
* If any of those requests fail, then the server must respond with a list of ids which failed and the error
  associated with each list of ids. In the case of multiple failures for different queues each failure must be
  considered independently by the client. This greatly increases the complexity of the response structure for
  `/queue.complete`.
* The client must write additional code to handle parsing a complex response structure for items that failed to
  complete. In addition, it must handle each item separately to determine which of the failed ids can be retried via
  `/queue.complete` and which should be `/queue.retry` if any.
* The client must track which of the failed items are at risk of exceeding their `reserve_timeout` such that they
  can be handled appropriately.
* If the client has implemented such a batching optimization, it is likely the client has done so in an async
  manner separating the item from it's processing handler. In such a case, it is likely the item processed will be
  processed again if a successful `/queue.complete` cannot be preformed before `reserve_timeout` is reached and
  the handler is likely not aware of an item batched to `/queue.complete` had a failure.

All of this client complexity is discouraged by requiring the `queue_name` on several API calls. The design does
not restrict a client implementation from falling prey to these sort of optimizations.

##### Unique Clients
By requiring unique client ids when reserving, we hope to discourage bad behavior from client implementations where
they send many small batch requests to querator in an attempt to increase throughput. Throughput is best improved by
increasing the size of the batch.

##### Avoid Large Payloads
This decision protects the Querator service from abuse, limits memory usage, and can encourage clients to use 
smaller payloads, which can have throughput implications.

## Consequences

##### Queue Name Required
This design may result in client implementors complaining about the number of calls required in order to transition
queue items to different states. However, we would encourage clients to implement a request queue system for each 
queue the client handles, such that handlers that are waiting to be marked as completed will block until the client
can either confirm state transition, or fail the state transition. In such a case, the per queue handler can decide
what to do about the state transition failure.

Such a queue system is used within querator to communicate state transition with the queue via Request structs.

With HTTP/2 and HTTP/3 on the horizon, the issue of a single HTTP client opening many connections to Querator is 
less of a concern as both of these protocols can make use of multiple channels on a single transport connection.

##### Unique Clients
In the case where clients are producing and consuming very large item payloads, the client may wish to overcome the
payload size limitations by making more than one reserve request per client. The API does not deny clients this 
option, but instead encourages clients to consider the most efficient path first. See `Avoid Large Payloads`

##### Avoid Large Payloads
Users who cannot reduce the size of their payloads have the option of running Querator in a memory rich environment
and have the option of increasing the size of the maximum payload by changing a service configuration option.

The preferred solution for such clients is to decrease the size of the payloads by offloading large payload blobs
into secondary storage and only enqueue metadata about the blob and how to retrieve it. The blob can then be
retrieved when the consumer is processing the queue item.

We used such a system with great effect at Mailgun, at first glance, it can appear to be inefficient to implement
such off loading due to the extra step of sending the blob to separate storage. However, it allows operators the
ability to scale Querator storage independently of the secondary storage used to offload our large data blobs. 
This can have the additional benefit of allowing operators to store large blobs in a more cost-effective manner.
For example, storing such blobs in S3 can be many times cheaper than using an operator run database with disks.
