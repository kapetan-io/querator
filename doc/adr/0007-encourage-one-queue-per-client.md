# 7. encourage one queue per client

Date: 2024-06-28

## Status

Accepted

## Context

To simplify client implementations and facilitate easy adoption of Querator, we need to make deliberate design choices.

## Decision

We will require clients to provide the queue name for the `/queue.produce`, `/queue.complete`, and `/queue.defer` 
APIs. This decision is made despite the fact that the item `id` retrieved during `/queue.reserve` includes
information about the item's storage location, likely including the name of the queue.

### Rational

By requiring the queue name, we discourage clients from implementing complex batching optimizations that can lead
to increased complexity and potential errors. For example, if the `/queue.complete` endpoint did not require a
queue name, clients might be tempted to combine multiple 'complete' operations for different queues into a single
request. This would force the server to separate the IDs by queue.

This has several implications:
* If any of those requests fail, then the server must respond with a list of ids which failed and the error
  associated with each list of ids. In the case of multiple failures for different queues each must be considered
  by the client. This greatly increases the complexity of the response structure for `/queue.complete`.
* The client must write additional code to handle parsing the complex response structure for items that failed to
  complete. and it must handle each item separately to determine which of the failed ids can be retried via
  `/queue.complete` and which should be `/queue.defer` if any.
* The client must track which of the failed items are at risk of exceeding their `reserve_timeout` such that they
  can be handled appropriately.
* If the client has implemented such a batching optimization, it is likely the client has done so in an async
  manner separating the item from it's processing handler. In such a case, it is likely the item processed will be
  processed again if a successful `/queue.complete` cannot be preformed before `reserve_timeout` is reached and
  the handler is likely not aware of an item batched to `/queue.complete` had a failure.

All of this client complexity is discouraged by requiring the `queue_name` on several API calls. The design does
not restrict a client implementation from falling prey to these sort of optimizations. The design is only to 
encourage client implementations to [Fall into the pit of success](https://blog.codinghorror.com/falling-into-the-pit-of-success/)

## Consequences

This design may result in client implementors complaining about the number of calls required in order to transition
queue items to different states. However, we would encourage clients to implement a queue system for each queue the
client handles, such that handlers that are waiting to be marked as completed will block until the client can 
either confirm state transition, or fail the state transition. In such a case, the handler can decide what to do
about the state transition failure.

Such a queue system is used within querator to communicate state transition with the queue via Request structs.

With HTTP/2 and HTTP/3 on the horizon, the issue of a single HTTP client opening many connections to Querator is 
less of a concern as both of these protocols can make use of multiple channels on a single transport connection.