# 18. queue complete error semantics

Date: 2024-09-05

## Status

Superceded by [20. API Semantics](0019-api-semantics.md)

## Context

The addition of partitions as first class citizens requires a change in semantics for the
`/queue.complete` endpoint and changes to how we distribute items to consumers.

## Decision

#### Partition Distribution
To minimize the workload of the `complete` operation, Querator should group partitions together
with individual clients as much as possible. When marking items as `/queue.complete`, this strategy
reduces the effort required to locate logical queues where each partition resides to confirm item
completion.

#### Complexity Introduced by Partitions
Introducing partitions as full-class citizens adds complexity to the `/queue.complete` request.
Although we aim to lump partitions together when a client reserves items from a queue, it is possible
that a `/queue.complete` request may include item IDs from multiple partitions. These partitions
might have been rebalanced or moved to a different Querator instance during the reservation period.
Consequently, the `/queue.complete` request may need to proxy the complete request to the owning
Querator instance.

##### Error Handling and Proxy Requests
Given the possibility that the proxy request fails or that the underlying data storage for a
partition is unavailable at the time of the `/queue.complete` request, some item IDs might be
marked as "completed" while others timeout or encounter errors. Returning individual errors for
each failed item ID is counterproductive to resilient operation. Instead, the `/queue.complete`
request should return success only if all item IDs in the request are successfully marked as
"completed."

##### Error Semantics
The error semantics for the `/queue.complete` call should be such that if any of the provided item
IDs cannot be completed, the entire `/queue.complete` call should return an error. This applies even
if some IDs were marked as completed while others were not. The semantics should ensure that a call
to `/queue.complete` either successfully marks all item IDs as "completed" or the entire request fails.

##### Avoid Rollbacks
The `/queue.complete` request should not roll back item IDs that have been successfully marked as
completed if a subset of item IDs fail. This is because the data storage managing the partition might
not be available for an extended period. In such cases, we want available partitions to continue
operating normally, including the ability to mark items as "complete."

##### Error Semantics Rationale
1. **Client Reaction to Failure:** The client's reaction to a failure should be consistent whether
a subset of IDs or all IDs fail to be marked as complete. The client should retry the full request 
until all IDs in the request are marked as complete. This means the client should continue retrying
until the underlying partition's data storage is restored or the client decides to give up.
2. **Simplifying Client Implementation:** Adding complexity by returning errors for a subset of IDs
that did not get marked as complete only complicates the client implementation and does not improve
reliability. Instead, it would confuse the proper course of action for the client implementation.
The correct course of action is to continue retrying until connectivity or the data storage of the
partition is restored.

## Consequences

A consequence of this semantic change to `/queue.complete` is that a request which receives item IDs
that have already been marked as "completed," should not return an error, but silently ignore these
item IDs, regardless of whether they exist in the partition or have already been marked as
"completed." This is because, a previously failed `/queue.complete` request may have marked some of
those item ids as completed, yet the entire request is retried, which includes items that have
already been marked as completed.

A side effect of ignoring item ids that don't exist in a partition is that users can send random
item ids to the `/queue.complete` endpoints, and the service will never acknowledge if those random
item ids actually existed, as the endpoint will ignore them.

Another possible side effect of lumping partitions together is that of un-even consumption by clients
who request more than one batch of items per request. If there are 10 consumers and 10 partitions,
9 consumers consume 1 item per request and one consumer consumes 100 items per request. It's possible
the consumer who consumes 100 items per request could conceivably by chance, or by sequence consume
from the same partition each time it makes a consume request. As a result, consumption from the 
partitions will be un-even. The simplest solution is for the operator to ensure every client consumes
the same number of items per request. It may also be possible to track consumers via their client id
and ensure a client doesn't consume from the same partition on each request via a round-robin
algorithm. I don't think we need to solve this problem today?
