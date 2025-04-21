# 20. API Semantics

Date: 2024-09-14

## Status

Proposed

Supercedes [18. queue complete error semantics](0018-queue-complete-error-semantics.md)

## Context

This ADR is a collection of explanations on API semantics

#### Immediate Error Return for /queue.complete
The server-side implementation of the `/queue.complete` endpoint should not continuously retry writing to a partition
that has failed or is returning errors. Instead, it immediately returns an error to the client, indicating that 
the client should retry the request. This behavior differs from other APIs, such as `/queue.produce` and
`/queue.lease`, which continue to attempt to fulfill the user's request until the `request_timeout` is reached.

The rationale behind this is that the client might intend to take different actions if it knows the item is about to
exceed its lease timeout and could be offered to a different consumer. Therefore, the `/queue.complete` call
should never be changed to retry partition errors until the request times out, unlike the behavior of
`/queue.produce` and `/queue.lease`.

#### Partition Per Request Endpoints
Although not explicitly discussed in [0019-partition-item-distribution.md](0019-partition-item-distribution.md), 
the partition per request design is applied to all APIs that interact with a queue. Each endpoint exposes both
the queue name and the partition to the users, without attempting to obscure partition assignments. This approach
significantly simplifies the implementation and interaction, creating a system that is easier for both the 
Querator operator and the client to understand when reasoning about its operation.

##### Partition Per Request Proxy Considerations
The Partition Per Request design supports Querator's ability to proxy client requests effectively. The queue 
API design should anticipate that Querator may redirect a client's request to a different Querator instance 
if the initial instance cannot fulfill the request. For instance, if a Querator instance receives a 
`/queue.complete` request for a queue and partition it no longer manages, it can proxy the request to another
instance that does manage that partition. 

##### Future API Considerations
Future APIs should avoid hiding partition details from users, as failures in a single partition can complicate
error handling for the client. In addition, future APIs should NOT allow clients to operate upon multiple partitions
in a single API call. For a detailed discussion, refer to ADR 
[0019-partition-item-distribution.md](0019-partition-item-distribution.md).

This decision supersedes [18. queue complete error semantics](0018-queue-complete-error-semantics.md) or similar
designs, with the following restrictions.
1. **Mandatory Partition**: Clients are required to indicate which partition the endpoint is operating upon.
2. **Single Partition Per Request**: Multiple partitions are not allowed to be included in a single request.

