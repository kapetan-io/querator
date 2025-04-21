# 13. pausing a queue

Date: 2024-07-16

## Status

Superceded by [16. Queue Partitions](0016-queue-partitions.md)

## Context

Querator queue's can and should assume they have exclusive access to the data store used to keep items in the queue.
However, their may be some operations which must occur on that underlying data store which require direct access to
the underlying data store thus bypassing Querator. In such situations, we need to quickly "pause" all operations to
the data store while maintenance is preformed.

Examples of maintenance.
* Resizing the database
* Clean up of items in the database via an out-of-band process
* Database upgrade and restart

The Pause feature is also useful in functional tests to reproduce scenarios which might not be easily reproducible 
without the ability to pause the queue. It may also be useful to client implementation wish to test some client 
corner case interactions with Querator.

## Decision

Provide a `/queue.pause` endpoint which will pause normal produce, lease, complete, retry operations until the pause
is resumed or the pause times out. Because the state of the underlying data store may have changed during the pause
Querator should not assume anything about the state of the data store after resuming, it should instead flush any
cached information it held and rebuild from what exists in the data store. 

During the pause, clients that make requests to or have in progress requests to produce, lease, complete or retry
operations will be held until their request timeout is reached, at which time they will receive a retry message, 
informing them to attempt the operation again. This interaction with the client should continue until the pause is 
cancelled.

During the pause, users will be able to access the underlying storage by using the `/storage` endpoints. This is 
allowed as those endpoints should not cache any information about the underlying store, as they are intended to 
provide direct read/write to the data store, and the end user may wish to use those endpoints to preform maintenance
on the data store -- for example, clearing all items from the queue. If the underlying data store cannot accept any 
access during maintenance then the user should avoid using the `/storage` endpoints during the pause. This is an 
acceptable trade-off as the pause function is intended to "pause" the hot path of producing and consuming items, 
and is not intended to restrict access to the data store.

## Consequences

The main queue loop will become a bit more complicated.
