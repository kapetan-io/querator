# 28. Storage write failures

Date: 2026-11-19

## Status

Accepted

## Context

The Logical Queue batches the produce, lease, complete and retry requests waiting on it, and sends each
kind to a partition as one storage call, with dead-letter hand-offs in a produce call of their own ([3.
RW Sync Point](0003-rw-sync-point.md)). A storage call that fails can leave the batch fully written,
partly written or not written, and a backend that loses its connection during a commit cannot tell which.
Each backend reports a failure to the Logical Queue, which decides how to answer the clients.

## Decision

A storage error fails every request in that call. Produce, complete and retry requests are answered with
a retry error and the client sends them again ([20. API semantics](0020-api-semantics.md)). Lease
requests keep waiting and are tried again until their request timeout. A problem with one request, such
as an unknown or not-leased item ID, is reported on that request and is not a storage error.

A backend returns an error from produce only when it cannot say the batch was stored. The Logical Queue
does not add the items of a failed produce to the partition's unleased count. The exception is a batch of
only dead-letter hand-offs, which a backend may store in parts. It records on each request how many of
its leading items were stored, and the Logical Queue counts those ([27. Dead-letter
queues](0027-dead-letter-queues.md)).

A failed lease hands out no item whose lease was not saved. A backend that cannot tell whether a lease
write applied takes back every item it marked leased for a request. An item whose lease did save then
stays leased with no consumer until the lease expires, and that lease counts as one of its attempts. That
is safe. Handing out an item whose lease did not save is not, because a second consumer could lease it
while the first works on it.

## Consequences

A client that retries a failed produce can store its items twice, since some or all of them may have been
stored. A client that retries a failed complete or retry on a backend without transactions sees "does not
exist" for the items the first attempt already removed.
