# 28. Storage write failures

Date: 2026-11-19

## Status

Accepted

## Context

Requests waiting on a queue are written to storage in batches ([3. RW Sync Point](0003-rw-sync-point.md)).
Say three clients each send a produce while the queue is busy. The Logical Queue sends all three to
the partition as one storage call. Leases, completes and retries are batched the same way, each kind
in its own call, and dead-letter hand-offs go in a produce call of their own.

When that call fails, storage may have written all of the batch, part of it, or none of it. A backend
that loses its connection while committing cannot tell which.

## Decision

**A failed call fails every request in it, and the client decides what to do next.**

| Request | What the client gets when the call fails |
|---|---|
| Produce, complete, retry | A retry error. The client sends the request again ([20. API semantics](0020-api-semantics.md)). |
| Lease | Nothing yet. The lease keeps waiting and is tried again until its request timeout. |

A problem with one request, such as an item ID that does not exist or is not leased, is reported on
that request alone. It is not a storage error and does not fail the others.

**A produce fails only when the backend cannot say the batch was stored.** The queue does not count
the items of a failed produce as waiting in the partition. A batch of only dead-letter hand-offs is
the exception. A backend may store it in parts, records how many items of each request it stored, and
the queue counts those ([27. Dead-letter queues](0027-dead-letter-queues.md)).

**A failed lease never hands out an item whose lease was not saved.** If a backend cannot tell whether
a lease write applied, it takes back every item it marked leased for that request. Some of those may
have been saved as leased. They stay leased with no consumer until the lease runs out, and that costs
the item one attempt. That is safe. Handing out an item whose lease was not saved is not, because a
second consumer could lease it while the first is still working on it.

## Consequences

A client that resends a failed produce can store its items twice, since some or all of them may
already be stored.

A client that resends a failed complete or retry on a backend without transactions gets "does not
exist" for the items the first attempt already removed.
