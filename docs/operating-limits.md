# Operating Limits

> This page lists limits a person running Querator sees or must work around. It changes only when what an
> operator sees or must do changes. Why Querator is built the way it is lives in the ADRs under
> [adr/](adr/), and the rules the code must keep live in its tests.

## A deleted API key keeps working on other nodes for up to 5 minutes

Each node caches the API keys it has seen for 5 minutes, and the cache time has no setting. Deleting a
key, or the user who owns it, removes it from the cache of the node that served the delete only. Every
other node accepts the key until its cached copy runs out. A key's expiry time is still honored on every
node.

**Workaround**: To revoke a key at once in a multi-node deployment, restart every node, which empties
their caches.

## Requests with an unknown API key always reach storage

A key that authenticated is cached, but a well-formed key that does not exist is looked up in storage on
every request. A client sending many unknown keys sends about as many reads to the storage backend, which
slows every other request. A malformed key is refused without a storage read. A storage outage during the
lookup is answered as an authentication failure, so when clients with valid keys suddenly fail to
authenticate, check storage first.

**Workaround**: Rate-limit unauthenticated or repeatedly failing clients at the network layer.

## Permission checks read storage on every request

Querator caches who a key belongs to, not what that user may do. A request reads the user's role bindings
in the target namespace, and in `_system` when those grant nothing, then reads the roles those bindings
name until one grants the permission. Latency grows with the number of bindings a user holds. At most 100
of the bindings a user holds in one namespace are read, so a user with more than 100 there can be denied
a permission one of the unread bindings grants.

**Workaround**: Keep each user well under 100 role bindings per namespace. Bind users to broad roles in a
namespace rather than to many narrow ones.

## Authentication with role storage starts with the door open

When Querator runs with authentication on and storage for users, roles and role bindings, it starts in
Open Door mode. It binds the anonymous user to the Admin role in the `_system` namespace, so any request
without an API key has full administrative access. A warning is logged when the binding is created. The
binding is created again at every start, so deleting it closes the door only until the next restart. The
standard roles are also written again at every start, replacing any change made to them.

With the `none` auth driver, the default, Querator checks no permissions, so any client that can reach
the API port can do anything.

**Workaround**: After every start, delete the anonymous user's Admin binding in `_system` with
`/v1/rolebindings.delete`. Keep the API port unreachable from untrusted networks until you have, and
always with the `none` driver. Create your own roles rather than editing the standard ones.

## Health and metrics are served without authentication on the API port

`GET /health` and `GET /metrics` answer any client that can reach the API port, so load balancers and
Prometheus need no key. `/health` shows the Querator version and whether queue storage is reachable.
`/metrics` shows request durations for each URL path a client called, including paths that do not exist.
Neither shows queue names, items or payloads. There is no separate port for them.

**Workaround**: Restrict the API port at the infrastructure level (firewall rules, a service mesh policy
or a Kubernetes NetworkPolicy) so that only trusted clients, probes and scrapers can reach it.

## Querator does not route requests around a failing or slow partition

A queue with several partitions does not move a request off a partition whose storage is failing or slow.

* When a write to a partition fails, produce, complete and retry requests on it get a retry error (HTTP
  454) at once. A lease request keeps waiting and is tried again, and gets no items if the partition is
  still failing when its request timeout passes.
* A produce answered with a retry error may have been stored in full or in part. A produce whose client
  stopped waiting on its own timer may also have been stored. Produce has no idempotency key, so sending
  either again can store the same items twice.
* Partitions of one queue are written one after another, so a slow partition delays every request on the
  queue, by up to the storage write timeout (3 seconds, with no setting in the server config) each time,
  and on BadgerDB by as long as the slow write takes. A reply can arrive after the request's
  `request_timeout`.
* When every partition of a queue has failed, produce requests wait. A partition that recovers while no
  lease request is waiting does not pick up the waiting produce requests until another produce, lease,
  complete or retry request arrives, so on an idle queue they get a retry error at their request timeout
  and their items are not written.
* After a failed write, the unleased and leased counts `/queue.stats` reports can be wrong until the
  queue is reloaded.

**Workaround**: Clients resend on a retry error or after their request timeout, and consumers must
tolerate duplicate items. Reload a queue with `/v1/queue.reload` to recount it from storage.

## A resent complete can stop at an item it already completed on MongoDB

MongoDB completes the items of a request one at a time, without a transaction. When a complete fails
partway, the items before the failure are already removed. Sending the request again fails with "does not
exist" at the first of those items, and the items after it are not completed. An immediate retry that
fails partway can likewise leave a second copy of an item when it is sent again.

**Workaround**: When a resent complete fails with "does not exist", drop the item it names and send the
complete again for the rest, repeating while it fails with "does not exist". Consumers must tolerate
duplicate items.

## A clear can be left half done

A `/v1/queue.clear` can remove only part of a queue. When its client stops waiting, the part of the queue
being cleared at that moment is cleared to the end, but the rest of the queue may not be cleared at all,
and the client gets no answer that says which. A node shutting down also stops a running clear early.

**Workaround**: When a clear fails, times out or is cut short by a shutdown, send it again once the queue
is served again. The second clear finishes the job.

## A large produce on BadgerDB can fail whole

BadgerDB refuses a write that holds too many items or too many bytes, and refuses all of it. A produce
request too large for one BadgerDB write fails with a retry error and stores none of its items, and
sending the same request again fails the same way.

**Workaround**: Send large produces as several smaller requests.

## A BadgerDB lease can find no items while items are waiting

A BadgerDB lease stops reading when it reaches the write timeout and keeps what it leased so far. The
next lease carries on from where it stopped. A consumer on a quiet queue with many leased items ahead of
the unleased ones can get "no items are in the queue" while unleased items wait further on.

**Workaround**: Lease again. Each lease reads further into the partition.

## Storage backends disagree on scheduled items that are due but not yet enqueued

A scheduled item is due once its enqueue time passes, and it stays scheduled until Querator moves it into
the queue moments later. During that window, PostgreSQL and MongoDB count it in a partition's total in
`/queue.stats` and remove it on a `/v1/queue.clear` with `queue` set. InMemory and BadgerDB count it only
as scheduled and keep it. A destructive clear of the queue on InMemory and BadgerDB also removes
scheduled items that are not yet due, which PostgreSQL and MongoDB keep unless `scheduled` is also set.

**Workaround**: Before comparing stats across backends, or clearing a queue that holds scheduled items,
wait a few seconds past their enqueue time so they have moved into the queue.

## Changing queues holds up every request on a node with many queues

Deleting a queue, or creating or updating one that names a dead-letter queue or has more than one
partition, reads the stored queues to find any queue that names it as its dead-letter queue. Every
request to that node waits while it does, so on a node with many queues those changes pause all traffic
for as long as the read takes.

**Workaround**: Make queue changes outside peak traffic.

## Dead-letter chains saved by older releases must be broken on update

Querator refuses a dead-letter queue that has its own dead-letter queue, but older releases did not check
this when the second queue was updated. Such chains stay in storage. An update to a queue in the chain
that sends its `dead_queue` unchanged is refused.

**Workaround**: When updating a queue in such a chain, clear its `dead_queue` or point it at a queue with
no dead-letter queue of its own.
