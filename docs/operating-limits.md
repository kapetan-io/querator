# Operating Limits

> This page lists limits a person running Querator sees or must work around. It changes only when what an
> operator sees or must do changes. Why Querator is built the way it is lives in the ADRs under
> [adr/](adr/), and the rules the code must keep live in its tests.

## A deleted API key keeps working on other nodes for a few minutes

Each node caches the API keys it has seen for a few minutes, and the cache time has no setting. Deleting a
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

Querator caches who a key belongs to, not what that user may do, so every request reads the user's role
bindings and the roles they name from storage. Latency grows with the number of bindings a user holds.
A user can hold at most 100 role bindings in one namespace, and creating a 101st is refused. The same
user can still be given bindings in another namespace.

**Workaround**: Keep each user to a few role bindings per namespace. Bind users to broad roles in a
namespace rather than to many narrow ones.

## Authentication with role storage starts with the door open

When Querator starts with auth turned on and storage for users, roles and role bindings, it starts in
Open Door mode. A config file with an `auth` section always starts this way. Open Door binds the anonymous
user to the Admin role in the `_system` namespace, so any request without an API key has full
administrative access. A warning is logged when the binding is created. The binding is created again at
every start, so deleting it closes the door only until the next restart, unless the anonymous user
already holds 100 other bindings in `_system`. Then it is not created, and a warning is logged. The
standard roles are also written again at every start, replacing any change made to them.

Without an `auth` section, the default, Querator checks no permissions, so any client that can reach
the API port can do anything.

**Workaround**: After every start, delete the anonymous user's Admin binding in `_system` with
`/v1/rolebindings.delete`. Keep the API port unreachable from untrusted networks until you have, and
always when auth is off. Create your own roles rather than editing the standard ones.

## Health and metrics are served without authentication on the API port

`GET /health` and `GET /metrics` answer any client that can reach the API port, so load balancers and
Prometheus need no key. `/health` shows the Querator version and whether queue storage is reachable.
`/metrics` shows request durations for each URL path a client called, including paths that do not exist.
Neither shows queue names, items or payloads. There is no separate port for them.

**Workaround**: Restrict the API port at the infrastructure level (firewall rules, a service mesh policy
or a Kubernetes NetworkPolicy) so that only trusted clients, probes and scrapers can reach it.

## A failed write does not take a partition out of use, and a slow partition is never avoided

A partition leaves the request distribution only when Querator cannot read it, except that a draining
partition takes no produce. A failed write does not take it out, a slow partition is never avoided, and
a complete or retry always goes to the partition that holds its items.

* When a write to a partition fails, produce, complete and retry requests on it get a retry error at
  once. A lease request keeps waiting and is tried again, and gets no items if the partition is still
  failing when its request timeout passes.
* A produce answered with a retry error may have been stored in full or in part. A produce whose client
  stopped waiting on its own timer may also have been stored. Produce has no idempotency key, so sending
  either again can store the same items twice.
* Partitions of one queue are written one after another, so a slow partition delays every request on the
  queue, by up to the storage write timeout each time, which has no setting in the server config. On
  BadgerDB a slow write is not cut short at the write timeout. A reply can arrive after the request's
  `request_timeout`.
* The leased count `/queue.stats` reports does not drop when the lifecycle deletes or dead-letters a
  leased item that ran out of attempts, so on a queue with `max_attempts` set it overstates the items
  leased (ENG-774). After a failed write the unleased count can be wrong.

**Workaround**: Clients resend on a retry error or after their request timeout, and consumers must
tolerate duplicate items. Reload a queue with `/v1/queue.reload` to recount it from storage.

## A resent complete can stop at an item it already completed on MongoDB

On MongoDB, a complete that fails partway may already have completed some of its items. Sending the
request again fails with "does not exist" at the first of those items, and the items after it are not
completed. An immediate retry that fails partway can likewise leave a second copy of an item when it is
sent again.

**Workaround**: When a resent complete fails with "does not exist", drop the item it names and send the
complete again for the rest, repeating while it fails with "does not exist". Consumers must tolerate
duplicate items.

## Produces sent together to BadgerDB can fail whole

BadgerDB refuses a write that holds too many items or too many bytes, and refuses all of it. Produce
requests that reach one partition together are stored in one write, so together they can pass the
limit. Each then fails with a retry error and stores none of its items. One produce on its own is below
the limit at the server's default request size cap.

**Workaround**: Resend. Spreading large produces over time, or over more partitions, avoids it.

## A BadgerDB lease can find no items while items are waiting

On a BadgerDB partition with many leased items ahead of its unleased ones, a lease can get "no items are
in the queue" while unleased items wait further on.

**Workaround**: Lease again. Repeated leases reach the waiting items.

## Stats and reload on a large BadgerDB partition hold up the queue

On BadgerDB, `/v1/queue.stats`, `/v1/queue.reload`, a queue's start-up and a failed partition's recovery
scan every item in the partition, at about 75,000 items a second on a laptop. The queue answers none of
its other requests until the scan finishes, so produces, leases and completes on a queue with a 500,000
item partition wait about 7 seconds behind each stats call. The scan runs past the read timeout
(`ReadTimeout` in the embedded server's `service.Config`, 3 seconds by default) rather than failing the
partition. A server shutdown waits for a scan in progress, or gives up at its own deadline while the scan
runs on. ENG-867 is the design that removes the full scan.

**Workaround**: Poll `/v1/queue.stats` rarely on queues with large BadgerDB partitions, and reload them
only when their counts look wrong.

## Storage backends count due scheduled items differently in stats

A scheduled item is due once its enqueue time passes, and it stays scheduled until Querator moves it into
the queue moments later. During that window, PostgreSQL and MongoDB count it in a partition's total in
`/queue.stats`. InMemory and BadgerDB count it only as scheduled.

**Workaround**: Before comparing stats across backends, wait a few seconds past the items' enqueue time
so they have moved into the queue.

## Listing storage backends loads every queue

`/v1/storage/backends.list` reads each partition's counts from the queue that owns it, and loads every
queue that is not loaded yet. On a node with many queues, or with large BadgerDB partitions, the first
list after a start takes as long as starting every queue. The unleased, leased and failures counts
are the ones `/queue.stats` reports, so they are wrong in the same cases. A backend that hangs rather
than failing makes each queue on it wait for its read timeout before the list replies.

**Workaround**: Call it rarely on a node with many queues, and reload a queue with `/v1/queue.reload`
when its counts look wrong.

## A saved affinity is lost on restart with memory queue storage

Querator saves a storage backend's affinity in queue storage. With the default memory queue storage
nothing survives a restart, so after a restart the config file's affinity is the one in force again,
and an affinity set with `/v1/storage/backends.update` is gone.

**Workaround**: Use badger, mongo or postgres queue storage to keep a saved affinity, or set the
affinity in the config file too.

## Changing queues holds up requests on a node with many queues

Deleting a queue, or creating or updating one that names a dead-letter queue or has more than one
partition, reads the stored queues to find any queue that names it as its dead-letter queue. Every
request that looks up a queue on that node waits while it does, so on a node with many queues those
changes pause traffic for as long as the read takes.

**Workaround**: Make queue changes outside peak traffic.

## Queues saved without the dead-letter checks must be fixed on update

Querator refuses a dead-letter queue that has its own dead-letter queue, but older releases did not check
this when the second queue was updated. Such chains stay in storage. In a chain where A's `dead_queue`
is B and B's is C, an update to A or B that leaves `dead_queue` out, or sends it unchanged, is refused. C
updates as usual. The same holds for a queue whose saved `dead_queue` does not exist or has more than one
partition, and for a dead-letter queue saved with more than one partition. Such queues
can already be in storage, for example from an older release's config file. A config file does not
change a queue already in storage.

**Workaround**: Fix the dead-letter setup in the same update.

* A queue whose `dead_queue` does not exist, has its own dead-letter queue or has more than one
  partition: send `"dead_queue": ""`, or name an existing single-partition queue with no dead-letter
  queue of its own.
* A dead-letter queue that has its own `dead_queue`: send `"dead_queue": ""`.
* A dead-letter queue saved with more than one partition: first fix every queue that names it as its
  `dead_queue`, as above.
* A config file that still declares the broken setup refuses to start, and the error names the queue
  and the rule. Fix the config file's entries as the error says, then fix the stored queues as above.
