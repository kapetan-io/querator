# 31. Partition placement and rebalance

Date: 2026-11-21

## Status

Accepted
Supersedes in part ADR 16
Supersedes in part ADR 23
Supersedes in part ADR 26
Supersedes in part ADR 27

## Context

This ADR supersedes in part [16. Queue Partitions](0016-queue-partitions.md),
[23. Partition Maintenance](0023-partition-maintenance.md),
[26. Queue partition limit](0026-queue-partition-limit.md) and
[27. Dead-letter queues](0027-dead-letter-queues.md).

An operator needs to change where a running queue's partitions live: to add a storage backend, weight one more
heavily, or empty one before retiring it, while producers and consumers keep working.

## Decision

An operator states what they want and Querator reconciles it.

* A queue's requested partition count and each storage backend's affinity are saved requests. Saving one changes
  nothing else.
* Rebalance is the only call that changes where an existing queue's partitions live. It covers every queue, takes
  no arguments, and may be called at any time, including while partitions are draining.
* Affinity is saved in the queue store and changed through the API. The config file's value is read once, when a
  storage backend is first seen.
* Each backend's share of a queue follows its affinity. Partitions are handed out one at a time to a backend that
  is below its proportional share, ranked by how far below it is and by a hash of the queue name and backend name.
  Placement does not depend on config order, the same saved facts always give the same answer, and while
  affinities are unchanged a lower count only drains and a higher count only creates.
* A partition that loses its place is marked draining in the same save that creates its replacement. It takes no
  new items, is leased from first, and is removed once empty. It is never made available again, and its number is
  never reused.
* Querator never moves an item between partitions, and no call names a partition to drain.
* A queue with one partition follows its share like any other queue. It keeps its order because lease takes the
  lowest-numbered draining partition first and waits instead of skipping a draining partition that is failing.
* No call replies that the work is finished. Progress is read from `queues.info` and `storage/backends.list`.
* A dead-letter queue has one partition taking produce. While it moves it also has a draining partition, and it
  skips an item held in either.
* The 1,024 partition limit of ADR 0026 applies wherever a count or a partition list is saved, including
  `queues.update` and a config-file queue that lists its partitions. Querator makes no exception for a queue above
  the limit.

## Consequences

* Every change creates new partitions, and the old ones stay until consumers empty them. A drain has no time limit.
* A one-partition queue stops leasing while the backend it is moving off is down.
* A backend that dies holding draining partitions has to be restored before those partitions can be removed.
* Rebalance may overlap an earlier one safely, because a draining partition never comes back and numbers are never
  reused.
* Changing the hash or the ranking formula changes where existing queues' partitions belong, so the next rebalance
  moves them.
* This design assumes one instance acts on a queue. Several instances acting on one queue must agree before any
  partition is removed, and this design does not provide that agreement.

## Supersedes

* ADR 0016, in part: "Partition Backend Storage Assignment", the "Scenarios" with every note under them, the notes
  about re-balance and the "Notes about storage backends" among them, including the rule that a rebalance must
  finish before another starts. Its logical queue sections stand.
* ADR 0023, in part: the "Draining" and "Migrating a Partition" sections.
* ADR 0026, in part: where the limit applies.
* ADR 0027, in part: a dead-letter queue has one partition taking produce, rather than one partition.
