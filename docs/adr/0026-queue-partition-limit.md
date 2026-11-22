# 26. Queue partition limit

Date: 2026-11-18

## Status

Accepted

## Context

A queue's partition count comes from the caller of `/queues.create` or from a `requested-partitions` entry
in the config file. Querator builds one `PartitionInfo` per requested partition before it stores the queue,
and every storage backend later creates one partition per entry. Before this decision the only check was a
lower bound of 1, so a request for 2,147,483,647 partitions made Querator allocate billions of values before
anything rejected it.

ADR 0016 expects queues with "hundreds of partitions". Measured through the HTTP client on the in-memory
backend, creating a queue with 1,024 partitions took 14 ms and 243 MB of heap, while 65,536 partitions took
15.9 GB. Every backend stores the whole partition list in a single queue record, which puts MongoDB's 16 MiB
document limit near 4,600 partitions in the worst case (the limit is from MongoDB's documentation, not a
run). The evidence supports any limit from about 256 to 4,096.

## Decision

A queue may request at most 1,024 partitions when it is created. A larger request through `/queues.create`
fails with an invalid-option error (HTTP 400) before any partition is placed, and a config-file queue that
asks for more fails startup with an error naming the queue. The limit is a constant in the store's validation, not a setting, and `store.PlacePartitions` enforces it.

The limit applies only where Querator places partitions. It is not checked on `/queues.update`, when a queue
loads from storage, or for config-file queues that list their partitions explicitly, so queues stored with
more than 1,024 partitions keep loading, serving and updating.

## Consequences

A deployment that needs more than 1,024 partitions in one queue needs a code change. Raising the limit
past about 4,600 also needs MongoDB to stop storing the whole partition list in the queue's single document.

The limit does not make 1,024 partitions cheap on every backend. Each Badger partition is its own database,
measured at 85 to 89 MB of heap and 16 goroutines, and a queue opens every partition when it loads.
