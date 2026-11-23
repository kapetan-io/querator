# Package daemon

Why `daemon` skips config-file queues that already exist. The package comment in
`daemon/doc.go` covers the rest of the package.

## Queues listed in the config file

`ApplyConfigFile` adds each queue listed under `queues:` to queue storage. A
queue that queue storage already holds is skipped, before any partition
placement runs. A queue that lists more than 1,024 partitions is not skipped: it
fails start on every start, whether or not queue storage holds it.

The skip lets a restart succeed when every partition storage has affinity 0.
It applies only with persistent queue storage (badger, mongo or postgres); with
the default memory queue storage nothing survives a restart, so every listed
queue is added again. A
listed queue with no `partitions:` gets its partitions placed by each backend's
saved affinity, and placement fails when no partition storage has a saved
affinity above 0. On the first
start that error is correct. On a restart the queue already exists with its
partitions, so placing them again is unnecessary, and failing would stop a
server whose config has not changed. The stored queue keeps the partitions it
was given on the first start; changes to a stored queue's entry in the file are
not applied.
