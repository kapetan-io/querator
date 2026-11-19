// Package internal is Querator's queue core. It exists so that every read and write to a queue's
// partitions happens on one goroutine, the request loop of a Logical Queue, which batches client
// requests into as few storage calls as it can (docs/adr/0003-rw-sync-point.md).
//
// QueuesManager creates, loads, updates and deletes queues, starts one Logical Queue for each
// queue that has partitions, and moves items whose lifecycle has ended to the queue's dead-letter
// queue. A Logical Queue owns its partitions: it assigns produce and lease requests to partitions
// by load, writes them to storage, answers waiting clients, and runs each partition's lifecycle
// (expired leases, expired items, scheduled items), deleting dead items itself when the queue has
// no dead-letter queue. AuthBackend and AuthCache authenticate API keys and check permissions
// against role bindings.
//
// Why the package works the way it does is in docs/queue-core.md.
package internal
