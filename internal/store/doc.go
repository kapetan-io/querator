// Package store keeps querator's durable state behind interfaces, so a queue runs unchanged on
// any backend. InMemory, Badger, Postgres and MongoDB are the backends today.
//
// Queues, Namespaces, Users, APIKeys, Roles and RoleBindings hold metadata. A Partition holds the
// items of one partition of one queue, and a PartitionStore hands out Partitions. Config gathers
// the backends the QueuesManager is built with, and PartitionStorage pairs a PartitionStore with
// its name and affinity. PlacePartitions spreads a new queue's partitions across the configured
// backends in proportion to their affinity.
//
// Every backend contacts storage lazily, on first use, and a Partition is driven by a single
// goroutine, the logical queue that owns it, except Info and UpdateQueueInfo, which must be safe
// for concurrent use. docs/storage/README.md covers configuring them.
//
// BadgerDB caps one transaction at 15% of a database's write buffer and refuses to open when
// ValueThreshold is above that cap, so a set BadgerConfig.MemTableSize lowers ValueThreshold to
// the cap when it must. badgerTransactionLimit copies the 15% from BadgerDB (db.go in v4.3.0) and
// must follow it on upgrade.
//
// MongoDB's Lease keeps the items it placed in the same order as its bulk write models, because
// a bulk write error names a failed update by its index in the models.
package store
