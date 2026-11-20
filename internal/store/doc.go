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
// for concurrent use. docs/storage/internals.md explains the contract and
// why each backend is built the way it is. docs/storage/README.md covers configuring them.
//
// BadgerConfig.MemTableSize sets the write buffer of each partition's database; zero keeps
// BadgerDB's default of 64 MiB, and the metadata databases always keep it. BadgerDB limits one
// transaction to 15% of that buffer, and badgerTransactionLimit copies that formula from BadgerDB
// (db.go in v4.3.0), so it must follow BadgerDB on upgrade. A value under ValueThreshold counts
// toward that limit in full, and BadgerDB refuses to open when ValueThreshold is above the limit,
// so a set MemTableSize lowers ValueThreshold to the limit when it must. Tests set a small buffer
// so small items reach the transaction limit.
//
// MongoDB's Lease keeps the items it placed in the same order as its bulk write models, because
// a bulk write error names a failed update by its index in the models.
package store
