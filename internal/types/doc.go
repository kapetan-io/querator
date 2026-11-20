/*
Copyright 2024 Derrick J. Wippler

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package types holds the values shared by the logical queue, the storage backends and
// the service. It sits below both package internal and the store package so either can
// import it.
//
// It holds items and queue info as storage holds them, the requests a logical queue
// batches on each pass of its request loop, partition state and stats, lifecycle
// actions, the auth records (users, API keys, roles, role bindings) and the errors those
// operations return.
//
// # Batches
//
// ProduceBatch.Reset keeps the requests that were not yet assigned a partition, so they
// wait for the next pass of the request loop instead of being dropped. Code that drops lease requests
// while iterating a LeaseBatch calls MarkNil and then FilterNils after the loop; marking
// in place is cheaper and safer than removing entries mid-iteration. LeaseBatch.Unassign
// takes particular items back off their requests when a backend could not save their
// leases, so a consumer receives only items storage shows as leased.
//
// CompleteRequest.Completed, RetryRequest.Retried and RetryRequest.UnLeased record how many
// items storage applied, since a request stops at its first refused item after applying the
// ones before it. A backend whose write rolls back calls MarkRolledBack, so the logical queue
// does not count items storage discarded.
//
// # Partition state
//
// PartitionState is the logical queue's in-memory view of a partition.
// MostRecentDeadline is the lease or expire deadline from the last produce or lease the
// logical queue wrote to the partition; while the partition has leased items and that
// deadline is still ahead but before the partition's next lifecycle run, the run is
// moved up to it. In PartitionStats, Total comes from storage while UnLeased and
// NumLeased come from that in-memory state, so the stats show both views and drift in the
// in-memory accounting can be spotted. The two views can differ while a write's outcome is
// unknown and after lifecycle actions, clear or import that do not move the in-memory counts,
// and Total leaves out scheduled items, so a gap alone is not drift.
//
// PartitionCompleter lets a LifeCycleRequest carry the partition's storage without this
// package importing the store package, which imports this one.
//
// Item.SourceID holds the ID an item had in its source queue. It is set when an item
// moves to a dead letter queue, or carried in when an item is imported with one, and
// is empty otherwise.
//
// # Errors
//
// The Kind constants and their Err sentinels exist for errors a caller must match with
// errors.Is; match against the sentinel rather than calling a constructor with an empty
// name. The API key errors are fixed sentinels that carry no details of the key that was
// presented; a caller learns only that it does not exist, has expired or is invalid.
package types
