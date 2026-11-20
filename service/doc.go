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

// Package service implements the Querator API. A Service method exists for each
// queue, storage, namespace, user, API key, role and role binding endpoint;
// transport.HTTPHandler decodes a request and calls the matching method, and
// the daemon package wires the two together behind an HTTP server.
//
// New builds a Service from a Config and runs Bootstrap. StorageConfig holds
// the storage backends for queues, partitions, namespaces and the auth records.
// InstanceID names this Querator instance; the daemon uses it in its error
// messages. Unset Config fields take these defaults: Log is slog.Default(),
// Clock is the real clock (tests pass a frozen one), ReadTimeout and
// WriteTimeout, the time a single batched read or write to storage should take,
// are 3 seconds, and Auth is auth.NoOpAuthBackend, which allows every request.
//
// MaxLeaseBatchSize, MaxProduceBatchSize and MaxCompleteBatchSize are the most
// items one lease request may ask for, one produce request may carry, and one
// complete request may name; left at 0 each is 1,000. MaxRequestsPerQueue is
// how many requests each logical queue holds before it reports itself
// overloaded; left at 0 it is 100. These defaults are applied when each logical
// queue starts. MaxConcurrentRequests is read only by the daemon, as its
// connection limit.
//
// QueueProduce, QueueLease, QueueComplete and QueueRetry hand the request to a
// logical queue and block until it answers or the context is canceled. They
// return at once with a retry error when the logical queue is overloaded, and
// with a request-failed error when it is shutting down. QueueLease also returns
// when its request timeout passes. QueueClear and QueueReload act on every
// logical queue of the queue in turn and return the first error, so a caller
// that gets an error knows the request did not complete on every logical queue.
//
// Service validates queue and namespace names when they are created, updated or
// deleted, so a malformed name is rejected before the request reaches storage.
// Namespace storage (StorageConfig.Namespaces) is optional: without it
// QueuesCreate does not check that the queue's namespace exists, and the
// namespace methods fail with "namespace storage not configured".
//
// Bootstrap runs on every start. When namespace storage is configured it
// creates the _system namespace if it is missing. When Auth is not the NoOp
// backend and user, role and role binding storage are all configured, it also
// creates the anonymous user if missing, writes the standard roles (Admin,
// NamespaceOwner, PublicViewer), and creates the anonymous to Admin role
// binding described in ADR 0025 if missing. A standard role that already exists
// is overwritten with the permissions the code defines, so standard roles are
// synced from the code at every start. Bootstrap writes these straight to storage
// because the API methods cannot create them: NamespacesCreate rejects reserved
// namespace names, RolesCreate rejects standard role names, and UsersCreate
// generates its own user ID, so it cannot create the anonymous user.
//
// PauseQueue and ResumeQueue hold and release a queue's produce, lease,
// complete and retry requests so tests can simulate high contention; no API
// endpoint calls them. Storage requests and queue list, create, update and
// delete requests are not held while a queue is paused.
//
// See docs/packages/service.md for why NamespacesDelete, UsersDelete and
// APIKeysDelete read, check and delete records in the order they do.
package service
