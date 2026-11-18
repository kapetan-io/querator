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

// Package transport serves the Querator API over HTTP. It decodes each request,
// authenticates it, calls a [Service] and writes the reply.
//
// [NewHTTPHandler] builds the [HTTPHandler] from an [HTTPHandlerConfig]. The
// handler serves GET /metrics and GET /health, and rejects any other request
// that is not a POST. An authenticated POST to a path that is not an API path
// gets a "no such method" error. A request carries its API key as
// "Authorization: Bearer <key>"; a request without the header is passed to the
// [auth.AuthBackend] with an empty token. When no AuthBackend is configured the
// handler uses [auth.NoOpAuthBackend], which allows every request.
// MaxProduceSize defaults to 5 MB.
//
// [Service] is the full API the handler calls. It is composed of smaller
// interfaces, one per area of the API ([QueueOps], [QueueAdmin],
// [StorageInspector], [NamespaceAdmin], [UsersAdmin], [APIKeysAdmin],
// [RolesAdmin] and [RoleBindingsAdmin]), plus Health.
//
// Errors that implement duh.Error are returned to the client as is. Any other
// error is logged, with the Authorization and Cookie headers redacted, and the
// client receives a generic "Internal Error" (see
// docs/adr/0006-error-handling.md).
//
// [RPCStorageItemsList] and [RPCStorageScheduledList] page by pivot, with the
// paging rules a caller sees described in package querator. The reply includes
// the pivot item because that maps to an efficient "primary_key >= pivot" query
// in an ordered store, and a missing pivot starts the reply at the next item so
// a caller can page through a list that keeps changing without "pivot not
// found" errors.
//
// [HealthResponse] follows the "Health Check Response Format for HTTP APIs"
// draft (draft-inadarei-api-health-check-06).
//
// [ErrRequestFailed], [ErrInvalidOption], [ErrRetryRequest] and [ErrConflict]
// mirror the errors in package reply. Only their own tests use them; return the
// errors from package reply instead.
package transport
