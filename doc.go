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

// Package querator is the Go client for a Querator server. A Client calls the
// Querator API over HTTP, sending each request as protobuf; the request and
// response types live in the proto package.
//
// Build a ClientConfig and pass it to NewClient. Endpoint is required and has the
// form <scheme>://<host>:<port>, except with WithConn, which uses http://memory
// because the connection is already open. The Client field is optional; set it to supply your own
// *http.Client, for example one with a custom TLS config. When APIKey is set, the
// Client sends it as a Bearer token on every request. WithNoTLS, WithTLS and
// WithConn build a ClientConfig for a plain address, a TLS address, or an existing
// net.Conn.
//
// The list methods take ListOptions to page through results by Pivot and Limit.
// StorageItemsList and StorageScheduledList return the item list starting at
// and including the pivot, so a caller paging through items drops the first
// item of each page after the first. If the pivot no longer exists they start
// at the nearest item after it instead of returning a "pivot not found" error,
// so a caller can page through a partition whose items are changing. A caller
// must therefore check that the first item is the pivot it asked for.
//
// The Msg constants are the error messages the server returns for request
// timeouts, duplicate client ids, shutdown and an overloaded queue, so callers
// can match on them. CollectIDs gathers the ids of a slice of items, for example
// to complete a batch of leased items.
//
// Version holds the Querator version; the build sets it with -ldflags (see
// docs/RELEASE.md).
package querator
