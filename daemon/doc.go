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

// Package daemon runs a Querator server: a service.Service behind an HTTP server
// built by transport.NewHTTPHandler. The querator command and the tests start
// Querator through NewDaemon.
//
// NewDaemon applies Config.SetDefaults, creates the Service and starts serving.
// Calling Start on a running Daemon returns an error. Shutdown stops the Service,
// then the HTTP server, and returns only after the server's goroutine has exited,
// even when ctx expires first. Waiting cannot hang, because http.Server.Shutdown
// closes the listener before it waits on connections, which makes Serve return.
// See docs/adr/0015-shutdown-errors.md for the errors shutdown reports.
//
// Config holds the service.Config as its Service field. SetDefaults fills unset
// fields: ListenAddress is localhost:2319, the batch and request limits take the
// internal defaults, queue storage is in memory with one in-memory partition
// storage named mem-0, and AuthBackend is auth.NoOpAuthBackend, which allows
// every request and suits internal deployments. When Service.Auth is unset it is
// set to AuthBackend, so the transport authenticates and the Service authorizes
// with the same backend. MaxProducePayloadSize caps the bytes read for one
// /v1/queue.produce request body; left at 0 the transport's default applies.
// Service.MaxConcurrentRequests caps the number of open network connections, so
// a flood of clients cannot exhaust the server's resources; the InMemoryListener
// is not capped. Without an InMemoryListener, the daemon serves HTTPS when
// TLS.ServerTLS is set, and Client connects with TLS.ClientTLS when TLS is set.
//
// With InMemoryListener set, the daemon ignores ListenAddress and TLS and accepts
// connections from an InMemoryListener instead of the network. Each call to
// Client or MustClient then returns a new client bound to its own net.Pipe. This
// serves tests that cannot use the loopback interface or that run under
// testing/synctest. Without it, Client returns the same client on every call.
//
// ReadConfig decodes a YAML config file into a File with strict field checking:
// an unknown or misspelled key, including an option placed under the wrong
// backend, is an error instead of being ignored, as is a second YAML document
// with content after a "---" separator. ApplyConfigFile applies a File to a
// Config. Queue storage takes at most one backend section (memory, badger,
// mongo or postgres) and uses memory when none is set; each partition storage
// takes exactly one. The memory backend has no options and is selected with an
// empty mapping (memory: {}). In queue storage and auth storage a backend key
// with no value, such as a bare "memory:" or "badger:", still names that
// backend; in partition storage it is an error. See docs/packages/daemon.md for
// how queues listed in the file are applied.
package daemon
