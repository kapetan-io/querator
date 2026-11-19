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

// Embedded is an example program that runs Querator inside a Go process. It
// starts a daemon with in-memory storage, listening on QUERATOR_ADDRESS (or the
// daemon's default address when unset), creates a queue, produces two items,
// leases and completes them through the daemon's client, then serves until it
// receives SIGINT or SIGTERM and shuts the daemon down.
package main
