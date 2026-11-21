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

// Package reply holds the errors whose message is returned to the client.
// Each implements duh.Error, so the transport replies with the error's code and
// message instead of a generic "Internal Error" (see
// docs/adr/0006-error-handling.md):
//
//   - [ErrRequestFailed]: the request was valid but failed.
//   - [ErrInvalidOption]: an option in the request was invalid.
//   - [ErrRetryRequest]: the request was valid and the server did not fail, but
//     the request did not succeed; the client should retry.
//   - [ErrConflict]: the request conflicts with existing state.
//   - [ErrUnauthorized]: authentication is missing or invalid.
//   - [ErrForbidden]: the caller is authenticated but lacks permission.
//
// [NewInvalidOptionKind] tags an [ErrInvalidOption] with a [Kind] so callers
// can tell one condition apart from other invalid-option errors with
// errors.Is. A target with no kind matches any ErrInvalidOption; a target
// with a kind matches only an error with the same kind.
package reply
