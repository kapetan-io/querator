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

// Package auth holds the authentication and authorization vocabulary shared
// by the transport, the service and the auth backends: API keys, permissions,
// standard roles and the [Principal] that makes a request. See
// docs/adr/0025-multi-tenancy-and-auth.md for the model.
//
// # API keys
//
// [GenerateAPIKey] creates a key in the form qtr-<tag>-<entropy>. An empty tag
// becomes [DefaultKeyTag]. A tag is lowercase alphanumeric and at most
// [MaxKeyTagLength] characters. The returned [GeneratedKey] holds the full Key,
// to be returned to the user once; the KeyHash from [HashAPIKey], which is what
// gets stored; and a Prefix (qtr-<tag>- plus the first 8 entropy characters)
// that is safe to display. [ValidateAPIKeyFormat] checks a key's shape before
// any lookup. [KeyLength] is the number of random bytes in the secret part
// of a key.
//
// # Permissions and roles
//
// Permissions are strings such as "queue.produce". [IsValidPermission] reports
// whether a string is one of them. [AllPermissions], [AdminPermissions],
// [NamespaceOwnerPermissions] and [PublicViewerPermissions] return a new slice
// on every call so a caller cannot change the canonical lists. [RoleAdmin],
// [RoleNamespaceOwner] and [RolePublicViewer] are the standard roles;
// [IsStandardRole] reports whether a name is one of them. The role API cannot
// modify or delete a standard role; the service writes their permissions from
// the code at every start. [SystemNamespace] is
// the reserved _system namespace.
//
// # Principals and backends
//
// An [AuthBackend] turns an API key into a [Principal] and answers whether a
// principal holds a permission in a namespace ([Access]). A backend may cache
// the principal for an API key; InvalidateUser and InvalidateKey drop cached
// entries after a user or key changes. [NoOpAuthBackend] allows every request and is
// the default when no backend is configured.
//
// [ContextWithPrincipal] stores the principal on a request context and
// [PrincipalFromContext] reads it back, returning [AnonymousPrincipal] when
// none is set. The anonymous principal's UserID is [AnonymousUserID] because
// the service creates a user with that ID at bootstrap and binds it to the
// Admin role (when its auth backend is not the NoOp backend and user, role and
// role binding storage are configured), so permission checks for
// unauthenticated requests resolve through that role binding.
package auth
