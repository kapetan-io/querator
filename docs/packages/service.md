# Package service

Why `service` deletes namespaces, users and API keys in the order it does. The
package comment in `service/doc.go` covers the rest of the package.

## Deleting a namespace

ADR 0025 says a namespace cannot be deleted while it holds queues, custom roles
or role bindings. `NamespacesDelete` checks them in this order: role bindings,
then roles, then queues. A role binding refers to a role, so an administrator
must delete the bindings before the roles. Checking bindings first means the
first error names the records to delete first, and following the errors in the
order they appear works.

## Deleting a user

`UsersDelete` deletes the user record first, then invalidates the user in the
auth backend, then deletes the user's role bindings and API keys. With the user
record gone, authentication with any of the user's API keys fails at once, even
if deleting the role bindings or API keys fails afterward. Those two deletes
are best effort: both run, and their errors are joined into the returned error.

## Deleting an API key

`APIKeysDelete` reads the API key before it deletes it. The auth backend caches
keys by their hash, and the hash is only on the stored record, so the read is
what lets the service invalidate the cached key once the record is gone.
