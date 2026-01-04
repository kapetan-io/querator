# 25. Multi-Tenancy and Authentication Architecture

Date: 2026-01-04

## Status

Accepted

## Context
Querator requires a robust multi-tenancy system to support both:
1.  **SaaS Environments**: Strict isolation between untrusted tenants.
2.  **Internal Teams**: Logical separation of workloads (e.g., `production` vs. `staging`) within a single organization.

The existing system lacks authentication, treating all requests as privileged. We need a design that introduces security without compromising the high-throughput performance characteristics of a distributed queue.

Key constraints:
- **Performance**: Authorization checks must be sub-millisecond to avoid slowing down the hot path (produce/consume).
- **Simplicity**: Client integration should be trivial (standard headers).
- **Flexibility**: Support for both manual API Key management (Service Accounts) and future SSO/OIDC integrations (Human Users).
## Decision
We will implement a **Namespace-based Isolation Model** coupled with a **Key-Centric Authentication System**.
### Namespace Isolation
- **Flat Namespace Model**: Resources (Queues) belong to exactly one namespace. This simplifies the data model compared to hierarchical structures.
- **Hard Isolation**: A user's access is strictly bounded by the namespaces they are authorized for.
- **Reserved Names**: The `_system` namespace is reserved for cluster-wide administration.
- **Naming Convention**: Resource names use `snake_case` or `kebab-case`. The `_` prefix in `_system` ensures collision avoidance (users can name their namespace `system` if they wish) and visual distinction in lists.
- **Deletion Constraints**: To prevent accidental data loss, a namespace **cannot** be deleted if it contains any resources (queues), custom roles, or active role-bindings. These must be explicitly deleted by an administrator before the namespace itself can be removed. API Keys scoped to a deleted namespace become immediately invalid.

### Authentication (Passwordless API Keys)
- **Primary Credential**: Long-lived API Keys (`sk-live-...`) are the *only* credential type.
- **No Passwords**: The `User` entity has no password field. This eliminates the need for password hashing complexity (bcrypt), password reset flows, and brute-force protection logic.
- **Provisioning Model**: 
    - **Bootstrap (Open Door)**: On startup, the `Anonymous` principal is granted `Admin` privileges by default. This allows immediate access for setup and testing. To secure the cluster, the admin must delete the `Anonymous` -> `Admin` role binding. They may optionally bind `Anonymous` to the `PublicViewer` role (read-only health/metrics) if public monitoring is desired.
    - **User Onboarding**: Admins create keys for other users via the API. The API returns the raw key in the response *once*.
    - **Distribution**: Admins are responsible for delivering this key to the user (e.g., via Vault/Email). Using a "One-Time Secret" link service is recommended to mitigate interception risks.
- **Mechanism**: `Authorization: Bearer <key>`.
- **Storage**: Keys are stored as SHA-256 hashes. The raw key is returned only once upon creation and never stored.
- **Default Scoping**: If an API Key is created *without* a specific `namespace_scope`, it defaults to **Inheritance Mode**, meaning it inherits all permissions the User has across all namespaces.

### Key Format & Design
We adopt a **Prefixed Token Format** (inspired by Stripe) to improve security and developer experience.
- **Format**: `sk-[env]-[entropy]`
    - `sk` = **Secret Key**. Explicitly identifies the credential as sensitive.
    - `[env]` = Environment/Tag. **MANDATORY**. Defaults to `live` (e.g., `sk-live-...`). Can be customized (e.g., `sk-ci-...`, `sk-test-...`) for audit clarity.
    - `[entropy]` = 32+ characters of cryptographically secure random string (Base62).
    - Example: `sk-live-9d8f7e6a5b4c3d2e1f0a9b8c7d6e5f4a`
- **Parsing Rules**: The system treats the **entire string** (prefix included) as the authentication token. The `[env]` tag is not parsed for logic; it exists solely for human identification and scanner targeting.
- **Rationale**:
    1.  **Regex Reliability**: Enforcing the 3-part structure allows security scanners to use strict patterns (e.g., `^sk-[a-z0-9]+-[a-zA-Z0-9]{32}$`), eliminating false positives from random variables.
    2.  **Visual Safety**: The `live` tag clearly communicates "Production Credential", reducing the risk of accidental usage in test environments.
    3.  **API Consistency**: Using hyphens (`-`) matches the hyphenated naming convention used across the REST-RPC method names.

### Authorization (RBAC & Cascading)
- **Role-Based**: Permissions are assigned via Roles (e.g., `NamespaceOwner`, `QueueProducer`).
- **Code-First Roles**: To prevent security drift, "Standard Roles" (like `NamespaceOwner`) are defined in the codebase and synchronized to the database at startup. Wildcard permissions (e.g., `queue.*`) are forbidden in the database to ensure explicit auditing.
- **Cascading Logic**: Authorization follows a strict 3-step check:
    1.  **Scope Check**: If an API Key is "scoped" to a specific namespace (e.g., `production`), it is mathematically impossible for that key to access any other namespace, regardless of the user's roles.
    2.  **Target Check**: If not scoped, the system checks if the User has a Role Binding in the target namespace.
    3.  **System Check (The Admin Cascade)**: Finally, the system checks if the User has a Role Binding in the `_system` namespace. Permissions granted in `_system` automatically cascade to *all* namespaces.

### The Logic Gate (Mathematical Constraint)
The "mathematical impossibility" of a scoped key accessing other namespaces is enforced via a boolean logic intersection. Access is calculated as:
> `Effective_Access = (Key_Scope_Match) AND (User_Has_Permission)`

If a key is scoped to `NS-A` and the target is `NS-B`, the `Key_Scope_Match` evaluates to `FALSE` (0). Since `0 AND X = 0`, access is denied immediately, regardless of the user's roles or admin status. The Key acts as a hard filter over the User's permissions.

A scoped API Key represents a **Snapshot of Intent** at the time of creation.
- **Scenario**: If a user creates a key scoped to `NS-A`, and is later granted permissions for `NS-B`, the original key **cannot** access `NS-B`.
- **Reasoning**: The key's constraint (`Scope=NS-A`) remains active. This is a security feature: it prevents "Privilege Escalation" for existing worker scripts if a user is promoted.
- **Solution**: To access the new namespace, the user must generate a new key (either scoped to `NS-B` or Unscoped).

### Auditability
The system leverages the Key-Centric model for high-fidelity auditing:
- **Key-Level Attribution**: Audit logs record the specific API Key ID used for an action, not just the User ID. This allows distinguishing between a user's "Laptop CLI" key and their "Production Worker" key.
- **Scope logging**: Failed attempts to access out-of-scope resources are logged as security alerts.

### Admin Terminology
- **Admin**: A user with broad privileges defined in the `_system` namespace (cascading access).
- **NamespaceOwner**: A user with full control over a specific namespace (isolated access).

### External Auth Strategy (Token Exchange)
To support future SSO (Okta, Google, OIDC) without compromising performance:
- **No Direct JWT Validation**: The system will *not* validate third-party JWTs on the hot path (produce/consume) due to the high cost of RSA signature verification and potential network introspection.
- **Token Exchange Pattern**:
    1.  User logs in via Okta and gets an OIDC ID Token.
    2.  Client calls `POST /auth/login-oidc` with the token.
    3.  Querator validates the token *once*, maps it to a local `User`, and issues a **Short-Lived API Key** (e.g., 12 hours).
    4.  Client uses the high-performance native API Key for all subsequent requests.

### Self-Lockout Policy
- **Security First**: Users are strictly allowed to delete their last or currently active key. This ensures immediate revocation is always possible in case of compromise.
- **Behavior**: Deleting the active key results in an immediate `401 Unauthorized` on the next request, effectively acting as a "Logout".

## Consequences

### Positive
- **Performance**: SHA-256 hash checks are nanoseconds fast, whereas JWT/RSA checks are milliseconds slow. This preserves the throughput of the queue.
- **Safety**: Scoped API keys allow "Least Privilege" for worker scripts (e.g., a script can be restricted to `staging` only).
- **Simplicity**: The "One Key, One Header" model is universally supported by all HTTP clients.
- **Evolution**: The `_system` reservation and "Exchange" pattern pave the way for future enterprise features without breaking changes to the core.

### Negative
- **Security Default**: The "Open Door" default (Anonymous=Admin) requires an explicit lock-down step for production environments, which could be missed.
- **Implementation Complexity**: The "Cascading Auth" logic must be implemented carefully in the application layer to ensure the `_system` fallback works correctly.
- **Key Management Burden**: Without a self-service "Forgot Password" flow, Admins must manually handle key rotation and replacement for users who lose their keys.
- **Distribution Risk**: The reliance on out-of-band distribution (sending keys via email/chat) introduces a potential security weakness if not handled via secure channels (like One-Time Secrets).