# Technical Specification: Multi-Tenancy, User/Roles, and API Key Management

## 1. Overview

This specification defines a comprehensive multi-tenancy system for Querator that includes:
- **Namespaces** for tenant isolation (hard isolation, NATS-style).
- **Users and Roles** with flexible permission models (custom roles only).
- **API Keys** as user credentials (unified model).
- **Auth Interface** supporting built-in and external identity providers.

The design supports both **SaaS multi-tenancy** (untrusted tenants, strict isolation) and **internal team isolation** (teams within an organization).

### Key Design Decisions

| Decision            | Resolution                                                                                                 |
| ------------------- | ---------------------------------------------------------------------------------------------------------- |
| **Admin**           | Managed via roles in the reserved `_system` namespace. Permissions granted here cascade to all namespaces. |
| **Namespace Logic** | Flat namespaces; `_system` is reserved for global operations.                                              |
| **API Key Scoping** | Keys can be strictly scoped to a single namespace or inherit the user's full access.                       |
| **Bootstrap**       | Config file creates an initial admin user bound to `_system`.                                              |
| **Public Access**   | Handled via an explicit `Anonymous` principal with restricted roles (e.g., Health/Metrics).                |
| **Wildcards** | No wildcards in DB. NamespaceOwner roles are auto-populated/maintained by the system at startup (See Section 9.4). |
| **Key Rotation**    | Immediate replacement (no overlap period).                                                                 |

### 1.1 Key Definitions

- **Principal**: The transient security identity associated with a specific request. It encapsulates the **User** and any request-specific constraints (e.g., an API Key restricted to a single namespace). Authorization checks are performed against the Principal.
- **Namespace**: A logical isolation boundary. Resources (queues) belong to exactly one namespace.
- **Admin**: A user with privileges in the reserved `_system` namespace. Their permissions cascade to all other namespaces.
- **NamespaceOwner**: A standard role granting full control over a specific namespace, but no access to others.
- **Role Binding**: The association that grants a specific Role to a User within a specific Namespace context.

---

## 2. Current State Analysis

### Affected Components
- `transport/http.go`: HTTP handler (must add auth middleware that assigns Anonymous/User principals).
- `service/service.go`: Service layer (must add tenant context and cascading permission checks).
- `internal/queues_manager.go`: Queue management (operations must be namespace-scoped).
- `internal/store/store.go`: Storage interface (must add auth/namespace storage tables).
- `client.go`: Client (must add API key header support).

### Relevant ADRs
- **ADR-0002 (Design Goals)**: Simplicity/Efficiency.
- **ADR-0025 (Proposed)**: Multi-Tenancy and Authentication Architecture.

---

## 3. Architectural Context

### Design Principles
1. **Simplicity for Clients**: Authentication is just a header (`Authorization: Bearer <key>`).
2. **Explicit Authority**: No hidden flags (`IsAdmin`). Authority comes strictly from Role Bindings.
3. **Cascading Permissions**: Admins are simply users who possess permissions in the `_system` namespace.
4. **Least Privilege Defaults**: The system defaults to denying access unless explicitly granted.

---

## 4. Requirements

### 4.1 Functional Requirements

#### Namespace Management
- **NS-001**: System shall support creating flat-named namespaces (e.g., `production`).
- **NS-002**: Queues shall belong to exactly one namespace.
- **NS-003**: Namespace names shall be unique cluster-wide.
- **NS-004**: Reserved namespace `_system` shall exist for global administrative permissions.
- **NS-005**: Deleting a namespace shall require all queues, custom roles, and role bindings within that namespace to be deleted first.
- **NS-006**: Target namespace must be provided by the client (header/payload) or resolved from API Key scope.

#### User Management
- **USER-001**: Users shall be identifiable by unique username or external ID.
- **USER-002**: Users shall be able to have roles in multiple namespaces.
- **USER-003**: A user's role in the `_system` namespace cascades to all other namespaces (Admin).

#### Role Management
- **ROLE-001**: Roles shall be custom-defined.
- **ROLE-002**: Roles shall be namespace-scoped (defined within a namespace, including `_system`).
- **ROLE-003**: Permissions shall be simple operation identifiers (no wildcards).

#### API Key Management
- **KEY-001**: API keys are credentials for users.
- **KEY-002**: API keys can optionally be **scoped** to a single namespace.
- **KEY-003**: API keys shall support optional expiration.
- **KEY-004**: API key rotation shall be immediate.
- **KEY-005**: API keys shall follow the 3-part format `sk-[env]-[entropy]`. The `[env]` tag is mandatory (defaulting to `live`) and is not parsed for logic.
- **AUTH-001**: Authenticate requests via API key (`Authorization: Bearer <key>`).
- **AUTH-001**: Authenticate requests via API key (`Authorization: Bearer <key>`).
- **AUTH-002**: Requests without headers are authenticated as the `Anonymous` principal.
- **AUTH-003**: `Anonymous` principal shall have **full administrative permissions** (Role `Admin` in `_system`) by default. This ensures the system is usable out-of-the-box for development and testing. To secure the system, administrators must remove this binding.
- **AUTH-004**: Authorization follows a **Cascading Check**:
    1. Check API Key Scope (if set, restrict to that namespace).
    2. Check Role Binding in Target Namespace.
    3. Check Role Binding in `_system` Namespace.
- **AUTH-005**: Failed authorization returns HTTP 403 Forbidden.

### 4.2 Non-Functional Requirements
- **PERF-001**: Permission checks < 1ms latency (cached).
- **SEC-001**: API keys stored as SHA-256 hashes.
- **SEC-002**: Public endpoints (Health/Metrics) managed via RBAC, not hardcoded middleware bypasses.

---

## 5. Technical Approach

### 5.1 Layered Auth with Cascading Checks

```
┌─────────────────────────────────────────────────────────────────┐
│ HTTP Layer                                                      │
│ - Extract API key OR assign "Anonymous" principal               │
│ - Resolve Identity (User + Key Constraints)                     │
│ - Inject Principal into Context                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ Auth Backend Interface                                          │
│ Authenticate(ctx, creds) → (Principal, error)                   │
│ HasPermission(ctx, principal, targetNS, perm) → bool            │
│                         │                                       │
│                         ├─ 1. Check Key Scope (fail if mismatch)│
│                         ├─ 2. Check Role in targetNS            │
│                         └─ 3. Check Role in "_system" (Cascade) │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. Data Model

### 6.1 Namespace

```go
Namespace {
    Name        string    // unique identifier
    Description string    // human-readable description
    CreatedAt   time.Time
}
```

**Reserved Names:** `_system` (System Administration).

#### Naming Convention Rationale
The decision to use the `_` prefix for reserved namespaces (e.g., `_system`) serves three purposes:
1.  **Collision Avoidance:** Allows users to create a workspace named "system" without conflict.
2.  **Simplified Validation:** We reserve the entire `_*` pattern, future-proofing for potential internal additions (e.g., `_audit`) without maintaining a complex blocklist.
3.  **Visual Distinction:** In most sorting algorithms, `_` ensures administrative namespaces appear first, and clearly designates them as internal/meta constructs.

### 6.2 User

```go
User {
    ID          string    // unique identifier
    Username    string
    ExternalID  string    // optional
    Email       string    // optional
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

### 6.3 Queue

```go
Queue {
    Namespace           string    // NEW: partition/tenant isolation
    Name                string
    LeaseTimeout        time.Duration
    DeadQueue           string
    MaxAttempts         int
    RequestedPartitions int
    CreatedAt           time.Time
    UpdatedAt           time.Time
}
```

### 6.4 Role

```go
Role {
    ID          string
    Namespace   string    // owning namespace (can be "_system")
    Name        string
    Permissions []string
    CreatedAt   time.Time
}
```

### 6.4 RoleBinding

```go
RoleBinding {
    ID          string
    Namespace   string    // scope (can be "_system")
    UserID      string
    RoleID      string
    CreatedAt   time.Time
}
```

### 6.5 APIKey

```go
APIKey {
    ID             string
    UserID         string
    NamespaceScope *string    // OPTIONAL: If set, key is strictly limited to this NS
    Name           string
    KeyHash        string     // SHA-256 hash
    KeyPrefix      string     // first 8 chars
    ExpiresAt      *time.Time
    LastUsedAt     *time.Time
    CreatedAt      time.Time
}
```

*Note: If NamespaceScope is set, the Principal cannot access any other namespace, even if the User has _system roles.*

---

## 7. API Design

### 7.1 Authentication Header

```
Authorization: Bearer <api_key>
```

*Missing header implies Anonymous principal.*

### 7.2 API Endpoints (RPC Style)

All endpoints expect `POST` requests with JSON bodies and return `application/json`.

#### Namespace Management
```
POST /v1/namespaces.create   { name, description } → Reply
POST /v1/namespaces.delete   { name } → Reply
POST /v1/namespaces.list     { limit, pivot } → { items: []NamespaceInfo }
```

#### User Management
```
POST /v1/users.create   { username, email, external_id } → Reply
POST /v1/users.delete   { id } → Reply
POST /v1/users.list     { limit, pivot } → { items: []User }
```

#### Role Definitions (Custom Roles)
```
POST /v1/roles.create   { namespace, name, permissions[] } → Reply
POST /v1/roles.delete   { namespace, name } → Reply
POST /v1/roles.list     { namespace, limit, pivot } → { items: []Role }
```

#### Access Control (Role Bindings)
```
POST /v1/role-bindings.create   { namespace, role_name, user_id } → Reply
POST /v1/role-bindings.delete   { namespace, role_name, user_id } → Reply
POST /v1/role-bindings.list     { namespace, user_id_filter, limit, pivot } → { items: []RoleBinding }
```

#### API Key Management
```
POST /v1/api-keys.create
{
  "user_id": "...",
  "name": "production-worker",
  "namespace_scope": "production",
  "expires_at": "2025-12-31T00:00:00Z"
}
→ { id, key, prefix } // "key" is shown ONLY here

POST /v1/api-keys.delete   { id } → Reply
POST /v1/api-keys.list     { user_id, limit, pivot } → { items: []KeyMetadata }
```

#### Common Response Object (Reply)
```json
{
  "code": 200,
  "code_text": "OK",
  "message": "Operation successful"
}
```

### 7.3 Public Access (System Configuration)

On startup, the system ensures:

1. A role `PublicViewer` exists in `_system` with permissions `system.health`, `system.metrics`.
2. The reserved `Anonymous` user is bound to the **`Admin`** role in `_system`.

---

## 8. Auth Backend Interface

### 8.1 Interface Definition

```go
type AuthBackend interface {
    // Returns Principal with User and Key Constraints
    Authenticate(ctx context.Context, token string) (Principal, error)

    // Implements the Cascading Check logic
    HasPermission(ctx context.Context, principal Principal, targetNS string, perm string) (bool, error)
}

type Principal struct {
    User           User
    NamespaceScope *string // Limits context if set
}
```

### 8.2 Logic Flow (Pseudocode)

```go
func HasPermission(p Principal, targetNS, perm) bool {
    // 1. Enforce Key Scope
    if p.NamespaceScope != nil && *p.NamespaceScope != targetNS {
        return false
    }

    // 2. Check Target Namespace
    if store.HasRole(p.User.ID, targetNS, perm) {
        return true
    }

    // 3. Check System Namespace (Cascading Admin)
    if store.HasRole(p.User.ID, "_system", perm) {
        return true
    }

    return false
}
```

---

## 9. Implementation Notes

### Bootstrap Procedure

The config file defines the initial state.

```yaml
auth:
  bootstrap:
    admin_username: "admin"
    # If no keys exist for this user, one is generated/logged on startup.
    # This user is automatically bound to "Admin" role in "_system".
```

### Migration Strategy

1. **Phase 1**: Implement storage tables and new data structs.
2. **Phase 2**: Implement AuthBackend with cascading logic.
3. **Phase 3**: Update HTTP middleware to use Anonymous principal instead of hardcoded bypass.
4. **Phase 4**: Enable auth checks in Service layer.

### Testing Strategy

- **Scope Testing**: Verify an API key scoped to ns-A cannot access ns-B even if the user is an Admin.
- **Cascading Testing**: Verify a user with queue.delete in `_system` can delete a queue in ns-production.
- **Anonymous Testing**: Verify accessing /metrics works without a header, but /v1/queue.create fails.

### 9.4 Code-First Role Management (No Wildcards)

To ensure security and predictability, the system does not support wildcard permissions (e.g., `*` or `queue.*`) in the database. Instead, "Standard Roles" (like `NamespaceOwner`) are defined in code and synchronized to the database at startup.

**The Mechanism:**
1.  **Source of Truth**: The codebase defines the exact list of permissions that constitute the `NamespaceOwner` role.
2.  **Startup Synchronization**: On server boot, the system checks the `_system` namespace for these standard roles.
3.  **Automatic Update**: If the role is missing, it is created. If it exists but lacks new permissions (e.g., after a software upgrade introduces `queue.export`), the system automatically appends the new permissions.

This ensures that upgrading the software automatically "upgrades" the capabilities of valid owners without risking accidental privilege escalation via wildcards.

### 9.5 Provisioning Workflow (The Open Door)

The system defaults to an "Open Door" policy to facilitate easy setup and testing (Docker, CI/CD).

#### Initial Bootstrap
1.  On startup, the `Anonymous` user is granted `Admin` privileges.
2.  Users can immediately interact with the API to create their own Users and API Keys.
3.  **Lock Down**: To secure the system:
    - Delete the `RoleBinding` between `Anonymous` and `Admin`.
    - (Optional) Create a `RoleBinding` between `Anonymous` and `PublicViewer` to allow unauthenticated health checks.

#### User Provisioning
1.  **Creation**: An existing Admin (or Anonymous) calls `POST /v1/user.create`.
2.  **Key Generation**: The Admin calls `POST /v1/apikey.create` providing the new `user_id`.
3.  **One-Time Secret**: The API returns the raw API key in the JSON response. This is the **only time** the raw key is visible (the system only stores the SHA-256 hash).
4.  **Distribution**: The Admin is responsible for securely distributing the key to the intended user (e.g., via a secure secret manager or encrypted channel).

#### Key Rotation/Loss
- If a user loses their key, an Admin must delete the old `APIKey` record and create a new one.
- Since keys are not stored in plain text, they cannot be "recovered."

### 9.6 Key Revocation & Self-LockoutPolicy

The system **shall not** prevent a user from deleting their last valid API Key.

- **Security Priority**: Revocation must always be allowed. If a user suspects compromise, they must be able to delete all credentials immediately.
- **Consequence**: Deleting the key currently used for authentication is functionally equivalent to a "Logout". The next request will return `401 Unauthorized`.
- **UI Behavior**: Clients should detect when the active key is being deleted and warn the user: *"Deleting this key will end your current session."*
- **Recovery**:
    - **SSO Users**: Simply log in again to generate a new session key.
    - **Manual Users**: Must request a new key from an Administrator.
    - **Admins**: If an Admin deletes their only key, restarting the server triggers the Bootstrap logic to generate a new one.

### 9.7 Authorization Logic Details

#### The Boolean Logic Gate
Access is determined by the intersection of Key Constraints and User Permissions:
> `Allow = (Key_Scope_Match) AND (User_Has_Permission)`

If a key is scoped to `NS-A` but the request targets `NS-B`, the `Key_Scope_Match` is `FALSE`. The system denies access immediately without checking user roles.

#### Scope Immutability
A scoped API Key is a snapshot of intent. If a user creates a key scoped to `NS-A` and is later granted access to `NS-B`, the existing key **remains restricted** to `NS-A`. To access the new namespace, the user must generate a new key.

---

## 10. Comparison with Database Permissions

This architecture adopts a **Key-Centric** model (similar to AWS IAM or Stripe) rather than a traditional **Database User** model (like PostgreSQL/MySQL).

### 10.1 Pros of Querator Design

1.  **Zero-Downtime Rotation**: Users can hold multiple active API keys. A new key can be distributed and deployed before revoking the old one, preventing service outages during rotation.
2.  **Granular Scoping**: A single User (e.g., "DevOps Team") can generate separate keys for separate environments (Production vs. Staging). A compromised "Staging" key creates no risk to "Production".
3.  **Audit Fidelity**: The system logs *which specific key* performed an action. This distinguishes between different services/workers acting as the same user.
4.  **Client Simplicity**: Standard HTTP `Bearer` tokens are easier to integrate across languages/platforms than database connection strings or handshake protocols.

### 10.2 Cons / Trade-offs

1.  **Implementation Effort**: Authorization logic (`HasPermission`) resides in the application layer, requiring careful testing, whereas DBs handle `GRANT/REVOKE` natively.
2.  **Security Default**: The "Open Door" default (Anonymous=Admin) requires an explicit lock-down step for production environments.

---

## 11. Integration with External Identity Providers (Future)

To support SSO (Okta, Google, OIDC) without compromising the performance of the core queueing loop, the system uses a **Token Exchange Pattern**.

### 11.1 The Challenge
Directly validating third-party JWTs on every high-throughput request (e.g., `publish` / `consume`) is undesirable because:
1.  **Latency**: RSA/ECDSA signature verification is CPU-intensive compared to SHA-256 hashing.
2.  **Complexity**: The core authorization loop would need to handle complex OAuth claims mapping.

### 11.2 The "Token Exchange" Workflow
External auth is used solely to **exchange** a trusted third-party identity for a native Querator API Key.

1.  **User Login**: User authenticates via the IdP (e.g., Okta) and receives an OIDC ID Token (JWT).
2.  **Exchange Request**: Client calls `POST /auth/login-oidc` with the JWT.
3.  **Validation & Mapping**:
    - Querator validates the JWT signature against the IdP's public keys.
    - Querator extracts the identity (e.g., `email`) and finds/provisions the local `User` record.
4.  **Issuance**: Querator generates a **Short-Lived API Key** (e.g., 12-24 hours) and returns it to the client.
5.  **Standard Operation**: The client uses this native API Key for all subsequent requests.

This ensures the internal permission system remains unified and highly performant, treating all requests as standard Key-based access.

