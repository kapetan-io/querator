# Technical Specification: Multi-Tenancy, User/Roles, and API Key Management

## Review Status
- Review Cycles Completed: 1
- Final Approval: **Approved** (2025-12-31)
- Outstanding Concerns: None

---

## 1. Overview

This specification defines a comprehensive multi-tenancy system for Querator that includes:
- **Namespaces** for tenant isolation (hard isolation, NATS-style)
- **Users and Roles** with flexible permission models (custom roles only, no wildcards)
- **API Keys** as user credentials (unified model - API keys are just users)
- **Auth Interface** supporting built-in and external identity providers (auth callout pattern)

The design supports both **SaaS multi-tenancy** (untrusted tenants, strict isolation) and **internal team isolation** (teams within an organization).

### Key Design Decisions (from review)
| Decision | Resolution |
|----------|------------|
| Bootstrap method | Config file; allow non-auth operations if no admin configured |
| Queue name format | Namespace resolved from API key context; queue names remain simple |
| Namespace structure | Flat namespaces with configurable default |
| Wildcards | No wildcards in permissions - explicit permissions only. Namespace wildcard supported in RoleBindings. |
| Super admin | Global Role Binding (RBAC), not a user attribute |
| API key model | Unified with users - API keys are just users without passwords |
| Cluster sync | Same patterns as queue metadata (ADR-0017) |
| Public endpoints | `/health` and `/metrics` bypass auth |
| Key rotation | Immediate replacement (no overlap period) |
| OIDC support | Interface only in this phase; implementation deferred |
| Audit logging | Via existing slog.Logger |

---

## 2. Current State Analysis

### Affected Components
- `transport/http.go` - HTTP handler (add auth middleware)
- `service/service.go` - Service layer (add tenant context, permission checks)
- `internal/queues_manager.go` - Queue management (namespace-scoped operations)
- `internal/store/store.go` - Storage interface (add auth/namespace storage)
- `client.go` - Client (add API key support)

### Current Behavior
- No authentication or authorization
- Queues identified by flat string names
- Context flows through system but carries no identity
- All API endpoints are open

### Relevant ADRs
- **ADR-0002 (Design Goals)**: Simplicity and efficiency first - auth system must not add significant complexity to client implementations
- **ADR-0007 (Encourage Simple Clients)**: Complexity on server, simple clients - auth should be transparent to basic operations
- **ADR-0017 (Cluster Operation)**: Leader-based architecture - auth metadata needs coordination in cluster mode

### Technical Debt Identified
- No abstraction layer for metadata storage
- HTTP handler has no middleware chain

---

## 3. Architectural Context

### Design Principles from ADRs

1. **Simplicity for Clients** (ADR-0007): Authentication should be as simple as adding an API key header
2. **Efficiency First** (ADR-0002): Permission checks must be fast and cached
3. **Disaggregated Storage** (Architecture): Auth metadata can use the same storage backends as queues

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Hard namespace isolation | Matches NATS account model; prevents accidental cross-tenant access |
| Custom roles only | Maximum flexibility; no assumptions about permission structure |
| Auth callout interface | Enables external IdP integration without coupling |
| Flat namespace naming | Simpler than hierarchical; sufficient for most use cases |

---

## 4. Requirements

### 4.1 Functional Requirements

#### Namespace Management
- **NS-001**: System shall support creating flat-named namespaces (e.g., `production`, `team-payments`)
- **NS-002**: Queues shall belong to exactly one namespace
- **NS-003**: Namespace names shall be unique cluster-wide
- **NS-004**: Namespaces shall be completely isolated - queues invisible across namespaces
- **NS-005**: Deleting a namespace shall require all queues to be deleted first (or cascade delete option)
- **NS-006**: A configurable default namespace shall exist for users with no explicit namespace
- **NS-007**: Namespace shall be resolved from API key/user context, NOT from queue name (users never see namespace prefix)

#### User Management
- **USER-001**: Users shall be identifiable by unique username or external ID
- **USER-002**: Users shall be able to have roles in multiple namespaces
- **USER-003**: A user's role in namespace A is independent of their role in namespace B
- **USER-004**: Users shall be able to be bound to roles globally (across all namespaces) using a wildcard identifier
- **USER-005**: Users bound to roles within a specific namespace shall only manage resources in that namespace

#### Role Management
- **ROLE-001**: Roles shall be custom-defined (no built-in roles)
- **ROLE-002**: Roles shall contain a list of permissions
- **ROLE-003**: Permissions shall be simple operation identifiers (no wildcards)
- **ROLE-004**: Roles shall be namespace-scoped (defined within a namespace)

#### API Key Management (Unified with Users)
- **KEY-001**: API keys are credentials for users (not separate principals)
- **KEY-002**: Users may have zero or more API keys
- **KEY-003**: API keys shall support optional expiration
- **KEY-004**: API key rotation shall be immediate (old key invalid immediately)
- **KEY-005**: API keys shall have metadata (name, description, created_at, last_used)
- **KEY-006**: API keys shall support revocation

#### Authentication
- **AUTH-001**: System shall authenticate requests via API key in HTTP header
- **AUTH-002**: System shall support an auth callout interface for external providers
- **AUTH-003**: Built-in auth backend shall store users/keys in queue storage backend
- **AUTH-004**: Auth callout implementations shall receive credentials and return permissions
- **AUTH-005**: `/health` and `/metrics` endpoints shall bypass authentication
- **AUTH-006**: If no admin credentials are configured, non-auth operations shall assume admin role (development mode)
- **AUTH-007**: Auth events (logins, failures, permission denials) shall be logged via slog

#### Authorization
- **AUTHZ-001**: All queue operations shall check user permissions
- **AUTHZ-002**: Permission checks shall be namespace-scoped (except for global bindings)
- **AUTHZ-003**: Global role bindings shall grant permissions across all namespaces
- **AUTHZ-004**: Failed authorization shall return HTTP 403 Forbidden

### 4.2 Non-Functional Requirements

#### Performance
- **PERF-001**: Permission checks shall add < 1ms latency to requests
- **PERF-002**: Auth metadata shall be cached to avoid storage lookups per request
- **PERF-003**: Cache invalidation shall occur within 5 seconds of changes

#### Security
- **SEC-001**: API keys shall be stored as secure hashes (SHA-256 or bcrypt)
- **SEC-002**: API keys shall never be logged in full
- **SEC-003**: Failed auth attempts shall be rate-limited
- **SEC-004**: Auth callout payloads shall support encryption

#### Scalability
- **SCALE-001**: Auth system shall scale with number of namespaces (target: 10,000+)
- **SCALE-002**: Auth system shall scale with number of users (target: 100,000+)
- **SCALE-003**: Auth metadata shall be partitionable in cluster mode

---

## 5. Technical Approach

### 5.1 Chosen Solution: Layered Auth with Callout Interface

The design separates concerns into distinct layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                         HTTP Layer                              │
│  - Extract API key from Authorization header                    │
│  - Call Auth Backend to validate and get identity               │
│  - Attach identity to request context                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Auth Backend Interface                    │
│  Authenticate(ctx, credentials) → (Principal, error)            │
│  GetPermissions(ctx, principal, namespace) → ([]Permission)     │
│  ValidateAccess(ctx, principal, namespace, permission) → bool   │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
      ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
      │  Built-in    │ │    OIDC      │ │  Callout     │
      │  Backend     │ │  Validator   │ │  Backend     │
      └──────────────┘ └──────────────┘ └──────────────┘
              │
              ▼
      ┌──────────────────────────────────────────────────────────┐
      │                    Auth Storage Interface                 │
      │  (Uses same storage backend as queues)                    │
      │  - NamespaceStore                                         │
      │  - UserStore                                              │
      │  - RoleStore                                              │
      │  - APIKeyStore                                            │
      └──────────────────────────────────────────────────────────┘
```

### 5.2 Rationale

| Alternative | Pros | Cons | Why Not Chosen |
|-------------|------|------|----------------|
| Hardcoded roles | Simpler | Inflexible, doesn't fit all use cases | Contradicts "custom roles only" requirement |
| Per-queue ACLs | Fine-grained | Complex to manage at scale | Too granular for initial implementation |
| External-only auth | Fewer moving parts | Can't work standalone | Need built-in for simple deployments |

### 5.3 ADR Alignment

- **ADR-0002**: Auth adds minimal client complexity (single header)
- **ADR-0007**: Complex permission logic stays server-side
- **ADR-0017**: Auth metadata follows same storage/sync patterns as queue metadata

### 5.4 Component Changes

#### Transport Layer (`transport/`)
- Add `AuthMiddleware` before request routing
- Extract API key from `Authorization: Bearer <key>` header
- Inject authenticated principal into context
- Return 401 for missing/invalid credentials
- Return 403 for insufficient permissions

#### Service Layer (`service/`)
- Extract principal from context in each method
- Resolve namespace from queue name or request
- Check permissions before operations
- Pass namespace context to QueuesManager

#### QueuesManager (`internal/`)
- Add namespace parameter to queue operations
- Namespace-prefix queue names in storage
- Validate namespace existence on queue create

#### Storage Layer (`internal/store/`)
- Add `AuthStore` interface for auth metadata
- Implement for each backend (InMemory, BadgerDB, PostgreSQL)
- Tables/collections: namespaces, users, roles, role_bindings, api_keys

#### Client (`client.go`)
- Add `APIKey` field to `ClientConfig`
- Attach `Authorization` header to all requests

---

## 6. Data Model

### 6.1 Namespace
```
Namespace {
    Name        string    // unique identifier, e.g., "production"
    Description string    // human-readable description
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

### 6.2 User
```
User {
    ID              string    // unique identifier
    Username        string    // human-readable name
    ExternalID      string    // optional, for external IdP mapping
    Email           string    // optional
    DefaultNamespace string   // user's default namespace for queue operations
    CreatedAt       time.Time
    UpdatedAt       time.Time
}
```

### 6.3 Role
```
Role {
    ID          string       // unique within namespace
    Namespace   string       // owning namespace
    Name        string       // human-readable name
    Permissions []string     // list of permission identifiers (no wildcards)
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

### 6.4 RoleBinding
```
RoleBinding {
    ID          string    // unique identifier
    Namespace   string    // namespace scope (use "*" for global binding)
    UserID      string    // user being granted the role
    RoleID      string    // role being granted
    CreatedAt   time.Time
}
```

### 6.5 APIKey (User Credential)
```
APIKey {
    ID          string     // unique identifier
    UserID      string     // owning user (API keys always belong to users)
    Name        string     // human-readable name
    KeyHash     string     // SHA-256 hash of actual key
    KeyPrefix   string     // first 8 chars for identification (e.g., "qk_a1b2c3")
    ExpiresAt   *time.Time // optional expiration
    LastUsedAt  *time.Time
    CreatedAt   time.Time
}
```

**API Key Format**: `qk_<32-char-hex>` (e.g., `qk_a1b2c3d4e5f67890123456789abcdef0`)
- Prefix `qk_` identifies Querator keys in logs and secret scanning
- 32 hex characters = 128-bit entropy
- Total length: 35 characters

**Constraints**:
- Maximum 5 API keys per user (hard limit)
- Keys are stored as SHA-256 hashes (never plaintext)
- Only the first 8 characters (`KeyPrefix`) are stored for identification

Note: API keys are simply credentials for users. A user authenticating via API key
has the same permissions as if they authenticated by other means. There is no
distinction between "service keys" and "user keys" - all keys belong to users.

### 6.6 Permission Identifiers

```
// Namespace operations
namespace.create
namespace.delete
namespace.update
namespace.info
namespace.list

// Queue operations
queue.create
queue.delete
queue.update
queue.info
queue.list
queue.produce
queue.lease
queue.complete
queue.retry
queue.stats
queue.clear
queue.reload

// Storage operations (admin)
storage.list
storage.import
storage.delete

// User/Role management (admin)
user.create
user.delete
user.update
user.list
role.create
role.delete
role.update
role.list
role.bind
role.unbind
apikey.create
apikey.delete
apikey.list
```

---

## 7. API Design

### 7.1 Authentication Header
```
Authorization: Bearer <api_key>
```

### 7.2 New Endpoints

#### Namespace Management
```
POST /v1/namespace.create   { name, description }
POST /v1/namespace.delete   { name }
POST /v1/namespace.update   { name, description }
POST /v1/namespace.info     { name } → NamespaceInfo
POST /v1/namespace.list     { } → []NamespaceInfo
```

#### User Management
```
POST /v1/user.create   { username, email }
POST /v1/user.delete   { id }
POST /v1/user.update   { id, username, email }
POST /v1/user.info     { id } → UserInfo
POST /v1/user.list     { namespace? } → []UserInfo
```

#### Role Management
```
POST /v1/role.create   { namespace, name, permissions }
POST /v1/role.delete   { namespace, id }
POST /v1/role.update   { namespace, id, name, permissions }
POST /v1/role.info     { namespace, id } → RoleInfo
POST /v1/role.list     { namespace } → []RoleInfo
POST /v1/role.bind     { namespace, role_id, subject_type, subject_id }
POST /v1/role.unbind   { namespace, role_id, subject_type, subject_id }
```

#### API Key Management
```
POST /v1/apikey.create   { user_id, name, expires_at? } → { id, key }
POST /v1/apikey.delete   { id }
POST /v1/apikey.rotate   { id } → { id, key }  // Old key immediately invalidated
POST /v1/apikey.info     { id } → APIKeyInfo
POST /v1/apikey.list     { user_id } → []APIKeyInfo
```

### 7.3 Modified Existing Endpoints

All queue endpoints now require:
1. Valid API key in Authorization header
2. Permission check for the operation
3. Namespace resolved from authenticated user's context (NOT from queue name)

**Important**: Queue names remain simple (e.g., `orders-queue`), NOT prefixed with namespace.
The namespace is determined by the authenticated user's default namespace or explicit
namespace parameter in the request.

### 7.4 Public Endpoints (No Auth Required)
```
GET /health   → { status: "ok" }
GET /metrics  → Prometheus metrics
```

---

## 8. Auth Backend Interface

### 8.1 Core Interface

```go
type AuthBackend interface {
    // Authenticate validates credentials and returns the authenticated user
    Authenticate(ctx context.Context, credentials Credentials) (User, error)

    // GetPermissions returns all permissions for a user in a namespace
    GetPermissions(ctx context.Context, user User, namespace string) ([]string, error)

    // HasPermission checks if user has specific permission in namespace
    HasPermission(ctx context.Context, user User, namespace string, permission string) (bool, error)
}

type Credentials struct {
    Type   string // "apikey", "jwt" (future)
    Token  string
}
```

Note: Since API keys belong to users, authentication always returns a User.
The `Credentials.Type` indicates how the user authenticated, but the result
is always a user identity.

### 8.2 Built-in Implementation

Uses queue storage backend for auth metadata:
- Validates API keys against stored hashes
- Resolves role bindings for permission checks
- Caches results with configurable TTL
- Logs auth events via slog

### 8.3 Auth Callout Implementation

Delegates to external service:
```go
type CalloutConfig struct {
    Endpoint    string        // HTTP endpoint for auth callout
    Timeout     time.Duration
    EncryptKey  string        // Optional encryption for payloads
}
```

Request to external service:
```json
{
    "credentials": { "type": "apikey", "token": "..." },
    "namespace": "production",
    "operation": "queue.produce",
    "resource": "orders-queue"
}
```

Response from external service:
```json
{
    "allowed": true,
    "user": {
        "id": "user-123",
        "username": "service-a",
        "default_namespace": "production"
    },
    "permissions": ["queue.produce", "queue.lease", "queue.complete"]
}
```

### 8.4 OIDC/JWT Validator (Interface Only)

This phase defines the interface for OIDC token validation. Implementation is deferred.

```go
type OIDCConfig struct {
    IssuerURL     string   // OIDC discovery endpoint
    Audience      string   // Expected audience claim
    UsernameClaim string   // Claim to use as username (e.g., "sub", "email")
    ScopeMapping  map[string][]string // Map OIDC scopes to Querator permissions
}
```

The OIDC backend would validate JWT tokens and map claims to User/permissions.

---

## 9. Dependencies and Impacts

### External Dependencies
- None required for built-in auth
- Optional: HTTP client for auth callout
- Future: JWT libraries for OIDC validation

### Internal Dependencies
- Storage backend for auth metadata
- Context propagation through service layer
- Cache layer for performance

### Database Impacts
New tables/collections required:
- `namespaces`
- `users`
- `roles`
- `role_bindings`
- `api_keys`

Queue name format:
- **No change to queue names** - they remain simple strings (e.g., `orders-queue`)
- Namespace is resolved from authenticated user's context
- Storage internally prefixes with namespace for isolation

---

## 10. Backward Compatibility

### Is This Project in Production?
- [ ] Yes - Must maintain backward compatibility
- [x] No - Breaking changes are permitted

### Breaking Changes
- All endpoints (except /health, /metrics) require authentication header when auth is configured
- New storage tables required for auth metadata
- Queue names remain unchanged (no namespace prefix in API)

### Migration Strategy
1. **Development Mode**: If no admin credentials in config, auth is disabled and all operations assume admin role
2. **First Configuration**: Admin credentials defined in config file; first user created from config
3. **Default Namespace**: Create "default" namespace on first startup for existing queues
4. **Gradual Rollout**: Can run without auth initially, then configure admin credentials to enable

### Bootstrap Procedure
```yaml
# Example config file
auth:
  admin:
    username: admin
    # API key generated and logged on first startup if not specified
  default_namespace: default
```

If `auth.admin` is not configured, the system operates in development mode where all
requests are treated as admin requests. This is detected and logged as a warning.

---

## 11. Testing Strategy

### Unit Testing
- Auth backend interface implementations
- Permission checking logic
- API key hashing and validation
- Role binding resolution

### Integration Testing
- Full auth flow through HTTP layer
- Multi-namespace access patterns
- Auth callout with mock external service
- Cache invalidation scenarios

### Security Testing
- Invalid API key rejection
- Permission boundary enforcement
- Rate limiting on failed auth
- No key leakage in logs

---

## 12. Implementation Notes

### Estimated Complexity
- **High** - Touches all layers of the system, introduces new concepts

### Suggested Implementation Order
1. Auth storage interface and built-in implementation
2. Auth backend interface and built-in implementation
3. HTTP middleware for auth
4. Service layer permission checks
5. Management API endpoints (namespace, user, role, apikey)
6. Client library updates
7. Auth callout implementation

### Code Style Considerations
- Follow existing patterns in `internal/store/`
- Use interfaces for all auth components
- Permission strings as constants
- Comprehensive error types for auth failures

### Rollback Strategy
- Config flag to disable auth enforcement
- No data migration needed for rollback
- Auth tables can remain unused

---

## 13. ADR Recommendation

This change warrants a new ADR:
- **ADR-0025: Multi-Tenancy and Authentication Architecture**
- Documents the namespace isolation model
- Documents the auth callout pattern decision
- Documents permission model design

---

## 14. Resolved Questions (from Review)

| Question | Resolution |
|----------|------------|
| Queue names and namespaces | Namespace from user context; queue names remain simple |
| Deleted users | Defer decision - can be addressed in implementation |
| API key lifetime | Unlimited by default; optional expiration supported |
| System namespace | Reserved namespace `system` created for global role definitions |
| Cluster auth replication | Same patterns as queue metadata (ADR-0017) |
| Service keys vs user keys | Unified model - all API keys belong to users |
| Wildcard permissions | Not supported - explicit permissions only |
| OIDC implementation | Deferred - interface only in this phase |

### Final Decisions (from review)

| Question | Decision |
|----------|----------|
| API key format | `qk_<32-char-hex>` (e.g., `qk_a1b2c3d4e5f67890123456789abcdef0`) |
| Max API keys per user | Hard limit of 5 keys per user |
| Role binding expiration | No expiration - bindings are permanent until removed |

---

## 15. Research Sources

### Multi-Tenancy Patterns
- [Supabase Row Level Security](https://supabase.com/docs/guides/database/postgres/row-level-security)
- [Kubernetes RBAC Authorization](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)
- [NATS Multi-Tenancy Using Accounts](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/accounts)
- [RabbitMQ Virtual Hosts](https://www.rabbitmq.com/docs/vhosts)
- [Kafka Multi-Tenancy](https://kafka.apache.org/41/operations/multi-tenancy/)
- [Redis ACL System](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/)

### API Key Management
- [Supabase API Keys](https://supabase.com/docs/guides/api/api-keys)
- [Kubernetes ServiceAccount Tokens](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)

### Identity Integration
- [SCIM Protocol Overview](https://www.descope.com/learn/post/scim)
- [RabbitMQ OAuth 2.0 Integration](https://www.rabbitmq.com/docs/oauth2)
- [NATS Auth Callout](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_callout)
- [OIDC Best Practices 2025](https://www.infisign.ai/blog/what-is-openid-connect-oidc)
