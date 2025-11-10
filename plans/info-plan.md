# Implementation Plan: `/v1/queues.info` Handler

## Overview

This document outlines the implementation plan for the `/v1/queues.info` endpoint, which will provide detailed
information about a specific queue. The endpoint is already documented in the OpenAPI specification but lacks implementation.

## Current Status

- ✅ **OpenAPI Documentation**: Complete
- ✅ **Transport Constants**: `RPCQueuesInfo` defined
- ✅ **Response Type**: `QueueInfo` protobuf message exists
- ❌ **Request Type**: `QueuesInfoRequest` protobuf missing
- ❌ **Service Interface**: Method signature missing
- ❌ **HTTP Handler**: Implementation missing
- ❌ **Service Implementation**: Business logic missing
- ❌ **HTTP Routing**: Switch case missing

## Architecture Overview

The implementation follows the standard 3-layer architecture:

```
HTTP Layer (transport/http.go)
    ↓
Service Layer (service.go)  
    ↓
QueuesManager Layer (internal/queues_manager.go)
```

## Implementation Steps

### Step 1: Define Protobuf Request Type

**File**: `proto/queues.proto`

Add the missing request message:

```protobuf
message QueuesInfoRequest {
  // The name of the queue to retrieve information about
  string queueName = 1 [json_name = "queue_name"];
}
```

**Rationale**: This matches the OpenAPI specification which expects a request body with a `queue_name` field.

### Step 2: Regenerate Protobuf Files

```bash
make proto
```

This will generate the Go structs from the updated protobuf definitions.

### Step 3: Add Service Interface Method

**File**: `transport/http.go` (lines ~85-102)

Add to the `Service` interface:

```go
QueuesInfo(context.Context, *pb.QueuesInfoRequest, *pb.QueueInfo) error
```

**Pattern**: Follows the same signature pattern as `QueuesList` but returns a single `QueueInfo` instead of a list.

### Step 4: Implement HTTP Handler

**File**: `transport/http.go` (after line ~297)

```go
func (h *HTTPHandler) QueuesInfo(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req pb.QueuesInfoRequest
	if err := duh.ReadRequest(r, &req, 256*duh.Kilobyte); err != nil {
		h.ReplyError(w, r, err)
		return
	}

	var resp pb.QueueInfo
	if err := h.service.QueuesInfo(ctx, &req, &resp); err != nil {
		h.ReplyError(w, r, err)
		return
	}
	duh.Reply(w, r, duh.CodeOK, &resp)
}
```

**Size Limit**: Uses 256KB like other queue management endpoints.

### Step 5: Add HTTP Routing

**File**: `transport/http.go` (lines ~149-191)

Add to the switch statement:

```go
case RPCQueuesInfo:
	h.QueuesInfo(ctx, w, r)
	return
```

**Location**: Add between `RPCQueuesDelete` and `RPCStorageItemsList` cases.

### Step 6: Implement Service Method

**File**: `service.go` (after `QueuesDelete` method, line ~348)

```go
func (s *Service) QueuesInfo(ctx context.Context, req *proto.QueuesInfoRequest, resp *proto.QueueInfo) error {
	// Validate queue name
	if strings.TrimSpace(req.QueueName) == "" {
		return transport.NewInvalidOption("'queue_name' cannot be empty")
	}

	// Get queue information from QueuesManager
	queue, err := s.queues.Get(ctx, req.QueueName)
	if err != nil {
		return err
	}

	// Extract queue info and convert to protobuf
	info := queue.Info()
	*resp = *info.ToProto(resp)
	return nil
}
```

**Error Handling**: Leverages existing error handling in `QueuesManager.Get()` which returns appropriate transport errors for queue not found scenarios.

### Step 7: Verify Queue.Info() Method

**File**: `internal/queue.go`

Ensure the `Queue` struct has an `Info()` method that returns `types.QueueInfo`. If missing, add:

```go
func (q *Queue) Info() types.QueueInfo {
	return q.info // or appropriate field access
}
```

**Note**: This may already exist based on the existing `QueuesManager.Get()` usage patterns.

## Data Flow

1. **HTTP Request**: Client sends POST to `/v1/queues.info` with `{"queue_name": "example"}`
2. **HTTP Handler**: Parses request into `QueuesInfoRequest` protobuf
3. **Service Layer**: Validates queue name and calls `QueuesManager.Get()`
4. **QueuesManager**: Retrieves queue from storage and returns `Queue` object
5. **Service Layer**: Extracts `QueueInfo` and converts to protobuf
6. **HTTP Handler**: Returns `QueueInfo` as JSON response

## Error Scenarios

| Scenario | HTTP Status | Error Type | Message |
|----------|-------------|------------|---------|
| Empty queue name | 400 | InvalidOption | "'queue_name' cannot be empty" |
| Queue not found | 404 | NotFound | "queue 'name' not found" |
| Storage error | 500 | InternalError | Storage-specific error |

## Testing Strategy

### Unit Tests

**File**: `service_test.go` (new test cases)

```go
func TestService_QueuesInfo(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		// Test successful queue info retrieval
	})
	
	t.Run("EmptyQueueName", func(t *testing.T) {
		// Test validation error for empty queue name
	})
	
	t.Run("QueueNotFound", func(t *testing.T) {
		// Test queue not found error
	})
}
```

### Integration Tests

**File**: `queue_test.go` (add to existing test suites)

Add test cases to existing queue management test functions to verify the endpoint works end-to-end.

## Dependencies

### Existing Code Leveraged

- **QueuesManager.Get()**: Existing method for queue retrieval
- **QueueInfo.ToProto()**: Existing conversion method
- **Transport Error Handling**: Existing error types and patterns
- **HTTP Handler Patterns**: Existing duh library usage

### New Dependencies

- None - implementation uses existing infrastructure

## Validation

### Request Validation

- **Queue Name**: Must not be empty or whitespace-only
- **Request Size**: Limited to 256KB (standard for queue management endpoints)

### Response Validation

- **QueueInfo Fields**: All fields populated from storage
- **JSON Serialization**: Proper snake_case field names via protobuf json_name tags

## Performance Considerations

- **Single Queue Lookup**: O(1) operation via QueuesManager index
- **No Heavy Computation**: Simple data retrieval and conversion
- **Memory Usage**: Single QueueInfo object allocation per request
- **Concurrency**: Thread-safe via QueuesManager mutex protection

## Rollout Plan

### Phase 1: Implementation
1. Add protobuf definition
2. Implement service and transport layers
3. Add unit tests

### Phase 2: Testing
1. Integration testing with existing test suites
2. API consistency verification
3. Error scenario validation

### Phase 3: Documentation
1. Update API documentation if needed
2. Add usage examples
3. Update client library documentation

## Risk Assessment

**Low Risk Implementation**:
- Uses well-established patterns
- No breaking changes
- Minimal new code surface area
- Leverages existing error handling
- No storage layer changes required

## Success Criteria

- [ ] Endpoint returns correct queue information for existing queues
- [ ] Proper 404 error for non-existent queues
- [ ] Proper 400 error for invalid requests
- [ ] Response format matches OpenAPI specification
- [ ] Integration tests pass
- [ ] Performance meets existing queue management endpoint standards