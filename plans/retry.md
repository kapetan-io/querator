# Implementation Plan: /queue.retry Endpoint

## Overview

Implement the missing `/v1/queue.retry` endpoint that allows consumers to retry leased items at a future
time or mark them as dead for placement in the dead letter queue. This endpoint is currently defined in
the protocol buffers and OpenAPI specification but lacks implementation.

## Background

Based on ADR 0024-scheduled-items.md, retry functionality is closely tied to the scheduled items system.
When items are retried with a future `retry_at` timestamp, they are placed into the scheduled queue of
the same partition and later moved to the main queue when the scheduled time arrives.

### Key Concepts:
- **Retry vs Complete**: Unlike `/queue.complete` which removes items, retry reschedules them
- **Partition Affinity**: Retried items remain in their original partition for lifecycle
- **Scheduled Integration**: Future retries use the scheduled items infrastructure
- **Dead Letter Queue**: Items marked as `dead: true` go directly to the dead letter queue
- **Attempt Counter**: Each retry increments the attempt counter

## Current State Analysis

### ✅ Already Implemented:
- Protocol definition (`proto/queue.proto:140-197`)
- OpenAPI specification documenting the endpoint
- HTTP route constant (`RPCQueueRetry = "/v1/queue.retry"`)
- Scheduled items infrastructure (`ActionQueueScheduledItem`)
- Storage interface support (`ScanForScheduled`, `TakeAction`)

### ❌ Missing Implementation:
- HTTP handler implementation
- Service method (`QueueRetry`)
- Client method (`QueueRetry`)
- Request validation logic
- Storage retry logic integration

## Implementation Plan

### Phase 1: Core Service Implementation

#### 1.1 Add Service Method Interface
**File**: `service.go`
- Add `QueueRetry(ctx context.Context, req *proto.QueueRetryRequest) error` method signature
- Follow existing patterns from `QueueComplete` and `QueueProduce`

#### 1.2 Implement Service Method
**File**: `service.go`
- **Input Validation**:
  - Check `retry_at` timestamps (past dates become immediate)
  - Validate request timeout
- **Business Logic**:
  - For `dead: true` items: Move directly to dead letter queue
  - For scheduled retries: Add to scheduled queue with `retry_at` timestamp
  - For immediate retries (`retry_at` empty/past): Re-enqueue immediately
  - Increment attempt counter for all retried items
  - Respect `max_attempts` limit (dead letter if exceeded)
- **Error Handling**:
  - Return appropriate error codes for validation failures
  - Handle storage errors gracefully
  - Maintain transactional semantics where possible

### Phase 2: HTTP Transport Layer

#### 2.1 Implement HTTP Handler
**File**: `transport/http.go`
- Add `QueueRetry(ctx context.Context, w http.ResponseWriter, r *http.Request)` method
- Follow existing handler patterns from `QueueComplete`
- **Request Processing**:
  - Unmarshal `QueueRetryRequest` from request body
  - Call service method
  - Return appropriate HTTP status codes
  - Handle timeout and validation errors

#### 2.2 Wire HTTP Route
**File**: `transport/http.go` (line ~157)
- Fix the missing handler implementation in the switch statement:
```go
case RPCQueueRetry:
    h.QueueRetry(ctx, w, r)
    return
```

### Phase 3: Client Implementation

#### 3.1 Add Client Method
**File**: `client.go`
- Add `QueueRetry(ctx context.Context, req *pb.QueueRetryRequest) error` method
- Follow existing patterns from `QueueComplete`
- Use protobuf marshaling and HTTP POST to `/v1/queue.retry`

### Phase 4: Storage Integration

#### 4.1 Enhance Request Types
**File**: `internal/types/requests.go`
- Add `RetryRequest` type for internal processing
- Include fields for item ID, retry timestamp, dead flag
- Add validation methods

#### 4.2 Storage Implementation Updates
**Files**: `internal/store/memory.go`, `internal/store/badger.go`

**For Memory Storage:**
- Implement retry logic in a new method called `Retry` similar to `Complete` method
- Handle scheduled retry placement
- Manage dead letter queue transfers

**For Badger Storage:**
- Similar implementation following memory storage patterns
- Leverage existing scheduled items infrastructure

#### 4.3 Logical Queue Integration
**File**: `internal/logical.go`
- Update request processing to handle retry requests
- Ensure proper batch processing for retry operations
- Integrate with existing lifecycle and scheduled item systems

### Phase 5: Testing and Validation

#### 5.1 Unit Tests
- Test retry with immediate re-queuing
- Test retry with future scheduling
- Test dead letter queue functionality
- Test attempt counter incrementation
- Test validation error cases
- Test timeout scenarios

#### 5.2 Integration Tests
- End-to-end retry workflow testing
- Multi-partition retry scenarios
- Scheduled retry timing verification
- Dead letter queue integration

#### 5.3 Performance Testing
- Batch retry performance
- Memory usage with large retry batches
- Scheduled item handling efficiency

## Technical Details

### Validation
Validation of requests occurs at different packages or structs depending on who owns that parameter or data. Until the 
parameter or field is needed it is considered opaque.

For instance
- Validation of IDs occurs in the `store` package because the store owns the `id`, to all the other structs or packages
which pass along the request `id` is opaque. see `MemoryPartition.Complete()` for example.
- Validation of RequestTimeout occurs in `Service` because it has to convert from string to time.Duration.
The rest of the code requires RequestTimeout to be a valid time.Duration

### Retry Semantics
- **Immediate Retry**: Empty/past `retry_at` → increment attempts, re-enqueue
- **Scheduled Retry**: Future `retry_at` → place in scheduled queue  
- **Dead Letter**: `dead: true` → move to dead letter queue
- **Max Attempts**: Check against queue's `max_attempts` setting

### Integration Points
- **Scheduled Items**: Use existing `ActionQueueScheduledItem` infrastructure
- **Lifecycle**: Integrate with `TakeAction` for scheduled→queued transitions
- **Dead Letter**: Use existing dead letter queue mechanisms
- **Partitions**: Maintain partition affinity per ADR 0024

### Error Handling
- **Timeout**: Return 408, allow client retry

## Files to Modify

1. **`service.go`** - Add `QueueRetry` service method
2. **`transport/http.go`** - Add handler and fix routing
3. **`client.go`** - Add client method
4. **`internal/types/requests.go`** - Add retry request types
5. **`internal/logical.go`** - Update request processing
6. **`internal/store/memory.go`** - Implement retry storage logic
7. **`internal/store/badger.go`** - Implement retry storage logic

## Testing Strategy

- **Functional Tests**: Test full retry workflows and scheduled integration
- **Error Tests**: Comprehensive error scenario coverage

## Dependencies

- Existing scheduled items infrastructure (already implemented)
- Dead letter queue functionality (already implemented)  
- Lease tracking system (already implemented)
- Partition management (already implemented)

## Success Criteria

1. `/v1/queue.retry` endpoint fully functional
2. Immediate and scheduled retries working correctly
3. Dead letter queue integration operational
4. Attempt counter incrementation accurate
5. All validation rules enforced
6. Client SDK method available
7. Comprehensive test coverage

This implementation will complete the retry functionality described in the OpenAPI specification and integrate seamlessly with the existing scheduled items infrastructure outlined in ADR 0024.