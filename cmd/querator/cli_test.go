package main_test

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kapetan-io/querator/cmd/querator"
)

// TestProduceCommand tests the produce command with mock server
func TestProduceCommand(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	// Clear any previous requests
	ms.ClearRequests()

	testFlags := main.FlagParams{
		Payload:  "test message payload",
		Endpoint: ms.URL(),
		Timeout:  "30s",
	}

	// Test produce command
	_, stderr, err := captureOutput(func() error {
		return main.RunProduce(testFlags, "test-queue")
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queue.produce", req.Path)

	// Check that request body is not empty (contains protobuf data)
	assert.NotEmpty(t, req.Body)

	// Produce command doesn't print success messages, just verify no error occurred
	assert.Empty(t, stderr)
}

// TestLeaseCommand tests the lease command with mock server
func TestLeaseCommand(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Test lease command with default flags
	testFlags := main.FlagParams{
		ClientId:     "test-client",
		Endpoint:     ms.URL(),
		LeaseTimeout: "30s",
		BatchSize:    1,
	}

	stdout, _, err := captureOutput(func() error {
		return main.RunLease(testFlags, "test-queue")
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queue.lease", req.Path)

	// Check that request body is not empty (contains protobuf data)
	assert.NotEmpty(t, req.Body)

	// Verify JSON output format
	assert.Contains(t, stdout, "{")
}

// TestCompleteCommand tests the complete command with mock server
func TestCompleteCommand(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Test complete command with item IDs
	testFlags := main.FlagParams{
		CompleteTimeout: "30s",
		Endpoint:        ms.URL(),
	}

	_, stderr, err := captureOutput(func() error {
		return main.RunComplete(testFlags, "test-queue", 0, []string{"item1", "item2"})
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queue.complete", req.Path)

	// Check that request body is not empty (contains protobuf data)
	assert.NotEmpty(t, req.Body)

	// Verify success message was printed
	assert.Contains(t, stderr, "Successfully completed 2 item")
}

// TestCreateCommand tests the create command with mock server
func TestCreateCommand(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Test create command with defaults
	testFlags := main.FlagParams{
		Endpoint:           ms.URL(),
		LeaseTimeoutCreate: "1m",
		Partitions:         1,
	}

	_, stderr, err := captureOutput(func() error {
		return main.RunCreate(testFlags, "test-queue")
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queues.create", req.Path)

	// Check that request body is not empty (contains protobuf data)
	assert.NotEmpty(t, req.Body)

	// Verify success message was printed
	assert.Contains(t, stderr, "Successfully created queue 'test-queue'")
}

// TestListCommand tests the list command with mock server
func TestListCommand(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Test list command
	testFlags := main.FlagParams{
		Endpoint: ms.URL(),
		Limit:    100,
	}

	stdout, _, err := captureOutput(func() error {
		return main.RunList(testFlags)
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queues.list", req.Path)

	// Verify JSON output format
	assert.Contains(t, stdout, "test-queue-1")
}

// TestUpdateCommand tests the update command with mock server
func TestUpdateCommand(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	testFlags := main.FlagParams{
		Endpoint:           ms.URL(),
		LeaseTimeoutUpdate: "2m",
	}

	// Test update command
	_, stderr, err := captureOutput(func() error {
		return main.RunUpdate(testFlags, "test-queue")
	})

	require.NoError(t, err)

	// Verify both info and update requests were captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 2)

	// First request should be queues.info to get current state
	assert.Equal(t, "/v1/queues.info", reqs[0].Path)

	// Second request should be queues.update with changes
	assert.Equal(t, "/v1/queues.update", reqs[1].Path)

	// Check that update request body is not empty (contains protobuf data)
	assert.NotEmpty(t, reqs[1].Body)

	// Verify success message was printed
	assert.Contains(t, stderr, "Successfully updated queue 'test-queue'")
}

// TestDeleteCommand tests the delete command with mock server
func TestDeleteCommand(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Test delete command with force flag
	testFlags := main.FlagParams{
		Endpoint: ms.URL(),
		Force:    true,
	}

	_, stderr, err := captureOutput(func() error {
		return main.RunDelete(testFlags, "test-queue")
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queues.delete", req.Path)

	// Check that request body is not empty (contains protobuf data)
	assert.NotEmpty(t, req.Body)

	// Verify success message was printed
	assert.Contains(t, stderr, "Successfully deleted queue 'test-queue'")
}

// TestProduceCommandWithFlags tests the produce command with various flags
func TestProduceCommandWithFlags(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	testFlags := main.FlagParams{
		Payload:   "test payload with metadata",
		Reference: "ref-123",
		Encoding:  "json",
		Kind:      "task",
		Timeout:   "30s",
		Endpoint:  ms.URL(),
	}

	// Test produce command with flags
	_, stderr, err := captureOutput(func() error {
		return main.RunProduce(testFlags, "test-queue")
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queue.produce", req.Path)

	// Check that request body is not empty (contains protobuf data with metadata)
	assert.NotEmpty(t, req.Body)

	// Should complete without error
	assert.Empty(t, stderr)
}

// TestLeaseCommandWithFlags tests the lease command with various flags
func TestLeaseCommandWithFlags(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Test lease command with flags
	testFlags := main.FlagParams{
		Endpoint:     ms.URL(),
		BatchSize:    5,
		ClientId:     "test-client-123",
		LeaseTimeout: "45s",
	}

	stdout, _, err := captureOutput(func() error {
		return main.RunLease(testFlags, "test-queue")
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queue.lease", req.Path)

	// Check that request body is not empty (contains protobuf data with custom flags)
	assert.NotEmpty(t, req.Body)

	// Verify JSON output format
	assert.Contains(t, stdout, "{")
}

// TestCreateCommandWithAllFlags tests the create command with all available flags
func TestCreateCommandWithAllFlags(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Test create command with all flags
	testFlags := main.FlagParams{
		LeaseTimeoutCreate: "5m",
		ExpireTimeout:      "2h",
		MaxAttempts:        5,
		DeadQueue:          "dead-letter-queue",
		ReferenceCreate:    "test-ref-456",
		Partitions:         3,
		Endpoint:           ms.URL(),
	}

	_, stderr, err := captureOutput(func() error {
		return main.RunCreate(testFlags, "comprehensive-test-queue")
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queues.create", req.Path)

	// Check that request body is not empty (contains protobuf data with all configuration)
	assert.NotEmpty(t, req.Body)

	// Verify success message was printed with correct queue name
	assert.Contains(t, stderr, "Successfully created queue 'comprehensive-test-queue'")
}

// TestCompleteCommandWithFile tests the complete command with file input
func TestCompleteCommandWithFile(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Create a temporary file with item IDs
	tempFile, err := os.CreateTemp("", "item-ids-*.txt")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tempFile.Name()) }()

	// Write item IDs to file
	itemIds := "item-1\nitem-2\nitem-3\n"
	_, _ = tempFile.WriteString(itemIds)
	_ = tempFile.Close()

	// Test complete command with file input
	testFlags := main.FlagParams{
		File:            tempFile.Name(),
		CompleteTimeout: "30s",
		Endpoint:        ms.URL(),
	}

	_, stderr, err := captureOutput(func() error {
		return main.RunComplete(testFlags, "test-queue", 0, []string{})
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queue.complete", req.Path)

	// Check that request body is not empty (contains protobuf data)
	assert.NotEmpty(t, req.Body)

	// Verify success message for 3 items from file
	assert.Contains(t, stderr, "Successfully completed 3 item")
}

// TestListCommandWithPagination tests the list command with pagination flags
func TestListCommandWithPagination(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Test list command with pagination
	testFlags := main.FlagParams{
		Pivot:    "test-queue-5",
		Endpoint: ms.URL(),
		Limit:    10,
	}

	stdout, _, err := captureOutput(func() error {
		return main.RunList(testFlags)
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "POST", req.Method)
	assert.Equal(t, "/v1/queues.list", req.Path)

	// Check that request body is not empty (contains protobuf data with pagination)
	assert.NotEmpty(t, req.Body)

	// Verify JSON output format
	assert.Contains(t, stdout, "test-queue-1")
}

// TestMockServerCapturesMultipleRequests tests that mock server properly captures multiple sequential requests
func TestMockServerCapturesMultipleRequests(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Execute multiple commands
	_, _, err1 := captureOutput(func() error {
		return main.RunProduce(
			main.FlagParams{
				Payload:  "test payload",
				Endpoint: ms.URL(),
				Timeout:  "30s",
			}, "queue-1")
	})

	_, _, err2 := captureOutput(func() error {
		return main.RunList(main.FlagParams{
			Endpoint: ms.URL(),
			Limit:    100,
		})
	})

	testFlags3 := main.FlagParams{
		Endpoint: ms.URL(),
		Force:    true,
	}
	_, _, err3 := captureOutput(func() error {
		return main.RunDelete(testFlags3, "queue-1")
	})

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NoError(t, err3)

	// Verify all requests were captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 3)

	// Verify request paths
	expectedPaths := []string{"/v1/queue.produce", "/v1/queues.list", "/v1/queues.delete"}
	for i, req := range reqs {
		assert.Equal(t, expectedPaths[i], req.Path, "request %d path", i+1)
		assert.Equal(t, "POST", req.Method, "request %d method", i+1)
	}
}

// TestMockServerClearRequests tests the request clearing functionality
func TestMockServerClearRequests(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	// Execute a command
	testFlags := main.FlagParams{
		Payload:  "test payload",
		Endpoint: ms.URL(),
		Timeout:  "30s",
	}
	_, _, err := captureOutput(func() error {
		return main.RunProduce(testFlags, "test-queue")
	})

	require.NoError(t, err)

	// Verify request was captured
	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)

	// Clear requests
	ms.ClearRequests()

	// Verify requests were cleared
	reqs = ms.GetRequests()
	assert.Len(t, reqs, 0)

	// Execute another command
	_, _, err = captureOutput(func() error {
		return main.RunProduce(testFlags, "test-queue-2")
	})

	require.NoError(t, err)

	// Verify only the new request is captured
	reqs = ms.GetRequests()
	assert.Len(t, reqs, 1)
}

// TestMockServerGetLastRequest tests the GetLastRequest functionality
func TestMockServerGetLastRequest(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	ms.ClearRequests()

	// Initially no requests
	lastReq := ms.GetLastRequest()
	assert.Nil(t, lastReq)

	// Execute commands
	testFlags1 := main.FlagParams{
		Payload:  "first payload",
		Endpoint: ms.URL(),
		Timeout:  "30s",
	}
	_, _, err1 := captureOutput(func() error {
		return main.RunProduce(testFlags1, "queue-1")
	})

	testFlags2 := main.FlagParams{
		Payload:  "second payload",
		Endpoint: ms.URL(),
		Timeout:  "30s",
	}
	_, _, err2 := captureOutput(func() error {
		return main.RunProduce(testFlags2, "queue-2")
	})

	require.NoError(t, err1)
	require.NoError(t, err2)

	// Verify GetLastRequest returns the most recent request
	lastReq = ms.GetLastRequest()
	require.NotNil(t, lastReq)

	assert.Equal(t, "/v1/queue.produce", lastReq.Path)
	assert.Equal(t, "POST", lastReq.Method)
}

// Test helper to capture stdout/stderr
func captureOutput(fn func() error) (stdout, stderr string, err error) {
	oldStdout := os.Stdout
	oldStderr := os.Stderr

	stdoutR, stdoutW, _ := os.Pipe()
	stderrR, stderrW, _ := os.Pipe()

	os.Stdout = stdoutW
	os.Stderr = stderrW

	err = fn()

	_ = stdoutW.Close()
	_ = stderrW.Close()

	stdoutBytes, _ := io.ReadAll(stdoutR)
	stderrBytes, _ := io.ReadAll(stderrR)

	os.Stdout = oldStdout
	os.Stderr = oldStderr

	return string(stdoutBytes), string(stderrBytes), err
}
