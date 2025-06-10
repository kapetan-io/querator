package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	protobuf "google.golang.org/protobuf/proto"
)

// MockServer captures HTTP requests for testing CLI commands.
// This mock server implementation provides a better testing approach compared to 
// running full querator instances. It captures all requests sent by CLI commands
// and returns appropriate mock responses, allowing tests to verify that CLI
// commands send correct API requests without requiring a full server setup.
type MockServer struct {
	server   *httptest.Server
	mu       sync.RWMutex
	requests []CapturedRequest
}

// CapturedRequest stores details about an HTTP request
type CapturedRequest struct {
	Method   string
	Path     string
	Headers  http.Header
	Body     []byte
	BodyJSON map[string]interface{} // Parsed JSON body for easy access
}

// NewMockServer creates a new mock server for testing
func NewMockServer() *MockServer {
	ms := &MockServer{
		requests: make([]CapturedRequest, 0),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", ms.handleRequest)

	ms.server = httptest.NewServer(mux)
	return ms
}

// URL returns the mock server's URL
func (ms *MockServer) URL() string {
	return ms.server.URL
}

// Close shuts down the mock server
func (ms *MockServer) Close() {
	ms.server.Close()
}

// GetRequests returns all captured requests
func (ms *MockServer) GetRequests() []CapturedRequest {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return append([]CapturedRequest(nil), ms.requests...)
}

// GetLastRequest returns the most recent request
func (ms *MockServer) GetLastRequest() *CapturedRequest {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.requests) == 0 {
		return nil
	}
	return &ms.requests[len(ms.requests)-1]
}

// ClearRequests clears all captured requests
func (ms *MockServer) ClearRequests() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.requests = ms.requests[:0]
}

// handleRequest captures all incoming requests and returns appropriate responses
func (ms *MockServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	// Parse JSON body if possible (for debugging - most requests will be protobuf)
	var bodyJSON map[string]interface{}
	if len(body) > 0 && r.Header.Get("Content-Type") == "application/json" {
		_ = json.Unmarshal(body, &bodyJSON)
	}

	// Capture the request
	captured := CapturedRequest{
		Method:   r.Method,
		Path:     r.URL.Path,
		Headers:  r.Header.Clone(),
		Body:     body,
		BodyJSON: bodyJSON,
	}

	ms.mu.Lock()
	ms.requests = append(ms.requests, captured)
	ms.mu.Unlock()

	// Return appropriate responses based on the endpoint
	ms.respondToRequest(w, r, captured)
}

// respondToRequest provides mock responses for different endpoints
func (ms *MockServer) respondToRequest(w http.ResponseWriter, r *http.Request, req CapturedRequest) {
	w.Header().Set("Content-Type", duh.ContentTypeProtoBuf)

	switch r.URL.Path {
	case transport.RPCQueueProduce:
		ms.handleQueueProduce(w, req)
	case transport.RPCQueueLease:
		ms.handleQueueLease(w, req)
	case transport.RPCQueueComplete:
		ms.handleQueueComplete(w, req)
	case transport.RPCQueuesCreate:
		ms.handleQueuesCreate(w, req)
	case transport.RPCQueuesList:
		ms.handleQueuesList(w, req)
	case transport.RPCQueuesUpdate:
		ms.handleQueuesUpdate(w, req)
	case transport.RPCQueuesDelete:
		ms.handleQueuesDelete(w, req)
	case transport.RPCQueuesInfo:
		ms.handleQueuesInfo(w, req)
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

func (ms *MockServer) handleQueueProduce(w http.ResponseWriter, req CapturedRequest) {
	reply := &v1.Reply{Code: duh.CodeOK}
	ms.writeProtoResponse(w, reply)
}

func (ms *MockServer) handleQueueLease(w http.ResponseWriter, req CapturedRequest) {
	resp := &proto.QueueLeaseResponse{
		QueueName: "test-queue",
		Partition: 0,
		Items:     []*proto.QueueLeaseItem{}, // Empty for testing
	}
	ms.writeProtoResponse(w, resp)
}

func (ms *MockServer) handleQueueComplete(w http.ResponseWriter, req CapturedRequest) {
	reply := &v1.Reply{Code: duh.CodeOK}
	ms.writeProtoResponse(w, reply)
}

func (ms *MockServer) handleQueuesCreate(w http.ResponseWriter, req CapturedRequest) {
	reply := &v1.Reply{Code: duh.CodeOK}
	ms.writeProtoResponse(w, reply)
}

func (ms *MockServer) handleQueuesList(w http.ResponseWriter, req CapturedRequest) {
	resp := &proto.QueuesListResponse{
		Items: []*proto.QueueInfo{
			{
				QueueName:           "test-queue-1",
				LeaseTimeout:        "1m",
				ExpireTimeout:       "60m",
				RequestedPartitions: 1,
			},
		},
	}
	ms.writeProtoResponse(w, resp)
}

func (ms *MockServer) handleQueuesUpdate(w http.ResponseWriter, req CapturedRequest) {
	reply := &v1.Reply{Code: duh.CodeOK}
	ms.writeProtoResponse(w, reply)
}

func (ms *MockServer) handleQueuesDelete(w http.ResponseWriter, req CapturedRequest) {
	reply := &v1.Reply{Code: duh.CodeOK}
	ms.writeProtoResponse(w, reply)
}

func (ms *MockServer) handleQueuesInfo(w http.ResponseWriter, req CapturedRequest) {
	resp := &proto.QueueInfo{
		QueueName:           "test-queue",
		LeaseTimeout:        "1m",
		ExpireTimeout:       "60m",
		RequestedPartitions: 1,
		MaxAttempts:         3,
	}
	ms.writeProtoResponse(w, resp)
}

func (ms *MockServer) writeProtoResponse(w http.ResponseWriter, msg interface{}) {
	var data []byte
	var err error

	// Handle protobuf messages
	if protoMsg, ok := msg.(protobuf.Message); ok {
		data, err = protobuf.Marshal(protoMsg)
	} else {
		data, err = json.Marshal(msg)
	}

	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(data)
}
