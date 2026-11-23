package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"

	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	protobuf "google.golang.org/protobuf/proto"
)

type MockServer struct {
	server   *httptest.Server
	mu       sync.RWMutex
	requests []CapturedRequest
}

type CapturedRequest struct {
	Method  string
	Path    string
	Headers http.Header
	Body    []byte
}

func NewMockServer() *MockServer {
	ms := &MockServer{
		requests: make([]CapturedRequest, 0),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", ms.handleRequest)

	ms.server = httptest.NewServer(mux)
	return ms
}

func (ms *MockServer) URL() string {
	return ms.server.URL
}

func (ms *MockServer) Close() {
	ms.server.Close()
}

func (ms *MockServer) GetRequests() []CapturedRequest {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return append([]CapturedRequest(nil), ms.requests...)
}

func (ms *MockServer) GetLastRequest() *CapturedRequest {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.requests) == 0 {
		return nil
	}
	return &ms.requests[len(ms.requests)-1]
}

func (ms *MockServer) ClearRequests() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.requests = ms.requests[:0]
}

func (ms *MockServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	captured := CapturedRequest{
		Method:  r.Method,
		Path:    r.URL.Path,
		Headers: r.Header.Clone(),
		Body:    body,
	}

	ms.mu.Lock()
	ms.requests = append(ms.requests, captured)
	ms.mu.Unlock()

	ms.respondToRequest(w, r, captured)
}

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
	case transport.RPCStorageBackendsUpdate:
		ms.handleStorageBackendsUpdate(w, req)
	case transport.RPCStorageBackendsList:
		ms.handleStorageBackendsList(w, req)
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

func (ms *MockServer) handleQueueProduce(w http.ResponseWriter, _ CapturedRequest) {
	reply := &v1.Reply{Code: duh.CodeOK}
	ms.writeProtoResponse(w, reply)
}

func (ms *MockServer) handleQueueLease(w http.ResponseWriter, _ CapturedRequest) {
	resp := &proto.QueueLeaseResponse{
		QueueName: "test-queue",
		Partition: 0,
		Items:     []*proto.QueueLeaseItem{},
	}
	ms.writeProtoResponse(w, resp)
}

func (ms *MockServer) handleQueueComplete(w http.ResponseWriter, _ CapturedRequest) {
	reply := &v1.Reply{Code: duh.CodeOK}
	ms.writeProtoResponse(w, reply)
}

func (ms *MockServer) handleQueuesCreate(w http.ResponseWriter, _ CapturedRequest) {
	reply := &v1.Reply{Code: duh.CodeOK}
	ms.writeProtoResponse(w, reply)
}

func (ms *MockServer) handleQueuesList(w http.ResponseWriter, _ CapturedRequest) {
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

func (ms *MockServer) handleQueuesUpdate(w http.ResponseWriter, _ CapturedRequest) {
	reply := &v1.Reply{Code: duh.CodeOK}
	ms.writeProtoResponse(w, reply)
}

func (ms *MockServer) handleQueuesDelete(w http.ResponseWriter, _ CapturedRequest) {
	reply := &v1.Reply{Code: duh.CodeOK}
	ms.writeProtoResponse(w, reply)
}

func (ms *MockServer) handleQueuesInfo(w http.ResponseWriter, _ CapturedRequest) {
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

	if protoMsg, ok := msg.(protobuf.Message); ok {
		data, err = protobuf.Marshal(protoMsg)
	} else {
		data, err = json.Marshal(msg)
	}

	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}

	if _, err := w.Write(data); err != nil {
		log.Printf("mock server: writing response: %v", err)
	}
}

func (ms *MockServer) handleStorageBackendsUpdate(w http.ResponseWriter, _ CapturedRequest) {
	ms.writeProtoResponse(w, &v1.Reply{Code: duh.CodeOK})
}

func (ms *MockServer) handleStorageBackendsList(w http.ResponseWriter, _ CapturedRequest) {
	ms.writeProtoResponse(w, &proto.StorageBackendsListResponse{
		Backends: []*proto.StorageBackend{
			{Backend: "test-backend", Affinity: new(1.0), Configured: new(true), SafeToStop: new(false)},
		},
	})
}
