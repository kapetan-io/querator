package internal

import (
	"context"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"log/slog"
)

const (
	LevelDebugAll = slog.LevelDebug
	LevelDebug    = slog.LevelDebug + 1
)

// Partition is the in memory representation of the state of a partition.
type Partition struct {
	// ProduceRequests is the batch of produce requests that are assigned to this partition
	ProduceRequests types.ProduceBatch
	// LeaseRequests is the batch of lease requests that are assigned to this partition
	LeaseRequests types.LeaseBatch
	// CompleteRequests is the batch of complete requests that are assigned to this partition
	CompleteRequests types.Batch[types.CompleteRequest]
	// LifeCycleRequests is a batch of lifecycle requests that are assigned to this partition
	LifeCycleRequests types.Batch[types.LifeCycleRequest]
	// Store is the storage for this partition
	Store store.Partition
	// State is the current state of the partition. It is updated by LifeCycle and storage backends
	State types.PartitionState
	// LifeCycle is the active life cycle associated with this partition
	LifeCycle *LifeCycle
	// Info is the info associated with this partition
	Info types.PartitionInfo
}

func (p *Partition) Reset() {
	p.ProduceRequests.Reset()
	p.LeaseRequests.Reset()
	p.CompleteRequests.Reset()
	p.LifeCycleRequests.Reset()
}

func (p *Partition) Produce(req *types.ProduceRequest) {
	p.State.UnLeased += len(req.Items)
	p.ProduceRequests.Add(req)
	req.Assigned = true
}

func (p *Partition) Lease(req *types.LeaseRequest) {
	// Use the Number of Requested items OR the count of items in the partition which ever is least.
	lease := min(p.State.UnLeased, req.NumRequested)
	// Increment the number of lease items in this distribution.
	p.State.NumLeased += lease
	// Decrement requested from the actual count
	p.State.UnLeased -= lease
	// Record which partition this request is assigned, so it can be retrieved by the client later.
	req.Partition = p.Info.PartitionNum
	// Add the request to this partitions lease batch
	p.LeaseRequests.Add(req)
}

type Request struct {
	// The API method called
	Method MethodKind
	// Context is the context of the request
	Context context.Context
	// The request struct for this method
	Request any
	// Used to wait for this request to complete
	ReadyCh chan struct{}
	// The error to be returned to the caller
	Err error
}

type StorageRequest struct {
	// Items is the items returned by the storage request
	Items *[]*types.Item
	// ID is the unique id of the item requested
	IDs []types.ItemID
	// Options used when listing
	Options types.ListOptions
	// Partition is the partition this request is for
	Partition int
}
