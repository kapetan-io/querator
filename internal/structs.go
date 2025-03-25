package internal

import (
	"context"
	"fmt"
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
	ProduceRequests types.Batch[types.ProduceRequest]
	// ReserveRequests is the batch of reserve requests that are assigned to this partition
	ReserveRequests types.ReserveBatch
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
	Info types.PartitionInfo // TODO: Stop using Store.Info().PartitionNum and use this instead
}

func (p *Partition) Reset() {
	p.ProduceRequests.Reset()
	p.ReserveRequests.Reset()
	p.CompleteRequests.Reset()
	p.LifeCycleRequests.Reset()
	p.State.NumReserved = 0
}

func (p *Partition) Produce(req *types.ProduceRequest) {
	p.State.UnReserved += len(req.Items)
	p.ProduceRequests.Add(req)
}

func (p *Partition) Reserve(req *types.ReserveRequest) {
	// Use the Number of Requested items OR the count of items in the partition which ever is least.
	fmt.Printf("UnReserved: %d Requested: %d\n", p.State.UnReserved, req.NumRequested)
	reserved := min(p.State.UnReserved, req.NumRequested)
	fmt.Printf("Reserved: %d\n", reserved)
	// Increment the number of reserved items in this distribution.
	p.State.NumReserved += reserved // TODO: This assignment is getting clobbered somehow
	// Decrement requested from the actual count
	p.State.UnReserved -= reserved
	// Record which partition this request is assigned, so it can be retrieved by the client later.
	req.Partition = p.Store.Info().PartitionNum
	// Add the request to this partitions reserve batch
	p.ReserveRequests.Add(req)
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
