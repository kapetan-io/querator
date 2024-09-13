package internal

import (
	"context"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
)

type PartitionDistribution struct {
	// ProduceRequests is the batch of produce requests that are assigned to this partition
	ProduceRequests types.Batch[types.ProduceRequest]
	// ReserveRequests is the batch of reserve requests that are assigned to this partition
	ReserveRequests types.ReserveBatch
	// CompleteRequests is the batch of complete requests that are assigned to this partition
	CompleteRequests types.Batch[types.CompleteRequest]
	// Partition is the partition this distribution is for
	Partition store.Partition
	// InFailure is true if the Partition has failed and should not be used
	InFailure bool
	// Count is the total number of un-reserved items in the partition
	Count int
}

func (p *PartitionDistribution) Produce(req *types.ProduceRequest) {
	p.Count += len(req.Items)
	p.ProduceRequests.Add(req)
}

func (p *PartitionDistribution) Reserve(req *types.ReserveRequest) {
	p.Count -= req.NumRequested
	if p.Count <= 0 {
		p.Count = 0
	}
	// Record which partition this request is assigned, so it can be retrieved by the client later.
	req.Partition = p.Partition.Info().Partition
	// Add the request to this partitions reserve batch
	p.ReserveRequests.Add(req)
}

//func (p *PartitionDistribution) ResetRequests() {
//	p.ProduceRequests.Reset()
//	p.ReserveRequests.Reset()
//	p.CompleteRequests.Reset()
//}

type QueueState struct {
	Reservations           types.ReserveBatch
	Producers              types.Batch[types.ProduceRequest]
	Completes              types.Batch[types.CompleteRequest]
	PartitionDistributions []PartitionDistribution
	ReserveIndex           int

	NextMaintenanceCh <-chan clock.Time
}

type QueueRequest struct {
	// The API method called
	Method int
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
