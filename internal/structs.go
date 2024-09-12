package internal

import (
	"context"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
)

type PartitionDistribution struct {
	// Batch is the batch of requests that are assigned to this distribution
	Batch types.Batch[types.ProduceRequest]
	// Partition is the partition
	Partition store.Partition
	// InFailure is true if the Partition has failed and should not be used
	InFailure bool
	// Count is the total number of un-reserved items in the partition
	Count int
}

func (p *PartitionDistribution) Add(req *types.ProduceRequest) {
	p.Count += len(req.Items)
	p.Batch.Add(req)
}

func (p *PartitionDistribution) Reset() {
	p.Batch.Reset()
}

type QueueState struct {
	Reservations           types.ReserveBatch
	Producers              types.Batch[types.ProduceRequest]
	Completes              types.Batch[types.CompleteRequest]
	PartitionDistributions []PartitionDistribution

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
}
