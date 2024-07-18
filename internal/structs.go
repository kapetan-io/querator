package internal

import (
	"context"
	"github.com/kapetan-io/querator/internal/types"
	"time"
)

type QueueState struct {
	Reservations types.ReserveBatch
	Producers    types.Batch[types.ProduceRequest]
	Completes    types.Batch[types.CompleteRequest]

	NextMaintenanceCh <-chan time.Time
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
	IDs []string
	// Options used when listing
	Options types.ListOptions
}
