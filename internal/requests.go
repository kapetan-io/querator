package internal

import (
	"context"
	"github.com/kapetan-io/querator/store"
	"time"
)

// TODO(thrawn01): Consider creating a pool of ReserveRequest and Item to avoid GC

type ReserveRequest struct {
	// The number of items requested from the queue.
	BatchSize int32
	// How long the caller expects Reserve() to block before returning
	// if no items are available to be reserved. Max duration is 5 minutes.
	RequestTimeout time.Duration
	// The id of the client
	ClientID string
	// The context of the requesting client
	Context context.Context
	// The result of the reservation
	Items []*store.Item

	// Is calculated from RequestTimeout and specifies a time in the future when this request
	// should be cancelled.
	requestDeadline time.Time
	// Used by Reserve() to wait for this request to complete
	readyCh chan struct{}
	// The error to be returned to the caller
	err error
}

type ProduceRequest struct {
	// How long the caller expects Produce() to block before returning
	RequestTimeout time.Duration
	// The context of the requesting client
	Context context.Context
	// The items to produce
	Items []*store.Item

	// Is calculated from RequestTimeout and specifies a time in the future when this request
	// should be cancelled.
	requestDeadline time.Time
	// Used by Produce() to wait for this request to complete
	readyCh chan struct{}
	// The error to be returned to the caller
	err error
}

type CompleteRequest struct {
	// How long the caller expects Complete() to block before returning
	RequestTimeout time.Duration
	// The context of the requesting client
	Context context.Context
	// The ids to complete
	Ids []string

	// Is calculated from RequestTimeout and specifies a time in the future when this request
	// should be cancelled.
	requestDeadline time.Time
	// Used by Complete() to wait for this request to complete
	readyCh chan struct{}
	// The error to be returned to the caller
	err error
}

type StorageRequest struct {
	Context context.Context
	Items   []*store.Item
	ID      string
	Pivot   string
	Limit   int

	readyCh chan struct{}
	err     error
}

type ShutdownRequest struct {
	Context context.Context
	readyCh chan struct{}
	Err     error
}
