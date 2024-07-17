package types

import (
	"context"
	"time"
)

// TODO(thrawn01): Consider creating a pool of Request structs and Item to avoid GC

type ReserveRequest struct {
	// The number of items requested from the queue.
	NumRequested int // TODO: Rename the protobuf variable this maps too
	// How long the caller expects Reserve() to block before returning
	// if no items are available to be reserved. Max duration is 5 minutes.
	RequestTimeout time.Duration
	// The id of the client
	ClientID string
	// The context of the requesting client
	Context context.Context
	// The result of the reservation
	Items []*Item
	// The RequestDeadline calculated from RequestTimeout
	RequestDeadline time.Time
	// Used to wait for this request to complete
	ReadyCh chan struct{}
	// The error to be returned to the caller
	Err error
}

type ProduceRequest struct {
	// How long the caller expects Produce() to block before returning
	RequestTimeout time.Duration
	// The context of the requesting client
	Context context.Context
	// The items to produce
	Items []*Item
	// The RequestDeadline calculated from RequestTimeout
	RequestDeadline time.Time
	// Used to wait for this request to complete
	ReadyCh chan struct{}
	// The error to be returned to the caller
	Err error
}

type CompleteRequest struct {
	// How long the caller expects Complete() to block before returning
	RequestTimeout time.Duration
	// The context of the requesting client
	Context context.Context
	// The ids to mark as complete
	Ids []string
	// The RequestDeadline calculated from RequestTimeout
	RequestDeadline time.Time
	// Used to wait for this request to complete
	ReadyCh chan struct{}
	// The error to be returned to the caller
	Err error
}

type StorageRequest struct {
	// Items is the items returned by the storage request
	Items []*Item
	// ID is the unique id of the item requested
	IDs []string
	// Pivot is included if requesting a list of storage items
	Pivot string
	// Limit is included if requesting a list of storage items
	Limit int
}

type PauseRequest struct {
	PauseDuration time.Duration
	Pause         bool
}

type ShutdownRequest struct {
	Context context.Context
	// Used to wait for this request to complete
	ReadyCh chan struct{}
	// The error to be returned to the caller
	Err error
}

type ListOptions struct {
	Pivot string
	Limit int
}

type QueueStats struct {
	// Total is the number of items in the queue
	Total int
	// TotalReserved is the number of items in the queue that are in reserved state
	TotalReserved int
	// AverageAge is the average age of all items in the queue
	AverageAge time.Duration
	// AverageReservedAge is the average age of reserved items in the queue
	AverageReservedAge time.Duration
	// ProduceWaiting is the number of `/queue.produce` requests currently waiting
	// to be processed by the sync loop
	ProduceWaiting int
	// ReserveWaiting is the number of `/queue.reserve` requests currently waiting
	// to be processed by the sync loop
	ReserveWaiting int
	// CompleteWaiting is the number of `/queue.complete` requests currently waiting
	// to be processed by the sync loop
	CompleteWaiting int
	// ReserveBlocked is the number of reservations which are blocked waiting for new item to enter the queue.
	ReserveBlocked int
}
