package types

import (
	"context"
	"github.com/kapetan-io/tackle/clock"
)

const UnSet = -1

// TODO(thrawn01): Consider creating a pool of Request structs and Item to avoid GC

type ReserveRequest struct {
	// The number of items requested from the queue.
	NumRequested int // TODO: Rename the protobuf variable this maps too
	// How long the caller expects Reserve() to block before returning
	// if no items are available to be reserved. Max duration is 5 minutes.
	RequestTimeout clock.Duration
	// The id of the client
	ClientID string
	// The context of the requesting client
	Context context.Context

	// The result of the reservation
	Items []*Item
	// The partition the reserved items are from
	Partition int

	// The RequestDeadline calculated from RequestTimeout
	RequestDeadline clock.Time
	// Used to wait for this request to complete
	ReadyCh chan struct{}
	// The error to be returned to the caller
	Err error
}

type ProduceRequest struct {
	// How long the caller expects Produce() to block before returning
	RequestTimeout clock.Duration
	// The context of the requesting client
	Context context.Context
	// The items to produce
	Items []*Item
	// The RequestDeadline calculated from RequestTimeout
	RequestDeadline clock.Time
	// Used to wait for this request to complete
	ReadyCh chan struct{}
	// The error to be returned to the caller
	Err error
}

type CompleteRequest struct {
	// How long the caller expects Complete() to block before returning
	RequestTimeout clock.Duration
	// The context of the requesting client
	Context context.Context
	// Partition is the partition the complete request is for
	Partition int
	// The ids to mark as complete
	Ids [][]byte
	// The RequestDeadline calculated from RequestTimeout
	RequestDeadline clock.Time
	// Used to wait for this request to complete
	ReadyCh chan struct{}
	// The error to be returned to the caller
	Err error
}

type ClearRequest struct {
	// Defer indicates the 'defer' queue will be cleared. If true, any items
	// scheduled to be retried at a future date will be removed.
	Defer bool // TODO: Implement
	// Scheduled indicates any 'scheduled' items in the queue will be
	// cleared. If true, any items scheduled to be enqueued at a future date
	// will be removed.
	Scheduled bool // TODO: Implement
	// Queue indicates any items currently waiting in the FIFO queue will
	// clear. If true, any items in the queue which have NOT been reserved
	// will be removed.
	Queue bool
	// Destructive indicates the Defer,Scheduled,Queue operations should be
	// destructive in that all data regardless of status will be removed.
	// For example, if used with ClearRequest.Queue = true, then ALL items
	// in the queue regardless of reserve status will be removed. This means
	// that clients who currently have ownership of those items will not be able
	// to "complete" those items, as querator will have no knowledge of those items.
	Destructive bool
}

type PauseRequest struct {
	Pause bool
}

type ShutdownRequest struct {
	Context context.Context
	// Used to wait for this request to complete
	ReadyCh chan struct{}
	// The error to be returned to the caller
	Err error
}

type ListOptions struct {
	Pivot []byte
	Limit int
}

type LogicalStats struct {
	// ProduceWaiting is the number of `/queue.produce` requests currently waiting
	// to be processed by the sync loop
	ProduceWaiting int
	// ReserveWaiting is the number of `/queue.reserve` requests currently waiting
	// to be processed by the sync loop
	ReserveWaiting int
	// CompleteWaiting is the number of `/queue.complete` requests currently waiting
	// to be processed by the sync loop
	CompleteWaiting int
	// InFlight is the number of requests currently in flight
	InFlight int

	Partitions []PartitionStats
}

type PartitionStats struct {
	// Partition is the partition these stats are for
	Partition int
	// Total is the number of items in the queue
	Total int
	// TotalReserved is the number of items in the queue that are in reserved state
	TotalReserved int
	// AverageAge is the average age of all items in the queue
	AverageAge clock.Duration
	// AverageReservedAge is the average age of reserved items in the queue
	AverageReservedAge clock.Duration
}

type LifeCycleRequest struct {
	RequestTimeout clock.Duration
	Actions        []Action
	PartitionNum   int
}

type PartitionStateChange struct {
	State        PartitionState
	PartitionNum int
}
