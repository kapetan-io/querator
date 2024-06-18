package internal

import (
	"context"
	"time"
)

type Store interface {
	// GetReservable gets up to 'limit' reservable items from a queue without marking the items as reserved
	ListReservable(ctx context.Context, items *[]*QueueItem, limit int) error

	// Reserve gets up to 'limit' reservable items from the queue and marks the items as reserved.
	Reserve(ctx context.Context, items *[]*QueueItem, limit int) error

	// ReadItems reads items in a queue. limit and offset allow the user to page through all the items
	// in the queue.
	Read(ctx context.Context, items *[]*QueueItem, pivot string, limit int) error

	// WriteItems writes the item to the queue and updates the item with the
	// unique id.
	Write(ctx context.Context, items []*QueueItem) error

	// DeleteItems removes the provided items from the queue
	Delete(ctx context.Context, items []*QueueItem) error

	Close(ctx context.Context) error
}

// QueueRecord is the internal representation of the item in the data store if appropriate for the data store
// and is dependent upon the data store. It should NOT be exposed via the Store interface.
type QueueRecord struct {
	ID         string    `json:"id"`
	ClientID   string    `json:"client_id"`
	IsReserved bool      `json:"is_reserved"`
	ExpireAt   time.Time `json:"expire_at"`
	Attempts   int       `json:"attempts"`
	Reference  string    `json:"reference"`
	Encoding   string    `json:"encoding"`
	Kind       string    `json:"kind"`
	Body       []byte    `json:"body"`
}
