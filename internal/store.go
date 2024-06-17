package internal

import "context"

type Store interface {
	// GetReservable gets all reservable items from a queue.
	GetReservable(ctx context.Context, name string, records []*QueueItem, limit int32) error

	// ReadItems reads items in a queue. limit and offset allow the user to page through all the items
	// in the queue.
	ReadItems(ctx context.Context, name string, records []*QueueItem, limit int32, offset int32) error

	// WriteItems writes the item to the queue and updates the item with the
	// unique id.
	WriteItems(ctx context.Context, name string, records []*QueueItem) error

	// DeleteItems removes the provided items from the queue
	DeleteItems(ctx context.Context, name string, records []*QueueItem) error
}

// QueueRecord is the internal representation of the item in the data store if appropriate for the data store
// and is dependent upon the data store. It should NOT be exposed via the Store interface.
type QueueRecord struct {
	ID       string
	ClientID string
}
