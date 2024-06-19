package store

import (
	"bytes"
	"context"
	"time"
)

type QueueStorage interface {
	// ListReservable lists up to 'limit' reservable items from a queue without marking the items as reserved
	ListReservable(ctx context.Context, items *[]*QueueItem, limit int) error

	// Reserve list up to 'limit' reservable items from the queue and marks the items as reserved.
	Reserve(ctx context.Context, items *[]*QueueItem, limit int) error

	// Read reads items in a queue. limit and offset allow the user to page through all the items
	// in the queue.
	Read(ctx context.Context, items *[]*QueueItem, pivot string, limit int) error

	// Write writes the item to the queue and updates the item with the
	// unique id.
	Write(ctx context.Context, items []*QueueItem) error

	// Delete removes the provided items from the queue
	Delete(ctx context.Context, items []*QueueItem) error

	Close(ctx context.Context) error
}

// QueueItem is the store and queue representation of an item in the queue.
type QueueItem struct {
	// ID is unique to each item in the data store. The ID style is different depending on the data store
	// implementation, and does not include the queue name.
	ID string
	// ClientID is the id of the client which reserved this item
	ClientID string // TODO: Remove the ClientID
	// IsReserved is true if the item has been reserved by a client
	IsReserved bool
	// ExpireAt is the time in the future the item will expire
	ExpireAt time.Time

	Attempts  int
	Reference string
	Encoding  string
	Kind      string
	Body      []byte
}

func (l *QueueItem) Compare(r *QueueItem) bool {
	if l.ID != r.ID {
		return false
	}
	if l.ClientID != r.ClientID {
		return false
	}
	if l.IsReserved != r.IsReserved {
		return false
	}
	if l.ExpireAt.Compare(r.ExpireAt) != 0 {
		return false
	}
	if l.Attempts != r.Attempts {
		return false
	}
	if l.Reference != r.Reference {
		return false
	}
	if l.Encoding != r.Encoding {
		return false
	}
	if l.Kind != r.Kind {
		return false
	}
	if l.Body != nil && !bytes.Equal(l.Body, r.Body) {
		return false
	}
	return true
}
