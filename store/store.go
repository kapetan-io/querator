package store

import (
	"bytes"
	"context"
	"time"
)

type ReserveOptions struct {
	// ReserveExpireAt is time in the future when a reservation should expire
	ReserveExpireAt time.Time

	// Limit is the max number of items to reserve
	Limit int
}

type Stats struct {
	// Total is the number of items in the queue
	Total int

	// TotalReserved is the number of items in the queue that are in reserved state
	TotalReserved int

	// AverageAge is the average age of all items in the queue
	AverageAge time.Duration

	// AverageReservedAge is the average age of reserved items in the queue
	AverageReservedAge time.Duration
}

type QueueStorage interface {
	// Stats returns stats about the queue
	Stats(ctx context.Context, stats *Stats) error

	// Reserve list up to 'limit' reservable items from the queue and marks the items as reserved.
	Reserve(ctx context.Context, items *[]*QueueItem, opts ReserveOptions) error

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

// TODO: ScheduledStorage interface {} - A place to store scheduled items to be queued. (Defer)
// TODO: QueueOptionStorage interface {} - A place to store queue options and a list of valid queues

// QueueItem is the store and queue representation of an item in the queue.
type QueueItem struct {
	// ID is unique to each item in the data store. The ID style is different depending on the data store
	// implementation, and does not include the queue name.
	ID string

	// ClientID is the id of the client which reserved this item
	ClientID string // TODO: Remove the ClientID

	// IsReserved is true if the item has been reserved by a client
	IsReserved bool

	// ReserveExpireAt is the time in the future when the reservation is
	// expired and can be reserved by another consumer
	ReserveExpireAt time.Time

	// ExpireAt is the time in the future the item will expire
	ExpireAt time.Time

	// Attempts is how many attempts this item has seen
	Attempts int

	// Reference is a user supplied field which could contain metadata or specify who owns this queue
	// Examples: "jake@statefarm.com", "stapler@office-space.com", "account-0001"
	Reference string

	// Encoding is a user specified field which indicates the encoding used to encode the 'body'
	Encoding string

	// Kind is the Kind or Type the body contains. Consumers can use this field to determine handling
	// of the body prior to unmarshalling. Examples: 'webhook-v2', 'webhook-v1',
	Kind string

	// Body is the body of the queue item
	Body []byte
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
	if l.ReserveExpireAt.Compare(r.ReserveExpireAt) != 0 {
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
