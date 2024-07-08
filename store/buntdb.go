package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/buntdb"
	"log/slog"
	"strings"
	"time"
)

type BuntOptions struct {
	Logger       duh.StandardLogger
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

// ---------------------------------------------
// Storage Implementation
// ---------------------------------------------

type BuntStorage struct {
	opts BuntOptions
}

var _ Storage = &BuntStorage{}

func (b *BuntStorage) ParseID(parse string, id *StorageID) error {
	parts := strings.Split(parse, "~")
	if len(parts) != 2 {
		return errors.New("expected format <queue_name>~<storage_id>")
	}
	id.Queue = parts[0]
	id.ID = parts[1]
	return nil
}

func (b *BuntStorage) CreateID(name, id string) string {
	return fmt.Sprintf("%s~%s", name, id)
}

func NewBuntStorage(opts BuntOptions) *BuntStorage {
	set.Default(&opts.WriteTimeout, 5*time.Second)
	set.Default(&opts.ReadTimeout, 5*time.Second)
	set.Default(&opts.Logger, slog.Default())

	return &BuntStorage{opts: opts}
}

// ---------------------------------------------
// Queue Implementation
// ---------------------------------------------

type BuntQueue struct {
	storage *BuntStorage
	opts    QueueOptions
	uid     ksuid.KSUID
	db      *buntdb.DB
}

var _ Queue = &BuntQueue{}

func (b *BuntStorage) NewQueue(opts QueueOptions) (Queue, error) {
	f := errors.Fields{"category", "bunt-db", "func", "NewQueue"}

	if strings.TrimSpace(opts.Name) == "" {
		return nil, transport.NewInvalidOption("'name' cannot be empty; must be a valid queue name")
	}
	set.Default(&opts.ReadTimeout, b.opts.ReadTimeout)
	set.Default(&opts.WriteTimeout, b.opts.WriteTimeout)

	// TODO: Ensure we can access the storage location
	// TODO: Check if the file exists

	// TODO: All queues are currently in memory, need to fix this
	db, err := buntdb.Open(":memory:")
	if err != nil {
		return nil, f.Errorf("opening buntdb: %w", err)
	}
	return &BuntQueue{
		uid:     ksuid.New(),
		opts:    opts,
		db:      db,
		storage: b,
	}, nil
}

func (s *BuntQueue) Produce(_ context.Context, batch Batch[transport.ProduceRequest]) error {
	f := errors.Fields{"category", "bunt-db", "func", "Produce"}

	tx, err := s.db.Begin(true)
	if err != nil {
		return f.Errorf("during Begin(): %w", err)
	}

	for _, r := range batch.Requests {
		for _, item := range r.Items {
			s.uid = s.uid.Next()
			item.ID = s.uid.String()

			if err := buntSet(f, tx, item); err != nil {
				return err
			}
			item.ID = s.storage.CreateID(s.opts.Name, item.ID)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("during Commit(): %w", err)
	}
	return nil
}

func (s *BuntQueue) Reserve(_ context.Context, batch Batch[transport.ReserveRequest], opts ReserveOptions) error {
	f := errors.Fields{"category", "bunt-db", "func", "Reserve"}

	tx, err := s.db.Begin(false)
	if err != nil {
		return f.Errorf("during Begin(false): %w", err)
	}

	var iterErr error
	var count, idx int

	err = tx.AscendGreaterOrEqual("", "", func(key, value string) bool {
		if count >= batch.TotalRequested {
			return false
		}

		// TODO: Grab from the memory pool
		item := new(transport.Item)
		if err := json.Unmarshal([]byte(value), item); err != nil {
			iterErr = f.Errorf("during json.Unmarshal(): %w", err)
			return false
		}

		if item.IsReserved {
			return true
		}

		item.ReserveDeadline = opts.ReserveDeadline
		item.IsReserved = true
		count++

		// The following code attempts to spread the items evenly across all the batches

		// Find the next request in the batch which has not met its NumRequested limit
		for len(batch.Requests[idx].Items) == batch.Requests[idx].NumRequested {
			// If TotalRequested is more than the total of all the ReserveRequest.NumRequested
			// then this code could get stuck in a loop
			idx++
			if idx == len(batch.Requests) {
				idx = 0
			}
		}

		batch.Requests[idx].Items = append(batch.Requests[idx].Items, item)
		idx++
		if idx == len(batch.Requests) {
			idx = 0
		}
		return true
	})

	if err != nil {
		return f.Errorf("during AscendGreaterOrEqual(): %w", err)
	}

	if err = tx.Rollback(); err != nil {
		return f.Errorf("during Rollback(): %w", err)
	}

	if iterErr != nil {
		return iterErr
	}

	// Update all the reserved items in the database
	tx, err = s.db.Begin(true)
	if err != nil {
		return f.Errorf("during Begin(true): %w", err)
	}

	for i := 0; i < len(batch.Requests); i++ {
		for _, item := range batch.Requests[i].Items {
			if err := buntSet(f, tx, item); err != nil {
				return err
			}
			item.ID = s.storage.CreateID(s.opts.Name, item.ID)
		}
	}

	if err = tx.Commit(); err != nil {
		return f.Errorf("during Commit(): %w", err)
	}

	return nil
}

func (s *BuntQueue) Complete(_ context.Context, batch Batch[transport.CompleteRequest]) error {
	f := errors.Fields{"category", "bunt-db", "func", "Complete"}

	tx, err := s.db.Begin(true)
	if err != nil {
		return f.Errorf("during Begin(): %w", err)
	}

nextBatch:
	for i := range batch.Requests {
		for _, id := range batch.Requests[i].Ids {
			var sid StorageID
			if err := s.storage.ParseID(id, &sid); err != nil {
				batch.Requests[i].Err = transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
				continue nextBatch
			}

			value, err := tx.Get(sid.ID)
			if err != nil {
				return fmt.Errorf("during Get(%s): %w", sid, err)
			}

			item := new(transport.Item)
			if err := json.Unmarshal([]byte(value), item); err != nil {
				return f.Errorf("during json.Unmarshal() of id '%s': %w", sid, err)
			}

			if !item.IsReserved {
				batch.Requests[i].Err = transport.NewConflict("item(s) cannot be completed; '%s' is not "+
					"marked as reserved", sid.ID)
				continue nextBatch
			}

			if _, err = tx.Delete(sid.ID); err != nil {
				return f.Errorf("during Delete(%s): %w", sid.ID, err)
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return f.Errorf("during Commit(): %w", err)
	}
	return nil
}

func (s *BuntQueue) Read(_ context.Context, items *[]*transport.Item, pivot string, limit int) error {
	f := errors.Fields{"category", "bunt-db", "func", "Read"}

	tx, err := s.db.Begin(false)
	if err != nil {
		return f.Errorf("during Begin(): %w", err)
	}

	var iterErr error
	var count int
	var sid StorageID
	if pivot != "" {
		if err := s.storage.ParseID(pivot, &sid); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", pivot, err)
		}
	}

	err = tx.AscendGreaterOrEqual("", sid.ID, func(key, value string) bool {
		if count >= limit {
			return false
		}
		// TODO: Grab from the memory pool
		item := new(transport.Item)
		if err := json.Unmarshal([]byte(value), item); err != nil {
			iterErr = f.Errorf("during json.Unmarshal(): %w", err)
			return false
		}
		item.ID = s.storage.CreateID(s.opts.Name, item.ID)
		*items = append(*items, item)
		count++
		return true
	})
	if err != nil {
		return f.Errorf("during AscendGreaterOrEqual(): %w", err)
	}

	if err = tx.Rollback(); err != nil {
		return fmt.Errorf("during Rollback(): %w", err)
	}

	if iterErr != nil {
		return iterErr
	}
	return nil
}

func (s *BuntQueue) Write(_ context.Context, items []*transport.Item) error {
	f := errors.Fields{"category", "bunt-db", "func", "Write"}

	tx, err := s.db.Begin(true)
	if err != nil {
		return f.Errorf("during Begin(): %w", err)
	}

	for _, item := range items {
		s.uid = s.uid.Next()
		item.ID = s.uid.String()

		if err := buntSet(f, tx, item); err != nil {
			return err
		}
		item.ID = s.storage.CreateID(s.opts.Name, item.ID)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("during Commit(): %w", err)
	}
	return nil
}

func (s *BuntQueue) Delete(_ context.Context, ids []string) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	for _, id := range ids {
		var sid StorageID
		if err := s.storage.ParseID(id, &sid); err != nil {
			return transport.NewInvalidOption("invalid storage id; '%s': %s", id, err)
		}
		_, err := tx.Delete(sid.ID)
		if err != nil {
			// I can't think of a reason why we would want alert the
			// caller that the ids they want to delete are already deleted.
			if errors.Is(err, buntdb.ErrNotFound) {
				continue
			}
			return fmt.Errorf("during delete: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("during commit: %w", err)
	}
	return nil
}

// Stats returns some stats about the current state of the storage and the items within
// TODO: Fix Stats, Stats items should be renamed and the stats collected corrected.
func (s *BuntQueue) Stats(_ context.Context, stats *Stats) error {
	f := errors.Fields{"category", "bunt-db", "func", "Stats"}

	tx, err := s.db.Begin(false)
	if err != nil {
		return f.Errorf("during Begin(): %w", err)
	}

	var iterErr error
	now := time.Now().UTC()

	err = tx.AscendGreaterOrEqual("", "", func(key, value string) bool {
		var item transport.Item
		if err := json.Unmarshal([]byte(value), &item); err != nil {
			iterErr = f.Errorf("during json.Unmarshal(): %w", err)
			return false
		}

		stats.Total++
		stats.AverageAge += item.DeadDeadline.Sub(now)
		if item.IsReserved {
			stats.AverageReservedAge += item.ReserveDeadline.Sub(now)
			stats.TotalReserved++
		}

		return true
	})
	if err != nil {
		return f.Errorf("during AscendGreaterOrEqual(): %w", err)
	}

	if err = tx.Rollback(); err != nil {
		return f.Errorf("during Rollback(): %w", err)
	}

	if iterErr != nil {
		return iterErr
	}

	stats.AverageAge = time.Duration(int64(stats.AverageAge) / int64(stats.Total))
	stats.AverageReservedAge = time.Duration(int64(stats.AverageReservedAge) / int64(stats.TotalReserved))

	return nil
}

func (s *BuntQueue) Close(_ context.Context) error {
	return s.db.Close()
}

func (s *BuntQueue) Options() QueueOptions {
	return s.opts
}

func buntSet(f errors.Fields, tx *buntdb.Tx, item *transport.Item) error {
	// TODO: Use something more efficient like protobuf,
	//	gob or https://github.com/capnproto/go-capnp
	b, err := json.Marshal(item)
	if err != nil {
		return f.Errorf("during json.Marshal(): %w", err)
	}

	_, _, err = tx.Set(item.ID, string(b), nil)
	if err != nil {
		return f.Errorf("during Set(%s): %w", item.ID, err)
	}
	return nil
}
