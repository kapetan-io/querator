package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
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
	if strings.TrimSpace(opts.Name) == "" {
		return nil, NewInvalidOption("'name' cannot be empty; must be a valid queue name")
	}
	set.Default(&opts.ReadTimeout, b.opts.ReadTimeout)
	set.Default(&opts.WriteTimeout, b.opts.WriteTimeout)

	// TODO: Ensure we can access the storage location
	// TODO: Check if the file exists

	// TODO: All queues are currently in memory, need to fix this
	db, err := buntdb.Open(":memory:")
	if err != nil {
		return nil, fmt.Errorf("opening buntdb: %w", err)
	}
	return &BuntQueue{
		uid:     ksuid.New(),
		opts:    opts,
		db:      db,
		storage: b,
	}, nil
}

func (s *BuntQueue) Stats(ctx context.Context, stats *Stats) error {
	tx, err := s.db.Begin(false)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	var iterErr error
	now := time.Now().UTC()

	err = tx.AscendGreaterOrEqual("", "", func(key, value string) bool {
		var item Item
		if err := json.Unmarshal([]byte(value), &item); err != nil {
			iterErr = fmt.Errorf("during unmarshal of db value: %w", err)
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
		return fmt.Errorf("during AscendGreaterOrEqual(): %w", err)
	}

	if err = tx.Rollback(); err != nil {
		return fmt.Errorf("during rollback: %w", err)
	}

	if iterErr != nil {
		return iterErr
	}

	stats.AverageAge = time.Duration(int64(stats.AverageAge) / int64(stats.Total))
	stats.AverageReservedAge = time.Duration(int64(stats.AverageReservedAge) / int64(stats.TotalReserved))

	return nil
}

func (s *BuntQueue) Reserve(_ context.Context, items *[]*Item, opts ReserveOptions) error {
	tx, err := s.db.Begin(false)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	var iterErr error
	var count int

	err = tx.AscendGreaterOrEqual("", "", func(key, value string) bool {
		if count >= opts.Limit {
			return false
		}
		// TODO: Grab from the memory pool
		item := new(Item)
		if err := json.Unmarshal([]byte(value), item); err != nil {
			iterErr = fmt.Errorf("during unmarshal of db value: %w", err)
			return false
		}

		if item.IsReserved {
			return true
		}

		item.ReserveDeadline = opts.ReserveDeadline
		item.IsReserved = true
		*items = append(*items, item)
		count++
		return true
	})

	if err != nil {
		return fmt.Errorf("during AscendGreaterOrEqual(): %w", err)
	}

	if err = tx.Rollback(); err != nil {
		return fmt.Errorf("during rollback: %w", err)
	}

	if iterErr != nil {
		return iterErr
	}

	// Update all the reserved items in the database
	tx, err = s.db.Begin(true)
	if err != nil {
		return fmt.Errorf("during writable begin: %w", err)
	}

	for i := 0; i < len(*items); i++ {
		if err := buntSet(tx, (*items)[i]); err != nil {
			return err
		}
		(*items)[i].ID = s.storage.CreateID(s.opts.Name, (*items)[i].ID)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("during writable commit: %w", err)
	}

	return nil
}

func (s *BuntQueue) Read(_ context.Context, items *[]*Item, pivot string, limit int) error {
	tx, err := s.db.Begin(false)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	var iterErr error
	var count int
	var sid StorageID
	if pivot != "" {
		if err := s.storage.ParseID(pivot, &sid); err != nil {
			return transport.NewInvalidRequest("invalid storage id; '%s': %s", pivot, err)
		}
	}

	err = tx.AscendGreaterOrEqual("", sid.ID, func(key, value string) bool {
		if count >= limit {
			return false
		}
		// TODO: Grab from the memory pool
		item := new(Item)
		if err := json.Unmarshal([]byte(value), item); err != nil {
			iterErr = fmt.Errorf("during unmarshal of db value: %w", err)
			return false
		}
		item.ID = s.storage.CreateID(s.opts.Name, item.ID)
		*items = append(*items, item)
		count++
		return true
	})
	if err != nil {
		return fmt.Errorf("during AscendGreaterOrEqual(): %w", err)
	}

	if err = tx.Rollback(); err != nil {
		return fmt.Errorf("during rollback: %w", err)
	}

	if iterErr != nil {
		return iterErr
	}
	return nil
}

func (s *BuntQueue) Write(_ context.Context, items []*Item) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	for _, item := range items {
		s.uid = s.uid.Next()
		item.ID = s.uid.String()

		if err := buntSet(tx, item); err != nil {
			return err
		}
		item.ID = s.storage.CreateID(s.opts.Name, item.ID)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("during commit: %w", err)
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
			return transport.NewInvalidRequest("invalid storage id; '%s': %s", id, err)
		}
		_, err := tx.Delete(sid.ID)
		if err != nil {
			return fmt.Errorf("during delete: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("during commit: %w", err)
	}
	return nil
}

func (s *BuntQueue) Close(_ context.Context) error {
	return s.db.Close()
}

func (s *BuntQueue) Options() QueueOptions {
	return s.opts
}

func buntSet(tx *buntdb.Tx, item *Item) error {
	// TODO: Use something more efficient like protobuf,
	//	gob or https://github.com/capnproto/go-capnp
	b, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshalling item: %w", err)
	}

	_, _, err = tx.Set(item.ID, string(b), nil)
	if err != nil {
		return fmt.Errorf("while writing item: %w", err)
	}
	return nil
}
