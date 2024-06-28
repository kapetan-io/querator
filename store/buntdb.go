package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/buntdb"
	"log/slog"
	"time"
)

type BuntOptions struct {
	File   string
	Logger duh.StandardLogger
}

type Bunt struct {
	db   *buntdb.DB
	opts BuntOptions
	uid  ksuid.KSUID
}

func NewBuntStore(opts BuntOptions) (*Bunt, error) {
	set.Default(&opts.Logger, slog.Default())
	if opts.File == "" {
		return nil, NewInvalidOption("BuntOptions.File cannot be empty")
	}

	// TODO: Check if the file exists

	db, err := buntdb.Open(opts.File)
	if err != nil {
		return nil, fmt.Errorf("opening buntdb: %w", err)
	}
	return &Bunt{
		uid:  ksuid.New(),
		opts: opts,
		db:   db,
	}, nil
}

func (s *Bunt) Stats(ctx context.Context, stats *Stats) error {
	tx, err := s.db.Begin(false)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	var iterErr error
	now := time.Now().UTC()

	err = tx.AscendGreaterOrEqual("", "", func(key, value string) bool {
		var item QueueItem
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

func (s *Bunt) Reserve(_ context.Context, items *[]*QueueItem, opts ReserveOptions) error {
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
		item := new(QueueItem)
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

	for _, item := range *items {
		if err := buntSet(tx, item); err != nil {
			return err
		}
	}

	// TODO: Rollback if the context has been cancelled
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("during writable commit: %w", err)
	}

	return nil
}

func (s *Bunt) Read(_ context.Context, items *[]*QueueItem, pivot string, limit int) error {
	tx, err := s.db.Begin(false)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	var iterErr error
	var count int

	err = tx.AscendGreaterOrEqual("", pivot, func(key, value string) bool {
		if count >= limit {
			return false
		}
		// TODO: Grab from the memory pool
		item := new(QueueItem)
		if err := json.Unmarshal([]byte(value), item); err != nil {
			iterErr = fmt.Errorf("during unmarshal of db value: %w", err)
			return false
		}
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

func (s *Bunt) Write(_ context.Context, items []*QueueItem) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	for _, item := range items {
		if item.ID == "" {
			s.uid = s.uid.Next()
			item.ID = s.uid.String()
		}

		if err := buntSet(tx, item); err != nil {
			return err
		}

	}

	// TODO: Rollback if the context has been cancelled
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("during commit: %w", err)
	}
	return nil
}

func (s *Bunt) Delete(ctx context.Context, items []*QueueItem) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	for _, item := range items {
		_, err := tx.Delete(item.ID)
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

func (s *Bunt) Close(_ context.Context) error {
	return s.db.Close()
}

func (s *Bunt) Options() QueueStorageOptions {
	return QueueStorageOptions{
		// TODO: Make these configurable
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	}
}

func buntSet(tx *buntdb.Tx, item *QueueItem) error {
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
