package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/tackle/set"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/buntdb"
	"log/slog"
)

type BuntOptions struct {
	Logger duh.StandardLogger
}

type Bunt struct {
	db   *buntdb.DB
	opts BuntOptions
}

func NewBuntStore(opts BuntOptions) (*Bunt, error) {
	set.Default(&opts.Logger, slog.Default())

	db, err := buntdb.Open(":memory:")
	if err != nil {
		return nil, fmt.Errorf("opening buntdb: %w", err)
	}
	return &Bunt{
		db: db,
	}, nil
}

func (s *Bunt) GetReservable(ctx context.Context, items []*QueueItem, limit int) error {
	//TODO implement me
	panic("implement me")
}

func (s *Bunt) Reserve(ctx context.Context, items []*QueueItem, limit int) error {
	//TODO implement me
	panic("implement me")
}

func (s *Bunt) ReadItems(ctx context.Context, items []*QueueItem, pivot string, limit int) error {
	tx, err := s.db.Begin(false)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	var iterErr error
	var count int
	err = tx.DescendEqual("", pivot, func(key, value string) bool {
		if count >= limit {
			return false
		}
		// TODO: Grab from the memory pool
		item := new(QueueItem)
		if err := json.Unmarshal([]byte(value), item); err != nil {
			iterErr = fmt.Errorf("during unmarshal of db value: %w", err)
			return false
		}
		items = append(items, item)
		count++
		return true
	})
	if err != nil {
		return fmt.Errorf("during DescendEqual(): %w", err)
	}

	if err = tx.Rollback(); err != nil {
		return fmt.Errorf("during rollback: %w", err)
	}

	if iterErr != nil {
		return iterErr
	}
	return nil
}

func (s *Bunt) WriteItems(_ context.Context, items []*QueueItem) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return fmt.Errorf("during begin: %w", err)
	}

	for _, record := range items {
		if record.ID == "" {
			record.ID = ksuid.New().String()
		}

		// TODO: Use something more efficient like protobuf,
		//  gob or https://github.com/capnproto/go-capnp
		b, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshalling record: %w", err)
		}

		_, _, err = tx.Set(record.ID, string(b), nil)
		if err != nil {
			return fmt.Errorf("while writing record: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("during commit: %w", err)
	}
	return nil
}

func (s *Bunt) DeleteItems(ctx context.Context, items []*QueueItem) error {
	//TODO implement me
	panic("implement me")
}

func (s *Bunt) Close(ctx context.Context) error {
	return s.db.Close()
}

type idxItem struct {
	ID  string
	Key string
}
