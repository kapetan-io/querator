package internal

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"sync"

	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/placement"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
)

type StorageBackends struct {
	queues     store.Queues
	configured []store.PartitionStorage
	log        *slog.Logger
	mutex      sync.Mutex
	saved      bool
}

func NewStorageBackends(queues store.Queues, configured []store.PartitionStorage, log *slog.Logger) *StorageBackends {
	return &StorageBackends{queues: queues, configured: configured, log: cmp.Or(log, slog.Default())}
}

func (b *StorageBackends) SaveFirstSeen(ctx context.Context) error {
	defer b.mutex.Unlock()
	b.mutex.Lock()
	return b.saveFirstSeen(ctx)
}

func (b *StorageBackends) saveFirstSeen(ctx context.Context) error {
	if b.saved {
		return nil
	}
	for _, backend := range b.configured {
		err := b.queues.AddBackend(ctx, types.Backend{Name: backend.Name, Affinity: backend.Affinity})
		if errors.Is(err, types.ErrBackendAlreadyExists) {
			continue
		}
		if errors.Is(err, &reply.ErrInvalidOption{}) {
			b.log.Warn("storage backend not saved; no partition is placed on it", "backend", backend.Name, "error", err)
			continue
		}
		if err != nil {
			return errors.Errorf("while saving storage backend '%s': %w", backend.Name, err)
		}
	}
	b.saved = true
	b.warnChangedAffinity(ctx)
	return nil
}

func (b *StorageBackends) warnChangedAffinity(ctx context.Context) {
	var saved []types.Backend
	if err := b.queues.ListBackends(ctx, &saved); err != nil {
		return
	}
	for _, backend := range saved {
		file := store.Find(backend.Name, b.configured)
		if file.Name != "" && file.Affinity != backend.Affinity {
			b.log.Warn("config file affinity differs from the saved affinity; the saved affinity is in force, "+
				"run 'querator backends update "+backend.Name+" --affinity <n>' to change it",
				"backend", backend.Name, "file", file.Affinity, "saved", backend.Affinity)
		}
	}
}

func (b *StorageBackends) List(ctx context.Context) ([]types.Backend, error) {
	defer b.mutex.Unlock()
	b.mutex.Lock()
	return b.list(ctx)
}

func (b *StorageBackends) list(ctx context.Context) ([]types.Backend, error) {
	if err := b.saveFirstSeen(ctx); err != nil {
		return nil, err
	}
	var saved []types.Backend
	if err := b.queues.ListBackends(ctx, &saved); err != nil {
		return nil, errors.Errorf("Queues.ListBackends(): %w", err)
	}
	return saved, nil
}

func (b *StorageBackends) Configured(name string) bool {
	return store.Find(name, b.configured).Name != ""
}

func (b *StorageBackends) Placeable(ctx context.Context) ([]types.Backend, error) {
	saved, err := b.List(ctx)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(saved, func(backend types.Backend) bool {
		return !b.Configured(backend.Name)
	}), nil
}

func (b *StorageBackends) PlaceableUnsaved(ctx context.Context) ([]types.Backend, error) {
	defer b.mutex.Unlock()
	b.mutex.Lock()
	var saved []types.Backend
	if err := b.queues.ListBackends(ctx, &saved); err != nil {
		return nil, errors.Errorf("Queues.ListBackends(): %w", err)
	}
	placeable := make([]types.Backend, 0, len(b.configured))
	for _, backend := range b.configured {
		found := slices.IndexFunc(saved, func(s types.Backend) bool { return s.Name == backend.Name })
		if found >= 0 {
			placeable = append(placeable, saved[found])
			continue
		}
		placeable = append(placeable, types.Backend{Name: backend.Name, Affinity: backend.Affinity})
	}
	return placeable, nil
}

type BackendUpdate struct {
	Name     string
	Affinity *float64
}

func (b *StorageBackends) Update(ctx context.Context, update BackendUpdate) error {
	defer b.mutex.Unlock()
	b.mutex.Lock()

	saved, err := b.list(ctx)
	if err != nil {
		return err
	}

	var found bool
	proposed := make([]types.Backend, 0, len(saved))
	for _, backend := range saved {
		if backend.Name == update.Name {
			found = true
			if update.Affinity != nil {
				backend.Affinity = *update.Affinity
			}
		}
		proposed = append(proposed, backend)
	}
	if !found {
		return unknownBackend(update.Name)
	}
	if update.Affinity == nil {
		return nil
	}
	if err := placement.CheckBackends(proposed); err != nil {
		return reply.NewInvalidOption("affinity is invalid; %s", err.Error())
	}

	err = b.queues.UpdateBackend(ctx, types.Backend{Name: update.Name, Affinity: *update.Affinity})
	if errors.Is(err, &reply.ErrInvalidOption{}) {
		return err
	}
	if errors.Is(err, types.ErrBackendNotExist) {
		return unknownBackend(update.Name)
	}
	if err != nil {
		return errors.Errorf("Queues.UpdateBackend(): %w", err)
	}
	return nil
}

func unknownBackend(name string) error {
	return reply.NewInvalidOption("backend is invalid; '%s' is not a known storage backend", name)
}
