// Package config provides functionality for loading and parsing Querator
// daemon configuration from a YAML file.The package includes utilities to
// convert the parsed configuration into a form usable by the Querator daemon
// runtime, initializing the appropriate backends and in-memory queue management
// structures.
//
// Additionally provides access to Backend and Queue structures in a generic
// format to allow users to instantiate querator "Service" instances in-code.

package config

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/clock"
	"gopkg.in/yaml.v3"
)

const (
	driverTypeMemory = "Memory"
	driverTypeBadger = "Badger"
)

type Config struct {
	Backends []Backend `yaml:"backends"`
	Queues   []Queue   `yaml:"queues"`
}

type Backend struct {
	Name     string            `yaml:"name"`
	Driver   string            `yaml:"driver"`
	Affinity int               `yaml:"affinity"`
	Config   map[string]string `yaml:"config"`
}

type Queue struct {
	Name                string        `yaml:"name"`
	DeadQueue           string        `yaml:"dead-queue"`
	LeaseTimeout        time.Duration `yaml:"lease-timeout"`
	ExpireTimeout       time.Duration `yaml:"expire-timeout"`
	MaxAttempts         int           `yaml:"max-attempts"`
	Reference           string        `yaml:"reference"`
	RequestedPartitions int           `yaml:"requested-partitions"`
	Partitions          []Partition   `yaml:"partitions"`
}

type Partition struct {
	Partition   int    `yaml:"partition"`
	ReadOnly    bool   `yaml:"read-only"`
	StorageName string `yaml:"storage-name"`
}

func SetupDaemonConfig(ctx context.Context, d *daemon.Config, reader io.Reader, stdout io.Writer) error {
	var conf Config
	decoder := yaml.NewDecoder(reader)
	if err := decoder.Decode(&conf); err != nil {
		return err
	}

	// TODO: Setup Logging based on the config provided
	var log = slog.New(slog.NewTextHandler(stdout, nil))

	if err := d.SetDefaults(); err != nil {
		return err
	}

	// setup backend for daemon
	backends := make([]store.Backend, 0, len(conf.Backends))
	for _, backend := range conf.Backends {
		partitionStore, err := getPartitionStore(backend.Driver, backend.Config, log)
		if err != nil {
			return err
		}

		backends = append(backends, store.Backend{
			Name:           backend.Name,
			Affinity:       float64(backend.Affinity),
			PartitionStore: partitionStore,
		})
	}

	d.StorageConfig.Backends = backends

	// setup queues for daemon
	// TODO(thrawn01): Shouldn't always be the in-memory queue
	queues := store.NewMemoryQueues(log)
	for _, queue := range conf.Queues {
		// TODO(thrawn01): doesn't handle if the queue already exists
		err := queues.Add(ctx, queue.ToQueueInfo())
		if err != nil {
			return err
		}
	}
	// Should add queued
	d.StorageConfig.Queues = queues

	return nil
}

// getPartitionStore converts a provided driver type and config map to a store.PartitionStore struct. Useful
// when manually defining a store.StorageConfig instance.
func getPartitionStore(driverType string, config map[string]string, log *slog.Logger) (store.PartitionStore, error) {
	var partitionStore store.PartitionStore

	switch driverType {
	case driverTypeMemory:
		partitionStore = store.NewMemoryPartitionStore(store.StorageConfig{Clock: clock.NewProvider()}, log)
	case driverTypeBadger:
		partitionStore = store.NewBadgerPartitionStore(store.BadgerConfig{Clock: clock.NewProvider(), Log: log, StorageDir: config["storage-dir"]})
	default:
		return partitionStore, ErrUnsupportedBackendDriverType{driverType: driverType}
	}

	return partitionStore, nil
}

// ToQueueInfo converts a Config.Queue instance to a types.QueueInfo instance
func (q Queue) ToQueueInfo() types.QueueInfo {
	partitionInfo := make([]types.PartitionInfo, 0, len(q.Partitions))

	for _, partition := range q.Partitions {
		partitionInfo = append(partitionInfo, partition.ToPartitionInfo())
	}

	return types.QueueInfo{
		Name:                q.Name,
		DeadQueue:           q.DeadQueue,
		LeaseTimeout:        q.LeaseTimeout,
		ExpireTimeout:       q.ExpireTimeout,
		MaxAttempts:         q.MaxAttempts,
		Reference:           q.Reference,
		RequestedPartitions: q.RequestedPartitions,
		PartitionInfo:       partitionInfo,
	}
}

// ToPartitionInfo will convert a config.Partition instance to a types.PartitionInfo instance
func (p Partition) ToPartitionInfo() types.PartitionInfo {
	return types.PartitionInfo{
		StorageName:  p.StorageName,
		ReadOnly:     p.ReadOnly,
		PartitionNum: p.Partition,
	}
}
