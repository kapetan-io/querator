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
	"log/slog"
	"os"
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

// LoadFile accepts a path string to a predefined config yaml file
func LoadFile(path string) (*Config, error) {
	if path == "" {
		return nil, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, ErrFileNotExist{Msg: err.Error()}
	}

	var cfg Config

	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, ErrYAMLParse{Msg: err.Error()}
	}

	return &cfg, nil
}

// ToDaemonConfig converts a Config struct to a daemon.Config struct
func (cfg *Config) ToDaemonConfig(ctx context.Context, log *slog.Logger) (daemon.Config, error) {
	var daemonCfg daemon.Config

	daemonCfg.SetDefaults()

	// setup backend for daemon
	backends := make([]store.Backend, 0, len(cfg.Backends))
	for _, backend := range cfg.Backends {
		partitionStore, err := getPartitionStore(backend.Driver, backend.Config, log)
		if err != nil {
			return daemon.Config{}, err
		}

		backends = append(backends, store.Backend{
			Name:           backend.Name,
			Affinity:       float64(backend.Affinity),
			PartitionStore: partitionStore,
		})
	}
	daemonCfg.StorageConfig.Backends = backends

	// setup queues for daemon
	queues := store.NewMemoryQueues(log)
	for _, queue := range cfg.Queues {
		queues.Add(ctx, queue.ToQueueInfo())
	}
	daemonCfg.StorageConfig.Queues = queues

	return daemonCfg, nil
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
