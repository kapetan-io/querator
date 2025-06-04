// Package config provides functionality for loading and parsing Querator
// daemon configuration from a YAML file.The package includes utilities to
// convert the parsed configuration into a form usable by the Querator daemon
// runtime, initializing the appropriate backends and in-memory queue management
// structures.
//
// Additionally, provides access to PartitionStorage and Queue structures in a generic
// format to allow users to instantiate querator "Service" instances in-code.

package config

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/daemon"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/tackle/color"
)

type File struct {
	// TODO(thrawn01): Add support for TLS config
	// TODO(thrawn01): Add support for changing the bind address and port
	Logging          Logging            `yaml:"logging"`
	PartitionStorage []PartitionStorage `yaml:"partition-storage"`
	QueueStorage     QueueStorage       `yaml:"queue-storage"`
	Queues           []Queue            `yaml:"queues"`
	// ConfigFile is the path to the config file that was loaded
	ConfigFile string
}

type Logging struct {
	Level   string `yaml:"level"`
	Handler string `yaml:"handler"`
}

type QueueStorage struct {
	Name   string            `yaml:"name"`
	Driver string            `yaml:"driver"`
	Config map[string]string `yaml:"config"`
}

type PartitionStorage struct {
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

func ApplyConfigFile(ctx context.Context, conf *daemon.Config, file File, w io.Writer) error {
	if err := setupLogger(file, w, conf); err != nil {
		return err
	}

	if err := setupPartitionStorage(file, conf); err != nil {
		return err
	}

	if err := setupQueueStorage(ctx, file, conf); err != nil {
		return err
	}

	// Apply defaults if there are required config items missing from the provided config file
	conf.SetDefaults()

	if file.ConfigFile != "" {
		conf.Log.Info("Loaded config from file", "file", file.ConfigFile)
	}
	return nil
}

func setupLogger(file File, w io.Writer, d *daemon.Config) error {
	switch file.Logging.Handler {
	case "color", "":
		d.Log = slog.New(color.NewLog(&color.LogOptions{
			HandlerOptions: slog.HandlerOptions{
				Level: toLogLevel(file.Logging.Level),
			},
			Writer: w,
		}))
		return nil
	case "text":
		d.Log = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
			Level: toLogLevel(file.Logging.Level),
		}))
		return nil
	case "json":
		d.Log = slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
			Level: toLogLevel(file.Logging.Level),
		}))
		return nil
	default:
		return fmt.Errorf("invalid handler; '%s' is not one of (color, text, json)",
			file.Logging.Handler)
	}
}

func toLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "error":
		return slog.LevelError
	case "warn":
		return slog.LevelWarn
	case "info":
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}

func setupPartitionStorage(file File, d *daemon.Config) error {
	for _, ps := range file.PartitionStorage {
		var s store.PartitionStore

		switch strings.ToLower(ps.Driver) {
		case "memory":
			s = store.NewMemoryPartitionStore(store.Config{}, d.Log)
		case "badger":
			s = store.NewBadgerPartitionStore(store.BadgerConfig{
				StorageDir: ps.Config["storage-dir"], // TODO(thrawn01): validate badger config options
				Log:        d.Log,
			})
		default:
			return fmt.Errorf("invalid driver; '%s' is not one of (Memory, Badger)", ps.Driver)
		}

		d.StorageConfig.PartitionStorage = append(d.StorageConfig.PartitionStorage, store.PartitionStorage{
			Name:           ps.Name,
			Affinity:       float64(ps.Affinity),
			PartitionStore: s,
		})
	}
	return nil
}

func setupQueueStorage(ctx context.Context, file File, conf *daemon.Config) error {
	switch strings.ToLower(file.QueueStorage.Driver) {
	case "memory", "":
		conf.StorageConfig.Queues = store.NewMemoryQueues(conf.Log)
	case "badger":
		conf.StorageConfig.Queues = store.NewBadgerQueues(store.BadgerConfig{
			StorageDir: file.QueueStorage.Config["storage-dir"], // TODO(thrawn01): validate bolt config options
			Log:        conf.Log,
		})
	default:
		return fmt.Errorf("invalid driver; '%s' is not one of (Memory, Badger)", file.QueueStorage.Driver)
	}

	for _, queue := range file.Queues {
		for _, p := range queue.Partitions {
			// Ensure the storage name referenced in the partition exists
			found := store.Find(p.StorageName, conf.StorageConfig.PartitionStorage)
			if found.Name == "" {
				return fmt.Errorf("invalid partition storage; queue '%s' references '%s' which is undefined",
					queue.Name, p.StorageName)
			}
			p.StorageName = found.Name
		}

		if err := conf.StorageConfig.Queues.Add(ctx, queue.ToQueueInfo()); err != nil {
			// Skip if the queue already exists, so pre-existing queues do not keep Querator from starting
			if errors.Is(err, store.ErrQueueAlreadyExists) {
				// TODO(thrawn01): We should probably update the queue if
				//  it already exists as the config might have changed
				continue
			}
			return err
		}
	}
	return nil
}

// ToQueueInfo converts a File.Queue instance to a types.QueueInfo instance
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
