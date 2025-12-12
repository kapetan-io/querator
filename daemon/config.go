package daemon

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/service"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/color"
	"github.com/kapetan-io/tackle/set"
)

type Config struct {
	// Explicit composition instead of embedding
	Service service.Config

	// TLS is the TLS config used for public server and clients
	TLS *duh.TLSConfig
	// ListenAddress is the address:port that Querator will listen on for public HTTP requests
	ListenAddress string

	// MaxProducePayloadSize is the maximum size in bytes Querator will read from a client
	// during the `/queue.produce` request. The Maximum size includes the entire payload for a
	// single `/queue.produce` request including the size of all fields in the marshalled protobuf.
	// The default size is 5MB.
	MaxProducePayloadSize int64

	// InMemoryListener is true if daemon should ignore ListenAddress and use net.Pipe to listen for
	// and handle new connections. When true, calls to Daemon.Client() and Daemon.MustClient() will return
	// a new instance of the client bound to the client portion of a net.Pipe. This is useful for testing
	// querator where access to the loop back is not allowed, or when using testing/synctest
	InMemoryListener bool
}

func (c *Config) ClientTLS() *tls.Config {
	if c.TLS != nil {
		return c.TLS.ClientTLS
	}
	return nil
}

func (c *Config) ServerTLS() *tls.Config {
	if c.TLS != nil {
		return c.TLS.ServerTLS
	}
	return nil
}

func (c *Config) SetDefaults() {
	set.Default(&c.Service.Clock, clock.NewProvider())
	set.Default(&c.Service.Log, slog.Default())
	set.Default(&c.ListenAddress, "localhost:2319")
	set.Default(&c.Service.MaxLeaseBatchSize, internal.DefaultMaxLeaseBatchSize)
	set.Default(&c.Service.MaxProduceBatchSize, internal.DefaultMaxProduceBatchSize)
	set.Default(&c.Service.MaxCompleteBatchSize, internal.DefaultMaxCompleteBatchSize)
	set.Default(&c.Service.MaxRequestsPerQueue, internal.DefaultMaxRequestsPerQueue)
	set.Default(&c.Service.MaxConcurrentRequests, internal.DefaultMaxConcurrentConnections)
	set.Default(&c.Service.StorageConfig.Queues, store.NewMemoryQueues(c.Service.Log))
	set.Default(&c.Service.StorageConfig.PartitionStorage, []store.PartitionStorage{
		{
			PartitionStore: store.NewMemoryPartitionStore(c.Service.StorageConfig, c.Service.Log),
			Name:           "mem-0",
			Affinity:       1,
		},
	})
}

// YAML config file types

type File struct {
	Address          string             `yaml:"address"`
	Logging          Logging            `yaml:"logging"`
	PartitionStorage []PartitionStorage `yaml:"partition-storage"`
	QueueStorage     QueueStorage       `yaml:"queue-storage"`
	Queues           []Queue            `yaml:"queues"`
	ConfigFile       string
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

func ApplyConfigFile(ctx context.Context, conf *Config, file File, w io.Writer) error {
	if err := setupLogger(file, w, conf); err != nil {
		return err
	}

	if err := setupPartitionStorage(file, conf); err != nil {
		return err
	}

	if err := setupQueueStorage(ctx, file, conf); err != nil {
		return err
	}

	if file.Address != "" {
		conf.ListenAddress = file.Address
	}

	conf.SetDefaults()

	if file.ConfigFile != "" {
		conf.Service.Log.Info("Loaded config from file", "file", file.ConfigFile)
	}
	return nil
}

func setupLogger(file File, w io.Writer, d *Config) error {
	switch file.Logging.Handler {
	case "color", "":
		d.Service.Log = slog.New(color.NewLog(&color.LogOptions{
			HandlerOptions: slog.HandlerOptions{
				Level: toLogLevel(file.Logging.Level),
			},
			Writer: w,
		}))
		return nil
	case "text":
		d.Service.Log = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
			Level: toLogLevel(file.Logging.Level),
		}))
		return nil
	case "json":
		d.Service.Log = slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
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

func setupPartitionStorage(file File, d *Config) error {
	for _, ps := range file.PartitionStorage {
		var s store.PartitionStore

		switch strings.ToLower(ps.Driver) {
		case "memory":
			s = store.NewMemoryPartitionStore(store.Config{}, d.Service.Log)
		case "badger":
			s = store.NewBadgerPartitionStore(store.BadgerConfig{
				StorageDir: ps.Config["storage-dir"],
				Log:        d.Service.Log,
			})
		default:
			return fmt.Errorf("invalid driver; '%s' is not one of (Memory, Badger)", ps.Driver)
		}

		d.Service.StorageConfig.PartitionStorage = append(d.Service.StorageConfig.PartitionStorage, store.PartitionStorage{
			Name:           ps.Name,
			Affinity:       float64(ps.Affinity),
			PartitionStore: s,
		})
	}
	return nil
}

func setupQueueStorage(ctx context.Context, file File, conf *Config) error {
	switch strings.ToLower(file.QueueStorage.Driver) {
	case "memory", "":
		conf.Service.StorageConfig.Queues = store.NewMemoryQueues(conf.Service.Log)
	case "badger":
		conf.Service.StorageConfig.Queues = store.NewBadgerQueues(store.BadgerConfig{
			StorageDir: file.QueueStorage.Config["storage-dir"],
			Log:        conf.Service.Log,
		})
	default:
		return fmt.Errorf("invalid driver; '%s' is not one of (Memory, Badger)", file.QueueStorage.Driver)
	}

	for _, queue := range file.Queues {
		for _, p := range queue.Partitions {
			found := store.Find(p.StorageName, conf.Service.StorageConfig.PartitionStorage)
			if found.Name == "" {
				return fmt.Errorf("invalid partition storage; queue '%s' references '%s' which is undefined",
					queue.Name, p.StorageName)
			}
			p.StorageName = found.Name
		}

		if err := conf.Service.StorageConfig.Queues.Add(ctx, queue.ToQueueInfo()); err != nil {
			if errors.Is(err, store.ErrQueueAlreadyExists) {
				continue
			}
			return err
		}
	}
	return nil
}

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

func (p Partition) ToPartitionInfo() types.PartitionInfo {
	return types.PartitionInfo{
		StorageName:  p.StorageName,
		ReadOnly:     p.ReadOnly,
		PartitionNum: p.Partition,
	}
}
