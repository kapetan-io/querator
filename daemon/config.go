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
	"github.com/kapetan-io/querator/transport/auth"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/color"
	"github.com/kapetan-io/tackle/set"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Service service.Config

	TLS           *duh.TLSConfig
	ListenAddress string

	MaxProducePayloadSize int64

	InMemoryListener bool

	AuthBackend auth.AuthBackend
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

	set.Default(&c.AuthBackend, auth.AuthBackend(&auth.NoOpAuthBackend{}))

	if c.Service.Auth == nil {
		c.Service.Auth = c.AuthBackend
	}
}

type File struct {
	Address          string             `yaml:"address"`
	Logging          Logging            `yaml:"logging"`
	PartitionStorage []PartitionStorage `yaml:"partition-storage"`
	QueueStorage     QueueStorage       `yaml:"queue-storage"`
	AuthBackend      AuthConfig         `yaml:"auth-backend"`
	Queues           []Queue            `yaml:"queues"`
	ConfigFile       string
}

type AuthConfig struct {
	Driver string `yaml:"driver"`
}

type Logging struct {
	Level   string `yaml:"level"`
	Handler string `yaml:"handler"`
}

type QueueStorage struct {
	Memory   *MemoryConfig   `yaml:"memory"`
	Badger   *BadgerConfig   `yaml:"badger"`
	Mongo    *MongoConfig    `yaml:"mongo"`
	Postgres *PostgresConfig `yaml:"postgres"`
}

type PartitionStorage struct {
	Name     string          `yaml:"name"`
	Affinity int             `yaml:"affinity"`
	Memory   *MemoryConfig   `yaml:"memory"`
	Badger   *BadgerConfig   `yaml:"badger"`
	Mongo    *MongoConfig    `yaml:"mongo"`
	Postgres *PostgresConfig `yaml:"postgres"`
}

type MemoryConfig struct{}

type BadgerConfig struct {
	StorageDir string `yaml:"storage-dir"`
}

func (c BadgerConfig) validate() error {
	if c.StorageDir == "" {
		return errors.New("badger: 'storage-dir' is required")
	}
	return nil
}

type MongoConfig struct {
	ConnectionString string `yaml:"connection-string"`
	Database         string `yaml:"database"`
	MaxPoolSize      uint64 `yaml:"max-pool-size"`
}

func (c MongoConfig) validate() error {
	if c.ConnectionString == "" {
		return errors.New("mongo: 'connection-string' is required")
	}
	return nil
}

type PostgresConfig struct {
	ConnectionString string `yaml:"connection-string"`
	MaxConns         int32  `yaml:"max-conns"`
	ScanBatchSize    int    `yaml:"scan-batch-size"`
}

func (c PostgresConfig) validate() error {
	if c.ConnectionString == "" {
		return errors.New("postgres: 'connection-string' is required")
	}
	return nil
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

func ReadConfig(r io.Reader) (File, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	var file File
	if err := dec.Decode(&file); err != nil {
		return File{}, fmt.Errorf("while reading config file: %w", err)
	}
	return file, nil
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

	if err := setupAuthBackend(file, conf); err != nil {
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

type backends struct {
	Memory   bool
	Badger   bool
	Mongo    bool
	Postgres bool
}

func (b backends) selected(section string) (string, error) {
	var names []string
	if b.Memory {
		names = append(names, "memory")
	}
	if b.Badger {
		names = append(names, "badger")
	}
	if b.Mongo {
		names = append(names, "mongo")
	}
	if b.Postgres {
		names = append(names, "postgres")
	}
	if len(names) > 1 {
		return "", fmt.Errorf("%s defines multiple backends; only one of (memory, badger, mongo, postgres) is allowed", section)
	}
	if len(names) == 0 {
		return "", nil
	}
	return names[0], nil
}

func setupPartitionStorage(file File, d *Config) error {
	for _, ps := range file.PartitionStorage {
		section := fmt.Sprintf("partition storage '%s'", ps.Name)
		name, err := backends{
			Memory:   ps.Memory != nil,
			Badger:   ps.Badger != nil,
			Mongo:    ps.Mongo != nil,
			Postgres: ps.Postgres != nil,
		}.selected(section)
		if err != nil {
			return err
		}
		if name == "" {
			return fmt.Errorf("%s must define exactly one backend (memory, badger, mongo, postgres)", section)
		}

		var s store.PartitionStore
		switch name {
		case "memory":
			s = store.NewMemoryPartitionStore(store.Config{}, d.Service.Log)
		case "badger":
			if err := ps.Badger.validate(); err != nil {
				return err
			}
			s = store.NewBadgerPartitionStore(store.BadgerConfig{
				StorageDir: ps.Badger.StorageDir,
				Log:        d.Service.Log,
			})
		case "mongo":
			if err := ps.Mongo.validate(); err != nil {
				return err
			}
			s = store.NewMongoPartitionStore(store.MongoConfig{
				ConnectionString: ps.Mongo.ConnectionString,
				Database:         ps.Mongo.Database,
				MaxPoolSize:      ps.Mongo.MaxPoolSize,
				Log:              d.Service.Log,
			})
		case "postgres":
			if err := ps.Postgres.validate(); err != nil {
				return err
			}
			s = store.NewPostgresPartitionStore(store.PostgresConfig{
				ConnectionString: ps.Postgres.ConnectionString,
				MaxConns:         ps.Postgres.MaxConns,
				ScanBatchSize:    ps.Postgres.ScanBatchSize,
				Log:              d.Service.Log,
			})
		default:
			return fmt.Errorf("unknown backend %q", name)
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
	qs := file.QueueStorage
	name, err := backends{
		Memory:   qs.Memory != nil,
		Badger:   qs.Badger != nil,
		Mongo:    qs.Mongo != nil,
		Postgres: qs.Postgres != nil,
	}.selected("queue storage")
	if err != nil {
		return err
	}

	switch name {
	case "", "memory":
		conf.Service.StorageConfig.Queues = store.NewMemoryQueues(conf.Service.Log)
	case "badger":
		if err := qs.Badger.validate(); err != nil {
			return err
		}
		conf.Service.StorageConfig.Queues = store.NewBadgerQueues(store.BadgerConfig{
			StorageDir: qs.Badger.StorageDir,
			Log:        conf.Service.Log,
		})
	case "mongo":
		if err := qs.Mongo.validate(); err != nil {
			return err
		}
		conf.Service.StorageConfig.Queues = store.NewMongoQueues(store.MongoConfig{
			ConnectionString: qs.Mongo.ConnectionString,
			Database:         qs.Mongo.Database,
			MaxPoolSize:      qs.Mongo.MaxPoolSize,
			Log:              conf.Service.Log,
		})
	case "postgres":
		if err := qs.Postgres.validate(); err != nil {
			return err
		}
		conf.Service.StorageConfig.Queues = store.NewPostgresQueues(store.PostgresConfig{
			ConnectionString: qs.Postgres.ConnectionString,
			MaxConns:         qs.Postgres.MaxConns,
			ScanBatchSize:    qs.Postgres.ScanBatchSize,
			Log:              conf.Service.Log,
		})
	default:
		return fmt.Errorf("unknown backend %q", name)
	}

	for _, queue := range file.Queues {
		var stored types.QueueInfo
		if err := conf.Service.StorageConfig.Queues.Get(ctx, queue.Name, &stored); err == nil {
			continue
		}
		if len(queue.Partitions) == 0 && !store.CanPlacePartitions(conf.Service.StorageConfig.PartitionStorage) {
			return fmt.Errorf("invalid queue '%s'; no storage backend can take partitions, "+
				"at least one partition storage needs an affinity above 0", queue.Name)
		}
		for _, p := range queue.Partitions {
			found := store.Find(p.StorageName, conf.Service.StorageConfig.PartitionStorage)
			if found.Name == "" {
				return fmt.Errorf("invalid partition storage; queue '%s' references '%s' which is undefined",
					queue.Name, p.StorageName)
			}
			p.StorageName = found.Name
		}

		info := queue.ToQueueInfo()
		if len(queue.Partitions) == 0 {
			placed, err := store.PlacePartitions(conf.Service.StorageConfig.PartitionStorage, info)
			if err != nil {
				return fmt.Errorf("invalid queue '%s'; %w", queue.Name, err)
			}
			info.PartitionInfo = placed
		}

		if err := conf.Service.StorageConfig.Queues.Add(ctx, info); err != nil {
			if errors.Is(err, store.ErrQueueAlreadyExists) {
				continue
			}
			return err
		}
	}
	return nil
}

func setupAuthBackend(file File, conf *Config) error {
	switch strings.ToLower(file.AuthBackend.Driver) {
	case "", "none":
	case "internal":
		conf.AuthBackend = internal.NewAuthBackend(internal.AuthBackendConfig{
			RoleBindings: conf.Service.StorageConfig.RoleBindings,
			APIKeys:      conf.Service.StorageConfig.APIKeys,
			Users:        conf.Service.StorageConfig.Users,
			Roles:        conf.Service.StorageConfig.Roles,
			Log:          conf.Service.Log,
		})
	default:
		return fmt.Errorf("invalid auth-backend driver; '%s' is not one of (none, internal)",
			file.AuthBackend.Driver)
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
