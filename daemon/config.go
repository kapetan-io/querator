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

	// AuthBackend is the authentication and authorization backend.
	// If nil, defaults to NoOpAuthBackend which allows all requests (open access).
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

	// Default to NoOp auth (open access for internal deployments)
	set.Default(&c.AuthBackend, auth.AuthBackend(&auth.NoOpAuthBackend{}))

	// Bridge: ensure service layer uses the same auth backend as the transport
	if c.Service.Auth == nil {
		c.Service.Auth = c.AuthBackend
	}
}

// YAML config file types

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

// QueueStorage selects the backend that stores queue metadata. Exactly one backend section
// (memory, badger, or mongo) may be set. When none is set, the in-memory backend is used.
type QueueStorage struct {
	Memory *MemoryConfig `yaml:"memory"`
	Badger *BadgerConfig `yaml:"badger"`
	Mongo  *MongoConfig  `yaml:"mongo"`
}

// PartitionStorage selects the backend for a named partition store. Exactly one backend section
// (memory, badger, or mongo) must be set.
type PartitionStorage struct {
	Name     string        `yaml:"name"`
	Affinity int           `yaml:"affinity"`
	Memory   *MemoryConfig `yaml:"memory"`
	Badger   *BadgerConfig `yaml:"badger"`
	Mongo    *MongoConfig  `yaml:"mongo"`
}

// MemoryConfig configures the in-memory backend. It accepts no options; select it with an empty
// mapping (`memory: {}`).
type MemoryConfig struct{}

// BadgerConfig configures the BadgerDB backend.
type BadgerConfig struct {
	StorageDir string `yaml:"storage-dir"`
}

func (c BadgerConfig) validate() error {
	if c.StorageDir == "" {
		return errors.New("badger: 'storage-dir' is required")
	}
	return nil
}

// MongoConfig configures the MongoDB backend.
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

// ReadConfig decodes a Querator YAML config from r with strict field checking enabled. Unknown or
// misspelled keys — including a config option placed under the wrong backend — are reported as an
// error instead of being silently ignored.
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

// selected reports the name of the single backend that is set and how many are set, so callers can
// enforce the "exactly one backend" rule with a clear error.
func selected(memory, badger, mongo bool) (name string, count int) {
	if memory {
		name, count = "memory", count+1
	}
	if badger {
		name, count = "badger", count+1
	}
	if mongo {
		name, count = "mongo", count+1
	}
	return name, count
}

func backendErr(what string, count int) error {
	if count == 0 {
		return fmt.Errorf("%s must define exactly one backend (memory, badger, mongo)", what)
	}
	return fmt.Errorf("%s defines multiple backends; only one of (memory, badger, mongo) is allowed", what)
}

func setupPartitionStorage(file File, d *Config) error {
	for _, ps := range file.PartitionStorage {
		name, count := selected(ps.Memory != nil, ps.Badger != nil, ps.Mongo != nil)
		if count != 1 {
			return backendErr(fmt.Sprintf("partition storage '%s'", ps.Name), count)
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
	name, count := selected(qs.Memory != nil, qs.Badger != nil, qs.Mongo != nil)
	if count > 1 {
		return backendErr("queue storage", count)
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
	default:
		return fmt.Errorf("unknown backend %q", name)
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

func setupAuthBackend(file File, conf *Config) error {
	switch strings.ToLower(file.AuthBackend.Driver) {
	case "", "none":
		// NoOp auth (open access) — applied in SetDefaults
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
