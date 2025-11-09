package daemon

import (
	"crypto/tls"
	"log/slog"

	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
)

type Config struct {
	// See ServiceConfig for a list of possible options
	querator.ServiceConfig
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
	set.Default(&c.Clock, clock.NewProvider())
	set.Default(&c.Log, slog.Default())
	set.Default(&c.ListenAddress, "localhost:2319")
	set.Default(&c.MaxLeaseBatchSize, internal.DefaultMaxLeaseBatchSize)
	set.Default(&c.MaxProduceBatchSize, internal.DefaultMaxProduceBatchSize)
	set.Default(&c.MaxCompleteBatchSize, internal.DefaultMaxCompleteBatchSize)
	set.Default(&c.MaxRequestsPerQueue, internal.DefaultMaxRequestsPerQueue)
	set.Default(&c.MaxConcurrentRequests, internal.DefaultMaxConcurrentConnections)
	set.Default(&c.StorageConfig.Queues, store.NewMemoryQueues(c.Log))
	set.Default(&c.StorageConfig.PartitionStorage, []store.PartitionStorage{
		{
			PartitionStore: store.NewMemoryPartitionStore(c.StorageConfig, c.Log),
			Name:           "mem-0",
			Affinity:       1,
		},
	})
}
