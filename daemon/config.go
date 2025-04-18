package daemon

import (
	"crypto/tls"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"log/slog"
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
	// The default size is 1MB.
	MaxProducePayloadSize int64
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
	set.Default(&c.MaxReserveBatchSize, internal.DefaultMaxReserveBatchSize)
	set.Default(&c.MaxProduceBatchSize, internal.DefaultMaxProduceBatchSize)
	set.Default(&c.MaxCompleteBatchSize, internal.DefaultMaxCompleteBatchSize)
	set.Default(&c.MaxRequestsPerQueue, internal.DefaultMaxRequestsPerQueue)
	set.Default(&c.MaxConcurrentRequests, internal.DefaultMaxConcurrentConnections)
	set.Default(&c.StorageConfig.Queues, store.NewMemoryQueues(c.Log))
	set.Default(&c.StorageConfig.Backends, []store.Backend{
		{
			PartitionStore: store.NewMemoryPartitionStore(c.StorageConfig, c.Log),
			Name:           "mem-0",
			Affinity:       1,
		},
	})
}

// TODO: Load from config system
