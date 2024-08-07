package daemon

import (
	"crypto/tls"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator"
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/internal/store"
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

func (c *Config) SetDefaults() error {
	if c.Storage == nil {
		c.Storage = store.NewBoltStorage(store.BoltConfig{})
	}
	set.Default(&c.Logger, slog.Default())
	set.Default(&c.MaxReserveBatchSize, internal.DefaultMaxReserveBatchSize)
	set.Default(&c.MaxProduceBatchSize, internal.DefaultMaxProduceBatchSize)
	set.Default(&c.MaxCompleteBatchSize, internal.DefaultMaxCompleteBatchSize)
	set.Default(&c.MaxRequestsPerQueue, internal.DefaultMaxRequestsPerQueue)
	return nil
}

// TODO: Load from config system
