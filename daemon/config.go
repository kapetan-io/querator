package daemon

import (
	"crypto/tls"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal/store"
)

// TODO: Document and configure
type Config struct {
	Logger        duh.StandardLogger
	TLS           *duh.TLSConfig
	Store         store.Storage
	ListenAddress string
	InstanceID    string
}

func (d *Config) ClientTLS() *tls.Config {
	if d.TLS != nil {
		return d.TLS.ClientTLS
	}
	return nil
}

func (d *Config) ServerTLS() *tls.Config {
	if d.TLS != nil {
		return d.TLS.ServerTLS
	}
	return nil
}

func (d *Config) SetDefaults() error {
	if d.Store == nil {
		d.Store = store.NewBoltStorage(store.BoltOptions{})
	}
	return nil
}
