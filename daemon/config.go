package daemon

import (
	"crypto/tls"
	"github.com/duh-rpc/duh-go"
)

type Config struct {
	TLS           *duh.TLSConfig
	ListenAddress string
	Logger        duh.StandardLogger
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
