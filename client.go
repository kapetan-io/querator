package querator

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	"github.com/kapetan-io/querator/internal"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/tackle/set"
	"google.golang.org/protobuf/proto"
	"net/http"
	"time"
)

type ClientOptions struct {
	// Users can provide their own http client with TLS config if needed
	Client *http.Client
	// The address of endpoint in the format `<scheme>://<host>:<port>`
	Endpoint string
}

type Client struct {
	client *duh.Client
	opts   ClientOptions
}

// NewClient creates a new instance of the Gubernator user client
func NewClient(opts ClientOptions) (*Client, error) {
	set.Default(&opts.Client, &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost:     2_000,
			MaxIdleConns:        2_000,
			MaxIdleConnsPerHost: 2_000,
			IdleConnTimeout:     60 * time.Second,
		},
	})

	if len(opts.Endpoint) == 0 {
		return nil, errors.New("opts.Endpoint is empty; must provide an http endpoint")
	}

	return &Client{
		client: &duh.Client{
			Client: opts.Client,
		},
		opts: opts,
	}, nil
}

func (c *Client) QueueProduce(ctx context.Context, req *pb.QueueProduceRequest, res *pb.QueueProduceResponse) error {
	payload, err := proto.Marshal(req)
	if err != nil {
		return duh.NewClientError("while marshaling request payload: %w", err, nil)
	}

	r, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s%s", c.opts.Endpoint, internal.RPCQueueProduce), bytes.NewReader(payload))
	if err != nil {
		return duh.NewClientError("", err, nil)
	}

	r.Header.Set("Content-Type", duh.ContentTypeProtoBuf)
	return c.client.Do(r, res)
}

// WithNoTLS returns ClientOptions suitable for use with NON-TLS clients
func WithNoTLS(address string) ClientOptions {
	return ClientOptions{
		Endpoint: fmt.Sprintf("http://%s", address),
		Client: &http.Client{
			Transport: &http.Transport{
				MaxConnsPerHost:     2_000,
				MaxIdleConns:        2_000,
				MaxIdleConnsPerHost: 2_000,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
}

// WithTLS returns ClientOptions suitable for use with TLS clients
func WithTLS(tls *tls.Config, address string) ClientOptions {
	return ClientOptions{
		Endpoint: fmt.Sprintf("https://%s", address),
		Client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:     tls,
				MaxConnsPerHost:     2_000,
				MaxIdleConns:        2_000,
				MaxIdleConnsPerHost: 2_000,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
}
