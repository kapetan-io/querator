package querator

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	"github.com/kapetan-io/querator/internal"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
	"github.com/kapetan-io/tackle/set"
	"google.golang.org/protobuf/proto"
	"net"
	"net/http"
)

// Version is the current version of Querator, set at build time via ldflags:
//
//	go build -ldflags "-X github.com/kapetan-io/querator.Version=1.0.0"
var Version = "dev-build"

const (
	MsgRequestTimeout    = internal.MsgRequestTimeout
	MsgDuplicateClientID = internal.MsgDuplicateClientID
	MsgServiceInShutdown = internal.MsgServiceInShutdown
	MsgQueueInShutdown   = internal.MsgQueueInShutdown
	MsgQueueOverLoaded   = internal.MsgQueueOverLoaded
)

type ListOptions struct {
	Pivot     string
	Limit     int
	Namespace string
}

type ClientConfig struct {
	// Users can provide their own http client with TLS config if needed
	Client *http.Client
	// The address of endpoint in the format `<scheme>://<host>:<port>`
	Endpoint string
	// APIKey is the API key used for authentication (sent as Bearer token)
	APIKey string
}

type Client struct {
	client *duh.Client
	conf   ClientConfig
}

// NewClient creates a new instance of the Gubernator user client
func NewClient(conf ClientConfig) (*Client, error) {
	set.Default(&conf.Client, &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost:     5_000,
			MaxIdleConns:        5_000,
			MaxIdleConnsPerHost: 5_000,
			IdleConnTimeout:     60 * clock.Second,
		},
	})

	if len(conf.Endpoint) == 0 {
		return nil, errors.New("conf.Endpoint is empty; must provide an http endpoint")
	}

	return &Client{
		client: &duh.Client{
			Client: conf.Client,
		},
		conf: conf,
	}, nil
}

func (c *Client) do(ctx context.Context, path string, req, res proto.Message) error {
	payload, err := proto.Marshal(req)
	if err != nil {
		return duh.NewClientError("while marshaling request payload: %w", err, nil)
	}

	r, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s%s", c.conf.Endpoint, path), bytes.NewReader(payload))
	if err != nil {
		return duh.NewClientError("", err, nil)
	}

	r.Header.Set("Content-Type", duh.ContentTypeProtoBuf)
	if c.conf.APIKey != "" {
		r.Header.Set("Authorization", "Bearer "+c.conf.APIKey)
	}
	return c.client.Do(r, res)
}

func (c *Client) QueueProduce(ctx context.Context, req *pb.QueueProduceRequest) error {
	return c.do(ctx, transport.RPCQueueProduce, req, &v1.Reply{})
}

func (c *Client) QueueLease(ctx context.Context, req *pb.QueueLeaseRequest, res *pb.QueueLeaseResponse) error {
	return c.do(ctx, transport.RPCQueueLease, req, res)
}

func (c *Client) QueueComplete(ctx context.Context, req *pb.QueueCompleteRequest) error {
	return c.do(ctx, transport.RPCQueueComplete, req, &v1.Reply{})
}

func (c *Client) QueueRetry(ctx context.Context, req *pb.QueueRetryRequest) error {
	return c.do(ctx, transport.RPCQueueRetry, req, &v1.Reply{})
}

func (c *Client) QueueClear(ctx context.Context, req *pb.QueueClearRequest) error {
	return c.do(ctx, transport.RPCQueueClear, req, &v1.Reply{})
}

func (c *Client) QueueReload(ctx context.Context, req *pb.QueueReloadRequest) error {
	return c.do(ctx, transport.RPCQueueReload, req, &v1.Reply{})
}

// -------------------------------------------------
// API to manage lists of queues
// -------------------------------------------------

func (c *Client) QueuesCreate(ctx context.Context, req *pb.QueueInfo) error {
	return c.do(ctx, transport.RPCQueuesCreate, req, &v1.Reply{})
}

func (c *Client) QueuesList(ctx context.Context, res *pb.QueuesListResponse, opts *ListOptions) error {
	var req pb.QueuesListRequest
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
		req.Namespace = opts.Namespace
	}
	return c.do(ctx, transport.RPCQueuesList, &req, res)
}

func (c *Client) QueuesUpdate(ctx context.Context, req *pb.QueueInfo) error {
	return c.do(ctx, transport.RPCQueuesUpdate, req, &v1.Reply{})
}

func (c *Client) QueuesDelete(ctx context.Context, req *pb.QueuesDeleteRequest) error {
	return c.do(ctx, transport.RPCQueuesDelete, req, &v1.Reply{})
}

func (c *Client) QueuesInfo(ctx context.Context, req *pb.QueuesInfoRequest, res *pb.QueueInfo) error {
	return c.do(ctx, transport.RPCQueuesInfo, req, res)
}

// TODO: Write an iterator we can use to iterate through list APIs
// TODO(scheduled): Listing enqueued items should NOT include scheduled items

// StorageItemsList lists current items in the queue.
// # NOTE
// If the pivot does not exist when calling StorageItemsList(), the endpoint will return the
// nearest next item in the list to the pivot provided. It is up to the caller to verify the
// list of items returned begins with the id specified in the pivot. This allows users to iterate
// through a constantly moving list without constantly running into "pivot not found" errors.
func (c *Client) StorageItemsList(ctx context.Context, name string, partition int, res *pb.StorageItemsListResponse,
	opts *ListOptions) error {

	if opts == nil {
		opts = &ListOptions{}
	}

	return c.do(ctx, transport.RPCStorageItemsList, &pb.StorageItemsListRequest{
		Limit:     int32(opts.Limit),
		Partition: int32(partition),
		Pivot:     opts.Pivot,
		QueueName: name,
	}, res)
}

// StorageScheduledList lists scheduled items in the partition.
// # NOTE
// If the pivot does not exist when calling StorageScheduledList(), the endpoint will return the
// nearest next item in the list to the pivot provided. It is up to the caller to verify the
// list of items returned begins with the id specified in the pivot. This allows users to iterate
// through a constantly moving list without constantly running into "pivot not found" errors.
func (c *Client) StorageScheduledList(ctx context.Context, name string, partition int, res *pb.StorageItemsListResponse,
	opts *ListOptions) error {

	if opts == nil {
		opts = &ListOptions{}
	}

	return c.do(ctx, transport.RPCStorageScheduledList, &pb.StorageItemsListRequest{
		Limit:     int32(opts.Limit),
		Partition: int32(partition),
		Pivot:     opts.Pivot,
		QueueName: name,
	}, res)
}

func (c *Client) StorageItemsImport(ctx context.Context, req *pb.StorageItemsImportRequest,
	res *pb.StorageItemsImportResponse) error {
	return c.do(ctx, transport.RPCStorageItemsImport, req, res)
}

func (c *Client) StorageItemsDelete(ctx context.Context, req *pb.StorageItemsDeleteRequest) error {
	return c.do(ctx, transport.RPCStorageItemsDelete, req, &v1.Reply{})
}

func (c *Client) QueueStats(ctx context.Context, req *pb.QueueStatsRequest,
	res *pb.QueueStatsResponse) error {
	return c.do(ctx, transport.RPCQueueStats, req, res)
}

// WithNoTLS returns ClientConfig suitable for use with NON-TLS clients
func WithNoTLS(address string) ClientConfig {
	return ClientConfig{
		Endpoint: fmt.Sprintf("http://%s", address),
		Client: &http.Client{
			Transport: &http.Transport{
				MaxConnsPerHost:     2_000,
				MaxIdleConns:        2_000,
				MaxIdleConnsPerHost: 2_000,
				IdleConnTimeout:     60 * clock.Second,
			},
		},
	}
}

// WithTLS returns ClientConfig suitable for use with TLS clients
func WithTLS(tls *tls.Config, address string) ClientConfig {
	return ClientConfig{
		Endpoint: fmt.Sprintf("https://%s", address),
		Client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:     tls,
				MaxConnsPerHost:     2_000,
				MaxIdleConns:        2_000,
				MaxIdleConnsPerHost: 2_000,
				IdleConnTimeout:     60 * clock.Second,
			},
		},
	}
}

// WithConn returns ClientConfig suitable for use with a custom net.Conn connection
func WithConn(conn net.Conn) ClientConfig {
	return ClientConfig{
		Endpoint: "http://memory",
		Client: &http.Client{
			Transport: &http.Transport{
				Dial: func(network, addr string) (net.Conn, error) {
					return conn, nil
				},
				MaxConnsPerHost:     1,
				MaxIdleConns:        1,
				MaxIdleConnsPerHost: 1,
				IdleConnTimeout:     60 * clock.Second,
			},
		},
	}
}

type ItemsWithIDs interface {
	GetId() string
}

func CollectIDs[S ~[]E, E ItemsWithIDs](items S) []string {
	var result []string
	for _, v := range items {
		result = append(result, v.GetId())
	}
	return result
}

// -------------------------------------------------
// Namespace Management API
// -------------------------------------------------

func (c *Client) NamespacesCreate(ctx context.Context, req *pb.NamespaceInfo) error {
	return c.do(ctx, transport.RPCNamespacesCreate, req, &v1.Reply{})
}

func (c *Client) NamespacesUpdate(ctx context.Context, req *pb.NamespaceInfo) error {
	return c.do(ctx, transport.RPCNamespacesUpdate, req, &v1.Reply{})
}

func (c *Client) NamespacesList(ctx context.Context, res *pb.NamespacesListResponse, opts *ListOptions) error {
	var req pb.NamespacesListRequest
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
	}
	return c.do(ctx, transport.RPCNamespacesList, &req, res)
}

func (c *Client) NamespacesDelete(ctx context.Context, req *pb.NamespacesDeleteRequest) error {
	return c.do(ctx, transport.RPCNamespacesDelete, req, &v1.Reply{})
}

// -------------------------------------------------
// User Management API
// -------------------------------------------------

func (c *Client) UsersCreate(ctx context.Context, req *pb.UserCreateRequest,
	res *pb.UserCreateResponse) error {
	return c.do(ctx, transport.RPCUsersCreate, req, res)
}

func (c *Client) UsersList(ctx context.Context, res *pb.UsersListResponse, opts *ListOptions) error {
	var req pb.UsersListRequest
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
	}
	return c.do(ctx, transport.RPCUsersList, &req, res)
}

func (c *Client) UsersDelete(ctx context.Context, req *pb.UsersDeleteRequest) error {
	return c.do(ctx, transport.RPCUsersDelete, req, &v1.Reply{})
}

// -------------------------------------------------
// API Key Management API
// -------------------------------------------------

func (c *Client) APIKeysCreate(ctx context.Context, req *pb.APIKeyCreateRequest,
	res *pb.APIKeyCreateResponse) error {
	return c.do(ctx, transport.RPCAPIKeysCreate, req, res)
}

func (c *Client) APIKeysList(ctx context.Context, res *pb.APIKeysListResponse, opts *ListOptions) error {
	var req pb.APIKeysListRequest
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
	}
	return c.do(ctx, transport.RPCAPIKeysList, &req, res)
}

func (c *Client) APIKeysListByUser(ctx context.Context, userID string, res *pb.APIKeysListResponse,
	opts *ListOptions) error {
	req := pb.APIKeysListRequest{
		UserId: userID,
	}
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
	}
	return c.do(ctx, transport.RPCAPIKeysList, &req, res)
}

func (c *Client) APIKeysDelete(ctx context.Context, req *pb.APIKeysDeleteRequest) error {
	return c.do(ctx, transport.RPCAPIKeysDelete, req, &v1.Reply{})
}

// -------------------------------------------------
// Role Management API
// -------------------------------------------------

func (c *Client) RolesCreate(ctx context.Context, req *pb.RoleCreateRequest,
	res *pb.RoleCreateResponse) error {
	return c.do(ctx, transport.RPCRolesCreate, req, res)
}

func (c *Client) RolesList(ctx context.Context, namespace string, res *pb.RolesListResponse,
	opts *ListOptions) error {
	req := pb.RolesListRequest{
		Namespace: namespace,
	}
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
	}
	return c.do(ctx, transport.RPCRolesList, &req, res)
}

func (c *Client) RolesUpdate(ctx context.Context, req *pb.RoleUpdateRequest) error {
	return c.do(ctx, transport.RPCRolesUpdate, req, &v1.Reply{})
}

func (c *Client) RolesDelete(ctx context.Context, req *pb.RolesDeleteRequest) error {
	return c.do(ctx, transport.RPCRolesDelete, req, &v1.Reply{})
}

// -------------------------------------------------
// Role Binding Management API
// -------------------------------------------------

func (c *Client) RoleBindingsCreate(ctx context.Context, req *pb.RoleBindingCreateRequest,
	res *pb.RoleBindingCreateResponse) error {
	return c.do(ctx, transport.RPCRoleBindingsCreate, req, res)
}

func (c *Client) RoleBindingsList(ctx context.Context, namespace string, res *pb.RoleBindingsListResponse,
	opts *ListOptions) error {
	req := pb.RoleBindingsListRequest{
		Namespace: namespace,
	}
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
	}
	return c.do(ctx, transport.RPCRoleBindingsList, &req, res)
}

func (c *Client) RoleBindingsDelete(ctx context.Context, req *pb.RoleBindingDeleteRequest) error {
	return c.do(ctx, transport.RPCRoleBindingsDelete, req, &v1.Reply{})
}