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
	Client   *http.Client
	Endpoint string
	APIKey   string
}

type Client struct {
	client *duh.Client
	conf   ClientConfig
}

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

type doArgs struct {
	Request proto.Message
	Reply   proto.Message
	Path    string
}

func (c *Client) do(ctx context.Context, call doArgs) error {
	payload, err := proto.Marshal(call.Request)
	if err != nil {
		return duh.NewClientError("while marshaling request payload: %w", err, nil)
	}

	r, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s%s", c.conf.Endpoint, call.Path), bytes.NewReader(payload))
	if err != nil {
		return duh.NewClientError("", err, nil)
	}

	r.Header.Set("Content-Type", duh.ContentTypeProtoBuf)
	if c.conf.APIKey != "" {
		r.Header.Set("Authorization", "Bearer "+c.conf.APIKey)
	}
	return c.client.Do(r, call.Reply)
}

func (c *Client) QueueProduce(ctx context.Context, req *pb.QueueProduceRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueueProduce, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) QueueLease(ctx context.Context, req *pb.QueueLeaseRequest, res *pb.QueueLeaseResponse) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueueLease, Request: req, Reply: res})
}

func (c *Client) QueueComplete(ctx context.Context, req *pb.QueueCompleteRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueueComplete, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) QueueRetry(ctx context.Context, req *pb.QueueRetryRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueueRetry, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) QueueClear(ctx context.Context, req *pb.QueueClearRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueueClear, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) QueueReload(ctx context.Context, req *pb.QueueReloadRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueueReload, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) QueuesCreate(ctx context.Context, req *pb.QueueInfo) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueuesCreate, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) QueuesList(ctx context.Context, res *pb.QueuesListResponse, opts *ListOptions) error {
	var req pb.QueuesListRequest
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
		req.Namespace = opts.Namespace
	}
	return c.do(ctx, doArgs{Path: transport.RPCQueuesList, Request: &req, Reply: res})
}

func (c *Client) QueuesUpdate(ctx context.Context, req *pb.QueuesUpdateRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueuesUpdate, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) QueuesDelete(ctx context.Context, req *pb.QueuesDeleteRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueuesDelete, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) QueuesInfo(ctx context.Context, req *pb.QueuesInfoRequest, res *pb.QueueInfo) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueuesInfo, Request: req, Reply: res})
}

func (c *Client) StorageItemsList(ctx context.Context, name string, partition int, res *pb.StorageItemsListResponse,
	opts *ListOptions) error {

	if opts == nil {
		opts = &ListOptions{}
	}

	return c.do(ctx, doArgs{
		Path: transport.RPCStorageItemsList,
		Request: &pb.StorageItemsListRequest{
			Limit:     int32(opts.Limit),
			Partition: int32(partition),
			Pivot:     opts.Pivot,
			QueueName: name,
		},
		Reply: res,
	})
}

func (c *Client) StorageScheduledList(ctx context.Context, name string, partition int, res *pb.StorageItemsListResponse,
	opts *ListOptions) error {

	if opts == nil {
		opts = &ListOptions{}
	}

	return c.do(ctx, doArgs{
		Path: transport.RPCStorageScheduledList,
		Request: &pb.StorageItemsListRequest{
			Limit:     int32(opts.Limit),
			Partition: int32(partition),
			Pivot:     opts.Pivot,
			QueueName: name,
		},
		Reply: res,
	})
}

func (c *Client) StorageItemsImport(ctx context.Context, req *pb.StorageItemsImportRequest,
	res *pb.StorageItemsImportResponse) error {
	return c.do(ctx, doArgs{Path: transport.RPCStorageItemsImport, Request: req, Reply: res})
}

func (c *Client) StorageItemsDelete(ctx context.Context, req *pb.StorageItemsDeleteRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCStorageItemsDelete, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) QueueStats(ctx context.Context, req *pb.QueueStatsRequest,
	res *pb.QueueStatsResponse) error {
	return c.do(ctx, doArgs{Path: transport.RPCQueueStats, Request: req, Reply: res})
}

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

func WithConn(conn net.Conn) ClientConfig {
	return ClientConfig{
		Endpoint: "http://memory",
		Client: &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
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

func (c *Client) NamespacesCreate(ctx context.Context, req *pb.NamespaceInfo) error {
	return c.do(ctx, doArgs{Path: transport.RPCNamespacesCreate, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) NamespacesUpdate(ctx context.Context, req *pb.NamespaceInfo) error {
	return c.do(ctx, doArgs{Path: transport.RPCNamespacesUpdate, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) NamespacesList(ctx context.Context, res *pb.NamespacesListResponse, opts *ListOptions) error {
	var req pb.NamespacesListRequest
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
	}
	return c.do(ctx, doArgs{Path: transport.RPCNamespacesList, Request: &req, Reply: res})
}

func (c *Client) NamespacesDelete(ctx context.Context, req *pb.NamespacesDeleteRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCNamespacesDelete, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) UsersCreate(ctx context.Context, req *pb.UserCreateRequest,
	res *pb.UserCreateResponse) error {
	return c.do(ctx, doArgs{Path: transport.RPCUsersCreate, Request: req, Reply: res})
}

func (c *Client) UsersList(ctx context.Context, res *pb.UsersListResponse, opts *ListOptions) error {
	var req pb.UsersListRequest
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
	}
	return c.do(ctx, doArgs{Path: transport.RPCUsersList, Request: &req, Reply: res})
}

func (c *Client) UsersDelete(ctx context.Context, req *pb.UsersDeleteRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCUsersDelete, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) APIKeysCreate(ctx context.Context, req *pb.APIKeyCreateRequest,
	res *pb.APIKeyCreateResponse) error {
	return c.do(ctx, doArgs{Path: transport.RPCAPIKeysCreate, Request: req, Reply: res})
}

func (c *Client) APIKeysList(ctx context.Context, res *pb.APIKeysListResponse, opts *ListOptions) error {
	var req pb.APIKeysListRequest
	if opts != nil {
		req.Limit = int32(opts.Limit)
		req.Pivot = opts.Pivot
	}
	return c.do(ctx, doArgs{Path: transport.RPCAPIKeysList, Request: &req, Reply: res})
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
	return c.do(ctx, doArgs{Path: transport.RPCAPIKeysList, Request: &req, Reply: res})
}

func (c *Client) APIKeysDelete(ctx context.Context, req *pb.APIKeysDeleteRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCAPIKeysDelete, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) RolesCreate(ctx context.Context, req *pb.RoleCreateRequest,
	res *pb.RoleCreateResponse) error {
	return c.do(ctx, doArgs{Path: transport.RPCRolesCreate, Request: req, Reply: res})
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
	return c.do(ctx, doArgs{Path: transport.RPCRolesList, Request: &req, Reply: res})
}

func (c *Client) RolesUpdate(ctx context.Context, req *pb.RoleUpdateRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCRolesUpdate, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) RolesDelete(ctx context.Context, req *pb.RolesDeleteRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCRolesDelete, Request: req, Reply: &v1.Reply{}})
}

func (c *Client) RoleBindingsCreate(ctx context.Context, req *pb.RoleBindingCreateRequest,
	res *pb.RoleBindingCreateResponse) error {
	return c.do(ctx, doArgs{Path: transport.RPCRoleBindingsCreate, Request: req, Reply: res})
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
	return c.do(ctx, doArgs{Path: transport.RPCRoleBindingsList, Request: &req, Reply: res})
}

func (c *Client) RoleBindingsDelete(ctx context.Context, req *pb.RoleBindingDeleteRequest) error {
	return c.do(ctx, doArgs{Path: transport.RPCRoleBindingsDelete, Request: req, Reply: &v1.Reply{}})
}
