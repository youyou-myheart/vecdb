// Package vectorstore provides an authenticated, deadline-bounded gRPC client.
package vectorstore

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"time"

	pb "example.com/vector-storage/api/vectorstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Options struct {
	Timeout   time.Duration
	TLSConfig *tls.Config
}
type Client struct {
	pb.VectorStoreClient
	conn       *grpc.ClientConn
	timeout    time.Duration
	dialOpts   []grpc.DialOption
	mu         sync.Mutex
	closed     bool
	partitions uint32
	proxyOnly  bool
	routes     map[uint32]*dataRoute
}

// Dial uses plaintext when TLSConfig is nil (for trusted local deployments).
// Writes are never automatically retried by the SDK. Reuse request_id on retry.
func Dial(ctx context.Context, target, token string, opts Options) (*Client, error) {
	if target == "" || token == "" {
		return nil, errors.New("target and API token are required")
	}
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.Timeout < 0 {
		return nil, errors.New("timeout must be positive")
	}
	transport := insecure.NewCredentials()
	var creds credentials.TransportCredentials = transport
	if opts.TLSConfig != nil {
		creds = credentials.NewTLS(opts.TLSConfig.Clone())
	}
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(1<<20)), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoke(ctx, method, req, reply, cc, options...)
	})}
	conn, e := grpc.DialContext(ctx, target, append(append([]grpc.DialOption(nil), dialOpts...), grpc.WithBlock())...)
	if e != nil {
		return nil, e
	}
	return &Client{VectorStoreClient: pb.NewVectorStoreClient(conn), conn: conn, dialOpts: dialOpts, timeout: opts.Timeout, routes: make(map[uint32]*dataRoute)}, nil
}
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for _, r := range c.routes {
		r.conn.Close()
	}
	clear(c.routes)
	return c.conn.Close()
}
func (c *Client) Insert(ctx context.Context, id string, v []float64, payload map[string]string, requestID string) (*pb.MutationResponse, error) {
	zero := uint64(0)
	return c.Put(ctx, &pb.PutRequest{Id: id, Vector: v, Payload: payload, RequestId: requestID, ExpectedRevision: &zero})
}
func (c *Client) Update(ctx context.Context, id string, v []float64, payload map[string]string, revision uint64, requestID string) (*pb.MutationResponse, error) {
	if revision == 0 {
		return nil, errors.New("update revision must be positive")
	}
	return c.Put(ctx, &pb.PutRequest{Id: id, Vector: v, Payload: payload, RequestId: requestID, ExpectedRevision: &revision})
}
