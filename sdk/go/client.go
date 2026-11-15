// Package vectorstore provides an authenticated, deadline-bounded gRPC client.
package vectorstore

import (
	"context"
	"crypto/tls"
	"errors"
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
	conn *grpc.ClientConn
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
	conn, e := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(creds), grpc.WithBlock(), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(1<<20)), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoke(ctx, method, req, reply, cc, options...)
	}))
	if e != nil {
		return nil, e
	}
	return &Client{VectorStoreClient: pb.NewVectorStoreClient(conn), conn: conn}, nil
}
func (c *Client) Close() error { return c.conn.Close() }
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
