package vectorstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"net"

	pb "example.com/vector-storage/api/vectorstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dataRoute struct {
	route *pb.WriteRoute
	link  *dataRouteConnection
	stale bool
}

// Route generations are immutable, but a refreshed generation at the same
// endpoint can share its transport. A timeout must not cause a reconnect storm.
type dataRouteConnection struct {
	conn    *grpc.ClientConn
	stub    pb.VectorStoreClient
	users   int
	retired bool
}

func shardFor(id string, count uint32) uint32 {
	digest := sha256.Sum256([]byte(id))
	return uint32(binary.BigEndian.Uint64(digest[:8]) % uint64(count))
}
func (c *Client) acquireRoute(ctx context.Context, id string) (*dataRoute, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, grpc.ErrClientConnClosing
	}
	if c.proxyOnly {
		c.mu.Unlock()
		return nil, nil
	}
	if c.partitions > 0 {
		if r := c.routes[shardFor(id, c.partitions)]; r != nil && !r.stale {
			r.link.users++
			c.mu.Unlock()
			return r, nil
		}
	}
	c.mu.Unlock()
	hint, err := c.VectorStoreClient.ResolveWriteRoute(ctx, &pb.GetRequest{Id: id})
	if err != nil && status.Code(err) != codes.Unimplemented {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, grpc.ErrClientConnClosing
	}
	if status.Code(err) == codes.Unimplemented || hint.GetEndpoint() == "" {
		// No advertised worker for this lookup. Only old servers lack discovery permanently.
		c.proxyOnly = status.Code(err) == codes.Unimplemented || hint.GetPartitions() == 0
		return nil, nil
	}
	if _, _, err = net.SplitHostPort(hint.Endpoint); err != nil || hint.Partitions == 0 || hint.Partitions > 64 || hint.Partition != shardFor(id, hint.Partitions) {
		return nil, status.Error(codes.Internal, "invalid data write route")
	}
	c.partitions = hint.Partitions
	old := c.routes[hint.Partition]
	if old != nil && !old.stale {
		old.link.users++
		return old, nil
	}
	var link *dataRouteConnection
	if old != nil && old.route.Endpoint == hint.Endpoint {
		link = old.link
	} else {
		conn, err := grpc.DialContext(ctx, hint.Endpoint, c.dialOpts...)
		if err != nil {
			return nil, err
		}
		link = &dataRouteConnection{conn: conn, stub: pb.NewVectorStoreClient(conn)}
	}
	if old != nil && old.link != link {
		old.link.retired = true
		if old.link.users == 0 {
			old.link.conn.Close()
		}
	}
	r := &dataRoute{route: hint, link: link}
	link.users++
	c.routes[hint.Partition] = r
	return r, nil
}
func (c *Client) releaseRoute(r *dataRoute, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	code := status.Code(err)
	if code == codes.FailedPrecondition || code == codes.Unavailable || code == codes.DeadlineExceeded {
		if c.routes[r.route.Partition] == r {
			r.stale = true
		}
	}
	r.link.users--
	if r.link.retired && r.link.users == 0 {
		r.link.conn.Close()
	}
}
func (c *Client) write(ctx context.Context, id string, call func(context.Context, pb.VectorStoreClient, *pb.WriteRoute) (*pb.MutationResponse, error)) (*pb.MutationResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	for attempt := 0; ; attempt++ {
		r, err := c.acquireRoute(ctx, id)
		if err != nil {
			return nil, err
		}
		if r == nil {
			return call(ctx, c.VectorStoreClient, nil)
		}
		reply, err := call(ctx, r.link.stub, r.route)
		c.releaseRoute(r, err)
		if attempt != 0 || status.Code(err) != codes.FailedPrecondition {
			return reply, err
		}
	}
}
func (c *Client) Put(ctx context.Context, q *pb.PutRequest, opts ...grpc.CallOption) (*pb.MutationResponse, error) {
	if q == nil {
		q = &pb.PutRequest{}
	}
	return c.write(ctx, q.GetId(), func(ctx context.Context, stub pb.VectorStoreClient, r *pb.WriteRoute) (*pb.MutationResponse, error) {
		return stub.Put(ctx, &pb.PutRequest{Id: q.GetId(), Vector: q.GetVector(), Payload: q.GetPayload(), RequestId: q.GetRequestId(), ExpectedRevision: q.ExpectedRevision, Route: r}, opts...)
	})
}
func (c *Client) Delete(ctx context.Context, q *pb.DeleteRequest, opts ...grpc.CallOption) (*pb.MutationResponse, error) {
	if q == nil {
		q = &pb.DeleteRequest{}
	}
	return c.write(ctx, q.GetId(), func(ctx context.Context, stub pb.VectorStoreClient, r *pb.WriteRoute) (*pb.MutationResponse, error) {
		return stub.Delete(ctx, &pb.DeleteRequest{Id: q.GetId(), RequestId: q.GetRequestId(), ExpectedRevision: q.ExpectedRevision, Route: r}, opts...)
	})
}
