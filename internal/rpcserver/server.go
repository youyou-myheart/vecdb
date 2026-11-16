package rpcserver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"strings"
	"time"

	pb "example.com/vector-storage/api/vectorstore/v1"
	"example.com/vector-storage/internal/vector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Backend interface {
	VectorDimension() int
	Get(context.Context, string) (vector.Record, error)
	Mutate(context.Context, string, vector.Mutation, bool) (vector.Record, error)
	ListRecords(context.Context, vector.ListOptions) (vector.RecordPage, error)
	Sync(context.Context) error
	SearchContext(context.Context, vector.Search) ([]vector.Hit, error)
	Layout() vector.PartitionLayout
	Checkpoint(context.Context) (vector.Manifest, error)
	Rebuild(context.Context) error
	PruneState(context.Context) (any, error)
}
type Server struct {
	pb.UnimplementedVectorStoreServer
	backend   Backend
	namespace string
}

func New(b Backend, token, namespace string, gateway bool, options ...grpc.ServerOption) *grpc.Server {
	options = append(options, grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(64<<20), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		if len(token) < 16 || len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid API_TOKEN")
		}
		switch info.FullMethod {
		case pb.VectorStore_Health_FullMethodName, pb.VectorStore_GetCluster_FullMethodName, pb.VectorStore_GetLayout_FullMethodName:
		default:
			if !gateway {
				return nil, status.Error(codes.PermissionDenied, "gateway role required")
			}
		}
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return handler(ctx, req)
	}))
	s := grpc.NewServer(options...)
	pb.RegisterVectorStoreServer(s, &Server{backend: b, namespace: namespace})
	return s
}
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, vector.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, vector.ErrConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, vector.ErrInvalidList):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}
func validID(id string) bool {
	return id != "" && len(id) <= 256 && !strings.Contains(id, "/") && id != "." && id != ".."
}
func record(r vector.Record) *pb.Record {
	return &pb.Record{Id: r.ID, Revision: r.Revision, Vector: r.Vector, Payload: r.Payload, Deleted: r.Deleted}
}
func mutation(r vector.Record, e error) (*pb.MutationResponse, error) {
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.MutationResponse{Durable: true, Token: &pb.Token{Id: r.ID, Revision: r.Revision}}, nil
}
func (s *Server) Put(ctx context.Context, r *pb.PutRequest) (*pb.MutationResponse, error) {
	if !validID(r.Id) || r.RequestId == "" || len(r.RequestId) > 256 {
		return nil, status.Error(codes.InvalidArgument, "valid id and request_id required")
	}
	if e := vector.ValidateVector(r.Vector, s.backend.VectorDimension()); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	rec, e := s.backend.Mutate(ctx, r.Id, vector.Mutation{RequestID: r.RequestId, Vector: r.Vector, Payload: r.Payload, ExpectedRevision: r.ExpectedRevision}, false)
	return mutation(rec, e)
}
func (s *Server) Get(ctx context.Context, r *pb.GetRequest) (*pb.Record, error) {
	if !validID(r.Id) {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	rec, e := s.backend.Get(ctx, r.Id)
	if e != nil {
		return nil, rpcError(e)
	}
	return record(rec), nil
}
func (s *Server) Delete(ctx context.Context, r *pb.DeleteRequest) (*pb.MutationResponse, error) {
	if !validID(r.Id) || r.RequestId == "" || len(r.RequestId) > 256 {
		return nil, status.Error(codes.InvalidArgument, "valid id and request_id required")
	}
	rec, e := s.backend.Mutate(ctx, r.Id, vector.Mutation{RequestID: r.RequestId, ExpectedRevision: r.ExpectedRevision}, true)
	return mutation(rec, e)
}
func (s *Server) List(ctx context.Context, r *pb.ListRequest) (*pb.ListResponse, error) {
	limit := r.Limit
	if limit == 0 {
		limit = 100
	}
	if limit > 1000 {
		return nil, status.Error(codes.InvalidArgument, "limit must be 1..1000")
	}
	page, e := s.backend.ListRecords(ctx, vector.ListOptions{Limit: int(limit), Cursor: r.Cursor, IncludeDeleted: r.IncludeDeleted})
	if e != nil {
		return nil, rpcError(e)
	}
	result := &pb.ListResponse{NextCursor: page.NextCursor}
	for _, v := range page.Records {
		result.Records = append(result.Records, record(v))
	}
	return result, nil
}
func (s *Server) Search(ctx context.Context, r *pb.SearchRequest) (*pb.SearchResponse, error) {
	if e := vector.ValidateVector(r.Vector, s.backend.VectorDimension()); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	if r.K < 1 || r.K > 1000 || r.EfSearch > 4096 {
		return nil, status.Error(codes.InvalidArgument, "k must be 1..1000; ef_search must be 0..4096")
	}
	q := vector.Search{Vector: r.Vector, K: int(r.K), Filter: r.Filter, Exact: r.Exact, EFSearch: int(r.EfSearch)}
	if r.MinVisible != nil {
		if !validID(r.MinVisible.Id) || r.MinVisible.Revision == 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid visibility token")
		}
		q.MinVisible = &vector.Token{ID: r.MinVisible.Id, Revision: r.MinVisible.Revision}
		if e := s.backend.Sync(ctx); e != nil {
			return nil, rpcError(e)
		}
	}
	hits, e := s.backend.SearchContext(ctx, q)
	if e != nil {
		return nil, rpcError(e)
	}
	result := &pb.SearchResponse{}
	for _, h := range hits {
		result.Hits = append(result.Hits, &pb.Hit{Id: h.ID, Revision: h.Revision, Distance: h.Distance, Payload: h.Payload})
	}
	return result, nil
}
func (s *Server) Health(context.Context, *pb.Empty) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Status: "ready", Namespace: s.namespace}, nil
}

// Operational objects share the existing JSON field names; protojson performs
// typed conversion without converting uint64 epochs/offsets through float64.
func convert(value any, target proto.Message) error {
	raw, e := json.Marshal(value)
	if e == nil {
		e = protojson.Unmarshal(raw, target)
	}
	if e != nil {
		return status.Error(codes.Internal, "response conversion failed")
	}
	return nil
}
func (s *Server) GetLayout(context.Context, *pb.Empty) (*pb.Layout, error) {
	out := &pb.Layout{}
	return out, convert(s.backend.Layout(), out)
}
func (s *Server) GetCluster(ctx context.Context, _ *pb.Empty) (*pb.ClusterStatus, error) {
	provider, ok := s.backend.(interface {
		Status(context.Context) (vector.ClusterStatus, error)
	})
	if !ok {
		return nil, status.Error(codes.Unimplemented, "cluster mode is disabled")
	}
	state, e := provider.Status(ctx)
	if e != nil {
		return nil, rpcError(e)
	}
	out := &pb.ClusterStatus{}
	return out, convert(state, out)
}
func (s *Server) Checkpoint(ctx context.Context, _ *pb.Empty) (*pb.Manifest, error) {
	m, e := s.backend.Checkpoint(ctx)
	if e != nil {
		return nil, rpcError(e)
	}
	out := &pb.Manifest{}
	return out, convert(m, out)
}
func (s *Server) Rebuild(ctx context.Context, _ *pb.Empty) (*pb.Manifest, error) {
	if e := s.backend.Rebuild(ctx); e != nil {
		return nil, rpcError(e)
	}
	return s.Checkpoint(ctx, &pb.Empty{})
}
func (s *Server) Prune(ctx context.Context, _ *pb.Empty) (*pb.PruneResponse, error) {
	state, e := s.backend.PruneState(ctx)
	if e != nil {
		return nil, rpcError(e)
	}
	out := &pb.PruneResponse{}
	return out, convert(state, out)
}

func (s *Server) ReadChanges(ctx context.Context, r *pb.ChangeRequest) (*pb.ChangePage, error) {
	reader, ok := s.backend.(interface {
		ReadChanges(context.Context, vector.ChangeRequest) (vector.ChangePage, error)
	})
	if !ok {
		return nil, status.Error(codes.Unimplemented, "change stream unavailable")
	}
	if r.Limit > 256 {
		return nil, status.Error(codes.InvalidArgument, "limit must be 0..256")
	}
	page, e := reader.ReadChanges(ctx, vector.ChangeRequest{Partition: vector.IndexPartitionID(r.Partition), Stream: r.Stream, After: r.After, Limit: int(r.Limit)})
	if errors.Is(e, vector.ErrInvalidSubscription) {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	if errors.Is(e, vector.ErrSubscriptionUnavailable) {
		return nil, status.Error(codes.Unimplemented, e.Error())
	}
	if e != nil {
		return nil, rpcError(e)
	}
	out := &pb.ChangePage{}
	return out, convert(page, out)
}

func (s *Server) ManageConsumer(ctx context.Context, r *pb.ConsumerRequest) (*pb.ConsumerState, error) {
	manager, ok := s.backend.(interface {
		ManageConsumer(context.Context, vector.ConsumerRequest) (vector.ConsumerState, error)
	})
	if !ok {
		return nil, status.Error(codes.Unimplemented, "consumer leases unavailable")
	}
	if r.TtlSeconds > 3600 {
		return nil, status.Error(codes.InvalidArgument, "TTL exceeds 3600")
	}
	result, e := manager.ManageConsumer(ctx, vector.ConsumerRequest{Partition: vector.IndexPartitionID(r.Partition), Stream: r.Stream, Consumer: r.Consumer, Session: r.Session, Epoch: r.Epoch, Ack: r.Ack, TTLSeconds: int(r.TtlSeconds), Action: r.Action})
	if errors.Is(e, vector.ErrInvalidSubscription) {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	if errors.Is(e, vector.ErrFenced) || errors.Is(e, vector.ErrLogCompacted) {
		return nil, status.Error(codes.FailedPrecondition, e.Error())
	}
	if errors.Is(e, vector.ErrSubscriptionUnavailable) {
		return nil, status.Error(codes.Unimplemented, e.Error())
	}
	if e != nil {
		return nil, rpcError(e)
	}
	out := &pb.ConsumerState{}
	return out, convert(result, out)
}
