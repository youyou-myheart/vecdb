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
	roles, ok := b.(interface{ HasRole(string) bool })
	worker := ok && roles.HasRole(vector.RoleWorker)
	options = append(options, grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(64<<20), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		if len(token) < 16 || len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid API_TOKEN")
		}
		switch info.FullMethod {
		case pb.VectorStore_Health_FullMethodName, pb.VectorStore_GetCluster_FullMethodName, pb.VectorStore_GetLayout_FullMethodName:
		case pb.VectorStore_Put_FullMethodName:
			if !gateway && (!worker || req.(*pb.PutRequest).GetRoute() == nil) {
				return nil, status.Error(codes.PermissionDenied, "direct data route required")
			}
		case pb.VectorStore_Delete_FullMethodName:
			if !gateway && (!worker || req.(*pb.DeleteRequest).GetRoute() == nil) {
				return nil, status.Error(codes.PermissionDenied, "direct data route required")
			}
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
	case errors.Is(err, vector.ErrFollowerQueueFull):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, vector.ErrFollowerReadUnsupported):
		return status.Error(codes.FailedPrecondition, err.Error())
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

type dataWriter interface {
	ResolveWriteOwner(context.Context, string) (vector.DataPartitionID, vector.Ownership, error)
	WriteNamespace() string
	MutateOwned(context.Context, string, vector.DataPartitionID, uint64, string, string, vector.Mutation, bool) (vector.Record, error)
}

func (s *Server) ResolveWriteRoute(ctx context.Context, r *pb.GetRequest) (*pb.WriteRoute, error) {
	if !validID(r.Id) {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	w, ok := s.backend.(dataWriter)
	if !ok {
		return &pb.WriteRoute{}, nil
	}
	part, o, err := w.ResolveWriteOwner(ctx, r.Id)
	if err != nil {
		return nil, rpcError(err)
	}
	if o.Partition == "" {
		return &pb.WriteRoute{}, nil
	}
	return &pb.WriteRoute{Partition: uint32(part), Partitions: uint32(s.backend.Layout().DataPartitions), Namespace: w.WriteNamespace(), Endpoint: o.WriteEndpoint, Epoch: o.Epoch, Session: o.Session}, nil
}
func (s *Server) applyMutation(ctx context.Context, route *pb.WriteRoute, id string, m vector.Mutation, deleted bool) (*pb.MutationResponse, error) {
	if route == nil {
		return mutation(s.backend.Mutate(ctx, id, m, deleted))
	}
	w, ok := s.backend.(dataWriter)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "stale data write route")
	}
	rec, err := w.MutateOwned(ctx, route.Namespace, vector.DataPartitionID(route.Partition), route.Epoch, route.Session, id, m, deleted)
	if errors.Is(err, vector.ErrFenced) {
		return nil, status.Error(codes.FailedPrecondition, "stale data write route")
	}
	return mutation(rec, err)
}
func (s *Server) Put(ctx context.Context, r *pb.PutRequest) (*pb.MutationResponse, error) {
	if !validID(r.Id) || r.RequestId == "" || len(r.RequestId) > 256 {
		return nil, status.Error(codes.InvalidArgument, "valid id and request_id required")
	}
	if e := vector.ValidateVector(r.Vector, s.backend.VectorDimension()); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	return s.applyMutation(ctx, r.Route, r.Id, vector.Mutation{RequestID: r.RequestId, Vector: r.Vector, Payload: r.Payload, ExpectedRevision: r.ExpectedRevision}, false)
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
	return s.applyMutation(ctx, r.Route, r.Id, vector.Mutation{RequestID: r.RequestId, ExpectedRevision: r.ExpectedRevision}, true)
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

func (s *Server) FollowerSearch(ctx context.Context, r *pb.SearchRequest) (*pb.SearchResponse, error) {
	q := vector.Search{Vector: r.Vector, K: int(r.K), Filter: r.Filter, Exact: r.Exact, EFSearch: int(r.EfSearch)}
	if r.MinVisible != nil {
		return nil, status.Error(codes.InvalidArgument, "follower read cannot be combined with min_visible")
	}
	if err := vector.ValidateFollowerSearch(q, s.backend.VectorDimension()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	backend, ok := s.backend.(interface {
		FollowerSearch(context.Context, vector.Search) (vector.FollowerSearchResult, error)
	})
	if !ok {
		return nil, rpcError(vector.ErrFollowerReadUnsupported)
	}
	result, err := backend.FollowerSearch(ctx, q)
	if err != nil {
		return nil, rpcError(err)
	}
	out := &pb.SearchResponse{}
	if err := convert(result, out); err != nil {
		return nil, err
	}
	return out, nil
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
	state := s.backend.Layout()
	routing := state.Routing
	state.Routing = nil
	err := convert(state, out)
	out.Routing = rpcRouting(routing)
	return out, err
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
	routing := state.Routing
	state.Routing = nil
	err := convert(state, out)
	out.Routing = rpcRouting(routing)
	return out, err
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

func rpcRouting(r *vector.ClusterRouting) *pb.ClusterRouting {
	if r == nil {
		return nil
	}
	out := &pb.ClusterRouting{Version: r.Version}
	for _, v := range r.Centers {
		out.Centers = append(out.Centers, &pb.Center{Vector: append([]float64(nil), v...)})
	}
	return out
}
