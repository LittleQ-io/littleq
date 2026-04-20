// Package scheduler implements the littleq gRPC Scheduler service.
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	lq "github.com/LittleQ-io/littleq"
	lqmath "github.com/LittleQ-io/littleq/internal/math"
	lqpb "github.com/LittleQ-io/littleq/proto/littleq/v1"
)

// IDCodec converts between a backend-native ID type and the string used in proto messages.
type IDCodec[I comparable] interface {
	Encode(I) string
	Decode(string) (I, error)
}

// StringIDCodec is IDCodec for backends whose native ID is already a string.
type StringIDCodec struct{}

func (StringIDCodec) Encode(id string) string          { return id }
func (StringIDCodec) Decode(s string) (string, error)  { return s, nil }

// Int64IDCodec is IDCodec for PostgreSQL (BIGSERIAL int64 IDs).
type Int64IDCodec struct{}

func (Int64IDCodec) Encode(id int64) string { return strconv.FormatInt(id, 10) }
func (Int64IDCodec) Decode(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

// Server[I] is the gRPC Scheduler service implementation.
// I is the backend-native ID type (string for Redis/MongoDB, int64 for PostgreSQL).
type Server[I comparable] struct {
	lqpb.UnimplementedSchedulerServer

	repo   lq.TaskRepository[I, json.RawMessage]
	codec  IDCodec[I]
	leader lq.LeaderElector // nil when backend doesn't implement LeaderElector
	health lq.HealthChecker // nil when backend doesn't implement HealthChecker
	opts   options

	// background loop lifecycle
	bgOnce sync.Once
	bgStop context.CancelFunc
	bgDone chan struct{}
}

// NewServer constructs a Scheduler gRPC server wrapping repo.
// The codec translates between the backend's native ID type and the proto string ID.
func NewServer[I comparable](
	repo lq.TaskRepository[I, json.RawMessage],
	codec IDCodec[I],
	opts ...Option,
) (*Server[I], error) {
	if repo == nil {
		return nil, fmt.Errorf("scheduler: repo must not be nil")
	}
	if codec == nil {
		return nil, fmt.Errorf("scheduler: codec must not be nil")
	}
	o := defaultOptions()
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	s := &Server[I]{
		repo:   repo,
		codec:  codec,
		opts:   o,
		bgDone: make(chan struct{}),
	}
	s.leader, _ = repo.(lq.LeaderElector)
	s.health, _ = repo.(lq.HealthChecker)
	return s, nil
}

// StartBackground launches the leader-only aging and zombie-requeue loops.
// It returns immediately; cancel ctx or call Close to stop the loops.
// Calling StartBackground more than once is a no-op.
func (s *Server[I]) StartBackground(ctx context.Context, taskType string) {
	s.bgOnce.Do(func() {
		ctx, cancel := context.WithCancel(ctx)
		s.bgStop = cancel
		go s.runBackground(ctx, taskType)
	})
}

func (s *Server[I]) runBackground(ctx context.Context, taskType string) {
	defer close(s.bgDone)

	ageTicker := time.NewTicker(s.opts.agingInterval)
	zombieTicker := time.NewTicker(s.opts.zombieInterval)
	defer ageTicker.Stop()
	defer zombieTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ageTicker.C:
			if s.leader == nil || s.leader.IsLeader() {
				_ = s.repo.AgeQueued(ctx, taskType, s.opts.agingAlpha)
			}
		case <-zombieTicker.C:
			if s.leader == nil || s.leader.IsLeader() {
				_ = s.repo.RequeueZombies(ctx, taskType, s.opts.zombieTimeout)
			}
		}
	}
}

// Close stops background loops and waits for them to finish.
func (s *Server[I]) Close() {
	if s.bgStop != nil {
		s.bgStop()
		<-s.bgDone
	}
}

// ─── gRPC service methods ─────────────────────────────────────────────────────

func (s *Server[I]) Enqueue(ctx context.Context, req *lqpb.EnqueueRequest) (*lqpb.EnqueueResponse, error) {
	if req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "type is required")
	}
	entry := lq.RawTaskEntry[json.RawMessage]{
		Type:          req.Type,
		Payload:       json.RawMessage(req.Payload),
		Capabilities:  req.Capabilities,
		Priority:      int(req.Priority),
		SchemaVersion: int(req.SchemaVersion),
		DedupKey:      req.DedupKey,
		SourceID:      req.SourceId,
		MaxRetries:    int(req.MaxRetries),
	}
	if req.Policy != nil {
		entry.Policy = pbToPolicy(req.Policy)
	}

	id, err := s.repo.Push(ctx, entry)
	if err != nil {
		if errors.Is(err, lq.ErrDuplicate) {
			return &lqpb.EnqueueResponse{TaskId: s.codec.Encode(id), Duplicate: true}, nil
		}
		return nil, grpcErr(err)
	}
	return &lqpb.EnqueueResponse{TaskId: s.codec.Encode(id)}, nil
}

func (s *Server[I]) Subscribe(req *lqpb.SubscribeRequest, stream lqpb.Scheduler_SubscribeServer) error {
	if req.WorkerId == "" {
		return status.Error(codes.InvalidArgument, "worker_id is required")
	}
	if req.Type == "" {
		return status.Error(codes.InvalidArgument, "type is required")
	}

	popTimeout := s.opts.popTimeout
	if req.PopTimeout != nil && req.PopTimeout.AsDuration() > 0 {
		popTimeout = req.PopTimeout.AsDuration()
	}

	ctx := stream.Context()
	for {
		task, err := s.repo.Pop(ctx, req.Type, req.WorkerId, req.Capabilities,
			lq.PopTimeout(popTimeout))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return grpcErr(err)
		}
		if err := stream.Send(rawToAssignment(task, s.codec)); err != nil {
			return err
		}
	}
}

func (s *Server[I]) Heartbeat(ctx context.Context, req *lqpb.HeartbeatRequest) (*lqpb.HeartbeatResponse, error) {
	if req.TaskId == "" || req.WorkerId == "" || req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "type, task_id and worker_id are required")
	}
	id, err := s.codec.Decode(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task_id: %v", err)
	}
	if err := s.repo.Heartbeat(ctx, req.Type, id, req.WorkerId); err != nil {
		return nil, grpcErr(err)
	}
	return &lqpb.HeartbeatResponse{}, nil
}

func (s *Server[I]) Complete(ctx context.Context, req *lqpb.CompleteRequest) (*lqpb.CompleteResponse, error) {
	if req.TaskId == "" || req.WorkerId == "" || req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "type, task_id and worker_id are required")
	}
	id, err := s.codec.Decode(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task_id: %v", err)
	}
	if err := s.repo.Complete(ctx, req.Type, id, req.WorkerId); err != nil {
		return nil, grpcErr(err)
	}
	return &lqpb.CompleteResponse{}, nil
}

func (s *Server[I]) Fail(ctx context.Context, req *lqpb.FailRequest) (*lqpb.FailResponse, error) {
	if req.TaskId == "" || req.WorkerId == "" || req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "type, task_id and worker_id are required")
	}
	id, err := s.codec.Decode(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task_id: %v", err)
	}
	reason := errors.New(req.Reason)
	if err := s.repo.Fail(ctx, req.Type, id, req.WorkerId, reason); err != nil {
		return nil, grpcErr(err)
	}
	return &lqpb.FailResponse{}, nil
}

func (s *Server[I]) ScalingEvents(req *lqpb.ScalingEventsRequest, stream lqpb.Scheduler_ScalingEventsServer) error {
	if req.Type == "" {
		return status.Error(codes.InvalidArgument, "type is required")
	}
	interval := 30 * time.Second
	if req.Interval != nil && req.Interval.AsDuration() > 0 {
		interval = req.Interval.AsDuration()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			m, err := s.repo.Metrics(ctx, req.Type, s.opts.scalingWindow)
			if err != nil {
				return grpcErr(err)
			}
			rec := recommendedWorkers(m, s.opts)
			ev := &lqpb.ScalingEvent{
				Type:               req.Type,
				RecommendedWorkers: int32(rec),
				ArrivalRate:        m.ArrivalRate,
				ServiceRate:        m.ServiceRate,
				QueueDepth:         m.QueueDepth,
				ScalingMode:        lqpb.ScalingMode(s.opts.scalingMode + 1), // enum offset
				Ts:                 timestamppb.Now(),
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}

func (s *Server[I]) GetMetrics(ctx context.Context, req *lqpb.MetricsRequest) (*lqpb.MetricsResponse, error) {
	if req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "type is required")
	}
	window := s.opts.scalingWindow
	if req.Window != nil && req.Window.AsDuration() > 0 {
		window = req.Window.AsDuration()
	}
	m, err := s.repo.Metrics(ctx, req.Type, window)
	if err != nil {
		return nil, grpcErr(err)
	}
	return &lqpb.MetricsResponse{
		Type:        m.Type,
		ArrivalRate: m.ArrivalRate,
		ServiceRate: m.ServiceRate,
		QueueDepth:  m.QueueDepth,
		AvgWait:     m.AvgWait,
	}, nil
}

func (s *Server[I]) Health(ctx context.Context, _ *lqpb.HealthRequest) (*lqpb.HealthResponse, error) {
	if s.health == nil {
		return &lqpb.HealthResponse{Healthy: true, Message: "ok"}, nil
	}
	if err := s.health.Health(ctx); err != nil {
		return &lqpb.HealthResponse{Healthy: false, Message: err.Error()}, nil
	}
	return &lqpb.HealthResponse{Healthy: true, Message: "ok"}, nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func grpcErr(err error) error {
	switch {
	case errors.Is(err, lq.ErrNotFound):
		return status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, lq.ErrNotOwner):
		return status.Errorf(codes.PermissionDenied, "%v", err)
	case errors.Is(err, lq.ErrAlreadyClaimed):
		return status.Errorf(codes.AlreadyExists, "%v", err)
	case errors.Is(err, lq.ErrDuplicate):
		return status.Errorf(codes.AlreadyExists, "%v", err)
	case errors.Is(err, lq.ErrInvalidArgument), errors.Is(err, lq.ErrInvalidPolicy), errors.Is(err, lq.ErrInvalidPriority):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, lq.ErrClosed):
		return status.Errorf(codes.Unavailable, "%v", err)
	case errors.Is(err, lq.ErrNotSupported):
		return status.Errorf(codes.Unimplemented, "%v", err)
	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "%v", err)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "%v", err)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}

func rawToAssignment[I comparable](t lq.RawTask[I, json.RawMessage], codec IDCodec[I]) *lqpb.TaskAssignment {
	a := &lqpb.TaskAssignment{
		TaskId:        codec.Encode(t.ID),
		Type:          t.Type,
		Payload:       []byte(t.Payload),
		Capabilities:  t.Capabilities,
		Priority:      int32(t.Priority),
		SchemaVersion: int32(t.SchemaVersion),
		RetryCount:    int32(t.RetryCount),
		CreatedAt:     timestamppb.New(t.CreatedAt),
		ClaimedAt:     timestamppb.New(t.ClaimedAt),
	}
	a.Policy = policyToPb(t.Policy)
	return a
}

func pbToPolicy(p *lqpb.Policy) lq.Policy {
	pol := lq.Policy{
		Name:       p.Name,
		AgingAlpha: p.AgingAlpha,
		MaxRetries: int(p.MaxRetries),
	}
	if p.MaxWait != nil {
		pol.MaxWait = p.MaxWait.AsDuration()
	}
	if p.ScalingMode > 0 {
		pol.ScalingMode = lq.ScalingMode(p.ScalingMode - 1)
	}
	return pol
}

func policyToPb(p lq.Policy) *lqpb.Policy {
	return &lqpb.Policy{
		Name:        p.Name,
		MaxWait:     durationpb.New(p.MaxWait),
		AgingAlpha:  p.AgingAlpha,
		MaxRetries:  int32(p.MaxRetries),
		ScalingMode: lqpb.ScalingMode(p.ScalingMode + 1),
	}
}

func recommendedWorkers(m lq.TaskMetrics, o options) int {
	var maxWait time.Duration
	if o.scalingWindow > 0 {
		maxWait = o.scalingWindow
	}
	raw := lqmath.RequiredWorkers(m.ArrivalRate, m.ServiceRate, maxWait)
	switch o.scalingMode {
	case lq.ScalingAggressive:
		raw = math.Ceil(raw * 1.25)
	case lq.ScalingEconomic:
		raw = math.Floor(raw * 0.85)
	default:
		raw = math.Ceil(raw)
	}
	if raw < 1 && (m.QueueDepth > 0 || m.ArrivalRate > 0) {
		return 1
	}
	return int(raw)
}
