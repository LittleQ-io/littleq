package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	lq "github.com/LittleQ-io/littleq"
	"github.com/LittleQ-io/littleq/internal/uid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mgopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	_ lq.TaskRepository[string, json.RawMessage] = (*Store)(nil)
	_ lq.LeaderElector                           = (*Store)(nil)
	_ lq.HealthChecker                           = (*Store)(nil)
)

// Store implements TaskRepository[string, json.RawMessage] backed by MongoDB.
// Payload is stored as a JSON string inside the BSON document.
// Requires a replica set for change-stream support (Pop notifications).
type Store struct {
	tasks  *mongo.Collection // lq_tasks
	leader *mongo.Collection // lq_leader
	opts   storeOptions
	closed atomic.Bool
	ldrID  atomic.Pointer[string]
}

// New creates a Store, ensuring all required indexes exist. db must be non-nil.
func New(db *mongo.Database, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("littleq/mongo: New: %w (nil database)", lq.ErrInvalidArgument)
	}
	o := defaultOptions()
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	s := &Store{
		tasks:  db.Collection(o.tasksCollection),
		leader: db.Collection(o.leaderCollection),
		opts:   o,
	}
	if err := s.ensureIndexes(context.Background()); err != nil {
		return nil, fmt.Errorf("littleq/mongo: ensure indexes: %w", err)
	}
	return s, nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	_, err := s.tasks.Indexes().CreateMany(ctx, []mongo.IndexModel{
		// Primary dispatch index: type + priority descending, queued only.
		{Keys: bson.D{{Key: "type", Value: 1}, {Key: "eff_priority", Value: -1}, {Key: "status", Value: 1}}},
		// Dedup: unique sparse on (type, dedup_key).
		{
			Keys: bson.D{{Key: "type", Value: 1}, {Key: "dedup_key", Value: 1}},
			Options: mgopts.Index().
				SetUnique(true).
				SetPartialFilterExpression(bson.D{{Key: "dedup_key", Value: bson.D{{Key: "$gt", Value: ""}}}}),
		},
		// Zombie detection: claimed tasks by heartbeat age.
		{Keys: bson.D{{Key: "type", Value: 1}, {Key: "heartbeat_at", Value: 1}, {Key: "status", Value: 1}}},
		// Metrics: arrivals and completions by time.
		{Keys: bson.D{{Key: "type", Value: 1}, {Key: "created_at", Value: 1}}},
		{Keys: bson.D{{Key: "type", Value: 1}, {Key: "completed_at", Value: 1}}},
	})
	return err
}

// --- stored BSON document ---

type storedTask struct {
	ID            string    `bson:"_id"`
	Type          string    `bson:"type"`
	Payload       string    `bson:"payload"` // JSON string
	Capabilities  []string  `bson:"capabilities"`
	Priority      int       `bson:"priority"`
	EffPriority   float64   `bson:"eff_priority"`
	PolicyName    string    `bson:"policy_name"`
	PolicyMaxWait int64     `bson:"policy_max_wait_ns"`
	PolicyAlpha   float64   `bson:"policy_alpha"`
	PolicyRetries int       `bson:"policy_max_retries"`
	PolicyScaling int8      `bson:"policy_scaling_mode"`
	Status        int8      `bson:"status"`
	SchemaVersion int       `bson:"schema_version"`
	DedupKey      string    `bson:"dedup_key,omitempty"`
	SourceID      string    `bson:"source_id,omitempty"`
	RetryCount    int       `bson:"retry_count"`
	MaxRetries    int       `bson:"max_retries"`
	WorkerID      string    `bson:"worker_id,omitempty"`
	CreatedAt     time.Time `bson:"created_at"`
	ClaimedAt     time.Time `bson:"claimed_at,omitempty"`
	HeartbeatAt   time.Time `bson:"heartbeat_at,omitempty"`
	CompletedAt   time.Time `bson:"completed_at,omitempty"`
	ErrorMsg      string    `bson:"error_msg,omitempty"`
}

func entryToDoc(id string, entry lq.RawTaskEntry[json.RawMessage], now time.Time) storedTask {
	return storedTask{
		ID:            id,
		Type:          entry.Type,
		Payload:       string(entry.Payload),
		Capabilities:  nonNilSlice(entry.Capabilities),
		Priority:      entry.Priority,
		EffPriority:   float64(entry.Priority),
		PolicyName:    entry.Policy.Name,
		PolicyMaxWait: entry.Policy.MaxWait.Nanoseconds(),
		PolicyAlpha:   entry.Policy.AgingAlpha,
		PolicyRetries: entry.Policy.MaxRetries,
		PolicyScaling: int8(entry.Policy.ScalingMode),
		Status:        int8(lq.StatusQueued),
		SchemaVersion: entry.SchemaVersion,
		DedupKey:      entry.DedupKey,
		SourceID:      entry.SourceID,
		MaxRetries:    entry.MaxRetries,
		CreatedAt:     now,
	}
}

func docToRaw(d storedTask) lq.RawTask[string, json.RawMessage] {
	return lq.RawTask[string, json.RawMessage]{
		ID:            d.ID,
		Type:          d.Type,
		Payload:       json.RawMessage(d.Payload),
		Capabilities:  d.Capabilities,
		Priority:      d.Priority,
		EffPriority:   d.EffPriority,
		Policy:        lq.Policy{Name: d.PolicyName, MaxWait: time.Duration(d.PolicyMaxWait), AgingAlpha: d.PolicyAlpha, MaxRetries: d.PolicyRetries, ScalingMode: lq.ScalingMode(d.PolicyScaling)},
		Status:        lq.Status(d.Status),
		SchemaVersion: d.SchemaVersion,
		DedupKey:      d.DedupKey,
		SourceID:      d.SourceID,
		RetryCount:    d.RetryCount,
		CreatedAt:     d.CreatedAt,
		ClaimedAt:     d.ClaimedAt,
		WorkerID:      d.WorkerID,
	}
}

// --- Push ---

func (s *Store) Push(ctx context.Context, entry lq.RawTaskEntry[json.RawMessage]) (string, error) {
	if s.closed.Load() {
		return "", lq.ErrClosed
	}

	id := uid.New()
	doc := entryToDoc(id, entry, time.Now())

	_, err := s.tasks.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Return existing ID from dedup index.
			var existing storedTask
			ferr := s.tasks.FindOne(ctx, bson.D{
				{Key: "type", Value: entry.Type},
				{Key: "dedup_key", Value: entry.DedupKey},
			}).Decode(&existing)
			if ferr == nil {
				return existing.ID, lq.ErrDuplicate
			}
			return "", lq.ErrDuplicate
		}
		return "", fmt.Errorf("littleq/mongo: push: %w", err)
	}
	return id, nil
}

// --- Pop ---

const popPollInterval = 50 * time.Millisecond

func (s *Store) Pop(ctx context.Context, typ, workerID string, caps []string, opts ...lq.PopOption) (lq.RawTask[string, json.RawMessage], error) {
	var zero lq.RawTask[string, json.RawMessage]
	if s.closed.Load() {
		return zero, lq.ErrClosed
	}
	if typ == "" || workerID == "" {
		return zero, fmt.Errorf("littleq/mongo: pop: %w (typ and workerID required)", lq.ErrInvalidArgument)
	}

	po := lq.ResolvePopOptions(opts...)

	var deadline <-chan time.Time
	if to := po.Timeout; to > 0 {
		t := time.NewTimer(to)
		defer t.Stop()
		deadline = t.C
	}

	for {
		task, ok, err := s.tryPop(ctx, typ, workerID, caps)
		if err != nil {
			return zero, err
		}
		if ok {
			return task, nil
		}

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-deadline:
			return zero, context.DeadlineExceeded
		case <-time.After(popPollInterval):
		}
	}
}

func (s *Store) tryPop(ctx context.Context, typ, workerID string, caps []string) (lq.RawTask[string, json.RawMessage], bool, error) {
	var zero lq.RawTask[string, json.RawMessage]
	now := time.Now()
	filter := capabilityFilter(typ, caps)
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "status", Value: int8(lq.StatusClaimed)},
		{Key: "worker_id", Value: workerID},
		{Key: "claimed_at", Value: now},
		{Key: "heartbeat_at", Value: now},
	}}}
	opts := mgopts.FindOneAndUpdate().
		SetSort(bson.D{{Key: "eff_priority", Value: -1}}).
		SetReturnDocument(mgopts.After)

	var doc storedTask
	err := s.tasks.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("littleq/mongo: pop: %w", err)
	}
	return docToRaw(doc), true, nil
}

// capabilityFilter builds a MongoDB filter that matches queued tasks whose
// capability requirements are satisfied by the worker.
//
// A nil workerCaps slice means the worker opts out of capability filtering and
// will accept any task. An empty slice restricts to tasks that require no
// capabilities.
func capabilityFilter(typ string, workerCaps []string) bson.D {
	base := bson.D{
		{Key: "type", Value: typ},
		{Key: "status", Value: int8(lq.StatusQueued)},
	}
	if workerCaps == nil {
		return base
	}
	// All required capabilities must be in workerCaps:
	// "$not $elemMatch {$nin: workerCaps}" = no required cap is missing from workerCaps.
	return append(base, bson.E{
		Key: "capabilities",
		Value: bson.D{{Key: "$not", Value: bson.D{
			{Key: "$elemMatch", Value: bson.D{{Key: "$nin", Value: workerCaps}}},
		}}},
	})
}

// --- Heartbeat ---

func (s *Store) Heartbeat(ctx context.Context, _ string, taskID, workerID string) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	if taskID == "" || workerID == "" {
		return fmt.Errorf("littleq/mongo: heartbeat: %w", lq.ErrInvalidArgument)
	}
	res, err := s.tasks.UpdateOne(ctx,
		bson.D{
			{Key: "_id", Value: taskID},
			{Key: "worker_id", Value: workerID},
			{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{int8(lq.StatusClaimed), int8(lq.StatusRunning)}}}},
		},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "heartbeat_at", Value: time.Now()},
			{Key: "status", Value: int8(lq.StatusRunning)},
		}}},
	)
	if err != nil {
		return fmt.Errorf("littleq/mongo: heartbeat: %w", err)
	}
	if res.MatchedCount == 0 {
		// Distinguish not-found, wrong-owner, and already-terminal.
		n, _ := s.tasks.CountDocuments(ctx, bson.D{{Key: "_id", Value: taskID}})
		if n == 0 {
			return fmt.Errorf("littleq/mongo: heartbeat: %w", lq.ErrNotFound)
		}
		// Task exists but wasn't updated: either not owned or already terminal.
		return fmt.Errorf("littleq/mongo: heartbeat: %w", lq.ErrNotOwner)
	}
	return nil
}

// --- Complete ---

func (s *Store) Complete(ctx context.Context, _ string, taskID, workerID string) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	if taskID == "" || workerID == "" {
		return fmt.Errorf("littleq/mongo: complete: %w", lq.ErrInvalidArgument)
	}
	return s.finalize(ctx, taskID, workerID, lq.StatusDone)
}

// --- Fail ---

func (s *Store) Fail(ctx context.Context, _ string, taskID, workerID string, reason error) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	if taskID == "" || workerID == "" {
		return fmt.Errorf("littleq/mongo: fail: %w", lq.ErrInvalidArgument)
	}

	var doc storedTask
	if err := s.tasks.FindOne(ctx, bson.D{{Key: "_id", Value: taskID}}).Decode(&doc); err != nil {
		return fmt.Errorf("littleq/mongo: fail load: %w", err)
	}

	var errMsg string
	if reason != nil {
		errMsg = reason.Error()
	}

	if doc.RetryCount < doc.MaxRetries {
		score := math.Max(0, float64(doc.Priority)-float64(doc.RetryCount+1))
		res, err := s.tasks.UpdateOne(ctx,
			bson.D{
				{Key: "_id", Value: taskID},
				{Key: "worker_id", Value: workerID},
				{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{int8(lq.StatusClaimed), int8(lq.StatusRunning)}}}},
			},
			bson.D{{Key: "$set", Value: bson.D{
				{Key: "status", Value: int8(lq.StatusQueued)},
				{Key: "worker_id", Value: ""},
				{Key: "eff_priority", Value: score},
				{Key: "retry_count", Value: doc.RetryCount + 1},
				{Key: "error_msg", Value: errMsg},
			}}},
		)
		if err != nil {
			return fmt.Errorf("littleq/mongo: fail requeue: %w", err)
		}
		if res.MatchedCount == 0 {
			return fmt.Errorf("littleq/mongo: fail requeue: %w", lq.ErrNotOwner)
		}
		return nil
	}
	return s.finalize(ctx, taskID, workerID, lq.StatusDead)
}

func (s *Store) finalize(ctx context.Context, taskID, workerID string, status lq.Status) error {
	res, err := s.tasks.UpdateOne(ctx,
		bson.D{
			{Key: "_id", Value: taskID},
			{Key: "worker_id", Value: workerID},
			{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{int8(lq.StatusClaimed), int8(lq.StatusRunning)}}}},
		},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "status", Value: int8(status)},
			{Key: "completed_at", Value: time.Now()},
		}}},
	)
	if err != nil {
		return fmt.Errorf("littleq/mongo: finalize: %w", err)
	}
	if res.MatchedCount == 0 {
		n, _ := s.tasks.CountDocuments(ctx, bson.D{{Key: "_id", Value: taskID}})
		if n == 0 {
			return lq.ErrNotFound
		}
		return lq.ErrNotOwner
	}
	return nil
}

// --- Leader-only ---

func (s *Store) AgeQueued(ctx context.Context, typ string, alpha float64) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	now := time.Now()
	// Bulk update Peff = priority + age_seconds * alpha for all queued tasks of this type.
	_, err := s.tasks.UpdateMany(ctx,
		bson.D{{Key: "type", Value: typ}, {Key: "status", Value: int8(lq.StatusQueued)}},
		bson.A{bson.D{{Key: "$set", Value: bson.D{
			{Key: "eff_priority", Value: bson.D{{Key: "$add", Value: bson.A{
				"$priority",
				bson.D{{Key: "$multiply", Value: bson.A{
					alpha,
					bson.D{{Key: "$divide", Value: bson.A{
						bson.D{{Key: "$subtract", Value: bson.A{now, "$created_at"}}},
						1000,
					}}},
				}}},
			}}}},
		}}}},
	)
	if err != nil {
		return fmt.Errorf("littleq/mongo: age: %w", err)
	}
	return nil
}

func (s *Store) RequeueZombies(ctx context.Context, typ string, timeout time.Duration) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	cutoff := time.Now().Add(-timeout)
	_, err := s.tasks.UpdateMany(ctx,
		bson.D{
			{Key: "type", Value: typ},
			{Key: "status", Value: int8(lq.StatusClaimed)},
			{Key: "heartbeat_at", Value: bson.D{{Key: "$lt", Value: cutoff}}},
		},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "status", Value: int8(lq.StatusQueued)},
			{Key: "worker_id", Value: ""},
		}}},
	)
	if err != nil {
		return fmt.Errorf("littleq/mongo: requeue zombies: %w", err)
	}
	return nil
}

// --- Metrics ---

func (s *Store) Metrics(ctx context.Context, typ string, window time.Duration) (lq.TaskMetrics, error) {
	if s.closed.Load() {
		return lq.TaskMetrics{}, lq.ErrClosed
	}
	since := time.Now().Add(-window)
	secs := window.Seconds()

	depth, err := s.tasks.CountDocuments(ctx, bson.D{
		{Key: "type", Value: typ}, {Key: "status", Value: int8(lq.StatusQueued)},
	})
	if err != nil {
		return lq.TaskMetrics{}, fmt.Errorf("littleq/mongo: metrics depth: %w", err)
	}
	arrivals, err := s.tasks.CountDocuments(ctx, bson.D{
		{Key: "type", Value: typ},
		{Key: "created_at", Value: bson.D{{Key: "$gte", Value: since}}},
	})
	if err != nil {
		return lq.TaskMetrics{}, fmt.Errorf("littleq/mongo: metrics arrivals: %w", err)
	}
	done, err := s.tasks.CountDocuments(ctx, bson.D{
		{Key: "type", Value: typ},
		{Key: "status", Value: int8(lq.StatusDone)},
		{Key: "completed_at", Value: bson.D{{Key: "$gte", Value: since}}},
	})
	if err != nil {
		return lq.TaskMetrics{}, fmt.Errorf("littleq/mongo: metrics done: %w", err)
	}

	var svcRate float64
	if done > 0 {
		svcRate = float64(done) / secs
	}
	return lq.TaskMetrics{
		Type:        typ,
		ArrivalRate: float64(arrivals) / secs,
		ServiceRate: svcRate,
		QueueDepth:  depth,
	}, nil
}

// --- Health / Close ---

func (s *Store) Health(ctx context.Context) error {
	return s.tasks.Database().Client().Ping(ctx, nil)
}

func (s *Store) Close() error {
	s.closed.Store(true)
	return nil
}

// --- helpers ---

func nonNilSlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
