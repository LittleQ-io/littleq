package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	lq "github.com/LittleQ-io/littleq"
	"github.com/LittleQ-io/littleq/internal/uid"
	rdb "github.com/redis/go-redis/v9"
)

var (
	_ lq.TaskRepository[string, json.RawMessage] = (*Store)(nil)
	_ lq.LeaderElector                           = (*Store)(nil)
	_ lq.HealthChecker                           = (*Store)(nil)
)

// Store implements TaskRepository[string, json.RawMessage] backed by Redis.
type Store struct {
	client   *rdb.Client
	opts     options
	closed   atomic.Bool
	scripts  scripts
	leaderID atomic.Pointer[string]
}

// New constructs a Store. The client must already be connected and non-nil.
func New(client *rdb.Client, opts ...Option) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("littleq/redis: New: %w (nil redis client)", lq.ErrInvalidArgument)
	}
	o := defaultOptions()
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	s := &Store{client: client, opts: o}
	s.scripts = loadScripts()
	return s, nil
}

// --- key helpers ---

func (s *Store) keyQueue(typ string) string      { return fmt.Sprintf("lq:%s:queue", typ) }
func (s *Store) keyClaimed(typ string) string    { return fmt.Sprintf("lq:%s:claimed", typ) }
func (s *Store) keyTask(typ, id string) string   { return fmt.Sprintf("lq:%s:task:%s", typ, id) }
func (s *Store) keyMeta(typ, id string) string   { return fmt.Sprintf("lq:%s:meta:%s", typ, id) }
func (s *Store) keyDedup(typ string) string      { return fmt.Sprintf("lq:%s:dedup", typ) }
func (s *Store) keyHB(typ, id string) string     { return fmt.Sprintf("lq:%s:hb:%s", typ, id) }
func (s *Store) keyHBPfx(typ string) string      { return fmt.Sprintf("lq:%s:hb:", typ) }
func (s *Store) keyMetaPfx(typ string) string    { return fmt.Sprintf("lq:%s:meta:", typ) }
func (s *Store) keyArrivals(typ string) string   { return fmt.Sprintf("lq:%s:metrics:arrivals", typ) }
func (s *Store) keyDone(typ string) string       { return fmt.Sprintf("lq:%s:metrics:done", typ) }
func (s *Store) keyNotify(typ string) string     { return fmt.Sprintf("lq:%s:notify", typ) }

// --- stored types ---

type storedTask struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
	Capabilities  []string        `json:"capabilities"`
	Priority      int             `json:"priority"`
	Policy        lq.Policy       `json:"policy"`
	Status        lq.Status       `json:"status"`
	SchemaVersion int             `json:"schema_version"`
	DedupKey      string          `json:"dedup_key,omitempty"`
	SourceID      string          `json:"source_id,omitempty"`
	RetryCount    int             `json:"retry_count"`
	MaxRetries    int             `json:"max_retries"`
	CreatedAt     time.Time       `json:"created_at"`
}

// --- Push ---

func (s *Store) Push(ctx context.Context, entry lq.RawTaskEntry[json.RawMessage]) (string, error) {
	if s.closed.Load() {
		return "", lq.ErrClosed
	}

	if entry.Priority < 0 {
		return "", lq.ErrInvalidPriority
	}

	id := uid.New()
	now := time.Now()

	task := storedTask{
		ID: id, Type: entry.Type, Payload: entry.Payload,
		Capabilities: entry.Capabilities, Priority: entry.Priority,
		Policy: entry.Policy, Status: lq.StatusQueued,
		SchemaVersion: entry.SchemaVersion, DedupKey: entry.DedupKey,
		SourceID: entry.SourceID, MaxRetries: entry.MaxRetries,
		CreatedAt: now,
	}
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return "", fmt.Errorf("littleq/redis: push marshal: %w", err)
	}

	res, err := s.scripts.push.Run(ctx, s.client,
		[]string{
			s.keyQueue(entry.Type),
			s.keyTask(entry.Type, id),
			s.keyDedup(entry.Type),
			s.keyArrivals(entry.Type),
			s.keyMeta(entry.Type, id),
		},
		id,
		float64(entry.Priority),
		entry.DedupKey,
		string(taskJSON),
		fmt.Sprintf("%f", nowF(now)),
		fmt.Sprintf("%f", entry.Policy.AgingAlpha),
	).StringSlice()
	if err != nil {
		return "", fmt.Errorf("littleq/redis: push: %w", err)
	}
	if len(res) < 2 {
		return "", fmt.Errorf("littleq/redis: push: unexpected script result")
	}
	if res[0] == "0" {
		return res[1], lq.ErrDuplicate
	}

	if err := s.client.Publish(ctx, s.keyNotify(entry.Type), id).Err(); err != nil {
		// Notification failure is non-fatal; pollers will pick up the task.
		_ = err
	}
	return id, nil
}

// --- Pop ---

const (
	defaultHBTTL    = 30 * time.Second
	popPollInterval = 50 * time.Millisecond
)

func (s *Store) Pop(ctx context.Context, typ, workerID string, caps []string, opts ...lq.PopOption) (lq.RawTask[string, json.RawMessage], error) {
	var zero lq.RawTask[string, json.RawMessage]
	if s.closed.Load() {
		return zero, lq.ErrClosed
	}
	if typ == "" || workerID == "" {
		return zero, fmt.Errorf("littleq/redis: pop: %w (typ and workerID required)", lq.ErrInvalidArgument)
	}

	po := lq.ResolvePopOptions(opts...)
	hbTTL := s.opts.heartbeatTTL
	if hbTTL <= 0 {
		hbTTL = defaultHBTTL
	}

	sub := s.client.Subscribe(ctx, s.keyNotify(typ))
	defer sub.Close()
	notifyCh := sub.Channel()

	var deadline <-chan time.Time
	if to := po.Timeout; to > 0 {
		t := time.NewTimer(to)
		defer t.Stop()
		deadline = t.C
	}

	for {
		task, ok, outcome, err := s.tryPopOnce(ctx, typ, workerID, caps, hbTTL)
		if err != nil {
			return zero, err
		}
		if ok {
			return task, nil
		}

		// On capability mismatch, a sub-poll-interval yield prevents the
		// single-worker livelock of repeatedly popping and releasing the
		// same task. Otherwise wait for a notification or the poll tick.
		if outcome == popOutcomeCapMismatch {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-deadline:
				return zero, context.DeadlineExceeded
			case <-time.After(popPollInterval):
			}
			continue
		}

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-deadline:
			return zero, context.DeadlineExceeded
		case <-notifyCh:
		case <-time.After(popPollInterval):
		}
	}
}

// popOutcome is a hint from tryPopOnce when it returns no task. It tells the
// Pop loop whether to block on a notification (queue empty) or to yield
// briefly and retry (capability mismatch — another worker may dequeue).
type popOutcome int

const (
	popOutcomeMatched popOutcome = iota
	popOutcomeEmpty
	popOutcomeCapMismatch
)

// tryPopOnce attempts to claim one task. The second return is true when a task
// was matched and claimed; the third return distinguishes "queue empty" from
// "capability mismatch" so the caller can pick the right blocking strategy.
func (s *Store) tryPopOnce(ctx context.Context, typ, workerID string, caps []string, hbTTL time.Duration) (lq.RawTask[string, json.RawMessage], bool, popOutcome, error) {
	var zero lq.RawTask[string, json.RawMessage]
	now := time.Now()
	res, err := s.scripts.pop.Run(ctx, s.client,
		[]string{s.keyQueue(typ), s.keyClaimed(typ), s.keyHBPfx(typ)},
		workerID, hbTTL.Milliseconds(), fmt.Sprintf("%f", nowF(now)),
	).StringSlice()
	if errors.Is(err, rdb.Nil) || len(res) == 0 {
		return zero, false, popOutcomeEmpty, nil
	}
	if err != nil {
		return zero, false, popOutcomeEmpty, fmt.Errorf("littleq/redis: pop: %w", err)
	}

	id := res[0]
	raw, err := s.client.Get(ctx, s.keyTask(typ, id)).Bytes()
	if err != nil {
		return zero, false, popOutcomeEmpty, fmt.Errorf("littleq/redis: pop load: %w", err)
	}
	var st storedTask
	if err := json.Unmarshal(raw, &st); err != nil {
		return zero, false, popOutcomeEmpty, fmt.Errorf("littleq/redis: pop unmarshal: %w", err)
	}

	if !capsSatisfied(caps, st.Capabilities) {
		if _, rerr := s.scripts.release.Run(ctx, s.client,
			[]string{s.keyClaimed(typ), s.keyQueue(typ), s.keyHB(typ, id), s.keyMeta(typ, id)},
			id,
		).Result(); rerr != nil && !errors.Is(rerr, rdb.Nil) {
			return zero, false, popOutcomeEmpty, fmt.Errorf("littleq/redis: release on cap mismatch: %w", rerr)
		}
		return zero, false, popOutcomeCapMismatch, nil
	}

	st.Status = lq.StatusClaimed
	updated, merr := json.Marshal(st)
	if merr != nil {
		return zero, false, popOutcomeEmpty, fmt.Errorf("littleq/redis: pop remarshal: %w", merr)
	}
	if err := s.client.Set(ctx, s.keyTask(typ, id), updated, 0).Err(); err != nil {
		return zero, false, popOutcomeEmpty, fmt.Errorf("littleq/redis: pop persist: %w", err)
	}
	// EffPriority: the score at the moment of pop came back from the script.
	effPrio := parseFloat(res[1])

	return lq.RawTask[string, json.RawMessage]{
		ID: id, Type: st.Type, Payload: st.Payload,
		Capabilities: st.Capabilities, Priority: st.Priority, EffPriority: effPrio,
		Policy: st.Policy, Status: lq.StatusClaimed,
		SchemaVersion: st.SchemaVersion, DedupKey: st.DedupKey,
		SourceID: st.SourceID, RetryCount: st.RetryCount,
		CreatedAt: st.CreatedAt, ClaimedAt: now, WorkerID: workerID,
	}, true, popOutcomeMatched, nil
}

// --- Heartbeat ---

func (s *Store) Heartbeat(ctx context.Context, typ string, taskID, workerID string) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	if typ == "" || taskID == "" || workerID == "" {
		return fmt.Errorf("littleq/redis: heartbeat: %w", lq.ErrInvalidArgument)
	}
	hbTTL := s.opts.heartbeatTTL
	if hbTTL <= 0 {
		hbTTL = defaultHBTTL
	}
	res, err := s.scripts.heartbeat.Run(ctx, s.client,
		[]string{s.keyHB(typ, taskID)},
		workerID, hbTTL.Milliseconds(),
	).Int()
	if err != nil {
		return fmt.Errorf("littleq/redis: heartbeat: %w", err)
	}
	switch res {
	case 1:
		return nil
	case 0:
		return fmt.Errorf("littleq/redis: heartbeat: %w", lq.ErrNotFound)
	case -1:
		return fmt.Errorf("littleq/redis: heartbeat: %w", lq.ErrNotOwner)
	default:
		return fmt.Errorf("littleq/redis: heartbeat: unexpected result %d", res)
	}
}

// --- Complete ---

func (s *Store) Complete(ctx context.Context, typ, taskID, workerID string) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	if typ == "" || taskID == "" || workerID == "" {
		return fmt.Errorf("littleq/redis: complete: %w", lq.ErrInvalidArgument)
	}
	return s.finalize(ctx, typ, taskID, workerID, "done")
}

// --- Fail ---

func (s *Store) Fail(ctx context.Context, typ, taskID, workerID string, reason error) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	if typ == "" || taskID == "" || workerID == "" {
		return fmt.Errorf("littleq/redis: fail: %w", lq.ErrInvalidArgument)
	}

	var errMsg string
	if reason != nil {
		errMsg = reason.Error()
	}

	res, err := s.scripts.fail.Run(ctx, s.client,
		[]string{
			s.keyHB(typ, taskID),
			s.keyClaimed(typ),
			s.keyTask(typ, taskID),
			s.keyQueue(typ),
			s.keyDone(typ),
			s.keyMeta(typ, taskID),
		},
		workerID,
		fmt.Sprintf("%f", nowF(time.Now())),
		errMsg,
	).Int()
	if err != nil {
		return fmt.Errorf("littleq/redis: fail: %w", err)
	}
	switch res {
	case 1, 2: // 1=requeued, 2=dead
		// Announce that the queue may have new work when we requeued.
		if res == 1 {
			_ = s.client.Publish(ctx, s.keyNotify(typ), taskID).Err()
		}
		return nil
	case 0:
		return fmt.Errorf("littleq/redis: fail: %w", lq.ErrNotFound)
	case -1:
		return fmt.Errorf("littleq/redis: fail: %w", lq.ErrNotOwner)
	default:
		return fmt.Errorf("littleq/redis: fail: unexpected result %d", res)
	}
}

func (s *Store) finalize(ctx context.Context, typ, taskID, workerID, status string) error {
	res, err := s.scripts.complete.Run(ctx, s.client,
		[]string{s.keyHB(typ, taskID), s.keyClaimed(typ), s.keyTask(typ, taskID), s.keyDone(typ)},
		workerID, status, fmt.Sprintf("%f", nowF(time.Now())),
	).Int()
	if err != nil {
		return fmt.Errorf("littleq/redis: finalize: %w", err)
	}
	switch res {
	case 1:
		return nil
	case 0:
		return lq.ErrNotFound
	case -1:
		return lq.ErrNotOwner
	default:
		return fmt.Errorf("littleq/redis: finalize: unexpected result %d", res)
	}
}

// --- Leader-only ---

func (s *Store) AgeQueued(ctx context.Context, typ string, alpha float64) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	_, err := s.scripts.age.Run(ctx, s.client,
		[]string{s.keyQueue(typ), s.keyMetaPfx(typ)},
		fmt.Sprintf("%f", alpha), fmt.Sprintf("%f", nowF(time.Now())), 100,
	).Int()
	if err != nil {
		return fmt.Errorf("littleq/redis: age: %w", err)
	}
	return nil
}

func (s *Store) RequeueZombies(ctx context.Context, typ string, _ time.Duration) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	// Redis uses heartbeat key TTL for zombie detection; the timeout param is unused.
	_, err := s.scripts.requeueZombies.Run(ctx, s.client,
		[]string{s.keyClaimed(typ), s.keyQueue(typ), s.keyHBPfx(typ), s.keyMetaPfx(typ)},
		fmt.Sprintf("%f", nowF(time.Now())), 50,
	).Int()
	if err != nil {
		return fmt.Errorf("littleq/redis: requeue zombies: %w", err)
	}
	return nil
}

// --- Metrics ---

func (s *Store) Metrics(ctx context.Context, typ string, window time.Duration) (lq.TaskMetrics, error) {
	if s.closed.Load() {
		return lq.TaskMetrics{}, lq.ErrClosed
	}
	since := fmt.Sprintf("%f", nowF(time.Now().Add(-window)))
	nowStr := fmt.Sprintf("%f", nowF(time.Now()))

	pipe := s.client.Pipeline()
	arrCmd := pipe.ZCount(ctx, s.keyArrivals(typ), since, nowStr)
	doneCmd := pipe.ZCount(ctx, s.keyDone(typ), since, nowStr)
	depthCmd := pipe.ZCard(ctx, s.keyQueue(typ))
	if _, err := pipe.Exec(ctx); err != nil {
		return lq.TaskMetrics{}, fmt.Errorf("littleq/redis: metrics: %w", err)
	}

	secs := window.Seconds()
	var svcRate float64
	if d := float64(doneCmd.Val()); d > 0 {
		svcRate = d / secs
	}
	return lq.TaskMetrics{
		Type:        typ,
		ArrivalRate: float64(arrCmd.Val()) / secs,
		ServiceRate: svcRate,
		QueueDepth:  depthCmd.Val(),
	}, nil
}

// --- Health / Close ---

func (s *Store) Health(ctx context.Context) error { return s.client.Ping(ctx).Err() }

func (s *Store) Close() error {
	s.closed.Store(true)
	return nil
}

// --- helpers ---

func nowF(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// capsSatisfied reports whether a worker satisfies a task's capability requirements.
//
// A nil workerCaps slice means the worker opts out of capability filtering and
// will accept tasks with any capability requirement. Use an explicit empty
// slice ([]string{}) to restrict the worker to tasks that require no capabilities.
func capsSatisfied(workerCaps, required []string) bool {
	if workerCaps == nil {
		return true
	}
	if len(required) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(workerCaps))
	for _, c := range workerCaps {
		set[c] = struct{}{}
	}
	for _, r := range required {
		if _, ok := set[r]; !ok {
			return false
		}
	}
	return true
}
