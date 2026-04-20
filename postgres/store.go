package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
	lq "github.com/LittleQ-io/littleq"
)

var (
	_ lq.TaskRepository[int64, json.RawMessage] = (*Store)(nil)
	_ lq.LeaderElector                          = (*Store)(nil)
	_ lq.HealthChecker                          = (*Store)(nil)
)

//go:embed migrations/001_schema.sql
var schemaDDL string

// Store implements TaskRepository[int64, json.RawMessage] backed by PostgreSQL.
// Uses SKIP LOCKED for contention-free atomic task claiming and
// LISTEN/NOTIFY for low-latency Pop blocking.
type Store struct {
	db     *sql.DB
	dsn    string // for pq.NewListener
	opts   storeOptions
	closed atomic.Bool
	ldrID  atomic.Pointer[string]
}

// New creates a Store and runs embedded schema migrations. db must be non-nil;
// dsn is required so the Store can open a dedicated LISTEN/NOTIFY connection
// in Pop without consuming a slot from the shared pool.
func New(db *sql.DB, dsn string, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("littleq/postgres: New: %w (nil database)", lq.ErrInvalidArgument)
	}
	if dsn == "" {
		return nil, fmt.Errorf("littleq/postgres: New: %w (empty DSN)", lq.ErrInvalidArgument)
	}
	o := defaultOptions()
	o.dsn = dsn
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	s := &Store{db: db, dsn: dsn, opts: o}
	if _, err := db.Exec(schemaDDL); err != nil {
		return nil, fmt.Errorf("littleq/postgres: migrate: %w", err)
	}
	return s, nil
}

const notifyChannel = "lq_notify"

// --- Push ---

func (s *Store) Push(ctx context.Context, entry lq.RawTaskEntry[json.RawMessage]) (int64, error) {
	if s.closed.Load() {
		return 0, lq.ErrClosed
	}

	payload, err := json.Marshal(entry.Payload)
	if err != nil {
		return 0, fmt.Errorf("littleq/postgres: push marshal: %w", err)
	}

	q := fmt.Sprintf(`
		INSERT INTO %s (
			type, payload, capabilities, priority, eff_priority,
			policy_name, policy_max_wait, policy_alpha, policy_retries, policy_scaling,
			schema_version, dedup_key, source_id, max_retries
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (type, dedup_key) WHERE dedup_key != '' DO NOTHING
		RETURNING id`, s.opts.table)

	var id int64
	err = s.db.QueryRowContext(ctx, q,
		entry.Type,                                // $1
		payload,                                   // $2
		pq.Array(nonNilSlice(entry.Capabilities)), // $3
		entry.Priority,                            // $4 priority (int)
		float64(entry.Priority),                   // $5 eff_priority (float8)
		entry.Policy.Name,                         // $6
		entry.Policy.MaxWait.Nanoseconds(),        // $7
		entry.Policy.AgingAlpha,                   // $8
		entry.Policy.MaxRetries,                   // $9
		int8(entry.Policy.ScalingMode),            // $10
		entry.SchemaVersion,                       // $11
		entry.DedupKey,                            // $12
		entry.SourceID,                            // $13
		entry.MaxRetries,                          // $14
	).Scan(&id)

	if errors.Is(err, sql.ErrNoRows) {
		// ON CONFLICT DO NOTHING fired — fetch existing ID.
		ferr := s.db.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT id FROM %s WHERE type=$1 AND dedup_key=$2`, s.opts.table),
			entry.Type, entry.DedupKey,
		).Scan(&id)
		if ferr != nil {
			return 0, lq.ErrDuplicate
		}
		return id, lq.ErrDuplicate
	}
	if err != nil {
		return 0, fmt.Errorf("littleq/postgres: push: %w", err)
	}

	// Notify waiting Pop callers.
	_, _ = s.db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, notifyChannel, entry.Type)
	return id, nil
}

// --- Pop ---

const popPollInterval = 50 * time.Millisecond

func (s *Store) Pop(ctx context.Context, typ, workerID string, caps []string, opts ...lq.PopOption) (lq.RawTask[int64, json.RawMessage], error) {
	var zero lq.RawTask[int64, json.RawMessage]
	if s.closed.Load() {
		return zero, lq.ErrClosed
	}
	if typ == "" || workerID == "" {
		return zero, fmt.Errorf("littleq/postgres: pop: %w (typ and workerID required)", lq.ErrInvalidArgument)
	}

	po := lq.ResolvePopOptions(opts...)

	// Set up LISTEN on a dedicated connection.
	listener := pq.NewListener(s.dsn, 100*time.Millisecond, time.Minute,
		func(_ pq.ListenerEventType, _ error) {})
	defer listener.Close()
	if err := listener.Listen(notifyChannel); err != nil {
		// Fallback to polling if LISTEN fails (e.g. no network).
		listener = nil
	}

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

		var notifyCh <-chan *pq.Notification
		if listener != nil {
			notifyCh = listener.Notify
		}

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-deadline:
			return zero, context.DeadlineExceeded
		case n := <-notifyCh:
			// n.Extra is the task type; ignore notifications for other types.
			if n == nil || n.Extra == typ || n.Extra == "" {
				continue // retry
			}
		case <-time.After(popPollInterval):
		}
	}
}

func (s *Store) tryPop(ctx context.Context, typ, workerID string, caps []string) (lq.RawTask[int64, json.RawMessage], bool, error) {
	var zero lq.RawTask[int64, json.RawMessage]
	now := time.Now()

	// A nil caps slice means the worker opts out of capability filtering.
	// An empty slice requires tasks with no capability requirements.
	var (
		capPredicate string
		args         []any
	)
	args = append(args, typ, workerID, now)
	if caps == nil {
		capPredicate = ""
	} else {
		capPredicate = " AND $4::text[] @> capabilities"
		args = append(args, pq.Array(caps))
	}

	q := fmt.Sprintf(`
		UPDATE %s
		SET status=1, worker_id=$2, claimed_at=$3, heartbeat_at=$3
		WHERE id = (
			SELECT id FROM %s
			WHERE type=$1 AND status=0%s
			ORDER BY eff_priority DESC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, type, payload, capabilities, priority, eff_priority,
		          policy_name, policy_max_wait, policy_alpha, policy_retries, policy_scaling,
		          schema_version, dedup_key, source_id, retry_count, max_retries, created_at, claimed_at`,
		s.opts.table, s.opts.table, capPredicate)

	row := s.db.QueryRowContext(ctx, q, args...)
	t, ok, err := scanTask(row)
	if err != nil {
		return zero, false, err
	}
	return t, ok, nil
}

// scanTask reads a task from a sql.Row. The bool result is false when the row
// yielded no rows (not an error).
func scanTask(row *sql.Row) (lq.RawTask[int64, json.RawMessage], bool, error) {
	var (
		t             lq.RawTask[int64, json.RawMessage]
		rawPayload    []byte
		caps          pq.StringArray
		policyName    string
		policyMaxWait int64
		policyAlpha   float64
		policyRetries int
		policyScaling int8
		claimedAt     sql.NullTime
	)
	err := row.Scan(
		&t.ID, &t.Type, &rawPayload, &caps,
		&t.Priority, &t.EffPriority,
		&policyName, &policyMaxWait, &policyAlpha, &policyRetries, &policyScaling,
		&t.SchemaVersion, &t.DedupKey, &t.SourceID,
		&t.RetryCount, &t.MaxRetries,
		&t.CreatedAt, &claimedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return t, false, nil
	}
	if err != nil {
		return t, false, fmt.Errorf("littleq/postgres: scan task: %w", err)
	}
	t.Payload = json.RawMessage(rawPayload)
	t.Capabilities = []string(caps)
	t.Policy = lq.Policy{
		Name:        policyName,
		MaxWait:     time.Duration(policyMaxWait),
		AgingAlpha:  policyAlpha,
		MaxRetries:  policyRetries,
		ScalingMode: lq.ScalingMode(policyScaling),
	}
	t.Status = lq.StatusClaimed
	if claimedAt.Valid {
		t.ClaimedAt = claimedAt.Time
	}
	return t, true, nil
}

// --- Heartbeat ---

func (s *Store) Heartbeat(ctx context.Context, _ string, taskID int64, workerID string) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	if taskID <= 0 || workerID == "" {
		return fmt.Errorf("littleq/postgres: heartbeat: %w", lq.ErrInvalidArgument)
	}
	res, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`UPDATE %s
			SET heartbeat_at=$1, status=2
			WHERE id=$2 AND worker_id=$3 AND status IN (1, 2)`,
			s.opts.table),
		time.Now(), taskID, workerID,
	)
	if err != nil {
		return fmt.Errorf("littleq/postgres: heartbeat: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Distinguish not-found, wrong-owner, or already-terminal.
		var exists bool
		_ = s.db.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE id=$1)`, s.opts.table), taskID,
		).Scan(&exists)
		if !exists {
			return fmt.Errorf("littleq/postgres: heartbeat: %w", lq.ErrNotFound)
		}
		return fmt.Errorf("littleq/postgres: heartbeat: %w", lq.ErrNotOwner)
	}
	return nil
}

// --- Complete ---

func (s *Store) Complete(ctx context.Context, _ string, taskID int64, workerID string) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	if taskID <= 0 || workerID == "" {
		return fmt.Errorf("littleq/postgres: complete: %w", lq.ErrInvalidArgument)
	}
	return s.finalize(ctx, taskID, workerID, lq.StatusDone)
}

// --- Fail ---

func (s *Store) Fail(ctx context.Context, _ string, taskID int64, workerID string, reason error) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	if taskID <= 0 || workerID == "" {
		return fmt.Errorf("littleq/postgres: fail: %w", lq.ErrInvalidArgument)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("littleq/postgres: fail begin tx: %w", err)
	}
	defer tx.Rollback()

	var retryCount, maxRetries, priority int
	err = tx.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT retry_count, max_retries, priority FROM %s
			WHERE id=$1 AND worker_id=$2 AND status IN (1, 2) FOR UPDATE`, s.opts.table),
		taskID, workerID,
	).Scan(&retryCount, &maxRetries, &priority)
	if errors.Is(err, sql.ErrNoRows) {
		// Distinguish not-found from wrong-owner/terminal-status.
		var exists bool
		_ = s.db.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE id=$1)`, s.opts.table), taskID,
		).Scan(&exists)
		if !exists {
			return lq.ErrNotFound
		}
		return lq.ErrNotOwner
	}
	if err != nil {
		return fmt.Errorf("littleq/postgres: fail load: %w", err)
	}

	var errMsg string
	if reason != nil {
		errMsg = reason.Error()
	}

	if retryCount < maxRetries {
		score := math.Max(0, float64(priority)-float64(retryCount+1))
		_, err = tx.ExecContext(ctx,
			fmt.Sprintf(`UPDATE %s SET status=0, worker_id='', eff_priority=$1, retry_count=$2, error_msg=$3
				WHERE id=$4`, s.opts.table),
			score, retryCount+1, errMsg, taskID,
		)
	} else {
		_, err = tx.ExecContext(ctx,
			fmt.Sprintf(`UPDATE %s SET status=4, completed_at=$1, error_msg=$2 WHERE id=$3`, s.opts.table),
			time.Now(), errMsg, taskID,
		)
	}
	if err != nil {
		return fmt.Errorf("littleq/postgres: fail update: %w", err)
	}
	return tx.Commit()
}

func (s *Store) finalize(ctx context.Context, taskID int64, workerID string, status lq.Status) error {
	res, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`UPDATE %s
			SET status=$1, completed_at=$2
			WHERE id=$3 AND worker_id=$4 AND status IN (1, 2)`,
			s.opts.table),
		int8(status), time.Now(), taskID, workerID,
	)
	if err != nil {
		return fmt.Errorf("littleq/postgres: finalize: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var exists bool
		_ = s.db.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE id=$1)`, s.opts.table), taskID,
		).Scan(&exists)
		if !exists {
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
	_, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`UPDATE %s SET eff_priority = priority + EXTRACT(EPOCH FROM (NOW()-created_at)) * $1
			WHERE type=$2 AND status=0`, s.opts.table),
		alpha, typ,
	)
	if err != nil {
		return fmt.Errorf("littleq/postgres: age: %w", err)
	}
	return nil
}

func (s *Store) RequeueZombies(ctx context.Context, typ string, timeout time.Duration) error {
	if s.closed.Load() {
		return lq.ErrClosed
	}
	_, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`UPDATE %s SET status=0, worker_id=''
			WHERE type=$1 AND status=1
			  AND heartbeat_at < NOW() - ($2::float8 * interval '1 second')`,
			s.opts.table),
		typ, timeout.Seconds(),
	)
	if err != nil {
		return fmt.Errorf("littleq/postgres: requeue zombies: %w", err)
	}
	return nil
}

// --- Metrics ---

func (s *Store) Metrics(ctx context.Context, typ string, window time.Duration) (lq.TaskMetrics, error) {
	if s.closed.Load() {
		return lq.TaskMetrics{}, lq.ErrClosed
	}
	secs := window.Seconds()
	q := fmt.Sprintf(`
		SELECT
			COUNT(*) FILTER (WHERE status=0) AS depth,
			COUNT(*) FILTER (WHERE created_at > NOW()-($2::float8 * interval '1 second')) AS arrivals,
			COUNT(*) FILTER (WHERE status=3 AND completed_at > NOW()-($2::float8 * interval '1 second')) AS done
		FROM %s WHERE type=$1`, s.opts.table)

	var depth, arrivals, done int64
	err := s.db.QueryRowContext(ctx, q, typ, secs).Scan(&depth, &arrivals, &done)
	if err != nil {
		return lq.TaskMetrics{}, fmt.Errorf("littleq/postgres: metrics: %w", err)
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

func (s *Store) Health(ctx context.Context) error { return s.db.PingContext(ctx) }

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
