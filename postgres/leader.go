package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/LittleQ-io/littleq/internal/uid"
)

// Campaign blocks until this node acquires the leader lease or ctx is cancelled.
// Lease expiry is enforced by NOW() > expires_at checks in tryAcquireLeader.
func (s *Store) Campaign(ctx context.Context) error {
	nodeID := uid.New()
	for {
		ok, err := s.tryAcquireLeader(ctx, nodeID)
		if err != nil {
			return fmt.Errorf("littleq/postgres: campaign: %w", err)
		}
		if ok {
			s.ldrID.Store(&nodeID)
			go s.refreshLeaseLoop(ctx, nodeID)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.opts.leaderRefresh):
		}
	}
}

func (s *Store) tryAcquireLeader(ctx context.Context, nodeID string) (bool, error) {
	expires := time.Now().Add(s.opts.leaderTTL)
	res, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, node_id, expires_at) VALUES ('leader', $1, $2)
		ON CONFLICT (id) DO UPDATE
			SET node_id=$1, expires_at=$2
			WHERE %s.expires_at < NOW()`,
		s.opts.leaderTable, s.opts.leaderTable),
		nodeID, expires,
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) refreshLeaseLoop(ctx context.Context, nodeID string) {
	ticker := time.NewTicker(s.opts.leaderRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.ldrID.Store(nil)
			return
		case <-ticker.C:
			res, err := s.db.ExecContext(ctx,
				fmt.Sprintf(`UPDATE %s SET expires_at=$1 WHERE id='leader' AND node_id=$2`, s.opts.leaderTable),
				time.Now().Add(s.opts.leaderTTL), nodeID,
			)
			if err != nil {
				s.ldrID.Store(nil)
				return
			}
			if n, _ := res.RowsAffected(); n == 0 {
				s.ldrID.Store(nil)
				return
			}
		}
	}
}

// Resign voluntarily releases the leader lease.
func (s *Store) Resign(ctx context.Context) error {
	p := s.ldrID.Load()
	if p == nil {
		return nil
	}
	s.ldrID.Store(nil)
	_, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE id='leader' AND node_id=$1`, s.opts.leaderTable), *p,
	)
	return err
}

// IsLeader reports whether this node currently holds the lease.
func (s *Store) IsLeader() bool { return s.ldrID.Load() != nil }
