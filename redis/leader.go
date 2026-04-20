package redis

import (
	"context"
	"fmt"
	"time"

	rdb "github.com/redis/go-redis/v9"

	"github.com/LittleQ-io/littleq/internal/uid"
)

var _ interface {
	Campaign(context.Context) error
	Resign(context.Context) error
	IsLeader() bool
} = (*Store)(nil)

// Campaign blocks until this node acquires the leader lease or ctx is cancelled.
// Uses SET NX EX — first writer wins; lease is refreshed every leaderRefresh interval.
func (s *Store) Campaign(ctx context.Context) error {
	nodeID := uid.New()
	ttl := s.opts.leaderTTL
	refresh := s.opts.leaderRefresh
	key := s.opts.leaderKey

	for {
		ok, err := s.client.SetNX(ctx, key, nodeID, ttl).Result()
		if err != nil {
			return fmt.Errorf("littleq/redis: campaign: %w", err)
		}
		if ok {
			s.leaderID.Store(&nodeID)
			go s.refreshLease(ctx, key, nodeID, ttl, refresh)
			return nil
		}
		// Wait before retrying.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(refresh):
		}
	}
}

// refreshLease keeps the leader lease alive until ctx is cancelled or the key is lost.
func (s *Store) refreshLease(ctx context.Context, key, nodeID string, ttl, refresh time.Duration) {
	ticker := time.NewTicker(refresh)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.leaderID.Store(nil)
			return
		case <-ticker.C:
			// Only refresh if we still own the key.
			res, err := s.client.GetEx(ctx, key, ttl).Result()
			if err != nil || res != nodeID {
				s.leaderID.Store(nil)
				return
			}
		}
	}
}

// Resign voluntarily releases the leader lease.
func (s *Store) Resign(ctx context.Context) error {
	p := s.leaderID.Load()
	if p == nil {
		return nil // not leader
	}
	nodeID := *p
	s.leaderID.Store(nil)

	// Only delete if we still own the key (atomic check-and-delete via Lua).
	script := rdb.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0
	`)
	return script.Run(ctx, s.client, []string{s.opts.leaderKey}, nodeID).Err()
}

// IsLeader reports whether this node currently holds the lease.
func (s *Store) IsLeader() bool {
	return s.leaderID.Load() != nil
}

