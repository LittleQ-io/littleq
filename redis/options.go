package redis

import "time"

// Option configures a Redis Store.
type Option func(*options)

type options struct {
	heartbeatTTL   time.Duration
	leaderKey      string
	leaderTTL      time.Duration
	leaderRefresh  time.Duration
}

func defaultOptions() options {
	return options{
		heartbeatTTL:  30 * time.Second,
		leaderKey:     "lq:leader",
		leaderTTL:     10 * time.Second,
		leaderRefresh: 5 * time.Second,
	}
}

// WithHeartbeatTTL sets the duration after which a claimed task with no heartbeat is
// considered a zombie and eligible for requeue.
func WithHeartbeatTTL(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.heartbeatTTL = d
		}
	}
}

// WithLeaderKey sets the Redis key used for leader election.
func WithLeaderKey(key string) Option {
	return func(o *options) {
		if key != "" {
			o.leaderKey = key
		}
	}
}

// WithLeaderTTL sets the TTL of the leader lease key.
func WithLeaderTTL(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.leaderTTL = d
		}
	}
}

// WithLeaderRefresh sets how often the leader refreshes its lease.
func WithLeaderRefresh(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.leaderRefresh = d
		}
	}
}
