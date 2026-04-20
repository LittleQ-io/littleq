package postgres

import "time"

// Option configures a PostgreSQL Store.
type Option func(*storeOptions)

type storeOptions struct {
	table         string
	leaderTable   string
	heartbeatTTL  time.Duration
	leaderTTL     time.Duration
	leaderRefresh time.Duration
	dsn           string // kept for pq.NewListener
}

func defaultOptions() storeOptions {
	return storeOptions{
		table:         "lq_tasks",
		leaderTable:   "lq_leader",
		heartbeatTTL:  30 * time.Second,
		leaderTTL:     10 * time.Second,
		leaderRefresh: 5 * time.Second,
	}
}

func WithTable(name string) Option {
	return func(o *storeOptions) {
		if name != "" {
			o.table = name
		}
	}
}

func WithHeartbeatTTL(d time.Duration) Option {
	return func(o *storeOptions) {
		if d > 0 {
			o.heartbeatTTL = d
		}
	}
}

func WithLeaderTTL(d time.Duration) Option {
	return func(o *storeOptions) {
		if d > 0 {
			o.leaderTTL = d
		}
	}
}

func WithLeaderRefresh(d time.Duration) Option {
	return func(o *storeOptions) {
		if d > 0 {
			o.leaderRefresh = d
		}
	}
}
