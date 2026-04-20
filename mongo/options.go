package mongo

import "time"

// Option configures a MongoDB Store.
type Option func(*storeOptions)

type storeOptions struct {
	tasksCollection  string
	leaderCollection string
	heartbeatTTL     time.Duration
	leaderKey        string
	leaderTTL        time.Duration
	leaderRefresh    time.Duration
}

func defaultOptions() storeOptions {
	return storeOptions{
		tasksCollection:  "lq_tasks",
		leaderCollection: "lq_leader",
		heartbeatTTL:     30 * time.Second,
		leaderKey:        "leader",
		leaderTTL:        10 * time.Second,
		leaderRefresh:    5 * time.Second,
	}
}

func WithTasksCollection(name string) Option {
	return func(o *storeOptions) {
		if name != "" {
			o.tasksCollection = name
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
