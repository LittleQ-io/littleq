package scheduler

import (
	"time"

	lq "github.com/LittleQ-io/littleq"
)

type options struct {
	popTimeout     time.Duration
	scalingWindow  time.Duration
	zombieTimeout  time.Duration
	agingAlpha     float64
	agingInterval  time.Duration
	zombieInterval time.Duration
	scalingMode    lq.ScalingMode
}

func defaultOptions() options {
	return options{
		popTimeout:     5 * time.Second,
		scalingWindow:  60 * time.Second,
		zombieTimeout:  30 * time.Second,
		agingAlpha:     0.1,
		agingInterval:  10 * time.Second,
		zombieInterval: 15 * time.Second,
		scalingMode:    lq.ScalingBalanced,
	}
}

// Option configures a Scheduler.
type Option func(*options)

// WithPopTimeout sets how long each Pop attempt blocks before retrying.
func WithPopTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.popTimeout = d
		}
	}
}

// WithScalingWindow sets the metrics window used for Little's Law calculations.
func WithScalingWindow(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.scalingWindow = d
		}
	}
}

// WithZombieTimeout sets the heartbeat staleness threshold for zombie reclaim.
func WithZombieTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.zombieTimeout = d
		}
	}
}

// WithAgingAlpha sets the priority aging coefficient α.
func WithAgingAlpha(alpha float64) Option {
	return func(o *options) {
		if alpha >= 0 {
			o.agingAlpha = alpha
		}
	}
}

// WithAgingInterval controls how often AgeQueued runs (leader only).
func WithAgingInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.agingInterval = d
		}
	}
}

// WithZombieInterval controls how often RequeueZombies runs (leader only).
func WithZombieInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.zombieInterval = d
		}
	}
}

// WithScalingMode sets the Little's Law scaling mode for recommendations.
func WithScalingMode(m lq.ScalingMode) Option {
	return func(o *options) { o.scalingMode = m }
}
