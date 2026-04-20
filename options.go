package littleq

import (
	"time"

	"github.com/LittleQ-io/littleq/internal/popopt"
)

// PopOption configures a Pop call.
//
// Options are plain functions; implementations of TaskRepository consume them
// by calling ResolvePopOptions (backends only) to flatten them into a
// single popopt.Resolved value.
type PopOption func(*popopt.Resolved)

// PopTimeout sets the maximum time Pop will block waiting for a task.
// If not set, Pop blocks until ctx is cancelled or a task arrives.
// Non-positive durations are ignored.
func PopTimeout(d time.Duration) PopOption {
	return func(r *popopt.Resolved) {
		if d > 0 {
			r.Timeout = d
		}
	}
}

// ResolvePopOptions is a backend-only helper that flattens a slice of
// PopOption into a Resolved struct. It is exported for use by
// TaskRepository implementations in subpackages; library consumers should
// never need to call it.
func ResolvePopOptions(opts ...PopOption) popopt.Resolved {
	appliers := make([]popopt.Applier, len(opts))
	for i, opt := range opts {
		appliers[i] = popopt.Applier(opt)
	}
	return popopt.Apply(appliers)
}
