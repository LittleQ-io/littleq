package littleq

import (
	"context"
	"fmt"
)

// Upcaster migrates a payload from one schema version to the next.
// Implementations must be deterministic and side-effect free.
type Upcaster[P any] interface {
	FromVersion() int
	ToVersion() int
	Upcast(ctx context.Context, p P) (P, error)
}

// UpcasterFunc wraps a plain function as an Upcaster.
func UpcasterFunc[P any](from, to int, fn func(context.Context, P) (P, error)) Upcaster[P] {
	return &funcUpcaster[P]{from: from, to: to, fn: fn}
}

type funcUpcaster[P any] struct {
	from, to int
	fn       func(context.Context, P) (P, error)
}

func (u *funcUpcaster[P]) FromVersion() int { return u.from }
func (u *funcUpcaster[P]) ToVersion() int   { return u.to }
func (u *funcUpcaster[P]) Upcast(ctx context.Context, p P) (P, error) {
	return u.fn(ctx, p)
}

// applyUpcasters walks the upcaster chain from fromVersion to targetVersion.
// Returns ErrNoUpcaster if any step in the chain is missing.
func applyUpcasters[P any](ctx context.Context, p P, fromVersion, targetVersion int, upcasters []Upcaster[P]) (P, error) {
	current := fromVersion
	for current < targetVersion {
		next := current + 1
		var found Upcaster[P]
		for _, u := range upcasters {
			if u.FromVersion() == current && u.ToVersion() == next {
				found = u
				break
			}
		}
		if found == nil {
			return p, fmt.Errorf("%w: %d→%d", ErrNoUpcaster, current, next)
		}
		var err error
		p, err = found.Upcast(ctx, p)
		if err != nil {
			return p, fmt.Errorf("littleq: upcast %d→%d: %w", current, next, err)
		}
		current = next
	}
	return p, nil
}
