package popopt_test

import (
	"testing"
	"time"

	"github.com/LittleQ-io/littleq/internal/popopt"
)

func TestApply_NilSafe(t *testing.T) {
	r := popopt.Apply([]popopt.Applier{nil, func(r *popopt.Resolved) { r.Timeout = 3 * time.Second }, nil})
	if r.Timeout != 3*time.Second {
		t.Errorf("timeout = %v, want 3s", r.Timeout)
	}
}

func TestApply_Empty(t *testing.T) {
	r := popopt.Apply(nil)
	if r.Timeout != 0 {
		t.Errorf("zero value timeout = %v, want 0", r.Timeout)
	}
}

func TestApply_LaterWins(t *testing.T) {
	r := popopt.Apply([]popopt.Applier{
		func(r *popopt.Resolved) { r.Timeout = time.Second },
		func(r *popopt.Resolved) { r.Timeout = 2 * time.Second },
	})
	if r.Timeout != 2*time.Second {
		t.Errorf("timeout = %v, want 2s", r.Timeout)
	}
}
