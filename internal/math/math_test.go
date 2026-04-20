package math_test

import (
	"math"
	"testing"
	"time"

	lqmath "github.com/LittleQ-io/littleq/internal/math"
)

func TestRequiredWorkers(t *testing.T) {
	cases := []struct {
		name                    string
		lambda, mu              float64
		wmax                    time.Duration
		wantMin, wantMax        float64
	}{
		{
			name: "balanced load",
			// λ=10/s, μ=5/s, Wmax=1s → N = (10/5)*(1 + 1/(5*1)) = 2 * 1.2 = 2.4
			lambda: 10, mu: 5, wmax: time.Second,
			wantMin: 2.3, wantMax: 2.5,
		},
		{
			name: "zero arrival rate",
			lambda: 0, mu: 5, wmax: time.Second,
			wantMin: 0, wantMax: 0,
		},
		{
			name: "zero service rate",
			lambda: 10, mu: 0, wmax: time.Second,
			wantMin: 0, wantMax: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := lqmath.RequiredWorkers(tc.lambda, tc.mu, tc.wmax)
			if got < tc.wantMin || got > tc.wantMax {
				t.Errorf("RequiredWorkers(%v, %v, %v) = %v, want [%v, %v]",
					tc.lambda, tc.mu, tc.wmax, got, tc.wantMin, tc.wantMax)
			}
		})
	}
}

func TestEffectivePriority(t *testing.T) {
	base := 10
	alpha := 1.0
	// With alpha=1 and ~0 age, Peff ≈ base.
	peff := lqmath.EffectivePriority(base, time.Now(), alpha)
	if math.Abs(peff-float64(base)) > 0.1 {
		t.Errorf("EffectivePriority(%d, now, %v) = %v, want ~%d", base, alpha, peff, base)
	}
}

func TestPressureScore(t *testing.T) {
	// At 50% of Wmax, pressure = 0.5.
	p := lqmath.PressureScore(500*time.Millisecond, time.Second)
	if math.Abs(p-0.5) > 0.001 {
		t.Errorf("PressureScore = %v, want 0.5", p)
	}

	// At Wmax, pressure = 1.0 (SLA breached).
	p2 := lqmath.PressureScore(time.Second, time.Second)
	if math.Abs(p2-1.0) > 0.001 {
		t.Errorf("PressureScore at Wmax = %v, want 1.0", p2)
	}

	// Zero Wmax returns 0.
	p3 := lqmath.PressureScore(time.Second, 0)
	if p3 != 0 {
		t.Errorf("PressureScore with zero Wmax = %v, want 0", p3)
	}
}
