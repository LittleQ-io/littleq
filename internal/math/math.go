package math

import "time"

// EffectivePriority computes the anti-starvation adjusted priority.
//
//	Peff = Pbase + (now - createdAt) * alpha
func EffectivePriority(base int, createdAt time.Time, alpha float64) float64 {
	age := time.Since(createdAt).Seconds()
	return float64(base) + age*alpha
}

// RequiredWorkers applies Little's Law to compute the minimum worker count
// needed to keep queue wait time within Wmax.
//
//	N = (λ/μ) * (1 + 1/(μ * Wmax))
//
// Returns 0 if inputs are degenerate (zero rates).
func RequiredWorkers(arrivalRate, serviceRate float64, maxWait time.Duration) float64 {
	if serviceRate <= 0 || arrivalRate <= 0 || maxWait <= 0 {
		return 0
	}
	wmax := maxWait.Seconds()
	return (arrivalRate / serviceRate) * (1 + 1/(serviceRate*wmax))
}

// PressureScore measures how urgently a task needs dispatch.
// A score >= 1.0 means the task has already violated its SLA.
//
//	Pressure = currentWait / policyMaxWait
func PressureScore(currentWait, maxWait time.Duration) float64 {
	if maxWait <= 0 {
		return 0
	}
	return float64(currentWait) / float64(maxWait)
}
