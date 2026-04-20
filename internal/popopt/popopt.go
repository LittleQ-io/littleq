// Package popopt provides backend-internal access to Pop option state.
//
// Users configure a Pop by passing littleq.PopOption values. Backends need
// to inspect the resolved options (timeout, etc.) but users should not:
// this package is the shared private conduit.
package popopt

import "time"

// Resolved is the materialized view of all PopOptions for one Pop call.
type Resolved struct {
	Timeout time.Duration
}

// Applier is the closure produced by a PopOption. Options target *Resolved.
type Applier func(*Resolved)

// Apply flattens a slice of Appliers into a Resolved.
func Apply(fns []Applier) Resolved {
	r := Resolved{}
	for _, fn := range fns {
		if fn != nil {
			fn(&r)
		}
	}
	return r
}
