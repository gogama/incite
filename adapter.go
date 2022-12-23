// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import "time"

// An adapter is a value that can be increased or decreased in response
// to external events. Adapters support adaptive capacity utilization
// behavior.
//
// Each value (e.g. mgr query parallelism, poller RPS) that requires
// adaptive capacity utilization receives its own instance of adapter.
//
// Instances of adapter are not safe for concurrent use from multiple
// goroutines.
type adapter interface {
	// The value method returns current value of the adapter.
	value() float64
	// The increase method signals an event that may cause an increase
	// in value. The return value indicates whether value actually
	// changed in response to the event.
	increase() bool
	// The decrease method signals an event that may cause a decrease
	// in value. The return value indicates whether value actually
	// change in response to the event.
	decrease() bool
}

// A disabledAdapter is used when mgr has adaptation disabled. Its value
// does not change when decrease or increase is called.
type disabledAdapter float64

func (a disabledAdapter) value() float64 {
	return float64(a)
}

func (a disabledAdapter) increase() bool {
	return false
}

func (a disabledAdapter) decrease() bool {
	return false
}

// A standardAdapter is used when mgr has adaptive behavior enabled. The
// value ranges between a defined minimum and maximum. A decrease event
// will reduce value by a fixed amount but will not allow value to go
// below the minimum. An increase event will increase value by a fixed
// rate per unit time elapsed since the last increase or decrease event.
type standardAdapter struct {
	val      float64   // Current value.
	max      float64   // Maximum value not to be exceeded.
	upPerS   float64   // Positive rate of increase of value per second elapsed from last change.
	min      float64   // Minimum value not to be undercut.
	downStep float64   // Positive rate of decrease of value per decrease.
	last     time.Time // Time of last change from an increase or decrease.
}

func (a *standardAdapter) value() float64 {
	return a.val
}

func (a *standardAdapter) increase() bool {
	if a.val < a.max {
		t := time.Now()
		d := t.Sub(a.last)
		seconds := float64(d) / float64(time.Second)
		val := a.val + seconds*a.upPerS
		if val > a.max {
			val = a.max
		}
		a.last = t
		a.val = val
		return true
	}
	return false
}

func (a *standardAdapter) decrease() bool {
	a.last = time.Now()
	val := a.val - a.downStep
	if val < a.min {
		val = a.min
	}
	if a.val != val {
		a.val = val
		return true
	}
	return false
}
