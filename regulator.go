// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"time"
)

// A regulator is an object that can regulate the rate at which events
// happen. It is a component of a worker and is used to limit the rate
// at which the worker manipulates chunks flowing into the worker. This
// helps the worker stay under CloudWatch Logs API RPS limits.
type regulator struct {
	close    <-chan struct{} // Short-circuits a wait when owning mgr is closed
	minDelay time.Duration   // Minimum delay enforced by wait between consecutive events
	lastReq  time.Time       // Time of last event
	timer    *time.Timer     // Timer for rate limiting
	ding     bool            // Flag indicating whether timer channel has been read
	rps      adapter         // RPS adapter whose current value is current RPS
}

const (
	rpsMin      = 1.0
	rpsDownStep = 1.0
	rpsUpPerS   = 0.5
)

func makeRegulator(close <-chan struct{}, rps, defaultRPS int, adapt bool) regulator {
	if rps <= 0 {
		rps = defaultRPS
	}
	var rpsAdapter adapter
	if adapt {
		rpsAdapter = &standardAdapter{
			val:      float64(rps),
			max:      float64(rps),
			upPerS:   rpsUpPerS,
			min:      rpsMin,
			downStep: rpsDownStep,
			last:     time.Now(),
		}
	} else {
		rpsAdapter = disabledAdapter(rps)
	}
	r := regulator{
		close: close,
		timer: time.NewTimer(1<<63 - 1),
		rps:   rpsAdapter,
	}
	r.setMinDelay()
	return r
}

func (r *regulator) wait(ctx context.Context) error {
	if !r.setTimerRPS() {
		return nil
	}

	select {
	case <-r.close:
		return errClosing
	case <-ctx.Done():
		return ctx.Err()
	case <-r.timer.C:
		r.ding = true
		return nil
	}
}

func (r *regulator) setTimer(d time.Duration) bool {
	if !r.ding && !r.timer.Stop() {
		<-r.timer.C
	} else {
		r.ding = false
	}

	if d > 0 {
		r.timer.Reset(d)
		return true
	}

	r.timer.Reset(1<<63 - 1)
	return false
}

func (r *regulator) setTimerRPS() bool {
	delaySoFar := time.Since(r.lastReq)
	delayRem := r.minDelay - delaySoFar
	if delayRem <= 0 {
		return false
	}
	return r.setTimer(delayRem)
}

func (r *regulator) setMinDelay() {
	r.minDelay = time.Second / time.Duration(r.rps.value())
}

func (r *regulator) throttled() bool {
	if r.rps.decrease() {
		r.setMinDelay()
		return true
	}
	return false
}

func (r *regulator) notThrottled() bool {
	if r.rps.increase() {
		r.setMinDelay()
		return true
	}
	return false
}
