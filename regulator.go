// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"time"
)

type regulator struct {
	close    <-chan struct{}
	minDelay time.Duration
	lastReq  time.Time
	timer    *time.Timer
	ding     bool
}

func makeRegulator(close <-chan struct{}, rps, defaultRPS int) regulator {
	if rps <= 0 {
		rps = defaultRPS
	}
	return regulator{
		close:    close,
		minDelay: time.Second / time.Duration(rps),
		timer:    time.NewTimer(1<<63 - 1),
	}
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
