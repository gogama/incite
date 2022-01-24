// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMakeRegulator(t *testing.T) {
	testCases := []struct {
		name            string
		rps, defaultRPS int
		minDelay        time.Duration
	}{
		{
			name:     "Explicit RPS[1]",
			rps:      1,
			minDelay: time.Second,
		},
		{
			name:       "Explicit RPS[2]",
			rps:        2,
			defaultRPS: 10,
			minDelay:   time.Second / 2,
		},
		{
			name:       "Default RPS[1]",
			defaultRPS: 1,
			minDelay:   time.Second,
		},
		{
			name:       "Default RPS[2]",
			rps:        -1,
			defaultRPS: 2,
			minDelay:   time.Second / 2,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			closer := make(chan struct{})
			var c <-chan struct{}
			c = closer
			r := makeRegulator(c, testCase.rps, testCase.defaultRPS)
			t.Cleanup(func() {
				r.timer.Stop()
				close(closer)
			})

			assert.Equal(t, c, r.close)
			assert.Equal(t, testCase.minDelay, r.minDelay)
		})
	}
}

func TestRegulator_Wait(t *testing.T) {
	t.Run("Already Ready", func(t *testing.T) {
		r := makeRegulator(nil, 1, 0)
		t.Cleanup(func() {
			r.timer.Stop()
		})

		err := r.wait(context.Background())

		assert.NoError(t, err)
	})

	t.Run("Closed", func(t *testing.T) {
		closer := make(chan struct{})
		r := makeRegulator(closer, 1, 0)
		t.Cleanup(func() {
			r.timer.Stop()
		})
		r.lastReq = time.Now().Add(100 * time.Hour)
		go func() {
			close(closer)
		}()

		err := r.wait(context.Background())

		assert.Same(t, errClosing, err)
	})

	t.Run("Context Ended", func(t *testing.T) {
		closer := make(chan struct{})
		r := makeRegulator(closer, 1, 0)
		t.Cleanup(func() {
			r.timer.Stop()
			close(closer)
		})
		r.lastReq = time.Now().Add(100 * time.Hour)
		ctx, _ := context.WithTimeout(context.Background(), time.Microsecond)

		err := r.wait(ctx)

		assert.Equal(t, context.DeadlineExceeded, err)
	})

	t.Run("Timer Fired", func(t *testing.T) {
		closer := make(chan struct{})
		r := makeRegulator(closer, 100_000, 0)
		t.Cleanup(func() {
			r.timer.Stop()
			close(closer)
		})
		r.lastReq = time.Now()

		err := r.wait(context.Background())

		assert.Nil(t, err)
	})
}

func TestRegulator_SetTimer(t *testing.T) {
	r := makeRegulator(nil, 1, 0)
	t.Cleanup(func() {
		r.timer.Stop()
	})

	testCases := []struct {
		name     string
		ding     bool
		d        time.Duration
		expected bool
	}{
		{
			name: "Negative",
			d:    -1,
		},
		{
			name: "Zero",
		},
		{
			name:     "NoDing.1",
			d:        1,
			expected: true,
		},
		{
			name:     "NoDing.11",
			d:        1,
			expected: true,
		},
		{
			name:     "NoDing.111",
			d:        1,
			expected: true,
		},
		{
			name:     "NoDing.1111",
			d:        1,
			expected: true,
		},
		{
			name:     "NoDing.11111",
			d:        1,
			expected: true,
		},
		{
			name:     "Ding.1",
			ding:     true,
			d:        1,
			expected: true,
		},
		{
			name:     "NoDing.2",
			d:        2,
			expected: true,
		},
		{
			name:     "NoDing.22",
			d:        2,
			expected: true,
		},
		{
			name:     "NoDing.222",
			d:        2,
			expected: true,
		},
		{
			name:     "Ding.2",
			ding:     true,
			d:        2,
			expected: true,
		},
		{
			name:     "NoDing.5",
			d:        5,
			expected: true,
		},
		{
			name:     "Ding.5",
			ding:     true,
			d:        5,
			expected: true,
		},
		{
			name:     "NoDing.500",
			d:        500,
			expected: true,
		},
		{
			name:     "Ding.500",
			ding:     true,
			d:        500,
			expected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.ding {
				<-r.timer.C
				r.ding = true
			}

			actual := r.setTimer(testCase.d)
			assert.Equal(t, testCase.expected, actual)
			assert.False(t, r.ding)
		})
	}
}

func TestRegulator_SetTimerRPS(t *testing.T) {
	t.Run("Past", func(t *testing.T) {
		r := makeRegulator(nil, 1, 0)
		t.Cleanup(func() {
			r.timer.Stop()
		})

		result := r.setTimerRPS()

		assert.False(t, result)
		assert.False(t, r.ding)
	})

	t.Run("Future", func(t *testing.T) {
		r := makeRegulator(nil, 1, 0)
		t.Cleanup(func() {
			r.timer.Stop()
		})
		r.lastReq = time.Now().Add(100 * time.Hour)

		result := r.setTimerRPS()

		assert.True(t, result)
		assert.False(t, r.ding)
	})
}
