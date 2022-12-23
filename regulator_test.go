// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeRegulator(t *testing.T) {
	testCases := []struct {
		name            string
		rps, defaultRPS int
		adapt           bool
		effectiveRPS    float64
		minDelay        time.Duration
	}{
		{
			name:         "Explicit RPS[1]",
			rps:          1,
			effectiveRPS: 1.0,
			minDelay:     time.Second,
		},
		{
			name:         "Explicit RPS[2]",
			rps:          2,
			defaultRPS:   10,
			adapt:        true,
			effectiveRPS: 2.0,
			minDelay:     time.Second / 2,
		},
		{
			name:         "Default RPS[1]",
			defaultRPS:   1,
			effectiveRPS: 1.0,
			minDelay:     time.Second,
		},
		{
			name:         "Default RPS[2]",
			rps:          -1,
			defaultRPS:   2,
			adapt:        true,
			effectiveRPS: 2.0,
			minDelay:     time.Second / 2,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			closer := make(chan struct{})
			var c <-chan struct{}
			c = closer
			x := time.Now()
			r := makeRegulator(c, testCase.rps, testCase.defaultRPS, testCase.adapt)
			t.Cleanup(func() {
				r.timer.Stop()
				close(closer)
			})

			assert.Equal(t, c, r.close)
			assert.Equal(t, testCase.minDelay, r.minDelay)
			assert.Equal(t, testCase.effectiveRPS, r.rps.value())
			if testCase.adapt {
				require.IsType(t, &standardAdapter{}, r.rps)
				rps := r.rps.(*standardAdapter)
				assert.Equal(t, 1.0, rps.min)
				assert.Equal(t, testCase.effectiveRPS, rps.max)
				assert.False(t, rps.last.Before(x))
			} else {
				require.IsType(t, disabledAdapter(0), r.rps)
			}
		})
	}
}

func TestRegulator_Wait(t *testing.T) {
	t.Run("Already Ready", func(t *testing.T) {
		r := makeRegulator(nil, 1, 0, false)
		t.Cleanup(func() {
			r.timer.Stop()
		})

		err := r.wait(context.Background())

		assert.NoError(t, err)
	})

	t.Run("Closed", func(t *testing.T) {
		closer := make(chan struct{})
		r := makeRegulator(closer, 1, 0, true)
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
		r := makeRegulator(closer, 1, 0, false)
		t.Cleanup(func() {
			r.timer.Stop()
			close(closer)
		})
		r.lastReq = time.Now().Add(100 * time.Hour)
		ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
		defer cancel()

		err := r.wait(ctx)

		assert.Equal(t, context.DeadlineExceeded, err)
	})

	t.Run("Timer Fired", func(t *testing.T) {
		closer := make(chan struct{})
		r := makeRegulator(closer, 100_000, 0, true)
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
	r := makeRegulator(nil, 1, 0, false)
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
		r := makeRegulator(nil, 1, 0, true)
		t.Cleanup(func() {
			r.timer.Stop()
		})

		result := r.setTimerRPS()

		assert.False(t, result)
		assert.False(t, r.ding)
	})

	t.Run("Future", func(t *testing.T) {
		r := makeRegulator(nil, 1, 0, false)
		t.Cleanup(func() {
			r.timer.Stop()
		})
		r.lastReq = time.Now().Add(100 * time.Hour)

		result := r.setTimerRPS()

		assert.True(t, result)
		assert.False(t, r.ding)
	})
}

func TestRegulator_SetMinDelay(t *testing.T) {
	a := newMockAdapter(t)
	r := &regulator{
		rps: a,
	}
	a.
		On("value").
		Return(10.0).
		Once()

	r.setMinDelay()

	assert.Equal(t, time.Second/10, r.minDelay)
	a.AssertExpectations(t)
}

func TestRegulator_Throttled(t *testing.T) {
	t.Run("No Adapt", func(t *testing.T) {
		r := makeRegulator(nil, 10, 0, false)

		changed := r.throttled()

		assert.False(t, changed)
		assert.Equal(t, time.Second/10, r.minDelay)
		assert.Equal(t, 10.0, r.rps.value())
	})

	t.Run("Adapt", func(t *testing.T) {
		r := makeRegulator(nil, 5, 0, true)

		testCases := []struct {
			f       func() bool
			changed bool
			rps     float64
		}{
			{r.throttled, true, 5.0 - 1*rpsDownStep},
			{r.throttled, true, 5.0 - 2*rpsDownStep},
			{r.throttled, true, 5.0 - 3*rpsDownStep},
			{r.throttled, true, 5.0 - 4*rpsDownStep},
			{r.throttled, false, rpsMin},
		}

		for i, testCase := range testCases {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				changed := testCase.f()

				assert.Equal(t, testCase.changed, changed)
				assert.Equal(t, testCase.rps, r.rps.value())
				assert.Equal(t, time.Duration(float64(time.Second)/testCase.rps), r.minDelay)
			})
		}
	})
}

func TestRegulator_NotThrottled(t *testing.T) {
	t.Run("No Adapt", func(t *testing.T) {
		r := makeRegulator(nil, 10, 0, false)

		changed := r.notThrottled()

		assert.False(t, changed)
		assert.Equal(t, time.Second/10, r.minDelay)
		assert.Equal(t, 10.0, r.rps.value())
	})

	t.Run("Adapt", func(t *testing.T) {
		before := time.Now()
		prevValue := 2.0 - rpsDownStep
		r := makeRegulator(nil, 2, 0, true)

		t.Run("Reduce from 2 to 1 to enable increases", func(t *testing.T) {
			changed := r.throttled()

			assert.True(t, changed)
			assert.Equal(t, prevValue, r.rps.value())
		})

		t.Run("Increase several times", func(t *testing.T) {
			for i := 0; i < 5; i++ {
				t.Run(fmt.Sprintf("Iteration %d", i), func(t *testing.T) {
					time.Sleep(50 * time.Microsecond)

					changed := r.notThrottled()
					nextValue := r.rps.value()

					assert.True(t, changed)
					if prevValue < 2.0 {
						assert.Greater(t, nextValue, prevValue)
					} else {
						assert.Equal(t, 2.0, nextValue)
					}
				})
			}
		})

		after := time.Now()
		epsilon := 1e-15
		maxExpectedValue := rpsMin + float64(after.Sub(before))/float64(time.Second)*rpsUpPerS + epsilon
		assert.LessOrEqual(t, r.rps.value(), maxExpectedValue)
	})
}
