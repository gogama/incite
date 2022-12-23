// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDisabledAdapter(t *testing.T) {
	a := disabledAdapter(10.0)

	t.Run("value", func(t *testing.T) {
		assert.Equal(t, 10.0, a.value())
	})

	t.Run("increase", func(t *testing.T) {
		changed := a.increase()

		assert.False(t, changed)
		assert.Equal(t, 10.0, a.value())
	})

	t.Run("decrease", func(t *testing.T) {
		changed := a.decrease()

		assert.False(t, changed)
		assert.Equal(t, 10.0, a.value())
	})
}

func TestStandardAdapter(t *testing.T) {
	t.Run("value", func(t *testing.T) {
		a := &standardAdapter{
			val: 1.5,
		}

		assert.Equal(t, 1.5, a.value())
	})

	t.Run("increase", func(t *testing.T) {
		t.Run("already at max value", func(t *testing.T) {
			a := &standardAdapter{
				val:    5.0,
				max:    5.0,
				upPerS: 1_000_000.0,
			}

			changed := a.increase()

			assert.False(t, changed)
			assert.Equal(t, 5.0, a.value())
		})

		t.Run("value clamped to max", func(t *testing.T) {
			a := &standardAdapter{
				val:    4.0,
				max:    5.0,
				upPerS: 50.0,
			}

			changed := a.increase()

			assert.True(t, changed)
			assert.Equal(t, 5.0, a.value())
		})

		t.Run("increase over time", func(t *testing.T) {
			a := &standardAdapter{
				val:    0.0,
				max:    5.0,
				upPerS: 1.0,
				last:   time.Now().Add(-2 * time.Second),
			}
			oldLast := a.last

			changed := a.increase()

			assert.True(t, changed)
			assert.GreaterOrEqual(t, a.value(), 2.0)
			assert.False(t, a.last.Before(oldLast))
			assert.LessOrEqual(t, a.value(), 5.0)
		})
	})

	t.Run("decrease", func(t *testing.T) {
		a := &standardAdapter{
			min:      -2.0,
			downStep: 0.75,
		}

		testCases := []struct {
			before, after float64
		}{
			{0.0, -0.75},
			{-0.75, -1.50},
			{-1.50, -2.0},
			{-2.0, -2.0},
		}

		for _, testCase := range testCases {
			t.Run(fmt.Sprintf("from %.2f to %.2f", testCase.before, testCase.after), func(t *testing.T) {
				x := time.Now()
				before := a.value()

				changed := a.decrease()
				after := a.value()

				assert.Equal(t, testCase.after != testCase.before, changed)
				assert.Equal(t, testCase.before, before)
				assert.Equal(t, testCase.after, after)
				assert.False(t, a.last.Before(x))
			})
		}
	})
}

func asStandardAdapter(t *testing.T, a adapter) *standardAdapter {
	require.IsType(t, &standardAdapter{}, a)
	return a.(*standardAdapter)
}

func standardAdapterLast(t *testing.T, a adapter) time.Time {
	return asStandardAdapter(t, a).last
}

type mockAdapter struct {
	mock.Mock
}

func newMockAdapter(t *testing.T) *mockAdapter {
	m := &mockAdapter{}
	m.Test(t)
	return m
}

func (m *mockAdapter) value() float64 {
	args := m.Called()
	return args.Get(0).(float64)
}

func (m *mockAdapter) increase() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *mockAdapter) decrease() bool {
	args := m.Called()
	return args.Get(0).(bool)
}
