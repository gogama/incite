// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHasSubSecond(t *testing.T) {
	testCases := []struct {
		name     string
		x        time.Time
		expected bool
	}{
		{
			name: "Zero",
		},
		{
			name: "Whole Second",
			x:    time.Unix(1, 0),
		},
		{
			name:     "Half Second",
			x:        time.Unix(1_000_000_000, int64(500*time.Millisecond)),
			expected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := hasSubSecond(testCase.x)

			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestHasSubSecondD(t *testing.T) {
	testCases := []struct {
		name     string
		d        time.Duration
		expected bool
	}{
		{
			name: "Zero",
		},
		{
			name: "Whole Second",
			d:    time.Second,
		},
		{
			name:     "Half Second",
			d:        500 * time.Millisecond,
			expected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := hasSubSecondD(testCase.d)

			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestEpochMillisecond(t *testing.T) {
	testCases := []struct {
		name     string
		t        time.Time
		expected int64
	}{
		{
			name:     "Zero",
			expected: -62135596800000,
		},
		{
			name: "Epoch",
			t:    time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Slightly Sub-Millisecond",
			t:    time.Date(1970, 1, 1, 0, 0, 0, 999*int(time.Microsecond), time.UTC),
		},
		{
			name:     "One Millisecond",
			t:        time.Date(1970, 1, 1, 0, 0, 0, 1*int(time.Millisecond), time.UTC),
			expected: 1,
		},
		{
			name:     "Slightly Super-Millisecond",
			t:        time.Date(1970, 1, 1, 0, 0, 0, 1_001*int(time.Microsecond), time.UTC),
			expected: 1,
		},
		{
			name:     "Very Specific Date",
			t:        time.Date(2022, 07, 18, 22, 33, 10, 123456789, time.UTC),
			expected: 1658183590123,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := epochMillisecond(testCase.t)

			assert.Equal(t, testCase.expected, actual)
		})
	}
}
