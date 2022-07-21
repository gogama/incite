// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHasSubMillisecond(t *testing.T) {
	testCases := []struct {
		name     string
		x        time.Time
		expected bool
	}{
		{
			name: "Zero",
		},
		{
			name: "Whole Millisecond",
			x:    time.Unix(1, int64(time.Millisecond)),
		},
		{
			name:     "Half Millisecond",
			x:        time.Unix(1_000_000_000, int64(500*time.Microsecond)),
			expected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := hasSubMillisecond(testCase.x)

			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestHasSubMillisecondD(t *testing.T) {
	testCases := []struct {
		name     string
		d        time.Duration
		expected bool
	}{
		{
			name: "Zero",
		},
		{
			name: "Whole Millisecond",
			d:    time.Millisecond,
		},
		{
			name:     "Half Millisecond",
			d:        500 * time.Microsecond,
			expected: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := hasSubMillisecondD(testCase.d)

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
