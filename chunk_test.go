// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestChunk_Duration(t *testing.T) {
	start := time.Date(2022, 1, 17, 21, 36, 29, 50, time.FixedZone("America/Los_Angeles", 28_800))
	d := 37 * time.Hour
	c := chunk{
		start: start,
		end:   start.Add(d),
	}

	assert.Equal(t, d, c.duration())
}

func TestChunk_Started(t *testing.T) {
	start := time.Date(2022, 1, 17, 21, 41, 37, 0, time.FixedZone("America/Los_Angeles", 28_800))
	d := 12 * time.Minute

	testCases := []struct {
		name string
		c    chunk
		s    Stats
	}{
		{
			name: "Zero",
		},
		{
			name: "Gen0",
			c: chunk{
				start: start,
				end:   start.Add(d),
			},
			s: Stats{
				RangeStarted: d,
			},
		},
		{
			name: "Gen0.Error",
			c: chunk{
				start: start,
				end:   start.Add(d),
				err:   errors.New("faily mcfailerton"),
			},
			s: Stats{
				RangeStarted: d,
				RangeFailed:  d,
			},
		},
		{
			name: "Gen1.Error",
			c: chunk{
				gen:   1,
				start: start,
				end:   start.Add(d),
				err:   errors.New("boaty mcboatface"),
			},
			s: Stats{
				RangeFailed: d,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.c.started()

			assert.Equal(t, testCase.s, testCase.c.Stats)
		})
	}
}

func TestChunk_Split(t *testing.T) {
	start := time.Date(1929, 1, 15, 1, 2, 3, 4, time.FixedZone("America/Atlanta", -18_000))
	d := time.Hour
	parent := chunk{
		stream: &stream{
			ctx: context.Background(),
		},
		chunkID: "FOO",
		start:   start,
		end:     start.Add(d),
	}

	testCases := []struct {
		name     string
		frac     time.Duration
		n        int
		expected chunk
	}{
		{
			name: "Less",
			frac: d / 2,
			n:    0,
			expected: chunk{
				stream:  parent.stream,
				gen:     1,
				chunkID: "FOOs0",
				start:   start,
				end:     start.Add(d / 2),
			},
		},
		{
			name: "Equal",
			frac: d,
			n:    1,
			expected: chunk{
				stream:  parent.stream,
				gen:     1,
				chunkID: "FOOs1",
				start:   start,
				end:     start.Add(d),
			},
		},
		{
			name: "Greater",
			frac: d + 1,
			n:    2,
			expected: chunk{
				stream:  parent.stream,
				gen:     1,
				chunkID: "FOOs2",
				start:   start,
				end:     start.Add(d),
			},
		},
	}

	for _, testCase := range testCases {
		actual := parent.split(start, testCase.frac, testCase.n)

		assert.Same(t, testCase.expected.stream, actual.stream)
		assert.Equal(t, testCase.expected.gen, actual.gen)
		assert.NotNil(t, actual.ctx)
		assert.Equal(t, testCase.expected.chunkID, actual.ctx.Value(chunkIDKey))
		assert.Equal(t, testCase.expected.chunkID, actual.chunkID)
		assert.Equal(t, testCase.expected.start, actual.start)
		assert.Equal(t, testCase.expected.end, actual.end)
	}
}
