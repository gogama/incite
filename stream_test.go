// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestStream_Close(t *testing.T) {
	// The test cases below work by setting up one or more queries that
	// "spin" within the QueryManager because their chunks stay
	// stuck in a Running state. Then we close the stream attached to
	// the query and assert that it all stops cleanly.

	testCases := []struct {
		streams int
		chunks  int
	}{
		{
			streams: 1,
			chunks:  1,
		},
		{
			streams: QueryConcurrencyQuotaLimit,
			chunks:  1,
		},
		{
			streams: 1,
			chunks:  QueryConcurrencyQuotaLimit,
		},
	}

	for _, testCase := range testCases {
		name := fmt.Sprintf("%s[streams=%d][chunks=%d]", t.Name(), testCase.streams, testCase.chunks)
		t.Run(name, func(t *testing.T) {
			// ARRANGE.
			actions := newMockActions(t)
			var wg sync.WaitGroup
			text := make([]string, testCase.streams)
			for si := 0; si < testCase.streams; si++ {
				text[si] = fmt.Sprintf("%s[si=%d]", name, si)
				for ci := 0; ci < testCase.chunks; ci++ {
					queryID := fmt.Sprintf("%s[ci=%d]", name, ci)
					startCall := actions.
						On("StartQueryWithContext", anyContext, startQueryInput(
							text[si],
							defaultStart.Add(time.Duration(ci)*time.Hour), defaultStart.Add(time.Duration(ci+1)*time.Hour),
							DefaultLimit, "baz"))
					if si*testCase.streams+testCase.chunks < QueryConcurrencyQuotaLimit {
						wg.Add(1)
						startCall.
							Run(func(_ mock.Arguments) { wg.Done() }).
							Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryID}, nil).
							Once()
					} else {
						startCall.
							Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryID}, nil).
							Maybe()
					}
					actions.
						On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryID}).
						Return(&cloudwatchlogs.GetQueryResultsOutput{Status: sp(cloudwatchlogs.QueryStatusRunning)}, nil).
						Maybe()
					actions.
						On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{QueryId: &queryID}).
						Return(&cloudwatchlogs.StopQueryOutput{}, nil).
						Maybe()
				}
			}
			m := NewQueryManager(Config{
				Actions:  actions,
				Parallel: QueryConcurrencyQuotaLimit,
				RPS:      lotsOfRPS,
			})
			t.Cleanup(func() {
				_ = m.Close()
			})
			streams := make([]Stream, testCase.streams)
			var err error
			for si := 0; si < testCase.streams; si++ {
				streams[si], err = m.Query(QuerySpec{
					Text:     text[si],
					Groups:   []string{"baz"},
					Start:    defaultStart,
					End:      defaultStart.Add(time.Duration(testCase.chunks) * time.Hour),
					Chunk:    time.Hour,
					Priority: si,
				})
				require.NoError(t, err, "failed to start stream %d", si)
				require.NotNil(t, streams[si], "stream %d is nil", si)
			}
			wg.Wait()

			// ACT.
			for si := 0; si < testCase.streams; si++ {
				err = streams[si].Close()
				assert.NoError(t, err, "error closing stream %d", si)
				err = streams[si].Close()
				assert.Same(t, ErrClosed, err, "stream %d failed to indicate already closed on second Close() call", si)
			}

			// ASSERT.
			actions.AssertExpectations(t)
			err = m.Close()
			assert.NoError(t, err)
		})
	}
}

func TestStream_Read(t *testing.T) {
	t.Run("Buffer is Shorter than Available Results", func(t *testing.T) {
		actions := newMockActions(t)
		actions.
			On("StartQueryWithContext", anyContext, anyStartQueryInput).
			Return(&cloudwatchlogs.StartQueryOutput{QueryId: sp("foo")}, nil).
			Once()
		actions.
			On("GetQueryResultsWithContext", anyContext, anyGetQueryResultsInput).
			Return(&cloudwatchlogs.GetQueryResultsOutput{
				Status: sp(cloudwatchlogs.QueryStatusComplete),
				Results: [][]*cloudwatchlogs.ResultField{
					{{Field: sp("@ptr"), Value: sp("1")}},
					{{Field: sp("@ptr"), Value: sp("2")}},
				},
			}, nil).
			Once()
		m := NewQueryManager(Config{
			Actions: actions,
		})
		t.Cleanup(func() {
			err := m.Close()
			assert.NoError(t, err)
		})
		s, err := m.Query(QuerySpec{
			Text:   "bar",
			Groups: []string{"baz"},
			Start:  defaultStart,
			End:    defaultEnd,
		})
		require.NotNil(t, s)
		require.NoError(t, err)

		t.Run("Read into Length Zero Buffer Succeeds with Zero Results", func(t *testing.T) {
			var p []Result
			n, err := s.Read(p)
			assert.Equal(t, 0, n)
			assert.NoError(t, err)
		})

		t.Run("Read Into Length One Buffer Succeeds with First of Two Results", func(t *testing.T) {
			p := make([]Result, 1)
			n, err := s.Read(p)
			assert.Equal(t, 1, n)
			assert.NoError(t, err)
			assert.Equal(t, []Result{{{"@ptr", "1"}}}, p)
		})

		t.Run("Read Into Length One Buffer Succeeds with Second of Two Results and Possible EOF", func(t *testing.T) {
			p := make([]Result, 1)
			n, err := s.Read(p)
			assert.Equal(t, 1, n)
			if err != nil {
				assert.Same(t, io.EOF, err)
			}
			assert.Equal(t, []Result{{{"@ptr", "2"}}}, p)
		})

		t.Run("Read Into Length One Buffer Returns EOF", func(t *testing.T) {
			p := make([]Result, 1)
			n, err := s.Read(p)
			assert.Equal(t, 0, n)
			assert.Same(t, io.EOF, err)
			assert.Equal(t, make([]Result, 1), p)
		})
	})
}

func TestStream_NextChunkRange(t *testing.T) {
	testCases := []*struct {
		name string
		s    stream
		c    chunk
	}{
		{
			name: "Partial Chunk",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart,
					End:   defaultStart.Add(defaultDuration / 4),
					Chunk: defaultDuration,
				},
			},
			c: chunk{
				start: defaultStart,
				end:   defaultStart.Add(defaultDuration / 4),
			},
		},
		{
			name: "Single Chunk Query, Aligned",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart,
					End:   defaultEnd,
					Chunk: defaultDuration,
				},
			},
			c: chunk{
				start: defaultStart,
				end:   defaultEnd,
			},
		},
		{
			name: "Single Chunk Query, Misaligned",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart.Add(-time.Hour),
					End:   defaultEnd.Add(time.Hour),
					Chunk: 2*time.Hour + defaultDuration,
				},
				n:    1,
				next: 0,
			},
			c: chunk{
				start: defaultStart.Add(-time.Hour),
				end:   defaultEnd.Add(time.Hour),
			},
		},
		{
			name: "1/2 Chunks Aligned Exact",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart,
					End:   defaultEnd,
					Chunk: defaultDuration / 2,
				},
			},
			c: chunk{
				start: defaultStart,
				end:   defaultStart.Add(defaultDuration / 2),
			},
		},
		{
			name: "2/2 Chunks Aligned Exact",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart,
					End:   defaultEnd,
					Chunk: defaultDuration / 2,
				},
				next: 1,
			},
			c: chunk{
				start: defaultStart.Add(defaultDuration / 2),
				end:   defaultEnd,
			},
		},
		{
			name: "1/2 Chunks Aligned Start",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart,
					End:   defaultEnd.Add(defaultDuration / 4),
					Chunk: defaultDuration,
				},
			},
			c: chunk{
				start: defaultStart,
				end:   defaultEnd,
			},
		},
		{
			name: "2/2 Chunks Aligned Start",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart,
					End:   defaultEnd.Add(defaultDuration / 4),
					Chunk: defaultDuration,
				},
				next: 1,
			},
			c: chunk{
				start: defaultEnd,
				end:   defaultEnd.Add(defaultDuration / 4),
			},
		},
		{
			name: "1/3 Chunks Aligned Start",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart,
					End:   defaultStart.Add(9 * defaultDuration / 4),
					Chunk: defaultDuration,
				},
			},
			c: chunk{
				start: defaultStart,
				end:   defaultStart.Add(defaultDuration),
			},
		},
		{
			name: "2/3 Chunks Aligned Start",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart,
					End:   defaultStart.Add(9 * defaultDuration / 4),
					Chunk: defaultDuration,
				},
				next: 1,
			},
			c: chunk{
				start: defaultStart.Add(defaultDuration),
				end:   defaultStart.Add(2 * defaultDuration),
			},
		},
		{
			name: "3/3 Chunks Aligned Start",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart,
					End:   defaultStart.Add(9 * defaultDuration / 4),
					Chunk: defaultDuration,
				},
				next: 2,
			},
			c: chunk{
				start: defaultStart.Add(2 * defaultDuration),
				end:   defaultStart.Add(9 * defaultDuration / 4),
			},
		},
		{
			name: "1/4 Chunks Misaligned Both",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart.Add(defaultDuration / 2),
					End:   defaultStart.Add(7 * defaultDuration / 2),
					Chunk: defaultDuration,
				},
			},
			c: chunk{
				start: defaultStart.Add(defaultDuration / 2),
				end:   defaultStart.Add(defaultDuration),
			},
		},
		{
			name: "2/4 Chunks Misaligned Both",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart.Add(defaultDuration / 2),
					End:   defaultStart.Add(7 * defaultDuration / 2),
					Chunk: defaultDuration,
				},
				next: 1,
			},
			c: chunk{
				start: defaultStart.Add(defaultDuration),
				end:   defaultStart.Add(2 * defaultDuration),
			},
		},
		{
			name: "3/4 Chunks Misaligned Both",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart.Add(defaultDuration / 2),
					End:   defaultStart.Add(7 * defaultDuration / 2),
					Chunk: defaultDuration,
				},
				next: 2,
			},
			c: chunk{
				start: defaultStart.Add(2 * defaultDuration),
				end:   defaultStart.Add(3 * defaultDuration),
			},
		},
		{
			name: "4/4 Chunks Misaligned Both",
			s: stream{
				QuerySpec: QuerySpec{
					Start: defaultStart.Add(defaultDuration / 2),
					End:   defaultStart.Add(7 * defaultDuration / 2),
					Chunk: defaultDuration,
				},
				next: 3,
			},
			c: chunk{
				start: defaultStart.Add(3 * defaultDuration),
				end:   defaultStart.Add(7 * defaultDuration / 2),
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			start, end := testCase.s.nextChunkRange()

			assert.Equal(t, testCase.c.start, start, "chunk start time should be %s but is %s", testCase.c.start, start)
			assert.Equal(t, testCase.c.end, end, "chunk end time should be %s but is %s", testCase.c.end, end)
		})
	}
}
