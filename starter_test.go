// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStarter(t *testing.T) {
	s, a, l := newTestableStarter(t, 25)

	require.NotNil(t, s)
	assert.Equal(t, time.Second/25, s.minDelay)
	var closer <-chan struct{}
	closer = s.m.close
	assert.Equal(t, closer, s.close)
	var in <-chan *chunk = s.m.start
	assert.Equal(t, in, s.in)
	var out chan<- *chunk = s.m.update
	assert.Equal(t, out, s.out)
	assert.Equal(t, "starter", s.name)
	assert.Equal(t, 10, s.maxTry)
	a.AssertExpectations(t)
	l.AssertExpectations(t)
}

func TestStarter_context(t *testing.T) {
	s, a, l := newTestableStarter(t, 100_000)
	expected := context.WithValue(context.Background(), "foo", "bar")

	actual := s.context(&chunk{
		ctx: expected,
	})

	assert.Same(t, expected, actual)
	l.AssertExpectations(t)
	a.AssertExpectations(t)
}

func TestStarter_manipulate(t *testing.T) {
	t.Run("Dead Stream", func(t *testing.T) {
		s, actions, logger := newTestableStarter(t, 1_000_000)
		c := &chunk{
			stream: &stream{
				err: errors.New("the fatal conceit"),
			},
		}

		result := s.manipulate(c)

		assert.True(t, result)
		actions.AssertExpectations(t)
		logger.AssertExpectations(t)
	})

	t.Run("Live Stream", func(t *testing.T) {
		text := "qqqq"
		start := time.Date(2022, 1, 24, 21, 50, 47, 0, time.UTC)
		end := start.Add(10 * time.Second)
		chunkID := "ham"
		queryID := "eggs"

		testCases := []struct {
			name     string
			setup    func(t *testing.T, logger *mockLogger)
			output   *cloudwatchlogs.StartQueryOutput
			err      error
			expected bool
		}{
			{
				name: "Temporary Error",
				setup: func(t *testing.T, logger *mockLogger) {
					logger.ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", t.Name(),
						"temporary failure to start", chunkID, text, start, end, "connection refused")
				},
				err: syscall.ECONNREFUSED,
			},
			{
				name: "Permanent Error",
				setup: func(t *testing.T, logger *mockLogger) {
					logger.ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", t.Name(),
						"permanent failure to start", chunkID, text, start, end, "fatal error from CloudWatch Logs: what do you imagine you can design")
				},
				err:      errors.New("what do you imagine you can design"),
				expected: true,
			},
			{
				name: "Nil Query IDs",
				setup: func(t *testing.T, logger *mockLogger) {
					logger.ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(),
						"nil query ID from CloudWatch Logs for", chunkID, text, start, end)
				},
				output:   &cloudwatchlogs.StartQueryOutput{},
				expected: true,
			},
			{
				name: "Successful Start",
				setup: func(t *testing.T, logger *mockLogger) {
					logger.ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(),
						"started", chunkID+"("+queryID+")", text, start, end)
				},
				output: &cloudwatchlogs.StartQueryOutput{
					QueryId: sp("eggs"),
				},
				expected: true,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				s, actions, logger := newTestableStarter(t, 1_000_000)
				groups := []*string{sp("g")}
				var limit int64 = 999
				actions.
					On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
						QueryString:   &text,
						StartTime:     startTimeSeconds(start),
						EndTime:       endTimeSeconds(end),
						LogGroupNames: groups,
						Limit:         &limit,
					}).
					Return(testCase.output, testCase.err).
					Once()
				testCase.setup(t, logger)
				c := &chunk{
					stream: &stream{
						QuerySpec: QuerySpec{
							Text:  text,
							Limit: limit,
						},
						groups: groups,
					},
					ctx:     context.Background(),
					chunkID: chunkID,
					start:   start,
					end:     end,
				}

				before := time.Now()
				actual := s.manipulate(c)
				after := time.Now()

				assert.Equal(t, testCase.expected, actual)
				assert.GreaterOrEqual(t, s.lastReq.Sub(before), time.Duration(0))
				assert.GreaterOrEqual(t, after.Sub(s.lastReq), time.Duration(0))
				actions.AssertExpectations(t)
				logger.AssertExpectations(t)
			})
		}
	})
}

func TestStarter_release(t *testing.T) {
	s, actions, logger := newTestableStarter(t, 1_000_000)
	logger.ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(),
		"releasing startable", "hello(world)", "bonjour monde!", time.Time{}, time.Time{})

	s.release(&chunk{
		stream: &stream{
			QuerySpec: QuerySpec{
				Text: "bonjour monde!",
			},
		},
		chunkID: "hello",
		queryID: "world",
	})

	actions.AssertExpectations(t)
	logger.AssertExpectations(t)
}

func newTestableStarter(t *testing.T, rps int) (s *starter, a *mockActions, l *mockLogger) {
	a = newMockActions(t)
	l = newMockLogger(t)
	s = newStarter(&mgr{
		Config: Config{
			Actions: a,
			RPS: map[CloudWatchLogsAction]int{
				StartQuery: rps,
			},
			Logger: l,
			Name:   t.Name(),
		},
		close:  make(chan struct{}),
		start:  make(chan *chunk),
		update: make(chan *chunk),
	})
	return
}
