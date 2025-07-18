// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPoller(t *testing.T) {
	p, a, l := newTestablePoller(t, 500)

	require.NotNil(t, p)
	assert.Equal(t, time.Second/500, p.minDelay)
	var closer <-chan struct{}
	closer = p.m.close
	assert.Equal(t, closer, p.close)
	var in <-chan *chunk = p.m.poll
	assert.Equal(t, in, p.in)
	var out chan<- *chunk = p.m.update
	assert.Equal(t, out, p.out)
	assert.Equal(t, "poller", p.name)
	assert.Equal(t, 10, p.maxTemporaryError)
	a.AssertExpectations(t)
	l.AssertExpectations(t)
}

func TestPoller_context(t *testing.T) {
	p, a, l := newTestablePoller(t, 1_000_000)
	expected := context.WithValue(context.Background(), "ham", "eggs")

	actual := p.context(&chunk{
		ctx: expected,
	})

	assert.Same(t, expected, actual)
	l.AssertExpectations(t)
	a.AssertExpectations(t)
}

func TestPoller_manipulate(t *testing.T) {
	t.Run("Dead Stream", func(t *testing.T) {
		p, actions, logger := newTestablePoller(t, 5_000_000)
		c := &chunk{
			stream: &stream{
				err: errors.New("malinvestment of inputs"),
			},
		}

		o := p.manipulate(c)

		assert.Equal(t, nothing, o)
		actions.AssertExpectations(t)
		logger.AssertExpectations(t)
	})

	t.Run("Live Stream", func(t *testing.T) {
		text := "text of the query"
		start := time.Date(2022, 1, 24, 23, 2, 0, 0, time.UTC)
		end := time.Date(2022, 1, 24, 23, 8, 0, 0, time.UTC)
		chunkID := "ID of the chunk"
		queryID := "ID of the query"
		logID := chunkID + "(" + queryID + ")"

		testCases := []struct {
			name               string
			setup              func(t *testing.T, logger *mockLogger, c *chunk)
			output             *cloudwatchlogs.GetQueryResultsOutput
			err                error
			expectedOutcome    outcome
			expectedStats      Stats
			expectedChunkErr   error
			expectedChunkState state
			expectedRestart    int
		}{
			{
				name:            "Throttling Error",
				err:             &types.ThrottlingException{Message: aws.String("foo")},
				expectedOutcome: throttlingError,
				expectedChunkErr: &UnexpectedQueryError{
					QueryID: queryID,
					Text:    text,
					Cause:   &types.ThrottlingException{Message: aws.String("foo")},
				},
			},
			{
				name:            "Temporary Error",
				err:             syscall.ECONNRESET,
				expectedOutcome: temporaryError,
				expectedChunkErr: &UnexpectedQueryError{
					QueryID: queryID,
					Text:    text,
					Cause:   syscall.ECONNRESET,
				},
			},
			{
				name: "Permanent Error",
				setup: func(t *testing.T, logger *mockLogger, c *chunk) {
					logger.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", t.Name(),
						"non-retryable failure to poll", logID, text, start, end, "error from CloudWatch Logs: a very permanent error")
				},
				err:             errors.New("a very permanent error"),
				expectedOutcome: finished,
				expectedChunkErr: &UnexpectedQueryError{
					QueryID: queryID,
					Text:    text,
					Cause:   errors.New("a very permanent error"),
				},
			},
			{
				name:            "Nil Status",
				output:          &cloudwatchlogs.GetQueryResultsOutput{},
				expectedOutcome: finished,
				expectedChunkErr: &TerminalQueryStatusError{
					QueryID: queryID,
					Status:  "",
					Text:    text,
				},
			},
			{
				name: "Status Scheduled",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Status: types.QueryStatusScheduled,
				},
				expectedOutcome: inconclusive,
			},
			{
				name: "Status Unknown",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Status: types.QueryStatusUnknown,
				},
				expectedOutcome: inconclusive,
			},
			{
				name: "Status Running, Non-Preview",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Status: types.QueryStatusRunning,
				},
				expectedOutcome: inconclusive,
			},
			{
				name: "Status Running, Preview, Translate Success",
				setup: func(_ *testing.T, _ *mockLogger, c *chunk) {
					c.ptr = make(map[string]bool)
				},
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]types.ResultField{
						{{Field: aws.String("@ptr"), Value: aws.String("123")}},
					},
					Statistics: &types.QueryStatistics{
						BytesScanned:   1.0,
						RecordsMatched: 2.0,
						RecordsScanned: 3.0,
					},
					Status: types.QueryStatusRunning,
				},
				expectedOutcome: inconclusive,
			},
			{
				name: "Status Complete, Non-Splittable, Translate Success",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]types.ResultField{
						{{Field: aws.String("@ptr"), Value: aws.String("123")}},
					},
					Statistics: &types.QueryStatistics{
						BytesScanned: 31,
					},
					Status: types.QueryStatusComplete,
				},
				expectedOutcome: finished,
				expectedStats: Stats{
					BytesScanned: 31,
					RangeDone:    6 * time.Minute,
				},
				expectedChunkState: complete,
			},
			{
				name: "Status Complete, Non-Splittable, Translate Error, No Key",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]types.ResultField{
						{{Field: nil, Value: aws.String("123")}},
					},
					Statistics: &types.QueryStatistics{
						RecordsScanned: 33,
					},
					Status: types.QueryStatusComplete,
				},
				expectedOutcome: finished,
				expectedChunkErr: &UnexpectedQueryError{
					QueryID: queryID,
					Text:    text,
					Cause:   errNoKey(),
				},
				expectedStats: Stats{
					RecordsScanned: 33,
					RangeDone:      6 * time.Minute,
				},
			},
			{
				name: "Status Complete, Non-Splittable, Translate Error, No Value",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]types.ResultField{
						{{Field: aws.String("@ptr"), Value: nil}},
					},
					Statistics: &types.QueryStatistics{
						BytesScanned:   34,
						RecordsMatched: 101,
					},
					Status: types.QueryStatusComplete,
				},
				expectedOutcome: finished,
				expectedStats: Stats{
					BytesScanned:   34,
					RecordsMatched: 101,
					RangeDone:      6 * time.Minute,
				},
				expectedChunkErr: &UnexpectedQueryError{
					QueryID: queryID,
					Text:    text,
					Cause:   errNoValue("@ptr"),
				},
			},
			{
				name: "Status Complete, Splittable",
				setup: func(t *testing.T, _ *mockLogger, c *chunk) {
					// Trigger spitting.
					t.Cleanup(func() {
						maxLimit = MaxLimit
					})
					maxLimit = 1
					c.stream.Limit = 1
				},
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]types.ResultField{
						{{Field: aws.String("@ptr"), Value: aws.String("123")}},
					},
					Statistics: &types.QueryStatistics{
						BytesScanned:   50,
						RecordsMatched: 55,
						RecordsScanned: 60,
					},
					Status: types.QueryStatusComplete,
				},
				expectedOutcome:  finished,
				expectedStats:    Stats{50, 55, 60, 0, 0, 0, 0, 0},
				expectedChunkErr: errSplitChunk,
			},
			{
				name: "Status Failed, Preview",
				setup: func(_ *testing.T, logger *mockLogger, c *chunk) {
					c.ptr = make(map[string]bool)
				},
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Statistics: &types.QueryStatistics{
						BytesScanned: 70,
					},
					Status: types.QueryStatusFailed,
				},
				expectedOutcome: finished,
				expectedStats:   Stats{70, 0, 0, 0, 0, 0, 0, 0},
				expectedChunkErr: &TerminalQueryStatusError{
					QueryID: queryID,
					Status:  string(types.QueryStatusFailed),
					Text:    text,
				},
			},
			{
				name: "Status Failed, Non-Preview, Restartable",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Statistics: &types.QueryStatistics{
						RecordsMatched: 85,
					},
					Status: types.QueryStatusFailed,
				},
				expectedOutcome:  finished,
				expectedStats:    Stats{0, 85, 0, 0, 0, 0, 0, 0},
				expectedChunkErr: errRestartChunk,
				expectedRestart:  1,
			},
			{
				name: "Status Failed, Non-Preview, Non-Restartable",
				setup: func(_ *testing.T, _ *mockLogger, c *chunk) {
					c.restart = maxRestart
				},
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Statistics: &types.QueryStatistics{
						RecordsScanned: 90,
					},
					Status: types.QueryStatusFailed,
				},
				expectedOutcome: finished,
				expectedStats:   Stats{0, 0, 90, 0, 0, 0, 0, 0},
				expectedChunkErr: &TerminalQueryStatusError{
					QueryID: queryID,
					Status:  string(types.QueryStatusFailed),
					Text:    text,
				},
				expectedRestart: maxRestart,
			},
			{
				name: "Status Cancelled",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Statistics: &types.QueryStatistics{
						BytesScanned:   222,
						RecordsMatched: 221,
						RecordsScanned: 223,
					},
					Status: types.QueryStatusCancelled,
				},
				expectedOutcome: finished,
				expectedStats:   Stats{222, 221, 223, 0, 0, 0, 0, 0},
				expectedChunkErr: &TerminalQueryStatusError{
					QueryID: queryID,
					Status:  string(types.QueryStatusCancelled),
					Text:    text,
				},
			},
			{
				name: "Status Fake",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Statistics: &types.QueryStatistics{
						BytesScanned:   375,
						RecordsMatched: 380,
						RecordsScanned: 385,
					},
					Status: "Fake Status",
				},
				expectedOutcome: finished,
				expectedStats:   Stats{375, 380, 385, 0, 0, 0, 0, 0},
				expectedChunkErr: &TerminalQueryStatusError{
					QueryID: queryID,
					Status:  "Fake Status",
					Text:    text,
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				p, actions, logger := newTestablePoller(t, 10_000_000)
				groups := []string{"a", "b"}
				var limit int32 = 1_000
				actions.
					On("GetQueryResults", anyContext, &cloudwatchlogs.GetQueryResultsInput{
						QueryId: &queryID,
					}).
					Return(testCase.output, testCase.err).
					Once()
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
					queryID: queryID,
					start:   start,
					end:     end,
				}
				c.stream.more = sync.NewCond(&c.stream.lock)
				if testCase.setup != nil {
					testCase.setup(t, logger, c)
				}

				before := time.Now()
				actualResult := p.manipulate(c)
				after := time.Now()

				assert.Equal(t, testCase.expectedOutcome, actualResult)
				assert.GreaterOrEqual(t, p.lastReq.Sub(before), time.Duration(0))
				assert.GreaterOrEqual(t, after.Sub(p.lastReq), time.Duration(0))
				assert.Equal(t, testCase.expectedStats, c.Stats)
				assert.Equal(t, testCase.expectedChunkErr, c.err)
				assert.Equal(t, testCase.expectedChunkState, c.state)
				assert.Equal(t, testCase.expectedRestart, c.restart)
				actions.AssertExpectations(t)
				logger.AssertExpectations(t)
			})
		}
	})
}

func TestPoller_release(t *testing.T) {
	p, actions, logger := newTestablePoller(t, 5_000_000)
	logger.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(),
		"releasing pollable", "wee sleekit cowrin(timrous beastie)", "oh what a panic", time.Time{}, time.Time{})

	p.release(&chunk{
		stream: &stream{
			QuerySpec: QuerySpec{
				Text: "oh what a panic",
			},
		},
		chunkID: "wee sleekit cowrin",
		queryID: "timrous beastie",
	})

	actions.AssertExpectations(t)
	logger.AssertExpectations(t)
}

func newTestablePoller(t *testing.T, rps int) (p *poller, a *mockActions, l *mockLogger) {
	a = newMockActions(t)
	l = newMockLogger(t)
	p = newPoller(&mgr{
		Config: Config{
			Actions: a,
			RPS: map[CloudWatchLogsAction]int{
				GetQueryResults: rps,
			},
			Logger: l,
			Name:   t.Name(),
		},
		close:  make(chan struct{}),
		poll:   make(chan *chunk),
		update: make(chan *chunk),
	})
	return
}

func Test_TranslateStats(t *testing.T) {
	testCases := []struct {
		name     string
		in       *types.QueryStatistics
		expected Stats
	}{
		{
			name: "Nil In",
		},
		{
			name: "Nil BytesScanned",
			in: &types.QueryStatistics{
				RecordsMatched: 2.0,
				RecordsScanned: 3.0,
			},
			expected: Stats{
				RecordsMatched: 2.0,
				RecordsScanned: 3.0,
			},
		},
		{
			name: "Nil RecordsMatched",
			in: &types.QueryStatistics{
				BytesScanned:   1.0,
				RecordsScanned: 3.0,
			},
			expected: Stats{
				BytesScanned:   1.0,
				RecordsScanned: 3.0,
			},
		},
		{
			name: "Nil RecordsScanned",
			in: &types.QueryStatistics{
				BytesScanned:   1.0,
				RecordsMatched: 2.0,
			},
			expected: Stats{
				BytesScanned:   1.0,
				RecordsMatched: 2.0,
			},
		},
		{
			name: "Normal",
			in: &types.QueryStatistics{
				BytesScanned:   5.0,
				RecordsMatched: 4.0,
				RecordsScanned: 3.0,
			},
			expected: Stats{
				BytesScanned:   5.0,
				RecordsMatched: 4.0,
				RecordsScanned: 3.0,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var actual Stats

			translateStats(testCase.in, &actual)

			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func Test_TranslateResultsPreview(t *testing.T) {
	text := "<query text>"
	queryID := "<query ID>"

	testCases := []struct {
		name                string
		ptrBefore, ptrAfter map[string]bool
		in                  [][]types.ResultField
		out                 []Result
		err                 error
	}{
		{
			name: "Nil",
			out:  make([]Result, 0),
		},
		{
			name: "Empty",
			in:   make([][]types.ResultField, 0),
			out:  make([]Result, 0),
		},
		{
			name: "@ptr already known, skip",
			ptrBefore: map[string]bool{
				"foo": true,
			},
			ptrAfter: map[string]bool{
				"foo": true,
			},
			in: [][]types.ResultField{
				{{Field: aws.String("@ptr"), Value: aws.String("foo")}},
			},
			out: make([]Result, 0),
		},
		{
			name: "@ptr already known, skip even if has nil ResultField",
			ptrBefore: map[string]bool{
				"foo": true,
			},
			ptrAfter: map[string]bool{
				"foo": true,
			},
			in: [][]types.ResultField{
				{{Field: aws.String("@ptr"), Value: aws.String("foo")}, {}}, // zero-value ResultField instead of nil
			},
			out: make([]Result, 0),
		},
		{
			name: "@ptr already known, skip even if has nil key",
			ptrBefore: map[string]bool{
				"foo": true,
			},
			ptrAfter: map[string]bool{
				"foo": true,
			},
			in: [][]types.ResultField{
				{{Field: aws.String("@ptr"), Value: aws.String("foo")}, {Value: aws.String("bar")}},
			},
			out: make([]Result, 0),
		},
		{
			name: "@ptr already known, skip even if has nil value",
			ptrBefore: map[string]bool{
				"foo": true,
			},
			ptrAfter: map[string]bool{
				"foo": true,
			},
			in: [][]types.ResultField{
				{{Field: aws.String("@ptr"), Value: aws.String("foo")}, {Field: aws.String("baz")}},
			},
			out: make([]Result, 0),
		},
		{
			name:      "@ptr not known, retain",
			ptrBefore: make(map[string]bool),
			ptrAfter: map[string]bool{
				"foo": true,
			},
			in: [][]types.ResultField{
				{{Field: aws.String("@ptr"), Value: aws.String("foo")}, {Field: aws.String("x"), Value: aws.String("y")}},
			},
			out: []Result{{{"@ptr", "foo"}, {"x", "y"}}},
		},
		{
			name: "@ptr not known, fail if nil key",
			in: [][]types.ResultField{
				{{Field: aws.String("@ptr"), Value: aws.String("foo")}, {Value: aws.String("value")}},
			},
			err: &UnexpectedQueryError{queryID, text, errNoKey()},
		},
		{
			name: "@ptr not known, fail if nil value",
			in: [][]types.ResultField{
				{{Field: aws.String("@ptr"), Value: aws.String("foo")}, {Field: aws.String("key")}},
			},
			err: &UnexpectedQueryError{queryID, text, errNoValue("key")},
		},
		{
			name: "No @ptr, retain",
			in: [][]types.ResultField{
				{{Field: aws.String("ham"), Value: aws.String("eggs")}},
			},
			out: []Result{{{"ham", "eggs"}}},
		},
		{
			name: "No @ptr, fail if nil key",
			in: [][]types.ResultField{
				{{Value: aws.String("SuperValu")}},
			},
			err: &UnexpectedQueryError{queryID, text, errNoKey()},
		},
		{
			name: "No @ptr, fail if nil value",
			in: [][]types.ResultField{
				{{Field: aws.String("Fields")}},
			},
			err: &UnexpectedQueryError{queryID, text, errNoValue("Fields")},
		},
		{
			name: "Missing @ptr previously visible, insert @deleted",
			ptrBefore: map[string]bool{
				"bar": true,
			},
			ptrAfter: map[string]bool{},
			in:       [][]types.ResultField{},
			out:      []Result{{{"@ptr", "bar"}, {"@deleted", "true"}}},
		},
		{
			name: "Had same @ptr previously, do not repeat it",
			ptrBefore: map[string]bool{
				"baz": true,
			},
			ptrAfter: map[string]bool{
				"baz": true,
			},
			in: [][]types.ResultField{
				{{Field: aws.String("@ptr"), Value: aws.String("baz")}},
			},
			out: make([]Result, 0),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			c := chunk{
				stream: &stream{
					QuerySpec: QuerySpec{
						Text: text,
					},
				},
				queryID: queryID,
				ptr:     testCase.ptrBefore,
			}

			r, err := translateResultsPreview(&c, testCase.in)

			assert.Equal(t, testCase.err, err)
			assert.Equal(t, testCase.out, r)
			assert.Equal(t, testCase.ptrAfter, c.ptr)
		})
	}
}
