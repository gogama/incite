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

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
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
				err:             awserr.New("throttled", "foo", nil),
				expectedOutcome: throttlingError,
				expectedChunkErr: &UnexpectedQueryError{
					QueryID: queryID,
					Text:    text,
					Cause:   awserr.New("throttled", "foo", nil),
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
				expectedChunkErr: &UnexpectedQueryError{
					QueryID: queryID,
					Text:    text,
					Cause:   errNilStatus(),
				},
			},
			{
				name: "Status Scheduled",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Status: sp(cloudwatchlogs.QueryStatusScheduled),
				},
				expectedOutcome: inconclusive,
			},
			{
				name: "Status Unknown",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Status: sp("Unknown"),
				},
				expectedOutcome: inconclusive,
			},
			{
				name: "Status Running, Non-Preview",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Status: sp(cloudwatchlogs.QueryStatusRunning),
				},
				expectedOutcome: inconclusive,
			},
			{
				name: "Status Running, Preview, Translate Success",
				setup: func(_ *testing.T, _ *mockLogger, c *chunk) {
					c.ptr = make(map[string]bool)
				},
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{
						{{Field: sp("@ptr"), Value: sp("123")}},
					},
					Statistics: &cloudwatchlogs.QueryStatistics{
						BytesScanned:   float64p(1),
						RecordsMatched: float64p(2),
						RecordsScanned: float64p(3),
					},
					Status: sp(cloudwatchlogs.QueryStatusRunning),
				},
				expectedOutcome: inconclusive,
			},
			{
				name: "Status Running, Preview, Translate Error",
				setup: func(_ *testing.T, _ *mockLogger, c *chunk) {
					c.ptr = make(map[string]bool)
				},
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{
						{nil},
					},
					Statistics: &cloudwatchlogs.QueryStatistics{
						BytesScanned:   float64p(6),
						RecordsMatched: float64p(5),
						RecordsScanned: float64p(4),
					},
					Status: sp(cloudwatchlogs.QueryStatusRunning),
				},
				expectedOutcome: finished,
				expectedStats:   Stats{6, 5, 4, 0, 0, 0, 0, 0},
				expectedChunkErr: &UnexpectedQueryError{
					QueryID: queryID,
					Text:    text,
					Cause:   errNilResultField(0),
				},
			},
			{
				name: "Status Complete, Non-Splittable, Translate Success",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{
						{{Field: sp("@ptr"), Value: sp("123")}},
					},
					Statistics: &cloudwatchlogs.QueryStatistics{
						BytesScanned: float64p(31),
					},
					Status: sp(cloudwatchlogs.QueryStatusComplete),
				},
				expectedOutcome: finished,
				expectedStats: Stats{
					BytesScanned: 31,
					RangeDone:    6 * time.Minute,
				},
				expectedChunkState: complete,
			},
			{
				name: "Status Complete, Non-Splittable, Translate Error, Nil ResultField",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{
						{nil},
					},
					Statistics: &cloudwatchlogs.QueryStatistics{
						RecordsMatched: float64p(32),
					},
					Status: sp(cloudwatchlogs.QueryStatusComplete),
				},
				expectedOutcome: finished,
				expectedChunkErr: &UnexpectedQueryError{
					QueryID: queryID,
					Text:    text,
					Cause:   errNilResultField(0),
				},
				expectedStats: Stats{
					RecordsMatched: 32,
					RangeDone:      6 * time.Minute,
				},
			},
			{
				name: "Status Complete, Non-Splittable, Translate Error, No Key",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{
						{{Field: nil, Value: sp("123")}},
					},
					Statistics: &cloudwatchlogs.QueryStatistics{
						RecordsScanned: float64p(33),
					},
					Status: sp(cloudwatchlogs.QueryStatusComplete),
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
					Results: [][]*cloudwatchlogs.ResultField{
						{{Field: sp("@ptr"), Value: nil}},
					},
					Statistics: &cloudwatchlogs.QueryStatistics{
						BytesScanned:   float64p(34),
						RecordsMatched: float64p(101),
					},
					Status: sp(cloudwatchlogs.QueryStatusComplete),
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
					Results: [][]*cloudwatchlogs.ResultField{
						{{Field: sp("@ptr"), Value: sp("123")}},
					},
					Statistics: &cloudwatchlogs.QueryStatistics{
						BytesScanned:   float64p(50),
						RecordsMatched: float64p(55),
						RecordsScanned: float64p(60),
					},
					Status: sp(cloudwatchlogs.QueryStatusComplete),
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
					Statistics: &cloudwatchlogs.QueryStatistics{
						BytesScanned: float64p(70),
					},
					Status: sp(cloudwatchlogs.QueryStatusFailed),
				},
				expectedOutcome: finished,
				expectedStats:   Stats{70, 0, 0, 0, 0, 0, 0, 0},
				expectedChunkErr: &TerminalQueryStatusError{
					QueryID: queryID,
					Status:  cloudwatchlogs.QueryStatusFailed,
					Text:    text,
				},
			},
			{
				name: "Status Failed, Non-Preview, Restartable",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Statistics: &cloudwatchlogs.QueryStatistics{
						RecordsMatched: float64p(85),
					},
					Status: sp(cloudwatchlogs.QueryStatusFailed),
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
					Statistics: &cloudwatchlogs.QueryStatistics{
						RecordsScanned: float64p(90),
					},
					Status: sp(cloudwatchlogs.QueryStatusFailed),
				},
				expectedOutcome: finished,
				expectedStats:   Stats{0, 0, 90, 0, 0, 0, 0, 0},
				expectedChunkErr: &TerminalQueryStatusError{
					QueryID: queryID,
					Status:  cloudwatchlogs.QueryStatusFailed,
					Text:    text,
				},
				expectedRestart: maxRestart,
			},
			{
				name: "Status Cancelled",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Statistics: &cloudwatchlogs.QueryStatistics{
						BytesScanned:   float64p(222),
						RecordsMatched: float64p(221),
						RecordsScanned: float64p(223),
					},
					Status: sp(cloudwatchlogs.QueryStatusCancelled),
				},
				expectedOutcome: finished,
				expectedStats:   Stats{222, 221, 223, 0, 0, 0, 0, 0},
				expectedChunkErr: &TerminalQueryStatusError{
					QueryID: queryID,
					Status:  cloudwatchlogs.QueryStatusCancelled,
					Text:    text,
				},
			},
			{
				name: "Status Fake",
				output: &cloudwatchlogs.GetQueryResultsOutput{
					Statistics: &cloudwatchlogs.QueryStatistics{
						BytesScanned:   float64p(375),
						RecordsMatched: float64p(380),
						RecordsScanned: float64p(385),
					},
					Status: sp("Fake Status"),
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
				groups := []*string{sp("a"), sp("b")}
				var limit int64 = 1_000
				actions.
					On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{
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
		in       *cloudwatchlogs.QueryStatistics
		expected Stats
	}{
		{
			name: "Nil In",
		},
		{
			name: "Nil BytesScanned",
			in: &cloudwatchlogs.QueryStatistics{
				RecordsMatched: float64p(2.0),
				RecordsScanned: float64p(3.0),
			},
			expected: Stats{
				RecordsMatched: 2.0,
				RecordsScanned: 3.0,
			},
		},
		{
			name: "Nil RecordsMatched",
			in: &cloudwatchlogs.QueryStatistics{
				BytesScanned:   float64p(1.0),
				RecordsScanned: float64p(3.0),
			},
			expected: Stats{
				BytesScanned:   1.0,
				RecordsScanned: 3.0,
			},
		},
		{
			name: "Nil RecordsScanned",
			in: &cloudwatchlogs.QueryStatistics{
				BytesScanned:   float64p(1.0),
				RecordsMatched: float64p(2.0),
			},
			expected: Stats{
				BytesScanned:   1.0,
				RecordsMatched: 2.0,
			},
		},
		{
			name: "Normal",
			in: &cloudwatchlogs.QueryStatistics{
				BytesScanned:   float64p(5.0),
				RecordsMatched: float64p(4.0),
				RecordsScanned: float64p(3.0),
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
		in                  [][]*cloudwatchlogs.ResultField
		out                 []Result
		err                 error
	}{
		{
			name: "Nil",
			out:  make([]Result, 0),
		},
		{
			name: "Empty",
			in:   make([][]*cloudwatchlogs.ResultField, 0),
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
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("@ptr"), Value: sp("foo")}},
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
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("@ptr"), Value: sp("foo")}, nil},
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
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("@ptr"), Value: sp("foo")}, {Value: sp("bar")}},
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
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("@ptr"), Value: sp("foo")}, {Field: sp("baz")}},
			},
			out: make([]Result, 0),
		},
		{
			name:      "@ptr not known, retain",
			ptrBefore: make(map[string]bool),
			ptrAfter: map[string]bool{
				"foo": true,
			},
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("@ptr"), Value: sp("foo")}, {Field: sp("x"), Value: sp("y")}},
			},
			out: []Result{{{"@ptr", "foo"}, {"x", "y"}}},
		},
		{
			name: "@ptr not known, fail if nil ResultField",
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("@ptr"), Value: sp("foo")}, nil},
			},
			err: &UnexpectedQueryError{queryID, text, errNilResultField(1)},
		},
		{
			name: "@ptr not known, fail if nil key",
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("@ptr"), Value: sp("foo")}, {Value: sp("value")}},
			},
			err: &UnexpectedQueryError{queryID, text, errNoKey()},
		},
		{
			name: "@ptr not known, fail if nil value",
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("@ptr"), Value: sp("foo")}, {Field: sp("key")}},
			},
			err: &UnexpectedQueryError{queryID, text, errNoValue("key")},
		},
		{
			name: "No @ptr, retain",
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("ham"), Value: sp("eggs")}},
			},
			out: []Result{{{"ham", "eggs"}}},
		},
		{
			name: "No @ptr, fail if nil ResultField",
			in: [][]*cloudwatchlogs.ResultField{
				{nil},
			},
			err: &UnexpectedQueryError{queryID, text, errNilResultField(0)},
		},
		{
			name: "No @ptr, fail if nil key",
			in: [][]*cloudwatchlogs.ResultField{
				{{Value: sp("SuperValu")}},
			},
			err: &UnexpectedQueryError{queryID, text, errNoKey()},
		},
		{
			name: "No @ptr, fail if nil value",
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("Fields")}},
			},
			err: &UnexpectedQueryError{queryID, text, errNoValue("Fields")},
		},
		{
			name: "Missing @ptr previously visible, insert @deleted",
			ptrBefore: map[string]bool{
				"bar": true,
			},
			ptrAfter: map[string]bool{},
			in:       [][]*cloudwatchlogs.ResultField{},
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
			in: [][]*cloudwatchlogs.ResultField{
				{{Field: sp("@ptr"), Value: sp("baz")}},
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
