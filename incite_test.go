// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestNewQueryManager(t *testing.T) {
	t.Run("Invalid Input", func(t *testing.T) {
		t.Run("Nil Actions", func(t *testing.T) {
			assert.PanicsWithValue(t, nilActionsMsg, func() {
				NewQueryManager(Config{})
			})
		})
	})

	t.Run("Valid Input", func(t *testing.T) {
		actions := newMockActions(t)

		t.Run("Simple Cases", func(t *testing.T) {
			minDelay := map[CloudWatchLogsAction]time.Duration{
				StartQuery:      time.Second / time.Duration(3),
				StopQuery:       time.Second / time.Duration(3),
				GetQueryResults: time.Second / time.Duration(3),
			}

			testCases := []struct {
				name          string
				before, after Config
				minDelay      map[CloudWatchLogsAction]time.Duration
			}{
				{
					name: "Zero(ish)",
					before: Config{
						Actions: actions,
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						Logger:   NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "Parallel.Negative",
					before: Config{
						Actions:  actions,
						Parallel: -1,
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						Logger:   NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "Parallel.Positive",
					before: Config{
						Actions:  actions,
						Parallel: 1,
					},
					after: Config{
						Actions:  actions,
						Parallel: 1,
						Logger:   NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "Parallel.AtLimit",
					before: Config{
						Actions:  actions,
						Parallel: QueryConcurrencyQuotaLimit,
					},
					after: Config{
						Actions:  actions,
						Parallel: QueryConcurrencyQuotaLimit,
						Logger:   NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "Parallel.AboveLimit",
					before: Config{
						Actions:  actions,
						Parallel: QueryConcurrencyQuotaLimit + 1,
					},
					after: Config{
						Actions:  actions,
						Parallel: QueryConcurrencyQuotaLimit,
						Logger:   NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "RPS.SameAsDefault",
					before: Config{
						Actions: actions,
						RPS:     RPSDefaults,
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS:      RPSDefaults,
						Logger:   NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "RPS.StartQueryOverride.Negative",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							StartQuery: -1,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							StartQuery: -1,
						},
						Logger: NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "RPS.StartQueryOverride.Zero",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							StartQuery: 0,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							StartQuery: 0,
						},
						Logger: NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "RPS.StartQueryOverride.Positive",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							StartQuery: 1,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							StartQuery: 1,
						},
						Logger: NopLogger,
					},
					minDelay: map[CloudWatchLogsAction]time.Duration{
						StartQuery:      time.Second,
						StopQuery:       time.Second / time.Duration(3),
						GetQueryResults: time.Second / time.Duration(3),
					},
				},
				{
					name: "RPS.StartQueryOverride.AtQuotaLimit",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							StartQuery: RPSQuotaLimits[StartQuery],
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							StartQuery: RPSQuotaLimits[StartQuery],
						},
						Logger: NopLogger,
					},
					minDelay: map[CloudWatchLogsAction]time.Duration{
						StartQuery:      time.Second / time.Duration(RPSQuotaLimits[StartQuery]),
						StopQuery:       time.Second / time.Duration(3),
						GetQueryResults: time.Second / time.Duration(3),
					},
				},
				{
					name: "RPS.StartQueryOverride.AboveQuotaLimit",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							StartQuery: RPSQuotaLimits[StartQuery] + 1,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							StartQuery: RPSQuotaLimits[StartQuery] + 1,
						},
						Logger: NopLogger,
					},
					minDelay: map[CloudWatchLogsAction]time.Duration{
						StartQuery:      time.Second / time.Duration(RPSQuotaLimits[StartQuery]+1),
						StopQuery:       time.Second / time.Duration(3),
						GetQueryResults: time.Second / time.Duration(3),
					},
				},
				{
					name: "RPS.StopQueryOverride.Negative",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							StopQuery: -1,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							StopQuery: -1,
						},
						Logger: NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "RPS.StopQueryOverride.Zero",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							StopQuery: 0,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							StopQuery: 0,
						},
						Logger: NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "RPS.StopQueryOverride.Positive",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							StopQuery: 1,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							StopQuery: 1,
						},
						Logger: NopLogger,
					},
					minDelay: map[CloudWatchLogsAction]time.Duration{
						StartQuery:      time.Second / time.Duration(3),
						StopQuery:       time.Second,
						GetQueryResults: time.Second / time.Duration(3),
					},
				},
				{
					name: "RPS.StopQueryOverride.AtQuotaLimit",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							StopQuery: RPSQuotaLimits[StopQuery],
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							StopQuery: RPSQuotaLimits[StopQuery],
						},
						Logger: NopLogger,
					},
					minDelay: map[CloudWatchLogsAction]time.Duration{
						StartQuery:      time.Second / time.Duration(3),
						StopQuery:       time.Second / time.Duration(RPSQuotaLimits[StopQuery]),
						GetQueryResults: time.Second / time.Duration(3),
					},
				},
				{
					name: "RPS.StopQueryOverride.AboveQuotaLimit",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							StopQuery: RPSQuotaLimits[StopQuery] + 1,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							StopQuery: RPSQuotaLimits[StopQuery] + 1,
						},
						Logger: NopLogger,
					},
					minDelay: map[CloudWatchLogsAction]time.Duration{
						StartQuery:      time.Second / time.Duration(3),
						StopQuery:       time.Second / time.Duration(RPSQuotaLimits[StopQuery]+1),
						GetQueryResults: time.Second / time.Duration(3),
					},
				},
				{
					name: "RPS.GetQueryResultsOverride.Negative",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							GetQueryResults: -1,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							GetQueryResults: -1,
						},
						Logger: NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "RPS.GetQueryResultsOverride.Zero",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							GetQueryResults: 0,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							GetQueryResults: 0,
						},
						Logger: NopLogger,
					},
					minDelay: minDelay,
				},
				{
					name: "RPS.GetQueryResultsOverride.Positive",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							GetQueryResults: 1,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							GetQueryResults: 1,
						},
						Logger: NopLogger,
					},
					minDelay: map[CloudWatchLogsAction]time.Duration{
						StartQuery:      time.Second / time.Duration(3),
						StopQuery:       time.Second / time.Duration(3),
						GetQueryResults: time.Second,
					},
				},
				{
					name: "RPS.GetQueryResultsOverride.AtQuotaLimit",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							GetQueryResults: RPSQuotaLimits[GetQueryResults],
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							GetQueryResults: RPSQuotaLimits[GetQueryResults],
						},
						Logger: NopLogger,
					},
					minDelay: map[CloudWatchLogsAction]time.Duration{
						StartQuery:      time.Second / time.Duration(3),
						StopQuery:       time.Second / time.Duration(3),
						GetQueryResults: time.Second / time.Duration(RPSQuotaLimits[GetQueryResults]),
					},
				},
				{
					name: "RPS.GetQueryResultsOverride.AboveQuotaLimit",
					before: Config{
						Actions: actions,
						RPS: map[CloudWatchLogsAction]int{
							GetQueryResults: RPSQuotaLimits[GetQueryResults] + 1,
						},
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS: map[CloudWatchLogsAction]int{
							GetQueryResults: RPSQuotaLimits[GetQueryResults] + 1,
						},
						Logger: NopLogger,
					},
					minDelay: map[CloudWatchLogsAction]time.Duration{
						StartQuery:      time.Second / time.Duration(3),
						StopQuery:       time.Second / time.Duration(3),
						GetQueryResults: time.Second / time.Duration(RPSQuotaLimits[GetQueryResults]+1),
					},
				},
				{
					name: "Logger.NopLogger",
					before: Config{
						Actions: actions,
						Logger:  NopLogger,
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						Logger:   NopLogger,
					},
					minDelay: minDelay,
				},
			}

			for _, testCase := range testCases {
				t.Run(testCase.name, func(*testing.T) {
					m := NewQueryManager(testCase.before)
					require.NotNil(t, m)
					defer func() {
						err := m.Close()
						assert.NoError(t, err)
					}()

					require.IsType(t, &mgr{}, m)
					m2 := m.(*mgr)
					assert.Equal(t, testCase.after, m2.Config)
					assert.Equal(t, testCase.minDelay, m2.minDelay)
					assert.NotNil(t, m2.timer)
					assert.NotNil(t, m2.close)
					assert.NotNil(t, m2.query)
				})
			}
		})

		t.Run("Custom Logger", func(t *testing.T) {
			logger := newMockLogger(t)
			logger.ExpectPrintf("incite: QueryManager (%p) start").Maybe()
			logger.ExpectPrintf("incite: QueryManager (%p) stop").Maybe()
			m := NewQueryManager(Config{
				Actions: actions,
				Logger:  logger,
			})
			require.NotNil(t, m)
			defer func() {
				err := m.Close()
				assert.NoError(t, err)
			}()

			require.IsType(t, &mgr{}, m)
			m2 := m.(*mgr)
			assert.Same(t, logger, m2.Logger)
		})
	})
}

func TestQueryManager_Close(t *testing.T) {
	t.Run("There Can Be Only One", func(t *testing.T) {
		m := NewQueryManager(Config{
			Actions: newMockActions(t),
		})
		require.NotNil(t, m)
		n := 1000
		ch := make(chan error, n)
		defer close(ch)

		for i := 0; i < n; i++ {
			go func() {
				ch <- m.Close()
			}()
		}

		var success bool
		for i := 0; i < n; i++ {
			err := <-ch
			if err == nil {
				assert.False(t, success, "Close should only return nil once")
			} else {
				assert.Same(t, ErrClosed, err, "Close should only return ErrClosed or nil")
			}
		}
	})

	t.Run("Close Cancels Queries", func(t *testing.T) {
		// TODO: Simple test case with multiple in-flight queries that get cancelled.
	})

	// TODO: Test multi-thread read case. Second reader should be blocking on
	//       the stream lock and not get in, but why not test?
	// TODO: PRIORITIZATION. Run a test where a master goroutine creates say
	//       1000 queries with priorities 1..1000 and then read them from, again,
	//       that single master goroutine. For each query i, the query text is just
	//       the stringized number i. Hook the StartQueryWithContext calls and put
	//       them into an array. Assert that it is in ascending order of
	//       priority. Have each query be in two chunks and have GetQueryResults
	//       return InProgress once and Completed on the second try. Hook the
	//       GetQueryResults calls and record them into an array. Assert the
	//       array is in priority order, and time order within the same query.
	//       When done, look within the *mgr and assert that pq and chunks are
	//       both empty.
	// TODO: Errors. Assert that all fatal errors returned from stream.Read are
	//       one of the three error tyeps defined in errors.go.
}

func TestQueryManager_GetStats(t *testing.T) {
	m := NewQueryManager(Config{
		Actions: newMockActions(t),
	})
	require.NotNil(t, m)
	defer func() {
		err := m.Close()
		assert.NoError(t, err)
	}()

	assert.Equal(t, Stats{}, m.GetStats())
}

func TestQueryManager_Query(t *testing.T) {
	t.Run("Invalid Input", func(t *testing.T) {
		testCases := []struct {
			QuerySpec
			name string
			err  string
		}{
			{
				name: "Text.Empty",
				QuerySpec: QuerySpec{
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"foo"},
				},
				err: textBlankMsg,
			},
			{
				name: "Text.Blank",
				QuerySpec: QuerySpec{
					Text:   " \t\r\n",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"bar"},
				},
				err: textBlankMsg,
			},
			{
				name: "Start.SubSecond",
				QuerySpec: QuerySpec{
					Text:   "baz",
					Start:  time.Date(2021, 7, 15, 3, 37, 25, 123, time.UTC),
					End:    defaultEnd,
					Groups: []string{"baz"},
				},
				err: startSubSecondMsg,
			},
			{
				name: "End.SubSecond",
				QuerySpec: QuerySpec{
					Text:   "qux",
					Start:  defaultStart,
					End:    time.Date(2021, 7, 15, 3, 37, 25, 123, time.UTC),
					Groups: []string{"qux", "jilly"},
				},
				err: endSubSecondMsg,
			},
			{
				name: "End.NotAfter.Start",
				QuerySpec: QuerySpec{
					Text:   "ham",
					Start:  defaultEnd,
					End:    defaultStart,
					Groups: []string{"ham"},
				},
				err: endNotBeforeStartMsg,
			},
			{
				name: "Groups.Nil",
				QuerySpec: QuerySpec{
					Text:  "ham",
					Start: defaultStart,
					End:   defaultEnd,
				},
				err: noGroupsMsg,
			},
			{
				name: "Groups.Empty",
				QuerySpec: QuerySpec{
					Text:   "ham",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{},
				},
				err: noGroupsMsg,
			},
			{
				name: "MaxLimit.Exceeded",
				QuerySpec: QuerySpec{
					Text:   "eggs",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"spam"},
					Limit:  MaxLimit + 1,
				},
				err: exceededMaxLimitMsg,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				m := NewQueryManager(Config{
					Actions: newMockActions(t),
				})
				require.NotNil(t, m)
				defer func() {
					err := m.Close()
					assert.NoError(t, err)
				}()

				s, err := m.Query(testCase.QuerySpec)

				assert.Nil(t, s)
				assert.EqualError(t, err, testCase.err)
			})
		}
	})

	t.Run("Valid Input But StartQuery Fails", func(t *testing.T) {
		// The purpose of these test cases is just to verify that the
		// QueryManage accepts valid input, successfully starts a query,
		// and fails it fast when StartQuery throws back an unexpected
		// error. These ones are meant to be simple. More complex
		// testing is done in the scenario tests below.

		testCases := []struct {
			name              string
			before            QuerySpec
			after             QuerySpec
			expectedN         int64
			expectedChunkHint uint16
			expectedGroups    []*string
			expectedNext      time.Time
		}{
			{
				name: "Zero",
				before: QuerySpec{
					Text:   "foo",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"bar", "Baz"},
				},
				after: QuerySpec{
					Text:   "foo",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"bar", "Baz"},
					Limit:  DefaultLimit,
					Chunk:  5 * time.Minute,
					Hint:   minHint,
				},
				expectedN:         1,
				expectedChunkHint: minHint,
				expectedGroups:    []*string{sp("bar"), sp("Baz")},
				expectedNext:      defaultStart,
			},
			{
				name: "ChunkExceedsRange",
				before: QuerySpec{
					Text:   "foo",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"bar", "Baz"},
					Chunk:  24 * time.Hour,
				},
				after: QuerySpec{
					Text:   "foo",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"bar", "Baz"},
					Limit:  DefaultLimit,
					Chunk:  5 * time.Minute,
					Hint:   minHint,
				},
				expectedN:         1,
				expectedChunkHint: minHint,
				expectedGroups:    []*string{sp("bar"), sp("Baz")},
				expectedNext:      defaultStart,
			},
			{
				name: "PartialChunk",
				before: QuerySpec{
					Text:   "foo",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"bar", "Baz"},
					Chunk:  4 * time.Minute,
				},
				after: QuerySpec{
					Text:   "foo",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"bar", "Baz"},
					Limit:  DefaultLimit,
					Chunk:  4 * time.Minute,
					Hint:   minHint,
				},
				expectedN:         2,
				expectedChunkHint: minHint,
				expectedGroups:    []*string{sp("bar"), sp("Baz")},
				expectedNext:      defaultStart,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				causeErr := errors.New("super fatal error")
				actions := newMockActions(t)
				actions.
					On("StartQueryWithContext", anyContext, anyStartQueryInput).
					Return(nil, causeErr).
					Once()
				m := NewQueryManager(Config{
					Actions: actions,
				})
				require.NotNil(t, m)
				defer func() {
					err := m.Close()
					require.NoError(t, err)
				}()

				s, err := m.Query(testCase.before)
				require.NotNil(t, s)
				assert.NoError(t, err)
				require.IsType(t, &stream{}, s)
				s2 := s.(*stream)
				assert.Equal(t, testCase.after, s2.QuerySpec)
				assert.Equal(t, testCase.expectedN, s2.n)
				assert.Equal(t, testCase.expectedChunkHint, s2.chunkHint)
				assert.Equal(t, testCase.expectedGroups, s2.groups)
				assert.Equal(t, testCase.expectedNext, s2.next)
				r := make([]Result, 1)
				n, err := s.Read(r)
				assert.Equal(t, 0, n)
				var sqe *StartQueryError
				assert.ErrorAs(t, err, &sqe)
				assert.Same(t, sqe.Cause, causeErr)
				assert.Equal(t, Stats{}, s.GetStats())

				err = s.Close()
				assert.NoError(t, err)
				err = s.Close()
				assert.Same(t, ErrClosed, err)
				assert.Equal(t, Stats{}, s.GetStats())

				actions.AssertExpectations(t)
			})
		}
	})

	t.Run("QueryManager Already Closed", func(t *testing.T) {
		actions := newMockActions(t)
		m := NewQueryManager(Config{
			Actions: actions,
		})
		require.NotNil(t, m)
		err := m.Close()
		assert.NoError(t, err)

		s, err := m.Query(QuerySpec{
			Text:   "this should fail because the manager is closed",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"g"},
		})

		assert.Nil(t, s)
		assert.Same(t, ErrClosed, err)
	})

	t.Run("Empty Read Buffer Does Not Block", func(t *testing.T) {
		// This test verifies that calling Read with an empty buffer
		// does not block even if there are no results available.
		//
		// We run a single one-chunk query and make it get stuck in the
		// StartQuery API call.

		// ARRANGE:
		event := make(chan time.Time)
		actions := newMockActions(t)
		actions.
			On("StartQueryWithContext", anyContext, anyStartQueryInput).
			WaitUntil(event).
			Return(nil, context.Canceled).
			Once()
		m := NewQueryManager(Config{
			Actions: actions,
		})
		require.NotNil(t, m)
		s, err := m.Query(QuerySpec{
			Text:   "I see the future and this query never happens.",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"/never/queried/group"},
		})
		require.NotNil(t, s)
		require.NoError(t, err)
		t.Cleanup(func() {
			close(event)
			_ = m.Close()
		})

		// ACT.
		b := make([]Result, 0)
		n, readErr := s.Read(b)
		event <- time.Now()
		closeErr := m.Close()

		// ASSERT.
		actions.AssertExpectations(t)
		assert.Equal(t, 0, n)
		assert.NoError(t, readErr)
		assert.NoError(t, closeErr)
	})

	t.Run("Closing Query Releases Chunk Resources", func(t *testing.T) {
		// This test verifies that Close-ing a query that is stuck on a
		// long CWL API call to GetQueryResults does cancel the
		// in-flight API calls and call StopQuery where appropriate.
		//
		// ARRANGE:
		var wg1, wg2, wg3 sync.WaitGroup
		wg1.Add(3)
		wg2.Add(3)
		wg3.Add(2)
		event := make(chan time.Time)
		actions := newMockActions(t)
		actions.
			On("StartQueryWithContext", anyContext, mock.MatchedBy(func(input *cloudwatchlogs.StartQueryInput) bool {
				return *input.QueryString == "uno"
			})).
			Run(func(_ mock.Arguments) { wg1.Done() }).
			Return(&cloudwatchlogs.StartQueryOutput{QueryId: sp("a")}, nil).
			Once()
		actions.
			On("StartQueryWithContext", anyContext, mock.MatchedBy(func(input *cloudwatchlogs.StartQueryInput) bool {
				return *input.QueryString == "due"
			})).
			Run(func(_ mock.Arguments) { wg2.Done() }).
			Return(&cloudwatchlogs.StartQueryOutput{QueryId: sp("b")}, nil).
			Once()
		actions.
			On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: sp("a")}).
			Run(func(_ mock.Arguments) {
				wg1.Done()
				<-event
			}).
			Return(nil, context.Canceled).
			Once()
		actions.
			On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: sp("b")}).
			Run(func(_ mock.Arguments) {
				wg2.Done()
				<-event
			}).
			Return(nil, context.Canceled).
			Once()
		actions.
			On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{QueryId: sp("a")}).
			Run(func(_ mock.Arguments) { wg3.Done() }).
			Return(&cloudwatchlogs.StopQueryOutput{}, nil).
			Once()
		actions.
			On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{QueryId: sp("b")}).
			Run(func(_ mock.Arguments) { wg3.Done() }).
			Return(&cloudwatchlogs.StopQueryOutput{}, nil).
			Once()
		m := NewQueryManager(Config{
			Actions: actions,
		})
		require.NotNil(t, m)
		var s1, s2 Stream
		var err1, err2 error
		s1, err1 = m.Query(QuerySpec{
			Text:     "uno",
			Start:    defaultStart,
			End:      defaultEnd,
			Groups:   []string{"/first/one"},
			Priority: 1,
		})
		require.NotNil(t, s1)
		require.NoError(t, err1)
		wg1.Done()
		go func() {
			s2, err2 = m.Query(QuerySpec{
				Text:     "due",
				Start:    defaultStart,
				End:      defaultEnd,
				Groups:   []string{"/second/one"},
				Priority: 2,
			})
			require.NotNil(t, s2)
			require.NoError(t, err2)
			wg2.Done()
		}()
		t.Cleanup(func() {
			close(event)
			_ = m.Close()
		})

		// ACT.
		wg1.Wait()
		err1 = s1.Close()
		event <- time.Now()
		wg2.Wait()
		err2 = s2.Close()
		event <- time.Now()
		err3 := m.Close()
		wg3.Wait()

		// Assert
		actions.AssertExpectations(t)
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.NoError(t, err3)
	})

	t.Run("Query Starts But GetQueryResults Output Corrupt", func(t *testing.T) {
		// This set of test cases verifies that the query manager
		// recovers gracefully in very unlikely edge cases where the
		// CloudWatch Logs service client populates the GetQueryResults
		// output object with certain invalid values, such as a nil
		// pointer for the status field.

		testCases := []struct {
			name      string
			cause     error
			gqrOutput *cloudwatchlogs.GetQueryResultsOutput
			gqrErr    error
		}{
			{
				name:      "Service Call Error",
				cause:     errors.New("call to CWL failed"),
				gqrOutput: nil,
				gqrErr:    errors.New("call to CWL failed"),
			},
			{
				name:  "Nil Status",
				cause: errNilStatus(),
				gqrOutput: &cloudwatchlogs.GetQueryResultsOutput{
					Statistics: &cloudwatchlogs.QueryStatistics{},
				},
			},
			{
				name:  "Nil Result Field",
				cause: errNilResultField(1),
				gqrOutput: &cloudwatchlogs.GetQueryResultsOutput{
					Status: sp(cloudwatchlogs.QueryStatusComplete),
					Results: [][]*cloudwatchlogs.ResultField{
						{{Field: sp("Foo"), Value: sp("10")}, nil, {Field: sp("@ptr"), Value: sp("ptr-val")}},
					},
				},
			},
			{
				name:  "No Key",
				cause: errNoKey(),
				gqrOutput: &cloudwatchlogs.GetQueryResultsOutput{
					Status: sp(cloudwatchlogs.QueryStatusComplete),
					Results: [][]*cloudwatchlogs.ResultField{
						{{Value: sp("orphan value")}},
					},
				},
			},
			{
				name:  "No Value",
				cause: errNoValue("orphan key"),
				gqrOutput: &cloudwatchlogs.GetQueryResultsOutput{
					Status: sp(cloudwatchlogs.QueryStatusComplete),
					Results: [][]*cloudwatchlogs.ResultField{
						{{Field: sp("orphan key")}},
					},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				for _, preview := range []string{"No Preview", "Preview"} {
					t.Run(preview, func(t *testing.T) {
						// ARRANGE.
						queryID := "bar"
						text := "query text that secretly triggers bad service behavior"
						actions := newMockActions(t)
						actions.
							On("StartQueryWithContext", anyContext, anyStartQueryInput).
							Return(&cloudwatchlogs.StartQueryOutput{QueryId: sp(queryID)}, nil).
							Once()
						actions.
							On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{
								QueryId: sp(queryID),
							}).
							Return(testCase.gqrOutput, testCase.gqrErr).
							Once()
						actions.
							On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{
								QueryId: sp(queryID),
							}).
							Return(&cloudwatchlogs.StopQueryOutput{}, nil).
							Maybe()
						m := NewQueryManager(Config{
							Actions: actions,
						})
						require.NotNil(t, m)
						s, err := m.Query(QuerySpec{
							Text:    text,
							Start:   defaultStart,
							End:     defaultEnd,
							Groups:  []string{"baz"},
							Preview: preview == "Preview",
						})
						require.NotNil(t, s)
						require.NoError(t, err)

						// ACT.
						p := make([]Result, 1)
						n, err := s.Read(p)

						// ASSERT.
						assert.Equal(t, 0, n)
						assert.Error(t, err)
						assert.Equal(t, &UnexpectedQueryError{
							QueryID: queryID,
							Text:    text,
							Cause:   testCase.cause,
						}, err)
						actions.AssertExpectations(t)
					})
				}
			})
		}
	})
}

func TestScenariosSerial(t *testing.T) {
	actions := newMockActions(t)
	m := NewQueryManager(Config{
		Actions: actions,
		RPS:     lotsOfRPS,
	})
	require.NotNil(t, m)
	defer func() {
		err := m.Close()
		assert.NoError(t, err)
	}()

	var allStats Stats
	for i, s := range scenarios {
		s.test(t, i, m, actions, false)
		allStats.add(s.stats)
	}

	assert.Equal(t, allStats, m.GetStats())
	actions.AssertExpectations(t)
}

func TestScenariosParallel(t *testing.T) {
	parallels := []int{1, 2, DefaultParallel, QueryConcurrencyQuotaLimit}
	for p := range parallels {
		parallel := parallels[p]
		t.Run(fmt.Sprintf("Parallel=%d", parallel), func(t *testing.T) {
			actions := newMockActions(t)
			m := NewQueryManager(Config{
				Actions:  actions,
				Parallel: parallel,
				RPS:      lotsOfRPS,
			})
			require.NotNil(t, m)
			t.Cleanup(func() {
				err := m.Close()
				if err != nil {
					t.Errorf("Cleanup: failed to close m: %s", err.Error())
				}
			})

			for i := range scenarios {
				scenarios[i].test(t, i, m, actions, true)
			}
		})
	}
}

var scenarios = []queryScenario{
	{
		note: "NoStart.InvalidQueryError",
		QuerySpec: QuerySpec{
			Text:   "a poorly written query",
			Start:  defaultStart,
			End:    defaultEnd,
			Limit:  50,
			Groups: []string{"/my/group/1", "/my/group/2"},
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("a poorly written query"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         int64p(50),
					LogGroupNames: []*string{sp("/my/group/1"), sp("/my/group/2")},
				},
				startQueryErrs: []error{
					cwlErr(cloudwatchlogs.ErrCodeInvalidParameterException, "terrible query writing there bud"),
				},
				startQuerySuccess: false,
			},
		},
		err: &StartQueryError{"a poorly written query", defaultStart, defaultEnd, cwlErr(cloudwatchlogs.ErrCodeInvalidParameterException, "terrible query writing there bud")},
	},
	{
		note: "NoStart.UnexpectedError",
		QuerySpec: QuerySpec{
			Text:   "an ill-fated query",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"/any/group"},
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("an ill-fated query"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/any/group")},
				},
				startQueryErrs:    []error{errors.New("pow exclamation point")},
				startQuerySuccess: false,
			},
		},
		err: &StartQueryError{"an ill-fated query", defaultStart, defaultEnd, errors.New("pow exclamation point")},
	},

	{
		note: "OneChunk.OnePoll.Empty",
		QuerySpec: QuerySpec{
			Text:   "empty",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"/my/empty/group"},
			Hint:   ^uint16(0),
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("empty"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/my/empty/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
					},
				},
			},
		},
		closeAfter: true,
	},
	{
		note: "OneChunk.OnePoll.Status.Cancelled",
		QuerySpec: QuerySpec{
			Text:     "destined for cancellation",
			Start:    defaultStart,
			End:      defaultEnd,
			Groups:   []string{"/any/group"},
			Priority: -5,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("destined for cancellation"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/any/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusCancelled,
					},
				},
			},
		},
		err: &TerminalQueryStatusError{"scenario:3|chunk:0|OneChunk.OnePoll.Status.Cancelled", "Cancelled", "destined for cancellation"},
	},
	{
		note: "OneChunk.OnePoll.Status.Failed",
		QuerySpec: QuerySpec{
			Text:    "fated for failure",
			Start:   defaultStart,
			End:     defaultEnd,
			Groups:  []string{"/any/group"},
			Preview: true,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("fated for failure"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/any/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusFailed,
					},
				},
			},
		},
		err: &TerminalQueryStatusError{"scenario:4|chunk:0|OneChunk.OnePoll.Status.Failed", "Failed", "fated for failure"},
	},
	{
		note: "OneChunk.OnePoll.Status.Timeout",
		QuerySpec: QuerySpec{
			Text:    "tempting a timeout",
			Start:   defaultStart,
			End:     defaultEnd,
			Groups:  []string{"/any/group"},
			Preview: true,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("tempting a timeout"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/any/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: "Timeout",
					},
				},
			},
		},
		err: &TerminalQueryStatusError{"scenario:5|chunk:0|OneChunk.OnePoll.Status.Timeout", "Timeout", "tempting a timeout"},
	},
	{
		note: "OneChunk.OnePoll.Status.Unexpected",
		QuerySpec: QuerySpec{
			Text:    "expecting the unexpected...status",
			Start:   defaultStart,
			End:     defaultEnd,
			Groups:  []string{"/any/group"},
			Preview: true,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("expecting the unexpected...status"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/any/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: "Did you see this coming?",
					},
				},
			},
		},
		expectStop: true,
		err:        &TerminalQueryStatusError{"scenario:6|chunk:0|OneChunk.OnePoll.Status.Unexpected", "Did you see this coming?", "expecting the unexpected...status"},
	},
	{
		note: "OneChunk.OnePoll.Error.Unexpected",
		QuerySpec: QuerySpec{
			Text:   "expecting the unexpected...error",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"/foo/bar"},
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("expecting the unexpected...error"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/foo/bar")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						err: errors.New("very bad news"),
					},
				},
			},
		},
		expectStop: true,
		err:        &UnexpectedQueryError{"scenario:7|chunk:0|OneChunk.OnePoll.Error.Unexpected", "expecting the unexpected...error", errors.New("very bad news")},
	},
	{
		note: "OneChunk.OnePoll.WithResults",
		QuerySpec: QuerySpec{
			Text:    "deliver me some results",
			Start:   defaultStart,
			End:     defaultEnd,
			Groups:  []string{"/any/group"},
			Preview: true,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("deliver me some results"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/any/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{
								{"@ptr", "123"},
								{"@MyField", "hello"},
							},
							{
								{"@ptr", "456"},
								{"@MyField", "goodbye"},
							},
						},
						stats: &Stats{1, 2, 3},
					},
				},
			},
		},
		results: []Result{
			{
				{"@ptr", "123"},
				{"@MyField", "hello"},
			},
			{
				{"@ptr", "456"},
				{"@MyField", "goodbye"},
			},
		},
		stats: Stats{1, 2, 3},
	},
	{
		note: "OneChunk.MultiPoll",
		QuerySpec: QuerySpec{
			Text:   "many happy results",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"/thomas/gray", "/thomas/aquinas"},
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("many happy results"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/thomas/gray"), sp("/thomas/aquinas")},
				},
				startQueryErrs: []error{
					cwlErr(cloudwatchlogs.ErrCodeLimitExceededException, "use less"),
					cwlErr("Throttling", "slow down"),
					cwlErr(cloudwatchlogs.ErrCodeServiceUnavailableException, "wait for it..."),
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusScheduled,
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						stats:  &Stats{3, 0, 1},
					},
					{
						err: cwlErr(cloudwatchlogs.ErrCodeServiceUnavailableException, "a blip in service"),
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{
								{"@ptr", "789"},
								{"MyField", "world"},
							},
						},
						stats: &Stats{99, 98, 97},
					},
					{
						err: cwlErr("throttling has occurred", "and you were the recipient of the throttling"),
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{
								{"@ptr", "101"},
								{"MyField", "hello"},
							},
							{
								{"@ptr", "789"},
								{"MyField", "world"},
							},
						},
						stats: &Stats{100, 99, 98},
					},
				},
			},
		},
		results: []Result{
			{
				{"@ptr", "101"},
				{"MyField", "hello"},
			},
			{
				{"@ptr", "789"},
				{"MyField", "world"},
			},
		},
		stats: Stats{100, 99, 98},
	},

	{
		note: "OneChunk.Preview.SimulateNormalQuery.NoPtr",
		QuerySpec: QuerySpec{
			Text:    "fields Foo, Bar",
			Start:   defaultStart,
			End:     defaultEnd,
			Groups:  []string{"/normal/log/group"},
			Limit:   MaxLimit,
			Preview: true,
			Hint:    5,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("fields Foo, Bar"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         int64p(MaxLimit),
					LogGroupNames: []*string{sp("/normal/log/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{status: cloudwatchlogs.QueryStatusScheduled},
					{status: cloudwatchlogs.QueryStatusRunning},
					{status: cloudwatchlogs.QueryStatusRunning},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"Foo", "Foo.0.0"}, {"Bar", "Bar.0.0"}, {"@ptr", "0"}},
						},
						stats: &Stats{1, 2, 3},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"Foo", "Foo.0.0"}, {"Bar", "Bar.0.0"}, {"@ptr", "0"}},
							{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
						},
						stats: &Stats{2, 4, 6},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
						},
						stats: &Stats{3, 6, 9},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
							{{"Foo", "Foo.2.0"}, {"Bar", "Bar.2.0"}, {"@ptr", "2"}},
						},
						stats: &Stats{4, 8, 12},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
							{{"Foo", "Foo.2.0"}, {"Bar", "Bar.2.0"}, {"@ptr", "2"}},
							{{"Foo", "Foo.3.0"}, {"Bar", "Bar.3.0"}, {"@ptr", "3"}},
						},
						stats: &Stats{5, 10, 15},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"Foo", "Foo.0.0"}, {"Bar", "Bar.0.0"}, {"@ptr", "0"}},
							{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
							{{"Foo", "Foo.3.0"}, {"Bar", "Bar.3.0"}, {"@ptr", "3"}},
							{{"Foo", "Foo.4.0"}, {"Bar", "Bar.4.0"}, {"@ptr", "4"}},
						},
						stats: &Stats{6, 12, 18},
					},
				},
			},
		},
		results: []Result{
			{{"Foo", "Foo.0.0"}, {"Bar", "Bar.0.0"}, {"@ptr", "0"}},
			{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
			{{"@ptr", "0"}, {"@deleted", "true"}},
			{{"Foo", "Foo.2.0"}, {"Bar", "Bar.2.0"}, {"@ptr", "2"}},
			{{"Foo", "Foo.3.0"}, {"Bar", "Bar.3.0"}, {"@ptr", "3"}},
			{{"Foo", "Foo.0.0"}, {"Bar", "Bar.0.0"}, {"@ptr", "0"}},
			{{"Foo", "Foo.4.0"}, {"Bar", "Bar.4.0"}, {"@ptr", "4"}},
			{{"@ptr", "2"}, {"@deleted", "true"}},
		},
		stats: Stats{6, 12, 18},
	},
	{
		note: "OneChunk.Preview.SimulateStatsCommand.NoPtr",
		QuerySpec: QuerySpec{
			Text:    "stats count_distinct(Foo) by bar",
			Start:   defaultStart.Add(-time.Hour),
			End:     defaultEnd.Add(time.Hour),
			Groups:  []string{"/trove/of/data"},
			Preview: true,
			Hint:    1,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("stats count_distinct(Foo) by bar"),
					StartTime:     startTimeSeconds(defaultStart.Add(-time.Hour)),
					EndTime:       endTimeSeconds(defaultEnd.Add(time.Hour)),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/trove/of/data")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{status: cloudwatchlogs.QueryStatusScheduled},
					{status: cloudwatchlogs.QueryStatusRunning},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"count_distinct(Foo)", "100"}, {"bar", "ham"}},
						},
						stats: &Stats{1, 2, 3},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"count_distinct(Foo)", "37"}, {"bar", "eggs"}},
							{{"count_distinct(Foo)", "100"}, {"bar", "ham"}},
						},
						stats: &Stats{2, 4, 6},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"count_distinct(Foo)", "200"}, {"bar", "ham"}},
							{{"count_distinct(Foo)", "41"}, {"bar", "eggs"}},
							{{"count_distinct(Foo)", "10"}, {"bar", "spam"}},
						},
						stats: &Stats{4, 5, 8},
					},
				},
			},
		},
		results: []Result{
			{{"count_distinct(Foo)", "100"}, {"bar", "ham"}},
			{{"count_distinct(Foo)", "37"}, {"bar", "eggs"}},
			{{"count_distinct(Foo)", "100"}, {"bar", "ham"}},
			{{"count_distinct(Foo)", "200"}, {"bar", "ham"}},
			{{"count_distinct(Foo)", "41"}, {"bar", "eggs"}},
			{{"count_distinct(Foo)", "10"}, {"bar", "spam"}},
		},
		stats: Stats{4, 5, 8},
	},

	{
		note: "MultiChunk.LessThanOne",
		QuerySpec: QuerySpec{
			Text:   "stats count_distinct(Eggs) as EggCount By Spam",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"/very/full/log/group"},
			Chunk:  6 * time.Minute,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("stats count_distinct(Eggs) as EggCount By Spam"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/very/full/log/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"EggCount", "1"}, {"Spam", "true"}},
							{{"EggCount", "2"}, {"Span", "false"}},
						},
						stats: &Stats{77, 777, 7},
					},
				},
			},
		},
		results: []Result{
			{{"EggCount", "1"}, {"Spam", "true"}},
			{{"EggCount", "2"}, {"Span", "false"}},
		},
		stats: Stats{77, 777, 7},
	},

	{
		note: "MultiChunk.OneAligned",
		QuerySpec: QuerySpec{
			Text:   "QuerySpec indicates chunking but chunk size is fully aligned with start/end to produce one real chunk",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"primo", "secondo"},
			Chunk:  5 * time.Minute,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking but chunk size is fully aligned with start/end to produce one real chunk"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("primo"), sp("secondo")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusScheduled,
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"ignore", "me"}},
						},
						stats: &Stats{-1, -2, -12},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"@ptr", "1111"}, {"Something", "wicked this way comes"}},
							{{"@ptr", "2222"}, {"Something", "else"}},
						},
						stats: &Stats{13, 8, 3},
					},
				},
			},
		},
		results: []Result{
			{{"@ptr", "1111"}, {"Something", "wicked this way comes"}},
			{{"@ptr", "2222"}, {"Something", "else"}},
		},
		stats: Stats{13, 8, 3},
	},

	{
		note: "MultiChunk.TwoAligned",
		QuerySpec: QuerySpec{
			Text:   "QuerySpec indicates chunking and [start, end) defines exactly two chunks",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"ein", "zwei"},
			Chunk:  150 * time.Second,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking and [start, end) defines exactly two chunks"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(150 * time.Second)),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("ein"), sp("zwei")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"@ptr", "aaaa"}, {"@timestamp", "2021-08-05 15:26:000.123"}},
							{{"@ptr", "bbbb"}, {"@timestamp", "2021-08-05 15:26:000.125"}},
						},
						stats: &Stats{1, 1, 1},
					},
				},
			},
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking and [start, end) defines exactly two chunks"),
					StartTime:     startTimeSeconds(defaultStart.Add(150 * time.Second)),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("ein"), sp("zwei")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"@ptr", "dddd"}, {"@timestamp", "2021-08-05 15:26:000.126"}},
							{{"@ptr", "cccc"}, {"@timestamp", "2021-08-05 15:26:000.124"}},
						},
						stats: &Stats{2, 2, 1},
					},
				},
			},
		},
		results: []Result{
			{{"@ptr", "aaaa"}, {"@timestamp", "2021-08-05 15:26:000.123"}},
			{{"@ptr", "cccc"}, {"@timestamp", "2021-08-05 15:26:000.124"}},
			{{"@ptr", "bbbb"}, {"@timestamp", "2021-08-05 15:26:000.125"}},
			{{"@ptr", "dddd"}, {"@timestamp", "2021-08-05 15:26:000.126"}},
		},
		postprocess: func(r []Result) {
			sort.Slice(r, func(i, j int) bool {
				return r[i].get("@timestamp") < r[j].get("@timestamp")
			})
		},
		stats: Stats{3, 3, 2},
	},

	{
		note: "MultiChunk.TwoMisaligned",
		QuerySpec: QuerySpec{
			Text:   "QuerySpec indicates chunking and [start, end) defines two chunks, the second of which is not full sized",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"forest"},
			Chunk:  4 * time.Minute,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking and [start, end) defines two chunks, the second of which is not full sized"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(4 * time.Minute)),
					LogGroupNames: []*string{sp("forest")},
					Limit:         int64p(DefaultLimit),
				},
				startQueryErrs: []error{
					cwlErr(cloudwatchlogs.ErrCodeLimitExceededException, "use less"),
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusScheduled,
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{Field: "@ptr", Value: "1"}},
							{{Field: "@ptr", Value: "2"}},
						},
						stats: &Stats{49, 23, 1},
					},
				},
			},
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking and [start, end) defines two chunks, the second of which is not full sized"),
					StartTime:     startTimeSeconds(defaultStart.Add(4 * time.Minute)),
					EndTime:       endTimeSeconds(defaultEnd),
					LogGroupNames: []*string{sp("forest")},
					Limit:         int64p(DefaultLimit),
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusRunning,
						stats:  &Stats{3, 2, 3},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{Field: "@ptr", Value: "3"}},
							{{Field: "@ptr", Value: "4"}},
						},
						stats: &Stats{51, 77, 99},
					},
				},
			},
		},
		results: []Result{
			{{Field: "@ptr", Value: "1"}},
			{{Field: "@ptr", Value: "2"}},
			{{Field: "@ptr", Value: "3"}},
			{{Field: "@ptr", Value: "4"}},
		},
		postprocess: func(r []Result) {
			sort.Slice(r, func(i, j int) bool {
				return r[i].get("@ptr") < r[j].get("@ptr")
			})
		},
		stats: Stats{100, 100, 100},
	},

	{
		note: "MultiChunk.ThreeMisaligned",
		QuerySpec: QuerySpec{
			Text:   "QuerySpec indicates chunking and [start, end) defines three chunks, the third of which is fractional sized",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"lumberyard"},
			Chunk:  2 * time.Minute,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking and [start, end) defines three chunks, the third of which is fractional sized"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(2 * time.Minute)),
					LogGroupNames: []*string{sp("lumberyard")},
					Limit:         int64p(DefaultLimit),
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{Field: "@ptr", Value: "1"}},
						},
						stats: &Stats{11, 22, 33},
					},
				},
			},
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking and [start, end) defines three chunks, the third of which is fractional sized"),
					StartTime:     startTimeSeconds(defaultStart.Add(2 * time.Minute)),
					EndTime:       endTimeSeconds(defaultStart.Add(4 * time.Minute)),
					LogGroupNames: []*string{sp("lumberyard")},
					Limit:         int64p(DefaultLimit),
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{Field: "@ptr", Value: "2"}},
							{{Field: "@ptr", Value: "3"}},
						},
						stats: &Stats{44, 55, 66},
					},
				},
			},
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking and [start, end) defines three chunks, the third of which is fractional sized"),
					StartTime:     startTimeSeconds(defaultStart.Add(4 * time.Minute)),
					EndTime:       endTimeSeconds(defaultEnd),
					LogGroupNames: []*string{sp("lumberyard")},
					Limit:         int64p(DefaultLimit),
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{Field: "@ptr", Value: "4"}},
						},
						stats: &Stats{77, 88, 99},
					},
				},
			},
		},
		results: []Result{
			{{Field: "@ptr", Value: "1"}},
			{{Field: "@ptr", Value: "2"}},
			{{Field: "@ptr", Value: "3"}},
			{{Field: "@ptr", Value: "4"}},
		},
		postprocess: func(r []Result) {
			sort.Slice(r, func(i, j int) bool {
				return r[i].get("@ptr") < r[j].get("@ptr")
			})
		},
		stats: Stats{132, 165, 198},
	},

	{
		note: "MultiChunk.Preview",
		QuerySpec: QuerySpec{
			Text:    "QuerySpec indicates a previewed query in three chunks",
			Groups:  []string{"fireplace"},
			Start:   defaultStart,
			End:     defaultEnd,
			Limit:   5,
			Chunk:   2 * time.Minute,
			Preview: true,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates a previewed query in three chunks"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(2 * time.Minute)),
					LogGroupNames: []*string{sp("fireplace")},
					Limit:         int64p(5),
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusScheduled,
					},
					{
						status: cloudwatchlogs.QueryStatusScheduled,
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"@ptr", "1"}, {"instance", "1"}},
						},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"@ptr", "1"}, {"instance", "1"}},
						},
						stats: &Stats{1, 1, 1},
					},
				},
			},
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates a previewed query in three chunks"),
					StartTime:     startTimeSeconds(defaultStart.Add(2 * time.Minute)),
					EndTime:       endTimeSeconds(defaultStart.Add(4 * time.Minute)),
					LogGroupNames: []*string{sp("fireplace")},
					Limit:         int64p(5),
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"@ptr", "2"}, {"instance", "1"}},
						},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"@ptr", "3"}, {"instance", "1"}},
						},
					},
				},
			},
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates a previewed query in three chunks"),
					StartTime:     startTimeSeconds(defaultStart.Add(4 * time.Minute)),
					EndTime:       endTimeSeconds(defaultEnd),
					LogGroupNames: []*string{sp("fireplace")},
					Limit:         int64p(5),
				},
				startQueryErrs: []error{
					cwlErr(cloudwatchlogs.ErrCodeServiceUnavailableException, "wait for it..."),
					cwlErr(cloudwatchlogs.ErrCodeLimitExceededException, "use less"),
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"@ptr", "4"}, {"instance", "1"}},
						},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"@ptr", "5"}, {"instance", "1"}},
							{{"@ptr", "6"}, {"instance", "1"}},
						},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"@ptr", "4"}, {"instance", "2"}},
							{{"@ptr", "6"}, {"instance", "1"}},
							{{"@ptr", "7"}, {"instance", "1"}},
							{{"@ptr", "8"}, {"instance", "1"}},
						},
						stats: &Stats{1, 1, 1},
					},
				},
			},
		},
		results: []Result{
			{{Field: "@ptr", Value: "1"}, {Field: "instance", Value: "1"}},
			{{Field: "@ptr", Value: "2"}, {Field: "instance", Value: "1"}},
			{{Field: "@ptr", Value: "2"}, {Field: "@deleted", Value: "true"}},
			{{Field: "@ptr", Value: "3"}, {Field: "instance", Value: "1"}},
			{{Field: "@ptr", Value: "4"}, {Field: "instance", Value: "1"}},
			{{Field: "@ptr", Value: "4"}, {Field: "@deleted", Value: "true"}},
			{{Field: "@ptr", Value: "4"}, {Field: "instance", Value: "2"}},
			{{Field: "@ptr", Value: "5"}, {Field: "instance", Value: "1"}},
			{{Field: "@ptr", Value: "5"}, {Field: "@deleted", Value: "true"}},
			{{Field: "@ptr", Value: "6"}, {Field: "instance", Value: "1"}},
			{{Field: "@ptr", Value: "7"}, {Field: "instance", Value: "1"}},
			{{Field: "@ptr", Value: "8"}, {Field: "instance", Value: "1"}},
		},
		postprocess: func(r []Result) {
			sort.SliceStable(r, func(i, j int) bool {
				pi, pj := r[i].get("@ptr"), r[j].get("@ptr")
				return pi < pj
			})
		},
		stats: Stats{2, 2, 2},
	},
}

type queryScenario struct {
	QuerySpec
	note        string         // Optional note describing the scenario
	chunks      []chunkPlan    // Sub-scenario for each chunk
	closeEarly  bool           // Whether to prematurely close the stream.
	err         error          // Final expected error.
	results     []Result       // Final results in the expected order after optional sorting using less.
	postprocess func([]Result) // Optional function to post-process (e.g. sort) results in-place.
	stats       Stats          // Final stats
	closeAfter  bool           // Whether to close the stream after the scenario.
	expectStop  bool           // Whether to expect a StopQuery call
}

func (qs *queryScenario) test(t *testing.T, i int, m QueryManager, actions *mockActions, parallel bool) {
	t.Run(fmt.Sprintf("Scenario=%d[%s]", i, qs.note), func(t *testing.T) {
		if parallel {
			t.Parallel()
		}
		qs.play(t, i, m, actions)
	})
}

func (qs *queryScenario) play(t *testing.T, i int, m QueryManager, actions *mockActions) {
	// Set up the chunk polling scenarios.
	for j := range qs.chunks {
		qs.chunks[j].setup(i, j, qs.note, qs.closeEarly, qs.expectStop, actions)
	}

	// Start the scenario query.
	s, err := m.Query(qs.QuerySpec)
	assert.NoError(t, err)
	require.NotNil(t, s)

	// If premature closure is desired, just close the stream and leave.
	if qs.closeEarly {
		err = s.Close()
		assert.NoError(t, err)
		return
	}

	// Read the whole stream.
	r, err := ReadAll(s)

	// Test against the expected results and/or errors.
	if qs.err == nil {
		assert.NoError(t, err)
		if qs.postprocess != nil {
			qs.postprocess(r)
		}
		expectedResults := qs.results
		if expectedResults == nil {
			expectedResults = []Result{}
		}
		assert.Equal(t, expectedResults, r)
	} else {
		assert.Equal(t, qs.err, err)
		assert.Empty(t, r)
	}
	assert.Equal(t, qs.stats, s.GetStats())

	// Close the stream at the end if desired.
	if qs.closeAfter {
		err = s.Close()
		assert.NoError(t, err)
		err = s.Close()
		assert.Same(t, ErrClosed, err)
	}
}

type chunkPlan struct {
	// Starting the chunk.
	startQueryInput   cloudwatchlogs.StartQueryInput
	startQueryErrs    []error // Initial failures before success, may be empty.
	startQuerySuccess bool

	// Polling the chunk.
	pollOutputs []chunkPollOutput
}

type chunkPollOutput struct {
	err     error
	results []Result
	status  string
	stats   *Stats
}

func (cp *chunkPlan) setup(i, j int, note string, closeEarly, cancelChunk bool, actions *mockActions) {
	actions.lock.Lock()
	defer actions.lock.Unlock()

	for k := range cp.startQueryErrs {
		actions.
			On("StartQueryWithContext", anyContext, &cp.startQueryInput).
			Return(nil, cp.startQueryErrs[k]).
			Once()
	}

	if !cp.startQuerySuccess {
		return
	}

	queryID := fmt.Sprintf("scenario:%d|chunk:%d", i, j)
	if note != "" {
		queryID += "|" + note
	}
	actions.
		On("StartQueryWithContext", anyContext, &cp.startQueryInput).
		Return(&cloudwatchlogs.StartQueryOutput{
			QueryId: &queryID,
		}, nil)

	for k := range cp.pollOutputs {
		pollOutput := cp.pollOutputs[k]
		input := &cloudwatchlogs.GetQueryResultsInput{
			QueryId: &queryID,
		}
		var call *mock.Call
		if pollOutput.err != nil {
			call = actions.
				On("GetQueryResultsWithContext", anyContext, input).
				Return(nil, pollOutput.err)
		} else {
			output := &cloudwatchlogs.GetQueryResultsOutput{}
			if pollOutput.status != "" {
				output.Status = &pollOutput.status
			}
			if pollOutput.results != nil {
				output.Results = make([][]*cloudwatchlogs.ResultField, len(pollOutput.results))
				for l := range pollOutput.results {
					output.Results[l] = pollOutput.results[l].backOut()
				}
			}
			if pollOutput.stats != nil {
				output.Statistics = pollOutput.stats.backOut()
			}
			call = actions.
				On("GetQueryResultsWithContext", anyContext, input).
				Return(output, nil)
		}
		if closeEarly {
			call.Maybe()
		} else {
			call.Once()
		}
	}

	if closeEarly || cancelChunk {
		input := &cloudwatchlogs.StopQueryInput{
			QueryId: &queryID,
		}
		call := actions.
			On("StopQueryWithContext", anyContext, input).
			Return(&cloudwatchlogs.StopQueryOutput{}, nil)
		if cancelChunk {
			call.Once()
		} else {
			call.Maybe()
		}
	}
}

func (r Result) backOut() (cwl []*cloudwatchlogs.ResultField) {
	for _, ff := range r {
		cwl = append(cwl, ff.backOut())
	}
	return // Will return nil if r is nil
}

func (f ResultField) backOut() *cloudwatchlogs.ResultField {
	return &cloudwatchlogs.ResultField{
		Field: &f.Field,
		Value: &f.Value,
	}
}

func (s *Stats) backOut() *cloudwatchlogs.QueryStatistics {
	return &cloudwatchlogs.QueryStatistics{
		BytesScanned:   &s.BytesScanned,
		RecordsMatched: &s.RecordsMatched,
		RecordsScanned: &s.RecordsScanned,
	}
}

func int64p(i int64) *int64 {
	return &i
}

func startTimeSeconds(t time.Time) *int64 {
	return int64p(t.Unix())
}

func endTimeSeconds(t time.Time) *int64 {
	return startTimeSeconds(t.Add(-time.Second))
}

func cwlErr(code, message string) error {
	return awserr.New(code, message, nil)
}

func (r Result) get(k string) (v string) {
	for _, f := range r {
		if f.Field == k {
			v = f.Value
			break
		}
	}
	return
}

var (
	defaultStart = time.Date(2020, 8, 25, 3, 30, 0, 0, time.UTC)
	defaultEnd   = defaultStart.Add(5 * time.Minute)
	defaultLimit = int64p(DefaultLimit)
	lotsOfRPS    = map[CloudWatchLogsAction]int{
		StartQuery:      100_000,
		GetQueryResults: 100_000,
		StopQuery:       100_000,
	}
	anyContext = mock.MatchedBy(func(ctx context.Context) bool {
		return ctx != nil
	})
	anyStartQueryInput = mock.AnythingOfType("*cloudwatchlogs.StartQueryInput")
)
