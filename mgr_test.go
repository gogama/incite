// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
			testCases := []struct {
				name          string
				before, after Config
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
				},
				{
					name: "Name",
					before: Config{
						Actions: actions,
						Name:    "foo",
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						Logger:   NopLogger,
						Name:     "foo",
					},
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
					if testCase.before.Name == "" {
						assert.Equal(t, fmt.Sprintf("%p", m2), m2.Name)
						testCase.after.Name = m2.Name
					}
					assert.Equal(t, testCase.after, m2.Config)
					assert.NotNil(t, m2.close)
					assert.NotNil(t, m2.query)
				})
			}
		})

		t.Run("Custom Logger", func(t *testing.T) {
			logger := newMockLogger(t)
			logger.expectPrintf("incite: QueryManager(%s) %s", t.Name(), "started").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s", t.Name(), "stopping...").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s", t.Name(), "stopped").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "starter", "started").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "starter", "stopping...").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "starter", "stopped").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "poller", "started").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "poller", "stopping...").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "poller", "stopped").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "stopper", "started").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "stopper", "stopping...").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "stopper", "stopped").Maybe()
			m := NewQueryManager(Config{
				Actions: actions,
				Logger:  logger,
				Name:    t.Name(),
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

		t.Run("Parallel Exceeds Default Service Quota Limit", func(t *testing.T) {
			logger := newMockLogger(t)
			logger.expectPrintf("incite: QueryManager(%s) warning: "+
				"parallel %d exceeds default service quota concurrency "+
				"limit %d", t.Name(), QueryConcurrencyQuotaLimit+1, QueryConcurrencyQuotaLimit).
				Once()
			logger.On("Printf", mock.AnythingOfType("string"), mock.Anything).Maybe()
			m := NewQueryManager(Config{
				Actions:  actions,
				Parallel: QueryConcurrencyQuotaLimit + 1,
				Logger:   logger,
				Name:     t.Name(),
			})
			require.NotNil(t, m)
			defer func() {
				err := m.Close()
				assert.NoError(t, err)
			}()

			require.IsType(t, &mgr{}, m)
			m2 := m.(*mgr)
			assert.Equal(t, QueryConcurrencyQuotaLimit+1, m2.Parallel)
			logger.AssertExpectations(t)
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

	t.Run("Close Cancels One Chunk", func(t *testing.T) {
		// ARRANGE.
		stopped := true
		actions := newMockActions(t)
		actions.
			On("StartQueryWithContext", anyContext, anyStartQueryInput).
			Return(&cloudwatchlogs.StartQueryOutput{QueryId: sp("qid")}, nil).
			Once()
		actions.
			On("GetQueryResultsWithContext", anyContext, mock.Anything).
			Return(&cloudwatchlogs.GetQueryResultsOutput{
				Status: sp(cloudwatchlogs.QueryStatusRunning),
			}, nil).
			Maybe()
		actions.
			On("StopQueryWithContext", anyContext, mock.Anything).
			Return(&cloudwatchlogs.StopQueryOutput{Success: &stopped}, nil).
			Maybe()
		m := NewQueryManager(Config{
			Actions: actions,
		})
		q := QuerySpec{
			Text:   "qt",
			Groups: []string{"qg"},
			Start:  defaultStart,
			End:    defaultEnd,
		}
		s1, err := m.Query(q)
		require.NotNil(t, s1)
		require.NoError(t, err)
		s2, err := m.Query(q)
		require.NotNil(t, s2)
		require.NoError(t, err)

		// ACT.
		err = m.Close()

		// ASSERT.
		assert.NoError(t, err)
		n1, err := s1.Read(make([]Result, 1))
		assert.Equal(t, 0, n1)
		assert.Same(t, ErrClosed, err)
		n2, err := s2.Read(make([]Result, 1))
		assert.Equal(t, 0, n2)
		assert.Same(t, ErrClosed, err)
		actions.AssertExpectations(t)
	})

	t.Run("Close Cancels Many Chunks", func(t *testing.T) {
		// ARRANGE.
		actions := newMockActions(t)
		text := "I will run forever until the QueryManager gets closed!"
		var wg sync.WaitGroup
		for i := 0; i < QueryConcurrencyQuotaLimit; i++ {
			queryID := fmt.Sprintf("%s[%d]", t.Name(), i)
			wg.Add(1)
			actions.
				On("StartQueryWithContext", anyContext, startQueryInput(
					text,
					defaultStart.Add(time.Duration(i)*time.Minute), defaultStart.Add(time.Duration(i+1)*time.Minute),
					DefaultLimit, "bar",
				)).
				Run(func(_ mock.Arguments) { wg.Done() }).
				Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryID}, nil).
				Once()
			actions.
				On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryID}).
				Return(&cloudwatchlogs.GetQueryResultsOutput{Status: sp(cloudwatchlogs.QueryStatusRunning)}, nil).
				Maybe()
			actions.
				On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{QueryId: &queryID}).
				Return(&cloudwatchlogs.StopQueryOutput{}, nil).
				Maybe()
		}
		m := NewQueryManager(Config{
			Actions:  actions,
			Parallel: QueryConcurrencyQuotaLimit,
			RPS:      lotsOfRPS,
		})
		require.NotNil(t, m)
		s, err := m.Query(QuerySpec{
			Text:   text,
			Groups: []string{"bar"},
			Start:  defaultStart,
			End:    defaultStart.Add(QueryConcurrencyQuotaLimit * time.Minute),
			Chunk:  time.Minute,
		})
		require.NoError(t, err)
		require.NotNil(t, s)
		wg.Wait()

		// ACT.
		err = m.Close()

		// ASSERT.
		assert.NoError(t, err)
		n1, err := s.Read(make([]Result, 1))
		assert.Equal(t, 0, n1)
		assert.Same(t, ErrClosed, err)
		actions.AssertExpectations(t)
	})

	t.Run("Close Resilient to Failure to Cancel Query", func(t *testing.T) {
		actions := newMockActions(t)
		actions.
			On("StartQueryWithContext", anyContext, anyStartQueryInput).
			Return(&cloudwatchlogs.StartQueryOutput{QueryId: sp("qid")}, nil)
		actions.
			On("GetQueryResultsWithContext", anyContext, mock.Anything).
			Return(&cloudwatchlogs.GetQueryResultsOutput{
				Status: sp(cloudwatchlogs.QueryStatusRunning),
			}, nil)
		actions.
			On("StopQueryWithContext", anyContext, mock.Anything).
			Return(nil, errors.New("bad error makes you fail"))
		m := NewQueryManager(Config{
			Actions: actions,
		})
		q := QuerySpec{
			Text:   "qt",
			Groups: []string{"qg"},
			Start:  defaultStart,
			End:    defaultEnd,
		}
		s1, err := m.Query(q)
		require.NotNil(t, s1)
		require.NoError(t, err)
		s2, err := m.Query(q)
		require.NotNil(t, s2)
		require.NoError(t, err)

		// ACT.
		err = m.Close()

		// ASSERT.
		assert.NoError(t, err)
		n1, err := s1.Read(make([]Result, 1))
		assert.Equal(t, 0, n1)
		assert.Same(t, ErrClosed, err)
		n2, err := s2.Read(make([]Result, 1))
		assert.Equal(t, 0, n2)
		assert.Same(t, ErrClosed, err)
	})
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
				name: "Start.SubMillisecond",
				QuerySpec: QuerySpec{
					Text:   "baz",
					Start:  time.Date(2021, 7, 15, 3, 37, 25, 123, time.UTC),
					End:    defaultEnd,
					Groups: []string{"baz"},
				},
				err: startSubMillisecondMsg,
			},
			{
				name: "End.SubMillisecond",
				QuerySpec: QuerySpec{
					Text:   "qux",
					Start:  defaultStart,
					End:    time.Date(2021, 7, 15, 3, 37, 25, 123, time.UTC),
					Groups: []string{"qux", "jilly"},
				},
				err: endSubMillisecondMsg,
			},
			{
				name: "End.NotAfter.Start",
				QuerySpec: QuerySpec{
					Text:   "ham",
					Start:  defaultEnd,
					End:    defaultStart,
					Groups: []string{"ham"},
				},
				err: endNotAfterStartMsg,
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
			{
				name: "Chunk.SubMillisecond",
				QuerySpec: QuerySpec{
					Text:   "ham",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"ham"},
					Chunk:  15 * time.Microsecond,
				},
				err: chunkSubMillisecondMsg,
			},
			{
				name: "SplitUntil.SubMillisecond",
				QuerySpec: QuerySpec{
					Text:       "Whose woods are these?\nI think I know",
					Start:      defaultStart,
					End:        defaultEnd,
					Groups:     []string{"His house is in the village", "Though"},
					Limit:      MaxLimit - 1,
					SplitUntil: time.Minute + 10*time.Microsecond,
				},
				err: splitUntilSubMillisecondMsg,
			},
			{
				name: "SplitUntil.With.Preview",
				QuerySpec: QuerySpec{
					Text:       "He will not see my stopping here",
					Start:      defaultStart,
					End:        defaultEnd,
					Groups:     []string{"To watch his woods", "fill up with snow"},
					Limit:      MaxLimit,
					Preview:    true,
					SplitUntil: time.Second,
				},
				err: splitUntilWithPreviewMsg,
			},
			{
				name: "SplitUntil.Without.MaxLimit",
				QuerySpec: QuerySpec{
					Text:  "My little horse must think it queer",
					Start: defaultStart,
					End:   defaultEnd,
					Groups: []string{
						"To stop without a farmhouse near",
						"Between the woods and frozen lake",
						"The darkest evening of the year",
					},
					Limit:      MaxLimit - 1,
					SplitUntil: time.Second,
				},
				err: splitUntilWithoutMaxLimitMsg,
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
		// error. These are meant to be simple. More complex testing is
		// done in the scenario tests below.

		causeErr := errors.New("super fatal error")

		testCases := []struct {
			name             string
			before           QuerySpec
			after            QuerySpec
			startQueryOutput *cloudwatchlogs.StartQueryOutput
			startQueryErr    error
			expectedN        int64
			expectedGroups   []*string
			expectedCauseErr error
			expectedStats    Stats
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
					Text:       "foo",
					Start:      defaultStart,
					End:        defaultEnd,
					Groups:     []string{"bar", "Baz"},
					Limit:      DefaultLimit,
					Chunk:      5 * time.Minute,
					SplitUntil: 5 * time.Minute,
				},
				startQueryErr:    causeErr,
				expectedN:        1,
				expectedGroups:   []*string{sp("bar"), sp("Baz")},
				expectedCauseErr: causeErr,
				expectedStats: Stats{
					RangeRequested: defaultDuration,
					RangeStarted:   defaultDuration,
					RangeFailed:    defaultDuration,
				},
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
					Text:       "foo",
					Start:      defaultStart,
					End:        defaultEnd,
					Groups:     []string{"bar", "Baz"},
					Limit:      DefaultLimit,
					Chunk:      5 * time.Minute,
					SplitUntil: 5 * time.Minute,
				},
				startQueryErr:    causeErr,
				expectedN:        1,
				expectedGroups:   []*string{sp("bar"), sp("Baz")},
				expectedCauseErr: causeErr,
				expectedStats: Stats{
					RangeRequested: defaultDuration,
					RangeStarted:   defaultDuration,
					RangeFailed:    defaultDuration,
				},
			},
			{
				name: "PartialChunk/2",
				before: QuerySpec{
					Text:       "foo",
					Start:      defaultStart,
					End:        defaultEnd,
					Groups:     []string{"bar", "Baz"},
					Limit:      MaxLimit,
					Chunk:      3 * time.Minute,
					SplitUntil: 3 * time.Minute,
				},
				after: QuerySpec{
					Text:       "foo",
					Start:      defaultStart,
					End:        defaultEnd,
					Groups:     []string{"bar", "Baz"},
					Limit:      MaxLimit,
					Chunk:      3 * time.Minute,
					SplitUntil: 3 * time.Minute,
				},
				startQueryErr:    causeErr,
				expectedN:        2,
				expectedGroups:   []*string{sp("bar"), sp("Baz")},
				expectedCauseErr: causeErr,
				expectedStats: Stats{
					RangeRequested: defaultDuration,
					RangeStarted:   3 * time.Minute,
					RangeFailed:    3 * time.Minute,
				},
			},
			{
				name: "PartialChunk/3",
				before: QuerySpec{
					Text:       "foo",
					Start:      defaultStart.Add(-time.Minute),
					End:        defaultEnd,
					Groups:     []string{"bar", "Baz"},
					Limit:      MaxLimit,
					Chunk:      3 * time.Minute,
					SplitUntil: 3 * time.Minute,
				},
				after: QuerySpec{
					Text:       "foo",
					Start:      defaultStart.Add(-time.Minute),
					End:        defaultEnd,
					Groups:     []string{"bar", "Baz"},
					Limit:      MaxLimit,
					Chunk:      3 * time.Minute,
					SplitUntil: 3 * time.Minute,
				},
				startQueryErr:    causeErr,
				expectedN:        3,
				expectedGroups:   []*string{sp("bar"), sp("Baz")},
				expectedCauseErr: causeErr,
				expectedStats: Stats{
					RangeRequested: defaultDuration + time.Minute,
					RangeStarted:   time.Minute,
					RangeFailed:    time.Minute,
				},
			},
			{
				name: "MissingQueryID",
				before: QuerySpec{
					Text:       "ham",
					Start:      defaultStart,
					End:        defaultEnd,
					Groups:     []string{"eggs", "Spam"},
					SplitUntil: -1,
				},
				after: QuerySpec{
					Text:       "ham",
					Start:      defaultStart,
					End:        defaultEnd,
					Groups:     []string{"eggs", "Spam"},
					Limit:      DefaultLimit,
					Chunk:      5 * time.Minute,
					SplitUntil: 5 * time.Minute,
				},
				startQueryOutput: &cloudwatchlogs.StartQueryOutput{},
				expectedN:        1,
				expectedGroups:   []*string{sp("eggs"), sp("Spam")},
				expectedCauseErr: errors.New(outputMissingQueryIDMsg),
				expectedStats: Stats{
					RangeRequested: defaultDuration,
					RangeStarted:   defaultDuration,
					RangeFailed:    defaultDuration,
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				var wg sync.WaitGroup
				wg.Add(1)
				actions := newMockActions(t)
				actions.
					On("StartQueryWithContext", anyContext, anyStartQueryInput).
					Return(testCase.startQueryOutput, testCase.startQueryErr).
					Run(func(_ mock.Arguments) { wg.Done() }).
					Once()
				m := NewQueryManager(Config{
					Actions:  actions,
					Parallel: 1,
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
				assert.Equal(t, testCase.expectedGroups, s2.groups)
				r := make([]Result, 1)
				n, err := s.Read(r)
				wg.Wait()
				assert.Equal(t, int64(1), s2.next)
				assert.Equal(t, 0, n)
				var sqe *StartQueryError
				assert.ErrorAs(t, err, &sqe)
				assert.Equal(t, sqe.Cause, testCase.expectedCauseErr)
				assert.Equal(t, testCase.expectedStats, s.GetStats())
				s.GetStats().checkInvariants(t, true, false)

				err = s.Close()
				assert.NoError(t, err)
				err = s.Close()
				assert.Same(t, ErrClosed, err)
				assert.Equal(t, testCase.expectedStats, s.GetStats())
				s.GetStats().checkInvariants(t, true, false)

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
		t.Cleanup(func() {
			close(event)
			_ = m.Close()
		})
		s, err := m.Query(QuerySpec{
			Text:   "I see the future and this query never happens.",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"/never/queried/group"},
		})
		require.NotNil(t, s)
		require.NoError(t, err)

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
			Run(func(_ mock.Arguments) {
				wg3.Done()
			}).
			Return(&cloudwatchlogs.StopQueryOutput{}, nil).
			Once()
		actions.
			On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{QueryId: sp("b")}).
			Run(func(_ mock.Arguments) {
				wg3.Done()
			}).
			Return(&cloudwatchlogs.StopQueryOutput{}, nil).
			Once()
		m := NewQueryManager(Config{
			Actions: actions,
		})
		require.NotNil(t, m)
		t.Cleanup(func() {
			close(event)
			_ = m.Close()
		})
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
						t.Cleanup(func() {
							_ = m.Close()
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

	t.Run("Query Starts But Failed Chunk Needs Restarting", func(t *testing.T) {
		// The purpose of this test case is to verify that when a query
		// which is not in preview mode suffers a failed chunk, the
		// chunk is restarted up to maxRestart times in order to get to
		// a success outcome.
		//
		// The test runs 10 times and each of the 10 iterations creates
		// `n` chunks, where `n` is the iteration number.

		text := "query text for which some chunks will fail"
		stats := cloudwatchlogs.QueryStatistics{
			BytesScanned:   float64p(1.0),
			RecordsMatched: float64p(1.0),
			RecordsScanned: float64p(1.0),
		}

		for n := 1; n <= 10; n++ {
			t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
				// ARRANGE.

				// Setup mock actions for each chunk.
				actions := newMockActions(t)
				for c := 1; c <= n; c++ {
					// Each chunk will fail maxRestart times before succeeding.
					for i := 1; i <= maxRestart; i++ {
						queryID := fmt.Sprintf("n=%d|c=%d|i=%d", n, c, i)
						actions.
							On("StartQueryWithContext", anyContext, startQueryInput(
								text,
								defaultStart.Add(time.Duration(c-1)*time.Minute), defaultStart.Add(time.Duration(c)*time.Minute),
								DefaultLimit, "foo",
							)).
							Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryID}, nil).
							Once()
						getCall := actions.
							On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryID})
						if i < maxRestart {
							getCall.Return(&cloudwatchlogs.GetQueryResultsOutput{
								Statistics: &stats,
								Status:     sp(cloudwatchlogs.QueryStatusFailed),
							}, nil)
						} else {
							getCall.Return(&cloudwatchlogs.GetQueryResultsOutput{
								Results: [][]*cloudwatchlogs.ResultField{
									{
										&cloudwatchlogs.ResultField{Field: sp("n"), Value: sp(strconv.Itoa(n))},
										&cloudwatchlogs.ResultField{Field: sp("c"), Value: sp(strconv.Itoa(c))},
									},
								},
								Statistics: &stats,
								Status:     sp(cloudwatchlogs.QueryStatusComplete),
							}, nil)
						}
						getCall.Once()
					}
				}
				// Create the query manager.
				m := NewQueryManager(Config{
					Actions:  actions,
					Parallel: QueryConcurrencyQuotaLimit,
					RPS:      lotsOfRPS,
				})
				require.NotNil(t, m)
				t.Cleanup(func() {
					_ = m.Close()
				})

				// ACT.
				// Run a query with 'n' chunks and collect the results.
				s, err := m.Query(QuerySpec{
					Text:   text,
					Groups: []string{"foo"},
					Start:  defaultStart,
					End:    defaultStart.Add(time.Duration(n) * time.Minute),
					Chunk:  time.Minute,
				})
				require.NoError(t, err)
				require.NotNil(t, s)
				r, err := ReadAll(s)

				// ASSERT.
				assert.NoError(t, err)
				assert.Len(t, r, n)
				sort.Slice(r, func(i, j int) bool {
					return r[i][0].Field < r[j][0].Field
				})
				expectedResults := make([]Result, n)
				for c := 1; c <= n; c++ {
					expectedResults[c-1] = Result{{"n", strconv.Itoa(n)}, {"c", strconv.Itoa(c)}}
				}
				assert.Equal(t, expectedResults, r)
			})
		}
	})

	t.Run("Priority is Respected", func(t *testing.T) {
		// The purpose of this test case is to ensure that query chunks
		// are started and polled in priority order.
		//
		// This test case starts M queries each of which consists of two
		// chunks. For query number i of M, the Priority is i; the query
		// text is stringization of i; and the query IDs are a string
		// consisting of the stringization of i, zero padded up to
		// length 4, plus a dot, plus the chunk number (1 or 2). So
		// query number 10, chunk 1 of 2, has Priority 10, text "10",
		// and query ID "0010.1".
		//
		// Each chunk is polled twice. The first time, the poll comes
		// back with status Scheduled; the second time, the poll comes
		// back with status Complete.
		//
		// The starts slice receives the query ID of each started chunk
		// as it occurs.

		// ARRANGE.
		M := 25 // Number of queries
		N := 4  // Number of chunks per query
		chunk := defaultDuration / time.Duration(N)
		starts := make([]string, 0, M*N)
		gets := make([]string, 0, M*N)
		actions := newMockActions(t)
		for i := 1; i <= M; i++ {
			for j := 1; j <= N; j++ {
				queryString := strconv.Itoa(i)
				queryID := fmt.Sprintf("%04d.%d", i, j)
				startTime := defaultStart.Add(time.Duration(j-1) * chunk)
				actions.
					On("StartQueryWithContext", anyContext, mock.MatchedBy(func(input *cloudwatchlogs.StartQueryInput) bool {
						return *input.QueryString == queryString && *input.StartTime == *startTimeMilliseconds(startTime)
					})).
					Run(func(_ mock.Arguments) {
						starts = append(starts, queryID)
					}).
					Return(&cloudwatchlogs.StartQueryOutput{
						QueryId: sp(queryID),
					}, nil).
					Once()
				actions.
					On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{
						QueryId: &queryID,
					}).
					Run(func(_ mock.Arguments) {
						gets = append(gets, queryID)
					}).
					Return(&cloudwatchlogs.GetQueryResultsOutput{
						Status: sp(cloudwatchlogs.QueryStatusComplete),
					}, nil).
					Once()
			}
		}
		m := NewQueryManager(Config{
			Actions:  actions,
			Parallel: QueryConcurrencyQuotaLimit,
			RPS:      lotsOfRPS,
		})
		t.Cleanup(func() {
			err := m.Close()
			assert.NoError(t, err)
		})

		// ACT.
		var wg sync.WaitGroup
		wg.Add(M)
		for i := 1; i <= M; i++ {
			s, err := m.Query(QuerySpec{
				Text:     strconv.Itoa(i),
				Groups:   []string{"group"},
				Start:    defaultStart,
				End:      defaultEnd,
				Chunk:    chunk,
				Priority: i,
			})
			require.NotNil(t, s, "i=%d", i)
			require.NoError(t, err, "i=%d", i)
			go func(i int) {
				_, err := ReadAll(s)
				wg.Done()
				assert.NoError(t, err, "i=%d", i)
			}(i)
		}
		wg.Wait()

		// ASSERT.
		actions.AssertExpectations(t)
		require.Len(t, starts, M*N)
		require.Len(t, gets, M*N)
		starts2 := make([]string, len(starts))
		copy(starts2, starts)
		gets2 := make([]string, len(gets))
		copy(gets2, gets)
		sort.Strings(starts2)
		sort.Strings(gets2)
		assert.Equal(t, starts2, starts)
		assert.Equal(t, gets2, gets)
	})

	t.Run("Logger Receives Expected Messages", func(t *testing.T) {
		t.Run("Successful Query", func(t *testing.T) {
			// This test case creates a two-chunk query which runs into
			// a few problems:
			// - The first chunk encounters an error starting.
			// - The second chunk fails the first time.

			// ARRANGE.
			logger := newMockLogger(t)
			logger.expectPrintf("incite: QueryManager(%s) %s", t.Name(), "started").Once()
			logger.expectPrintf("incite: QueryManager(%s) %s", t.Name(), "stopping...").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s", t.Name(), "stopped").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "starter", "started").Once()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "starter", "stopping...").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "starter", "stopped").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "poller", "started").Once()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "poller", "stopping...").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "poller", "stopped").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "stopper", "started").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "stopper", "stopping...").Maybe()
			logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "stopper", "stopped").Maybe()
			actions := newMockActions(t)
			text := "a query in two chunks which generates logs"
			// CHUNK 1.
			queryIDChunk1 := "foo"
			actions.
				On("StartQueryWithContext", anyContext, startQueryInput(text, defaultStart, defaultStart.Add(time.Second), DefaultLimit, "grp")).
				Return(nil, cwlErr(cloudwatchlogs.ErrCodeServiceUnavailableException, "foo")).
				Once()
			logger.
				expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", t.Name(), "starter temporary error", "0", text, defaultStart, defaultStart.Add(time.Second), "ServiceUnavailableException: foo").
				Once()
			actions.
				On("StartQueryWithContext", anyContext, startQueryInput(text, defaultStart, defaultStart.Add(time.Second), DefaultLimit, "grp")).
				Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryIDChunk1}, nil).
				Once()
			logger.
				expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "started", "0(foo)").
				Once()
			actions.
				On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryIDChunk1}).
				Return(&cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{},
					Status:  sp(cloudwatchlogs.QueryStatusComplete),
				}, nil).
				Once()
			logger.
				expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "completed", "0(foo)").
				Once()
			// CHUNK 2.
			queryIDChunk2 := []string{"bar.try1", "bar.try2"}
			actions.
				On("StartQueryWithContext", anyContext, startQueryInput(text, defaultStart.Add(time.Second), defaultStart.Add(2*time.Second), DefaultLimit, "grp")).
				Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryIDChunk2[0]}, nil).
				Once()
			logger.
				expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "started", "1(bar.try1)").
				Once()
			actions.
				On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryIDChunk2[0]}).
				Return(&cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{},
					Status:  sp(cloudwatchlogs.QueryStatusFailed),
				}, nil).
				Once()
			actions.
				On("StartQueryWithContext", anyContext, startQueryInput(text, defaultStart.Add(time.Second), defaultStart.Add(2*time.Second), DefaultLimit, "grp")).
				Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryIDChunk2[1]}, nil).
				Once()
			logger.
				expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "started", "1R(bar.try2)").
				Once()
			actions.
				On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryIDChunk2[1]}).
				Return(&cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{},
					Status:  sp(cloudwatchlogs.QueryStatusComplete),
				}, nil).
				Once()
			logger.
				expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "completed", "1R(bar.try2)", text, mock.Anything, mock.Anything).
				Once()
			// START QUERY.
			m := NewQueryManager(Config{
				Actions: actions,
				RPS:     lotsOfRPS,
				Logger:  logger,
				Name:    t.Name(),
			})
			require.NotNil(t, m)
			t.Cleanup(func() {
				_ = m.Close()
			})
			s, err := m.Query(QuerySpec{
				Text:   text,
				Groups: []string{"grp"},
				Start:  defaultStart,
				End:    defaultStart.Add(2 * time.Second),
				Chunk:  time.Second,
			})
			require.NoError(t, err)
			require.NotNil(t, s)

			// ACT.
			r, err1 := ReadAll(s)
			err2 := m.Close()

			// ASSERT.
			assert.NoError(t, err1)
			assert.Empty(t, r)
			assert.NoError(t, err2)
			actions.AssertExpectations(t)
			logger.AssertExpectations(t)
		})
	})

	t.Run("Chunks are Split as Expected", func(t *testing.T) {
		// Override maxLimit to 2 for testing purposes.
		maxLimit = 2
		t.Cleanup(func() {
			// Restore maxLimit after this test finishes.
			maxLimit = MaxLimit
		})

		// Define the test cases.
		type expectedChunk struct {
			size       time.Duration
			start, end int // Result inclusive start and exclusive end index
			chunks     []expectedChunk
		}
		testCases := []struct {
			name       string
			splitUntil time.Duration
			chunks     []expectedChunk
		}{
			{
				name:       "Already Minimum Chunk Size",
				splitUntil: time.Millisecond,
				chunks:     []expectedChunk{{time.Millisecond, 0, 2, nil}},
			},
			{
				name:       "One Split in Half",
				splitUntil: time.Millisecond,
				chunks: []expectedChunk{
					{
						size:  2 * time.Millisecond,
						start: 0,
						end:   2,
						chunks: []expectedChunk{
							{time.Millisecond, 0, 2, nil},
							{time.Millisecond, 2, 3, nil},
						},
					},
				},
			},
			{
				name:       "One Split in Thirds",
				splitUntil: time.Millisecond,
				chunks: []expectedChunk{
					{
						size:  3 * time.Millisecond,
						start: 0,
						end:   2,
						chunks: []expectedChunk{
							{time.Millisecond, 0, 1, nil},
							{time.Millisecond, 1, 3, nil},
							{time.Millisecond, 3, 5, nil},
						},
					},
				},
			},
			{
				name:       "One Split in Quarters",
				splitUntil: time.Millisecond,
				chunks: []expectedChunk{
					{
						size:  4 * time.Millisecond,
						start: 0,
						end:   2,
						chunks: []expectedChunk{
							{time.Millisecond, 0, 1, nil},
							{time.Millisecond, 1, 3, nil},
							{time.Millisecond, 3, 5, nil},
							{time.Millisecond, 5, 6, nil},
						},
					},
				},
			},
			{
				name:       "Odd Splits",
				splitUntil: time.Millisecond,
				chunks: []expectedChunk{
					{
						size:  5 * time.Millisecond,
						start: 0,
						end:   2,
						chunks: []expectedChunk{
							{2 * time.Millisecond, 0, 1, nil},
							{
								size:  2 * time.Millisecond,
								start: 1, end: 3,
								chunks: []expectedChunk{
									{time.Millisecond, 1, 2, nil},
									{time.Millisecond, 2, 3, nil},
								},
							},
							{time.Millisecond, 3, 5, nil},
						},
					},
				},
			},
			{
				name:       "Split Range Cannot Go Below SplitUntil",
				splitUntil: 50 * time.Second,
				chunks: []expectedChunk{
					{
						size:  100 * time.Second,
						start: 0,
						end:   2,
						chunks: []expectedChunk{
							{50 * time.Second, 0, 1, nil},
							{50 * time.Second, 1, 2, nil},
						},
					},
				},
			},
		}

		// Run the subtests.
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				// ARRANGE.
				logger := newMockLogger(t)
				actions := newMockActions(t)
				var expectedResults []Result
				var f func(string, time.Duration, expectedChunk)
				f = func(chunkID string, offset time.Duration, chunk expectedChunk) {
					actions.
						On("StartQueryWithContext", anyContext, startQueryInput(
							"foo",
							defaultStart.Add(offset),
							defaultStart.Add(offset).Add(chunk.size),
							maxLimit,
							"bar",
						)).
						Return(&cloudwatchlogs.StartQueryOutput{
							QueryId: sp(chunkID),
						}, nil).
						Once()
					chunkResults := resultSeries(chunk.start, chunk.end-chunk.start)
					actions.
						On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{
							QueryId: sp(chunkID),
						}).
						Return(&cloudwatchlogs.GetQueryResultsOutput{
							Results: backOut(chunkResults),
							Status:  sp(cloudwatchlogs.QueryStatusComplete),
						}, nil).Once()
					logger.
						expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "started", chunkID+"("+chunkID+")", "foo").
						Once()
					logger.
						expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "completed", chunkID+"("+chunkID+")", "foo").
						Maybe()
					if len(chunk.chunks) == 0 {
						expectedResults = append(expectedResults, chunkResults...)
						return
					}
					logger.
						expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", t.Name(), "split", chunkID+"("+chunkID+")", "foo").
						Once()
					for j := range chunk.chunks {
						f(chunkID+"s"+strconv.Itoa(j), offset, chunk.chunks[j])
						offset += chunk.chunks[j].size
					}
				}
				var offset time.Duration
				for i := range testCase.chunks {
					f(strconv.Itoa(i), offset, testCase.chunks[i])
					offset += testCase.chunks[i].size
				}
				logger.expectPrintf("incite: QueryManager(%s) %s", t.Name(), "started").Once()
				logger.expectPrintf("incite: QueryManager(%s) %s", t.Name(), "stopping...").Maybe()
				logger.expectPrintf("incite: QueryManager(%s) %s", t.Name(), "stopped").Maybe()
				logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "starter", "started").Once()
				logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "starter", "stopping...").Maybe()
				logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "starter", "stopped").Maybe()
				logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "poller", "started").Once()
				logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "poller", "stopping...").Maybe()
				logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "poller", "stopped").Maybe()
				logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "stopper", "started").Maybe()
				logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "stopper", "stopping...").Maybe()
				logger.expectPrintf("incite: QueryManager(%s) %s %s", t.Name(), "stopper", "stopped").Maybe()
				m := NewQueryManager(Config{
					Actions: actions,
					RPS:     lotsOfRPS,
					Logger:  logger,
					Name:    t.Name(),
				})
				t.Cleanup(func() {
					_ = m.Close()
				})
				s, err := m.Query(QuerySpec{
					Text:       "foo",
					Groups:     []string{"bar"},
					Start:      defaultStart,
					End:        defaultStart.Add(offset),
					Limit:      maxLimit,
					Chunk:      testCase.chunks[0].size,
					SplitUntil: testCase.splitUntil,
				})
				require.NoError(t, err)
				require.NotNil(t, s)

				// ACT.
				var actualResults []Result
				actualResults, err = ReadAll(s)

				// ASSERT.
				assert.NoError(t, err)
				sort.Slice(actualResults, func(i, j int) bool {
					return actualResults[i].get("@ptr") < actualResults[j].get("@ptr")
				})
				assert.Equal(t, expectedResults, actualResults)
				actions.AssertExpectations(t)
				logger.AssertExpectations(t)
			})
		}
	})

	t.Run("Issue #25 - Chunk Splitting Does Not Cause Empty Time Range", func(t *testing.T) {
		// Regression test for: https://github.com/gogama/incite/issues/25
		//
		// This test creates the following scenario:
		//    1. Parallelism of 2 so mgr can run maximum two chunks at a time.
		//    1. Chunk splitting on for the query.
		//    2. Three original generation 0 chunks.
		//    3. First chunk gets split into only two sub-chunks, both of which
		//       are started before the third generation 0 chunks can be
		//       started.
		//    4. Third generation 0 chunk starts and ends before one of the
		//       split sub-chunks ends.
		//
		// The above scenario tests that the mgr correctly detects that there
		// are no more generation 0 chunks to create and does not end up
		// creating an empty chunk whose start and end are the same.
		firstGen1ChunkStarting := make(chan time.Time)
		thirdGen0ChunkStarting := make(chan time.Time)
		actions := newMockActions(t)
		m := NewQueryManager(Config{
			Actions:  actions,
			Parallel: 2,
			RPS:      lotsOfRPS,
		})
		t.Cleanup(func() {
			maxLimit = MaxLimit
			_ = m.Close()
			close(firstGen1ChunkStarting)
			close(thirdGen0ChunkStarting)
		})
		maxLimit = 2
		text := "It's the ABC train, papa!"
		groups := []string{"regression/test/for", "https://github.com/gogama/incite/issues/25"}
		start := time.Date(2023, 1, 28, 16, 48, 13, 0, time.UTC)
		numChunks := 3
		chunkSize := 2 * time.Millisecond
		end := start.Add(time.Duration(numChunks) * chunkSize)

		// First chunk (generation 0).
		actions.
			On("StartQueryWithContext", anyContext, startQueryInput(text, start, start.Add(1*chunkSize), maxLimit, groups...)).
			Return(startQueryOutput("0"), nil).
			Once()
		actions.
			On("GetQueryResultsWithContext", anyContext, getQueryResultsInput("0")).
			Return(getQueryResultsOutput([]Result{{{"@ptr", "0/0"}}, {{"@ptr", "0/1"}}}, cloudwatchlogs.QueryStatusComplete, nil), nil).
			Once()
		// Second chunk (generation 0).
		actions.
			On("StartQueryWithContext", anyContext, startQueryInput(text, start.Add(1*chunkSize), start.Add(2*chunkSize), maxLimit, groups...)).
			Return(startQueryOutput("1"), nil).
			Once()
		actions.
			On("GetQueryResultsWithContext", anyContext, getQueryResultsInput("1")).
			WaitUntil(firstGen1ChunkStarting).
			Return(getQueryResultsOutput([]Result{{{"@ptr", "1/0"}}}, cloudwatchlogs.QueryStatusComplete, nil), nil).
			Once()
		// First chunk half 1 (generation 1).
		actions.
			On("StartQueryWithContext", anyContext, startQueryInput(text, start, start.Add(chunkSize/2), maxLimit, groups...)).
			Run(func(_ mock.Arguments) {
				firstGen1ChunkStarting <- time.Now()
			}).
			Return(startQueryOutput("0/0"), nil).
			Once()
		actions.
			On("GetQueryResultsWithContext", anyContext, getQueryResultsInput("0/0")).
			Return(getQueryResultsOutput([]Result{{{"@ptr", "0/0"}}}, cloudwatchlogs.QueryStatusComplete, nil), nil).
			Once()
		// First chunk half 2 (generation 1).
		actions.
			On("StartQueryWithContext", anyContext, startQueryInput(text, start.Add(chunkSize/2), start.Add(chunkSize), maxLimit, groups...)).
			Return(startQueryOutput("0/1"), nil).
			Once()
		actions.
			On("GetQueryResultsWithContext", anyContext, getQueryResultsInput("0/1")).
			WaitUntil(thirdGen0ChunkStarting).
			Return(getQueryResultsOutput([]Result{{{"@ptr", "1/1"}}}, cloudwatchlogs.QueryStatusComplete, nil), nil).
			Once()
		// Third chunk (generation 1).
		actions.
			On("StartQueryWithContext", anyContext, startQueryInput(text, start.Add(2*chunkSize), end, maxLimit, groups...)).
			Run(func(_ mock.Arguments) {
				thirdGen0ChunkStarting <- time.Now()
			}).
			Return(startQueryOutput("2"), nil).
			Once()
		actions.
			On("GetQueryResultsWithContext", anyContext, getQueryResultsInput("2")).
			Return(getQueryResultsOutput([]Result{{{"@ptr", "2/0"}}}, cloudwatchlogs.QueryStatusComplete, nil), nil).
			Once()
		s, err := m.Query(QuerySpec{
			Text:       text,
			Groups:     groups,
			Start:      start,
			End:        end,
			Limit:      maxLimit,
			Chunk:      chunkSize,
			SplitUntil: time.Millisecond,
		})
		require.NoError(t, err)

		var actualResults []Result
		actualResults, err = ReadAll(s)

		require.NoError(t, err)
		actions.AssertExpectations(t)
		sort.Slice(actualResults, func(i, j int) bool {
			return actualResults[i].get("@ptr") < actualResults[j].get("@ptr")
		})
		expectedResults := []Result{{{"@ptr", "0/0"}}, {{"@ptr", "1/0"}}, {{"@ptr", "1/1"}}, {{"@ptr", "2/0"}}}
		assert.Equal(t, expectedResults, actualResults)
	})

	t.Run("Maxed Chunks are Correctly Recorded", func(t *testing.T) {
		testCases := []struct {
			name    string
			preview bool
		}{
			{"NoPreview", false},
			{"Preview", true},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				// ARRANGE.
				actions := newMockActions(t)
				actions.
					On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
						QueryString:   sp("q"),
						StartTime:     startTimeMilliseconds(defaultStart),
						EndTime:       endTimeMilliseconds(defaultEnd),
						LogGroupNames: []*string{sp("a")},
						Limit:         int64p(1),
					}).
					Return(&cloudwatchlogs.StartQueryOutput{QueryId: sp("queryID")}, nil).
					Once()
				actions.
					On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: sp("queryID")}).
					Return(&cloudwatchlogs.GetQueryResultsOutput{
						Status: sp(cloudwatchlogs.QueryStatusComplete),
						Results: [][]*cloudwatchlogs.ResultField{
							{{Field: sp("@ptr"), Value: sp("1")}},
						},
					}, nil).
					Once()
				m := NewQueryManager(Config{
					Actions: actions,
				})
				t.Cleanup(func() {
					_ = m.Close()
				})
				s, err := m.Query(QuerySpec{
					Text:    "q",
					Groups:  []string{"a"},
					Start:   defaultStart,
					End:     defaultEnd,
					Limit:   1,
					Preview: testCase.preview,
				})
				require.NoError(t, err)
				require.NotNil(t, s)

				// ACT.
				r, err := ReadAll(s)
				ss := s.GetStats()
				ms := m.GetStats()

				// ASSERT.
				assert.NoError(t, err)
				assert.Equal(t, []Result{{{"@ptr", "1"}}}, r)
				assert.Equal(t, Stats{
					RangeRequested: defaultDuration,
					RangeStarted:   defaultDuration,
					RangeDone:      defaultDuration,
					RangeMaxed:     defaultDuration,
				}, ss)
				assert.Equal(t, ss, ms)
			})
		}
	})

	t.Run("Query Fails with Error if Chunk Exceeds Max Temporary Errors", func(t *testing.T) {
		text := "a query destined to exceed all maxima on temporary errors"
		groups := []string{"grpA", "grpB"}
		expectedErr := cwlErr(cloudwatchlogs.ErrCodeServiceUnavailableException, "we lacking service")

		testCases := []struct {
			name  string
			setup func(actions *mockActions)
		}{
			{
				name: "InStarter",
				setup: func(actions *mockActions) {
					actions.
						On("StartQueryWithContext", anyContext, startQueryInput(text, defaultStart, defaultEnd, DefaultLimit, groups...)).
						Return(nil, expectedErr).
						Times(maxTempStartingErrs)
				},
			},
			{
				name: "InPoller",
				setup: func(actions *mockActions) {
					queryID := "u-r-doomed"
					stopSuccess := true
					actions.
						On("StartQueryWithContext", anyContext, startQueryInput(text, defaultStart, defaultEnd, DefaultLimit, groups...)).
						Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryID}, nil).
						Once()
					actions.
						On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryID}).
						Return(nil, expectedErr).
						Times(maxTempPollingErrs)
					actions.
						On("StopQueryWithContext", anyContext, anyStopQueryInput).
						Return(&cloudwatchlogs.StopQueryOutput{Success: &stopSuccess}, nil).
						Maybe()
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				actions := newMockActions(t)
				testCase.setup(actions)
				m := NewQueryManager(Config{
					Actions: actions,
					RPS:     lotsOfRPS,
				})
				t.Cleanup(func() {
					_ = m.Close()
				})
				s, err := m.Query(QuerySpec{
					Text:   text,
					Groups: groups,
					Start:  defaultStart,
					End:    defaultEnd,
				})
				require.NoError(t, err)
				require.NotNil(t, s)

				_, err = ReadAll(s)

				assert.ErrorIs(t, err, expectedErr)
			})
		}
	})

	t.Run("RPS Adapts in Response to Throttling", func(t *testing.T) {
		// IDEA:
		// - Run a single one-chunk query.
		// - Have it never succeed, and cancel it by closing the stream so the
		//   stopper gets called.
		// - Call start twice (throttled once, succeed once), and verify that
		//   starter RPS is -0.75 from start.
		// - Call poll twice++ (throttled once, indeterminate once), and after
		//   the first two polls
		//     + Have it keep polling and getting indeterminate results.
		//     + Cancel the query.
		// - Call stop twice (throttled once, succeed once), and verify that
		//   stopper RPS is -0.75 from start.
		text := "what do my logs say?"
		groups := []string{"/log/group"}
		queryID := "qid"
		errThrottled := awserr.New("throttled", "throttled", nil)

		actions := newMockActions(t)
		m := NewQueryManager(Config{
			Actions: actions,
			RPS:     lotsOfRPS,
			Name:    t.Name(),
		})
		t.Cleanup(func() {
			_ = m.Close()
		})

		// Replace the starer and stopper RPS adapters with mocks.
		startAdapter := newMockAdapter(t)
		stopAdapter := newMockAdapter(t)
		m2 := m.(*mgr)
		m2.starter.rps = startAdapter
		m2.stopper.rps = stopAdapter

		// Allow StartQuery to be called twice, with the first call
		// being throttled and the second one succeeding.
		actions.
			On("StartQueryWithContext", anyContext, startQueryInput(text, defaultStart, defaultEnd, DefaultLimit, groups...)).
			Return(nil, errThrottled).
			Once()
		actions.
			On("StartQueryWithContext", anyContext, startQueryInput(text, defaultStart, defaultEnd, DefaultLimit, groups...)).
			Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryID}, nil).
			Once()
		startAdapter.
			On("decrease").
			Return(true).
			Once()
		startAdapter.
			On("value").
			Return(99_999.0).
			Times(2) // Calculate minDelay after RPS decrease, print decrease RPS log.
		startAdapter.
			On("increase").
			Return(true).
			Once()
		startAdapter.
			On("value").
			Return(99_999.5).
			Times(2) // Calculate minDelay after RPS increase, print increase RPS log.
		// Allow GetQueryResults to be called AT LEAST twice, with all
		// calls being throttled. The second call to GetQueryResults
		// unblocks the test, allowing it to close the stream and
		// trigger the StopQuery calls.
		var donePolling sync.WaitGroup
		donePolling.Add(2)
		actions.
			On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryID}).
			Run(func(_ mock.Arguments) { donePolling.Done() }).
			Return(nil, errThrottled).
			Times(2)
		actions.
			On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryID}).
			Return(nil, errThrottled).
			Maybe()
		// Allow StopQuery to be called twice, with the first call
		// being throttled and the second one succeeding. The second
		// call to StopQuery unblocks the test, allowing it to proceed
		// to assertions.
		var doneStopping sync.WaitGroup
		doneStopping.Add(2)
		actions.
			On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{QueryId: &queryID}).
			Run(func(_ mock.Arguments) { doneStopping.Done() }).
			Return(nil, errThrottled).
			Once()
		trueValue := true
		actions.
			On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{QueryId: &queryID}).
			Run(func(_ mock.Arguments) { doneStopping.Done() }).
			Return(&cloudwatchlogs.StopQueryOutput{Success: &trueValue}, nil).
			Once()
		stopAdapter.
			On("decrease").
			Return(true).
			Once()
		stopAdapter.
			On("value").
			Return(99_999.0).
			Times(2) // Calculate minDelay after RPS decrease, print decrease RPS log.
		stopAdapter.
			On("increase").
			Return(true).
			Maybe() // Increase RPS after second StopQuery. (May happen after test ends.)
		stopAdapter.
			On("value").
			Return(99_999.5).
			Maybe() // Calculate minDelay after RPS increase. (May happen after test ends.)
		stopAdapter.
			On("value").
			Return(99_999.5).
			Maybe() // Print increase RPS log. (May happen after test ends.)

		// Run query and cancel it as soon as we have polled at least twice.
		s, err := m.Query(QuerySpec{
			Text:   text,
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: groups,
		})
		require.NoError(t, err)
		donePolling.Wait()
		err = s.Close()
		assert.NoError(t, err)

		// Assert expectations.
		doneStopping.Wait()
		actions.AssertExpectations(t)
		startAdapter.AssertExpectations(t)
		stopAdapter.AssertExpectations(t)
		assert.LessOrEqual(t, m2.poller.rps.value(), float64(lotsOfRPS[GetQueryResults])-2*rpsDownStep)
	})
}
