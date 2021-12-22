// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestStats(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		testCases := []struct {
			name    string
			s, t, u Stats
		}{
			{
				name: "Zero.Plus.Zero",
			},
			{
				name: "Zero.Plus.One",
				t: Stats{
					BytesScanned:   1.0,
					RecordsMatched: 1.0,
					RecordsScanned: 1.0,

					RangeRequested: 1,
					RangeStarted:   1,
					RangeDone:      1,
					RangeFailed:    1,
				},
				u: Stats{
					BytesScanned:   1.0,
					RecordsMatched: 1.0,
					RecordsScanned: 1.0,

					RangeRequested: 1,
					RangeStarted:   1,
					RangeDone:      1,
					RangeFailed:    1,
				},
			},
			{
				name: "One.Plus.Zero",
				s: Stats{
					BytesScanned:   1.0,
					RecordsMatched: 1.0,
					RecordsScanned: 1.0,

					RangeRequested: 1,
					RangeStarted:   1,
					RangeDone:      1,
					RangeFailed:    1,
				},
				u: Stats{
					BytesScanned:   1.0,
					RecordsMatched: 1.0,
					RecordsScanned: 1.0,

					RangeRequested: 1,
					RangeStarted:   1,
					RangeDone:      1,
					RangeFailed:    1,
				},
			},
			{
				name: "Mish.Mash",
				s: Stats{
					BytesScanned:   1.0,
					RecordsMatched: 2.0,
					RecordsScanned: 3.0,

					RangeRequested: 4,
					RangeStarted:   5,
					RangeDone:      6,
					RangeFailed:    7,
				},
				t: Stats{
					BytesScanned:   11.0,
					RecordsMatched: 12.0,
					RecordsScanned: 13.0,

					RangeRequested: 14,
					RangeStarted:   15,
					RangeDone:      16,
					RangeFailed:    17,
				},
				u: Stats{
					BytesScanned:   12.0,
					RecordsMatched: 14.0,
					RecordsScanned: 16.0,

					RangeRequested: 18,
					RangeStarted:   20,
					RangeDone:      22,
					RangeFailed:    24,
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				v := testCase.s

				v.add(&testCase.t)

				assert.Equal(t, testCase.u, v)
			})
		}
	})
}

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
					if testCase.before.Name == "" {
						assert.Equal(t, fmt.Sprintf("%p", m2), m2.Name)
						testCase.after.Name = m2.Name
					}
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
			logger.ExpectPrintf("incite: QueryManager(%s) started").Maybe()
			logger.ExpectPrintf("incite: QueryManager(%s) stopping...").Maybe()
			logger.ExpectPrintf("incite: QueryManager(%s) stopped").Maybe()
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
				On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
					StartTime:     startTimeSeconds(defaultStart.Add(time.Duration(i) * time.Minute)),
					EndTime:       endTimeSeconds(defaultStart.Add(time.Duration(i+1) * time.Minute)),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("bar")},
					QueryString:   &text,
				}).
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
				name: "Chunk.SubSecond",
				QuerySpec: QuerySpec{
					Text:   "ham",
					Start:  defaultStart,
					End:    defaultEnd,
					Groups: []string{"ham"},
					Chunk:  15 * time.Millisecond,
				},
				err: chunkSubSecondMsg,
			},
			{
				name: "SplitUntil.SubSecond",
				QuerySpec: QuerySpec{
					Text:       "Whose woods are these?\nI think I know",
					Start:      defaultStart,
					End:        defaultEnd,
					Groups:     []string{"His house is in the village", "Though"},
					Limit:      MaxLimit - 1,
					SplitUntil: time.Minute + 10*time.Microsecond,
				},
				err: splitUntilSubSecondMsg,
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
				err: splitUntilWithoutMaxLimit,
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
			expectedNext     int64
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
				expectedNext:     1,
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
				expectedNext:     1,
				expectedCauseErr: causeErr,
				expectedStats: Stats{
					RangeRequested: defaultDuration,
					RangeStarted:   defaultDuration,
					RangeFailed:    defaultDuration,
				},
			},
			{
				name: "PartialChunk",
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
				expectedNext:     1,
				expectedCauseErr: causeErr,
				expectedStats: Stats{
					RangeRequested: defaultDuration,
					RangeStarted:   3 * time.Minute,
					RangeFailed:    3 * time.Minute,
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
				expectedNext:     1,
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
				assert.Equal(t, testCase.expectedGroups, s2.groups)
				r := make([]Result, 1)
				n, err := s.Read(r)
				wg.Wait()
				assert.Equal(t, testCase.expectedNext, s2.next)
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

	t.Run("Query Starts But Failed Chunk Needs Restarting", func(t *testing.T) {
		// The purpose of this test case is to verify that when a query
		// which is not in preview mode suffers a failed chunk, the
		// chunk is restarted as many times as it takes to get to a
		// non-failure outcome.
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
					// The chunk `c` will fail `c` times before succeeding.
					for i := 1; i <= c; i++ {
						queryID := fmt.Sprintf("n=%d|c=%d|i=%d", n, c, i)
						actions.
							On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
								StartTime:     startTimeSeconds(defaultStart.Add(time.Duration(c-1) * time.Minute)),
								EndTime:       endTimeSeconds(defaultStart.Add(time.Duration(c) * time.Minute)),
								Limit:         defaultLimit,
								LogGroupNames: []*string{sp("foo")},
								QueryString:   &text,
							}).
							Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryID}, nil).
							Once()
						getCall := actions.
							On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryID})
						if i < c {
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
						return *input.QueryString == queryString && *input.StartTime == *startTimeSeconds(startTime)
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
			logger.
				ExpectPrintf("incite: QueryManager(%s) started", t.Name()).
				Once()
			logger.
				ExpectPrintf("incite: QueryManager(%s) stopping...", t.Name()).
				Maybe()
			logger.
				ExpectPrintf("incite: QueryManager(%s) stopped", t.Name()).
				Maybe()
			actions := newMockActions(t)
			text := "a query in two chunks which generates logs"
			// CHUNK 1.
			queryIDChunk1 := "foo"
			actions.
				On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(time.Second)),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("grp")},
					QueryString:   &text,
				}).
				Return(nil, cwlErr(cloudwatchlogs.ErrCodeServiceUnavailableException, "foo")).
				Once()
			logger.
				ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", t.Name(), "temporary failure to start", "0", text, defaultStart, defaultStart.Add(time.Second), "ServiceUnavailableException: foo").
				Once()
			actions.
				On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(time.Second)),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("grp")},
					QueryString:   &text,
				}).
				Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryIDChunk1}, nil).
				Once()
			logger.
				ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "started", "0(foo)").
				Once()
			actions.
				On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryIDChunk1}).
				Return(&cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{},
					Status:  sp(cloudwatchlogs.QueryStatusComplete),
				}, nil).
				Once()
			logger.
				ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", t.Name(), "finished", "0(foo)").
				Once()
			// CHUNK 2.
			queryIDChunk2 := []string{"bar.try1", "bar.try2"}
			actions.
				On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
					StartTime:     startTimeSeconds(defaultStart.Add(time.Second)),
					EndTime:       endTimeSeconds(defaultStart.Add(2 * time.Second)),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("grp")},
					QueryString:   &text,
				}).
				Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryIDChunk2[0]}, nil).
				Once()
			logger.
				ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "started", "1(bar.try1)").
				Once()
			actions.
				On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryIDChunk2[0]}).
				Return(&cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{},
					Status:  sp(cloudwatchlogs.QueryStatusFailed),
				}, nil).
				Once()
			actions.
				On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
					StartTime:     startTimeSeconds(defaultStart.Add(time.Second)),
					EndTime:       endTimeSeconds(defaultStart.Add(2 * time.Second)),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("grp")},
					QueryString:   &text,
				}).
				Return(&cloudwatchlogs.StartQueryOutput{QueryId: &queryIDChunk2[1]}, nil).
				Once()
			logger.
				ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "started", "1R(bar.try2)").
				Once()
			actions.
				On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryIDChunk2[1]}).
				Return(&cloudwatchlogs.GetQueryResultsOutput{
					Results: [][]*cloudwatchlogs.ResultField{},
					Status:  sp(cloudwatchlogs.QueryStatusComplete),
				}, nil).
				Once()
			logger.
				ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", t.Name(), "finished", "1R(bar.try2)", text, mock.Anything, mock.Anything, "end of stream").
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
			name   string
			chunks []expectedChunk
		}{
			{
				name:   "Already Minimum Chunk Size",
				chunks: []expectedChunk{{time.Second, 0, 2, nil}},
			},
			{
				name: "One Split in Half",
				chunks: []expectedChunk{
					{
						size:  2 * time.Second,
						start: 0,
						end:   2,
						chunks: []expectedChunk{
							{time.Second, 0, 2, nil},
							{time.Second, 2, 3, nil},
						},
					},
				},
			},
			{
				name: "One Split in Thirds",
				chunks: []expectedChunk{
					{
						size:  3 * time.Second,
						start: 0,
						end:   2,
						chunks: []expectedChunk{
							{time.Second, 0, 1, nil},
							{time.Second, 1, 3, nil},
							{time.Second, 3, 5, nil},
						},
					},
				},
			},
			{
				name: "One Split in Quarters",
				chunks: []expectedChunk{
					{
						size:  4 * time.Second,
						start: 0,
						end:   2,
						chunks: []expectedChunk{
							{time.Second, 0, 1, nil},
							{time.Second, 1, 3, nil},
							{time.Second, 3, 5, nil},
							{time.Second, 5, 6, nil},
						},
					},
				},
			},
			{
				name: "Odd Splits",
				chunks: []expectedChunk{
					{
						size:  5 * time.Second,
						start: 0,
						end:   2,
						chunks: []expectedChunk{
							{2 * time.Second, 0, 1, nil},
							{
								size:  2 * time.Second,
								start: 1, end: 3,
								chunks: []expectedChunk{
									{time.Second, 1, 2, nil},
									{time.Second, 2, 3, nil},
								},
							},
							{time.Second, 3, 5, nil},
						},
					},
				},
			},
		}

		// Run the sub-tests.
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				// ARRANGE.
				logger := newMockLogger(t)
				actions := newMockActions(t)
				var expectedResults []Result
				var f func(string, time.Duration, expectedChunk)
				f = func(chunkID string, offset time.Duration, chunk expectedChunk) {
					actions.
						On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
							QueryString:   sp("foo"),
							LogGroupNames: []*string{sp("bar")},
							Limit:         int64p(maxLimit),
							StartTime:     startTimeSeconds(defaultStart.Add(offset)),
							EndTime:       endTimeSeconds(defaultStart.Add(offset).Add(chunk.size)),
						}).
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
						ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", t.Name(), "started", chunkID+"("+chunkID+")", "foo").
						Once()
					logger.
						ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", t.Name(), "finished", chunkID+"("+chunkID+")", "foo").
						Maybe()
					if len(chunk.chunks) == 0 {
						expectedResults = append(expectedResults, chunkResults...)
						return
					}
					logger.
						ExpectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", t.Name(), "split", chunkID+"("+chunkID+")", "foo").
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
				logger.
					ExpectPrintf("incite: QueryManager(%s) started", t.Name()).
					Once()
				logger.
					ExpectPrintf("incite: QueryManager(%s) stopping...", t.Name()).
					Maybe()
				logger.
					ExpectPrintf("incite: QueryManager(%s) stopped", t.Name()).
					Maybe()
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
					SplitUntil: time.Second,
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

		t.Run("Read Into Length One Buffer Succeeds with Second of Two Results and EOF", func(t *testing.T) {
			p := make([]Result, 1)
			n, err := s.Read(p)
			assert.Equal(t, 1, n)
			assert.Same(t, io.EOF, err)
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
						On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
							StartTime:     startTimeSeconds(defaultStart.Add(time.Duration(ci) * time.Hour)),
							EndTime:       endTimeSeconds(defaultStart.Add(time.Duration(ci+1) * time.Hour)),
							Limit:         defaultLimit,
							LogGroupNames: []*string{sp("baz")},
							QueryString:   &text[si],
						})
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

func TestIsTemporary(t *testing.T) {
	t.Run("True Cases", func(t *testing.T) {
		trueCases := []error{
			cwlErr(cloudwatchlogs.ErrCodeLimitExceededException, "stay under that limit"),
			cwlErr(cloudwatchlogs.ErrCodeServiceUnavailableException, "stand by for more great service"),
			cwlErr("tttthroTTLED!", "simmer down"),
			io.EOF,
			wrapErr{io.EOF},
			cwlErr("i am at the end of my file", "the end I say", io.EOF),
			syscall.ETIMEDOUT,
			wrapErr{syscall.ETIMEDOUT},
			cwlErr("my time has run out", "the end I say", syscall.ETIMEDOUT),
			syscall.ECONNREFUSED,
			wrapErr{syscall.ECONNREFUSED},
			cwlErr("let there be no connection", "for it has been refused", syscall.ECONNREFUSED),
			syscall.ECONNRESET,
			wrapErr{syscall.ECONNRESET},
			cwlErr("Reset that conn!", "Reset, reset!", syscall.ECONNRESET),
		}
		for i, trueCase := range trueCases {
			t.Run(fmt.Sprintf("trueCase[%d]=%s", i, trueCase), func(t *testing.T) {
				assert.True(t, isTemporary(trueCase))
			})
		}
	})

	t.Run("False Cases", func(t *testing.T) {
		falseCases := []error{
			nil,
			errors.New("bif"),
			cwlErr(cloudwatchlogs.ErrCodeInvalidOperationException, "foo"),
			cwlErr(cloudwatchlogs.ErrCodeInvalidParameterException, "bar"),
			cwlErr(cloudwatchlogs.ErrCodeMalformedQueryException, "baz"),
			cwlErr(cloudwatchlogs.ErrCodeResourceNotFoundException, "ham"),
			cwlErr(cloudwatchlogs.ErrCodeUnrecognizedClientException, "eggs"),
			syscall.ENETDOWN,
			wrapErr{syscall.ENETDOWN},
			cwlErr("Ain't no network", "It's down", syscall.ENETDOWN),
		}
		for i, falseCase := range falseCases {
			t.Run(fmt.Sprintf("trueCase[%d]=%s", i, falseCase), func(t *testing.T) {
				assert.False(t, isTemporary(falseCase))
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
	t.Cleanup(func() {
		_ = m.Close()
	})

	var allStats Stats
	for i, s := range scenarios {
		s.test(t, i, m, actions, false)
		allStats.add(&s.stats)
	}

	err := m.Close()
	assert.NoError(t, err)
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
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeFailed:    defaultDuration,
		},
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
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeFailed:    defaultDuration,
		},
	},

	{
		note: "OneChunk.OnePoll.Empty",
		QuerySpec: QuerySpec{
			Text:   "empty",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"/my/empty/group"},
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
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
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
		err: &TerminalQueryStatusError{"scenario:3|chunk:0|OneChunk.OnePoll.Status.Cancelled", cloudwatchlogs.QueryStatusCancelled, "destined for cancellation"},
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeFailed:    defaultDuration,
		},
	},
	{
		note: "OneChunk.OnePoll.Status.Timeout",
		QuerySpec: QuerySpec{
			Text:   "tempting a timeout",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"/any/group"},
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
		err: &TerminalQueryStatusError{"scenario:4|chunk:0|OneChunk.OnePoll.Status.Timeout", "Timeout", "tempting a timeout"},
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeFailed:    defaultDuration,
		},
	},
	{
		note: "OneChunk.OnePoll.Status.Unexpected",
		QuerySpec: QuerySpec{
			Text:   "expecting the unexpected...status",
			Start:  defaultStart,
			End:    defaultEnd,
			Groups: []string{"/any/group"},
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
		err: &TerminalQueryStatusError{"scenario:5|chunk:0|OneChunk.OnePoll.Status.Unexpected", "Did you see this coming?", "expecting the unexpected...status"},
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeFailed:    defaultDuration,
		},
		expectStop: true,
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
		err: &UnexpectedQueryError{"scenario:6|chunk:0|OneChunk.OnePoll.Error.Unexpected", "expecting the unexpected...error", errors.New("very bad news")},
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeFailed:    defaultDuration,
		},
		expectStop: true,
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
						stats: &Stats{1, 2, 3, 0, 0, 0, 0, 0},
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
		stats: Stats{
			BytesScanned:   1,
			RecordsMatched: 2,
			RecordsScanned: 3,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
		},
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
					io.EOF,
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusScheduled,
					},
					{
						err: io.EOF,
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						stats:  &Stats{3, 0, 1, 0, 0, 0, 0, 0},
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
						stats: &Stats{99, 98, 97, 0, 0, 0, 0, 0},
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
						stats: &Stats{100, 99, 98, 0, 0, 0, 0, 0},
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
		stats: Stats{
			BytesScanned:   100,
			RecordsMatched: 99,
			RecordsScanned: 98,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
		},
	},

	{
		note: "OneChunk.Preview.Status.Failed",
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
		err: &TerminalQueryStatusError{"scenario:9|chunk:0|OneChunk.Preview.Status.Failed", cloudwatchlogs.QueryStatusFailed, "fated for failure"},
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeFailed:    defaultDuration,
		},
	},
	{
		note: "OneChunk.Preview.Status.Cancelled",
		QuerySpec: QuerySpec{
			Text:    "preview of coming cancellations",
			Start:   defaultStart,
			End:     defaultEnd,
			Groups:  []string{"/some/group", "/other/group"},
			Preview: true,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("preview of coming cancellations"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/some/group"), sp("/other/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusCancelled,
					},
				},
			},
		},
		err: &TerminalQueryStatusError{"scenario:10|chunk:0|OneChunk.Preview.Status.Cancelled", cloudwatchlogs.QueryStatusCancelled, "preview of coming cancellations"},
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeFailed:    defaultDuration,
		},
	},
	{
		note: "OneChunk.Preview.Status.Timeout",
		QuerySpec: QuerySpec{
			Text:    "preview of coming timeouts",
			Start:   defaultStart,
			End:     defaultEnd,
			Groups:  []string{"/any/group"},
			Preview: true,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("preview of coming timeouts"),
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
		err: &TerminalQueryStatusError{"scenario:11|chunk:0|OneChunk.Preview.Status.Timeout", "Timeout", "preview of coming timeouts"},
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeFailed:    defaultDuration,
		},
	},
	{
		note: "OneChunk.Preview.Status.Unexpected",
		QuerySpec: QuerySpec{
			Text:    "preview of coming surprises...",
			Start:   defaultStart,
			End:     defaultEnd,
			Groups:  []string{"/any/group"},
			Preview: true,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("preview of coming surprises..."),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         defaultLimit,
					LogGroupNames: []*string{sp("/any/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: "I did NOT see this coming!",
					},
				},
			},
		},
		err: &TerminalQueryStatusError{"scenario:12|chunk:0|OneChunk.Preview.Status.Unexpected", "I did NOT see this coming!", "preview of coming surprises..."},
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeFailed:    defaultDuration,
		},
		expectStop: true,
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
						stats: &Stats{1, 2, 3, 0, 0, 0, 0, 0},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"Foo", "Foo.0.0"}, {"Bar", "Bar.0.0"}, {"@ptr", "0"}},
							{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
						},
						stats: &Stats{2, 4, 6, 0, 0, 0, 0, 0},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
						},
						stats: &Stats{3, 6, 9, 0, 0, 0, 0, 0},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
							{{"Foo", "Foo.2.0"}, {"Bar", "Bar.2.0"}, {"@ptr", "2"}},
						},
						stats: &Stats{4, 8, 12, 0, 0, 0, 0, 0},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
							{{"Foo", "Foo.2.0"}, {"Bar", "Bar.2.0"}, {"@ptr", "2"}},
							{{"Foo", "Foo.3.0"}, {"Bar", "Bar.3.0"}, {"@ptr", "3"}},
						},
						stats: &Stats{5, 10, 15, 0, 0, 0, 0, 0},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"Foo", "Foo.0.0"}, {"Bar", "Bar.0.0"}, {"@ptr", "0"}},
							{{"Foo", "Foo.1.0"}, {"Bar", "Bar.1.0"}, {"@ptr", "1"}},
							{{"Foo", "Foo.3.0"}, {"Bar", "Bar.3.0"}, {"@ptr", "3"}},
							{{"Foo", "Foo.4.0"}, {"Bar", "Bar.4.0"}, {"@ptr", "4"}},
						},
						stats: &Stats{6, 12, 18, 0, 0, 0, 0, 0},
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
		stats: Stats{
			BytesScanned:   6,
			RecordsMatched: 12,
			RecordsScanned: 18,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
		},
	},
	{
		note: "OneChunk.Preview.SimulateStatsCommand.NoPtr",
		QuerySpec: QuerySpec{
			Text:    "stats count_distinct(Foo) by bar",
			Start:   defaultStart.Add(-time.Hour),
			End:     defaultEnd.Add(time.Hour),
			Groups:  []string{"/trove/of/data"},
			Preview: true,
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
						stats: &Stats{1, 2, 3, 0, 0, 0, 0, 0},
					},
					{
						status: cloudwatchlogs.QueryStatusRunning,
						results: []Result{
							{{"count_distinct(Foo)", "37"}, {"bar", "eggs"}},
							{{"count_distinct(Foo)", "100"}, {"bar", "ham"}},
						},
						stats: &Stats{2, 4, 6, 0, 0, 0, 0, 0},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"count_distinct(Foo)", "200"}, {"bar", "ham"}},
							{{"count_distinct(Foo)", "41"}, {"bar", "eggs"}},
							{{"count_distinct(Foo)", "10"}, {"bar", "spam"}},
						},
						stats: &Stats{4, 5, 8, 0, 0, 0, 0, 0},
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
		stats: Stats{
			BytesScanned:   4,
			RecordsMatched: 5,
			RecordsScanned: 8,
			RangeRequested: defaultDuration + 2*time.Hour,
			RangeStarted:   defaultDuration + 2*time.Hour,
			RangeDone:      defaultDuration + 2*time.Hour,
		},
	},

	{
		note: "OneChunk.Split.Once",
		QuerySpec: QuerySpec{
			Text:       "display many, things",
			Start:      defaultStart,
			End:        defaultEnd,
			Limit:      MaxLimit,
			Groups:     []string{"/plentiful/logs/grouped/within"},
			Chunk:      defaultDuration,
			SplitUntil: time.Second,
		},
		chunks: []chunkPlan{
			// Original chunk.
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("display many, things"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         int64p(MaxLimit),
					LogGroupNames: []*string{sp("/plentiful/logs/grouped/within")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status:  cloudwatchlogs.QueryStatusComplete,
						results: maxLimitResults,
						stats:   &Stats{1, 1, 1, 0, 0, 0, 0, 0},
					},
				},
			},
			// Split chunk 1/4.
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("display many, things"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(defaultDuration / 4)),
					Limit:         int64p(MaxLimit),
					LogGroupNames: []*string{sp("/plentiful/logs/grouped/within")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status:  cloudwatchlogs.QueryStatusComplete,
						results: maxLimitResults[0 : MaxLimit/4],
						stats:   &Stats{2, 2, 2, 0, 0, 0, 0, 0},
					},
				},
			},
			// Split chunk 2/4.
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("display many, things"),
					StartTime:     startTimeSeconds(defaultStart.Add(defaultDuration / 4)),
					EndTime:       endTimeSeconds(defaultStart.Add(defaultDuration / 2)),
					Limit:         int64p(MaxLimit),
					LogGroupNames: []*string{sp("/plentiful/logs/grouped/within")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status:  cloudwatchlogs.QueryStatusComplete,
						results: maxLimitResults[MaxLimit/4 : MaxLimit/2],
						stats:   &Stats{3, 3, 3, 0, 0, 0, 0, 0},
					},
				},
			},
			// Split chunk 3/4.
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("display many, things"),
					StartTime:     startTimeSeconds(defaultStart.Add(defaultDuration / 2)),
					EndTime:       endTimeSeconds(defaultStart.Add(3 * defaultDuration / 4)),
					Limit:         int64p(MaxLimit),
					LogGroupNames: []*string{sp("/plentiful/logs/grouped/within")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status:  cloudwatchlogs.QueryStatusComplete,
						results: maxLimitResults[MaxLimit/2 : 3*MaxLimit/4],
						stats:   &Stats{4, 4, 4, 0, 0, 0, 0, 0},
					},
				},
			},
			// Split chunk 4/4.
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("display many, things"),
					StartTime:     startTimeSeconds(defaultStart.Add(3 * defaultDuration / 4)),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         int64p(MaxLimit),
					LogGroupNames: []*string{sp("/plentiful/logs/grouped/within")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status:  cloudwatchlogs.QueryStatusComplete,
						results: maxLimitResults[3*MaxLimit/4 : MaxLimit],
						stats:   &Stats{5, 5, 5, 0, 0, 0, 0, 0},
					},
				},
			},
		},
		results: maxLimitResults,
		stats: Stats{
			BytesScanned:   15,
			RecordsMatched: 15,
			RecordsScanned: 15,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
		},
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
						stats: &Stats{77, 777, 7, 0, 0, 0, 0, 0},
					},
				},
			},
		},
		results: []Result{
			{{"EggCount", "1"}, {"Spam", "true"}},
			{{"EggCount", "2"}, {"Span", "false"}},
		},
		stats: Stats{
			BytesScanned:   77,
			RecordsMatched: 777,
			RecordsScanned: 7,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
		},
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
						stats: &Stats{-1, -2, -12, 0, 0, 0, 0, 0},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"@ptr", "1111"}, {"Something", "wicked this way comes"}},
							{{"@ptr", "2222"}, {"Something", "else"}},
						},
						stats: &Stats{13, 8, 3, 0, 0, 0, 0, 0},
					},
				},
			},
		},
		results: []Result{
			{{"@ptr", "1111"}, {"Something", "wicked this way comes"}},
			{{"@ptr", "2222"}, {"Something", "else"}},
		},
		stats: Stats{
			BytesScanned:   13,
			RecordsMatched: 8,
			RecordsScanned: 3,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
		},
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
						stats: &Stats{1, 1, 1, 0, 0, 0, 0, 0},
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
						stats: &Stats{2, 2, 1, 0, 0, 0, 0, 0},
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
		stats: Stats{
			BytesScanned:   3,
			RecordsMatched: 3,
			RecordsScanned: 2,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
		},
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
						stats: &Stats{49, 23, 1, 0, 0, 0, 0, 0},
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
						stats:  &Stats{3, 2, 3, 0, 0, 0, 0, 0},
					},
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{Field: "@ptr", Value: "3"}},
							{{Field: "@ptr", Value: "4"}},
						},
						stats: &Stats{51, 77, 99, 0, 0, 0, 0, 0},
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
		stats: Stats{
			BytesScanned:   100,
			RecordsMatched: 100,
			RecordsScanned: 100,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
		},
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
						stats: &Stats{11, 22, 33, 0, 0, 0, 0, 0},
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
						stats: &Stats{44, 55, 66, 0, 0, 0, 0, 0},
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
						stats: &Stats{77, 88, 99, 0, 0, 0, 0, 0},
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
		stats: Stats{
			BytesScanned:   132,
			RecordsMatched: 165,
			RecordsScanned: 198,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
		},
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
						stats: &Stats{1, 1, 1, 0, 0, 0, 0, 0},
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
						stats: &Stats{1, 1, 1, 0, 0, 0, 0, 0},
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
		stats: Stats{
			BytesScanned:   2,
			RecordsMatched: 2,
			RecordsScanned: 2,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
		},
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
		m.GetStats().checkInvariants(t, false, false)
	})
}

func (qs *queryScenario) play(t *testing.T, i int, m QueryManager, actions *mockActions) {
	// Validate the expected status in advance.
	qs.stats.checkInvariants(t, true, false)

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
				output.Results = backOut(pollOutput.results)
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

func backOut(r []Result) (cwl [][]*cloudwatchlogs.ResultField) {
	for _, rr := range r {
		cwl = append(cwl, rr.backOut())
	}
	return // Will return nil if r is nil
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

func (s Stats) checkInvariants(t *testing.T, done bool, success bool) {
	// The following invariants must always be true regardless of whether
	// the Stream or QueryManager finished all requested work.
	assert.GreaterOrEqual(t, s.RangeRequested, s.RangeStarted, "RangeRequested must be greater than or equal to RangeStarted")
	assert.GreaterOrEqual(t, s.RangeStarted, s.RangeDone, "RangeStarted must be greater than or equal to RangeDone")
	assert.GreaterOrEqual(t, s.RangeStarted, s.RangeFailed, "RangeStarted must be greater than or equal to RangeFailed")
	assert.GreaterOrEqual(t, s.RangeStarted, s.RangeDone+s.RangeFailed, "RangeStarted must be greater than or equal to the sum of RangeDone + RangeFailed")
	// The following invariants are always true when the Stream or
	// QueryManager has finished all requested work even if some of it
	// failed.
	if done {
		assert.Equal(t, s.RangeDone+s.RangeFailed, s.RangeStarted, "the sum of RangeDone + RangeFailed must equal RangeStarted if all work is done")
	}
	// The following invariants are always true when the Stream or
	// QueryManager has finished all requested work successfully.
	if success {
		assert.Equal(t, s.RangeStarted, s.RangeRequested, "RangeStarted must equal RangeRequested if all work finished successfully")
		assert.Equal(t, s.RangeDone, s.RangeStarted, "RangeDone must equal RangeStarted if all work finished successfully")
		assert.Equal(t, 0, s.RangeFailed, "RangeFailed must equal zero if all work finished successfully")
	}
}

func int64p(i int64) *int64 {
	return &i
}

func float64p(f float64) *float64 {
	return &f
}

func startTimeSeconds(t time.Time) *int64 {
	return int64p(t.Unix())
}

func endTimeSeconds(t time.Time) *int64 {
	return startTimeSeconds(t.Add(-time.Second))
}

func cwlErr(code, message string, cause ...error) error {
	var origErr error
	if len(cause) == 1 {
		origErr = cause[0]
	} else if len(cause) > 1 {
		panic("only one cause allowed")
	}
	return awserr.New(code, message, origErr)
}

type wrapErr struct {
	cause error
}

func (err wrapErr) Error() string {
	return fmt.Sprintf("wrapped: %s", err.cause)
}

func (err wrapErr) Unwrap() error {
	return err.cause
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

func resultSeries(i, n int) []Result {
	series := make([]Result, n)
	for j := range series {
		series[j] = []ResultField{{"@ptr", strconv.Itoa(i + j)}}
	}
	return series
}

var (
	defaultDuration = 5 * time.Minute
	defaultStart    = time.Date(2020, 8, 25, 3, 30, 0, 0, time.UTC)
	defaultEnd      = defaultStart.Add(defaultDuration)
	defaultLimit    = int64p(DefaultLimit)
	lotsOfRPS       = map[CloudWatchLogsAction]int{
		StartQuery:      100_000,
		GetQueryResults: 100_000,
		StopQuery:       100_000,
	}
	anyContext = mock.MatchedBy(func(ctx context.Context) bool {
		return ctx != nil
	})
	anyStartQueryInput      = mock.AnythingOfType("*cloudwatchlogs.StartQueryInput")
	anyGetQueryResultsInput = mock.AnythingOfType("*cloudwatchlogs.GetQueryResultsInput")
	maxLimitResults         = resultSeries(0, MaxLimit)
)
