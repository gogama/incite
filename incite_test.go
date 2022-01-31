// Copyright 2022 The incite Authors. All rights reserved.
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
	"testing"
	"time"

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
		note: "OneChunk.Split.AndThenMaxed",
		QuerySpec: QuerySpec{
			Text:       "fields maximum, items",
			Start:      defaultStart,
			End:        defaultStart.Add(2 * time.Second),
			Limit:      MaxLimit,
			Groups:     []string{"/a/plethora/of/logs"},
			SplitUntil: time.Second,
		},
		chunks: []chunkPlan{
			// Original chunk, length two seconds.
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("fields maximum, items"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(2 * time.Second)),
					Limit:         int64p(MaxLimit),
					LogGroupNames: []*string{sp("/a/plethora/of/logs")},
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
			// Split chunk 1/2.
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("fields maximum, items"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(time.Second)),
					Limit:         int64p(MaxLimit),
					LogGroupNames: []*string{sp("/a/plethora/of/logs")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status:  cloudwatchlogs.QueryStatusComplete,
						results: maxLimitResults,
						stats:   &Stats{2, 2, 2, 0, 0, 0, 0, 0},
					},
				},
			},
			// Split chunk 2/2.
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("fields maximum, items"),
					StartTime:     startTimeSeconds(defaultStart.Add(time.Second)),
					EndTime:       endTimeSeconds(defaultStart.Add(2 * time.Second)),
					Limit:         int64p(MaxLimit),
					LogGroupNames: []*string{sp("/a/plethora/of/logs")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						stats:  &Stats{3, 3, 3, 0, 0, 0, 0, 0},
					},
				},
			},
		},
		results: maxLimitResults,
		stats: Stats{
			BytesScanned:   6,
			RecordsMatched: 6,
			RecordsScanned: 6,
			RangeRequested: 2 * time.Second,
			RangeStarted:   2 * time.Second,
			RangeDone:      2 * time.Second,
			RangeMaxed:     time.Second,
		},
	},

	{
		note: "OneChunk.Maxed",
		QuerySpec: QuerySpec{
			Text:   "display how_many_results_are_there",
			Start:  defaultStart,
			End:    defaultEnd,
			Limit:  1,
			Groups: []string{"/very/full/log/group"},
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("display how_many_results_are_there"),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         int64p(1),
					LogGroupNames: []*string{sp("/very/full/log/group")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"how_many_results_are_there", "too many!"}},
						},
					},
				},
			},
		},
		results: []Result{
			{{"how_many_results_are_there", "too many!"}},
		},
		stats: Stats{
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
			RangeMaxed:     defaultDuration,
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

	{
		note: "MultiChunk.OneMaxed",
		QuerySpec: QuerySpec{
			Text:   "QuerySpec indicates chunking; [start, end) defines exactly two chunks; and limit is 1.",
			Start:  defaultStart,
			End:    defaultEnd,
			Limit:  1,
			Groups: []string{"uno", "due"},
			Chunk:  150 * time.Second,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking; [start, end) defines exactly two chunks; and limit is 1."),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(150 * time.Second)),
					Limit:         int64p(1),
					LogGroupNames: []*string{sp("uno"), sp("due")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						stats:  &Stats{1, 0, 1, 0, 0, 0, 0, 0},
					},
				},
			},
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking; [start, end) defines exactly two chunks; and limit is 1."),
					StartTime:     startTimeSeconds(defaultStart.Add(150 * time.Second)),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         int64p(1),
					LogGroupNames: []*string{sp("uno"), sp("due")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"@ptr", "cccc"}, {"@timestamp", "2021-08-05 15:26:000.124"}},
						},
						stats: &Stats{2, 1, 1, 0, 0, 0, 0, 0},
					},
				},
			},
		},
		results: []Result{
			{{"@ptr", "cccc"}, {"@timestamp", "2021-08-05 15:26:000.124"}},
		},
		stats: Stats{
			BytesScanned:   3,
			RecordsMatched: 1,
			RecordsScanned: 2,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
			RangeMaxed:     150 * time.Second,
		},
	},

	{
		note: "MultiChunk.TwoMaxed",
		QuerySpec: QuerySpec{
			Text:   "QuerySpec indicates chunking; [start, end) defines exactly two chunks; and limit is 1.",
			Start:  defaultStart,
			End:    defaultEnd,
			Limit:  1,
			Groups: []string{"primo", "secondo"},
			Chunk:  150 * time.Second,
		},
		chunks: []chunkPlan{
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking; [start, end) defines exactly two chunks; and limit is 1."),
					StartTime:     startTimeSeconds(defaultStart),
					EndTime:       endTimeSeconds(defaultStart.Add(150 * time.Second)),
					Limit:         int64p(1),
					LogGroupNames: []*string{sp("primo"), sp("secondo")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"@ptr", "AAAAAA"}, {"@timestamp", "2021-08-05 15:26:000.123"}},
						},
						stats: &Stats{1, 1, 1, 0, 0, 0, 0, 0},
					},
				},
			},
			{
				startQueryInput: cloudwatchlogs.StartQueryInput{
					QueryString:   sp("QuerySpec indicates chunking; [start, end) defines exactly two chunks; and limit is 1."),
					StartTime:     startTimeSeconds(defaultStart.Add(150 * time.Second)),
					EndTime:       endTimeSeconds(defaultEnd),
					Limit:         int64p(1),
					LogGroupNames: []*string{sp("primo"), sp("secondo")},
				},
				startQuerySuccess: true,
				pollOutputs: []chunkPollOutput{
					{
						status: cloudwatchlogs.QueryStatusComplete,
						results: []Result{
							{{"@ptr", "BBBBBB"}, {"@timestamp", "2021-08-05 15:26:000.124"}},
						},
						stats: &Stats{2, 1, 1, 0, 0, 0, 0, 0},
					},
				},
			},
		},
		results: []Result{
			{{"@ptr", "AAAAAA"}, {"@timestamp", "2021-08-05 15:26:000.123"}},
			{{"@ptr", "BBBBBB"}, {"@timestamp", "2021-08-05 15:26:000.124"}},
		},
		postprocess: func(r []Result) {
			sort.Slice(r, func(i, j int) bool {
				return r[i].get("@timestamp") < r[j].get("@timestamp")
			})
		},
		stats: Stats{
			BytesScanned:   3,
			RecordsMatched: 2,
			RecordsScanned: 2,
			RangeRequested: defaultDuration,
			RangeStarted:   defaultDuration,
			RangeDone:      defaultDuration,
			RangeMaxed:     defaultDuration,
		},
	},
}

type queryScenario struct {
	QuerySpec
	note        string         // Optional note describing the scenario
	chunks      []chunkPlan    // Sub-scenario for each chunk
	closeEarly  bool           // Whether to prematurely close the stream. [TODO: Not currently used.]
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
