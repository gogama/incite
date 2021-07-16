package incite

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

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
						RPS:     DefaultRPS,
					},
					after: Config{
						Actions:  actions,
						Parallel: DefaultParallel,
						RPS:      DefaultRPS,
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
					name: "RPS.StartQueryOverride.AtLimit",
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
					name: "RPS.StartQueryOverride.AboveLimit",
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
						StartQuery:      time.Second / time.Duration(RPSQuotaLimits[StartQuery]),
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
					name: "RPS.StopQueryOverride.AtLimit",
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
					name: "RPS.StopQueryOverride.AboveLimit",
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
						StopQuery:       time.Second / time.Duration(RPSQuotaLimits[StopQuery]),
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
					name: "RPS.GetQueryResultsOverride.AtLimit",
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
					name: "RPS.GetQueryResultsOverride.AboveLimit",
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
						GetQueryResults: time.Second / time.Duration(RPSQuotaLimits[GetQueryResults]),
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
			expectedN         int
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

			// TODO: Add limit and update QuerySpec docs before adding
			//       more test cases here.
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
				r := make([]Result, 1)
				n, err := s.Read(r)
				assert.Equal(t, 0, n)
				assert.EqualError(t, err, `incite: fatal error from CloudWatch Logs for chunk "foo" [2020-08-25 03:30:00 +0000 UTC..2020-08-25 03:35:00 +0000 UTC): super fatal error`)
				assert.ErrorIs(t, err, causeErr)
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
		// TODO: Simple test case. Make query manager, close it, verify Query fails.
	})

	t.Run("Empty Read Buffer Does Not Block", func(t *testing.T) {
		// TODO: Need a test to verify that s.Read([]Result{}) does not block.
	})

	t.Run("Scenarios", func(t *testing.T) {
		// Run the scenarios serially first to catch the obvious issues.
		t.Run("Serial", func(t *testing.T) {
			actions := newMockActions(t)
			m := NewQueryManager(Config{
				Actions: actions,
			})
			require.NotNil(t, m)
			defer func() {
				err := m.Close()
				assert.NoError(t, err)
			}()

			for i, s := range scenarios {
				t.Run(fmt.Sprintf("Scenario=%d", i), func(t *testing.T) {
					s.play(t, i, m, actions)
				})
			}

			actions.AssertExpectations(t)
		})

		// Run the scenarios in parallel with varying levels of parallelism to
		// look for additional issues.
		for p := 0; p < QueryConcurrencyQuotaLimit; p++ {
			t.Run(fmt.Sprintf("Parallel=%d", p), func(t *testing.T) {
				for rps := 2; rps <= 5; rps++ {
					t.Run(fmt.Sprintf("RPS=%d", rps), func(t *testing.T) {
						actions := newMockActions(t)
						m := NewQueryManager(Config{
							Actions:  actions,
							Parallel: p,
							RPS: map[CloudWatchLogsAction]int{
								StartQuery:      rps,
								StopQuery:       rps,
								GetQueryResults: rps,
							},
						})
						require.NotNil(t, m)
						t.Cleanup(func() {
							err := m.Close()
							if err != nil {
								t.Errorf("Cleanup: failed to close m: %s", err.Error())
							}
							actions.AssertExpectations(t)
						})

						for i, s := range scenarios {
							t.Run(fmt.Sprintf("Scenario=%d", i), func(t *testing.T) {
								t.Parallel() // Run scenarios in parallel.
								s.play(t, i, m, actions)
							})
						}
					})
				}
			})
		}
	})

	// TODO: Need to think of cases. Some possibles:
	//        1. HAPPIEST PATH: One query, one chunk, one request.
	//        2. One query, two chunks, one request each.
	//        3. 10 queries, 10 chunks each, each chunk needing to be polled multiple times.
	//
	// For the above, can vary the following:
	//     1. Parallelism
	//     2. Chunking
	//     3. Result Limit
	//     4. Hint
}

var scenarios = []queryScenario{
	// NoStart.InvalidQueryError
	// NoStart.UnexpectedError
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
	// OneChunk.OnePoll.[3 cases = Cancelled,Failed,Timeout]
	// OneChunk.MultiPoll.(one case with limit exceeded, throttling, scheduled, unknown, and several running statuses with intermediate results that get ignored)
	// OneChunk.Preview.Stats (noPtr)
	// OneChunk.Preview.Normal (with @ptr)
	// MultiChunk.NoPreview
	// MultiChunk.Preview
}

type queryScenario struct {
	QuerySpec
	note       string              // Optional note describing the scenario
	chunks     []chunkPlan         // Sub-scenario for each chunk
	closeEarly bool                // Whether to prematurely close the stream.
	err        string              // Final expected error
	results    []Result            // Final results in the expected order after optional sorting using less.
	less       func(i, j int) bool // Optional less function for sorting results, needed for chunked scenarios.
	stats      Stats               // Final stats
	closeAfter bool                // Whether to close the stream after the scenario.
}

func (qs *queryScenario) play(t *testing.T, i int, m QueryManager, actions *mockActions) {
	// Set up the chunk scenarios.
	for j, chunkPlan := range qs.chunks {
		chunkPlan.setup(i, j, qs.note, qs.closeEarly, actions)
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
	if qs.err == "" {
		assert.NoError(t, err)
		if qs.less != nil {
			sort.Slice(r, qs.less)
		}
		expectedResults := qs.results
		if expectedResults == nil {
			expectedResults = []Result{}
		}
		assert.Equal(t, expectedResults, r)
	} else {
		assert.EqualError(t, err, qs.err)
		assert.Nil(t, r)
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

func (cp *chunkPlan) setup(i, j int, note string, closeEarly bool, actions *mockActions) {
	for _, err := range cp.startQueryErrs {
		actions.
			On("StartQueryWithContext", anyContext, &cp.startQueryInput).
			Return(nil, err).
			Once()
	}

	if !cp.startQuerySuccess {
		return
	}

	queryID := fmt.Sprintf("scenario:%d|chunk:%d", i, j)
	if note != "" {
		queryID += note
	}
	actions.
		On("StartQueryWithContext", anyContext, &cp.startQueryInput).
		Return(&cloudwatchlogs.StartQueryOutput{
			QueryId: &queryID,
		}, nil)

	for _, pollOutput := range cp.pollOutputs {
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
				for k := range pollOutput.results {
					output.Results[k] = pollOutput.results[k].backOut()
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

	if closeEarly {
		input := &cloudwatchlogs.StopQueryInput{
			QueryId: &queryID,
		}
		actions.
			On("StopQueryWithContext", anyContext, input).
			Return(&cloudwatchlogs.StopQueryOutput{}, nil).
			Maybe()
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

var (
	defaultStart = time.Date(2020, 8, 25, 3, 30, 0, 0, time.UTC)
	defaultEnd   = defaultStart.Add(5 * time.Minute)
	defaultLimit = int64p(DefaultLimit)
	anyContext   = mock.MatchedBy(func(ctx context.Context) bool {
		return ctx != nil
	})
	anyStartQueryInput = mock.AnythingOfType("*cloudwatchlogs.StartQueryInput")
)
