package incite

import (
	"testing"
	"time"

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
}

func TestQueryManager_GetStats(t *testing.T) {
	// TODO: This will be trivial but might as well capture the happy path here.
}

func TestQueryManager_Query(t *testing.T) {
	// TODO: Need to think of cases. Some possibles:
	//        1. HAPPIEST PATH: One query, one chunk, one request.
	//        2. One query, two chunks, one request each.
	//        3. 10 queries, 10 chunks each, each chunk needing to be polled multiple times.
	//
	// For the above, can vary the Parallelism 1, 2, ... Max.
}
