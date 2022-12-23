// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewStopper(t *testing.T) {
	s, a, l := newTestableStopper(t, 25)

	require.NotNil(t, s)
	assert.Equal(t, time.Second/25, s.minDelay)
	var closer <-chan struct{}
	closer = s.m.close
	assert.Equal(t, closer, s.close)
	var in <-chan *chunk = s.m.stop
	assert.Equal(t, in, s.in)
	var out chan<- *chunk = s.m.update
	assert.Equal(t, out, s.out)
	assert.Equal(t, "stopper", s.name)
	assert.Equal(t, 3, s.maxTemporaryError)
	a.AssertExpectations(t)
	l.AssertExpectations(t)
}

func TestStopper_context(t *testing.T) {
	s, a, l := newTestableStopper(t, 1_000_000)

	ctx := s.context(&chunk{})

	assert.Same(t, context.Background(), ctx)
	l.AssertExpectations(t)
	a.AssertExpectations(t)
}

var stopperManipulateCases = []struct {
	name     string
	setup    func(t *testing.T, actions *mockActions, logger *mockLogger)
	expected outcome
}{
	{
		name: "Throttling Error",
		setup: func(_ *testing.T, actions *mockActions, logger *mockLogger) {
			actions.
				On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{
					QueryId: sp("bar"),
				}).
				Return(nil, cwlErr("throttling has occurred", "baz", errors.New("qux"))).
				Once()
		},
		expected: throttlingError,
	},
	{
		name: "Temporary Error",
		setup: func(_ *testing.T, actions *mockActions, logger *mockLogger) {
			actions.
				On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{
					QueryId: sp("bar"),
				}).
				Return(nil, cwlErr(cloudwatchlogs.ErrCodeLimitExceededException, "baz", errors.New("qux"))).
				Once()
		},
		expected: temporaryError,
	},
	{
		name: "Permanent Error",
		setup: func(t *testing.T, actions *mockActions, logger *mockLogger) {
			actions.
				On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{
					QueryId: sp("bar"),
				}).
				Return(nil, cwlErr(cloudwatchlogs.ErrCodeInvalidParameterException, "baz", errors.New("qux"))).
				Once()
			logger.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s",
				t.Name(), "failed to stop", "foo(bar)", "", mock.Anything, mock.Anything, mock.Anything)
		},
		expected: finished,
	},
	{
		name: "Nil Success",
		setup: func(t *testing.T, actions *mockActions, logger *mockLogger) {
			actions.
				On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{
					QueryId: sp("bar"),
				}).
				Return(&cloudwatchlogs.StopQueryOutput{}, nil).
				Once()
			logger.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s",
				t.Name(), "failed to stop", "foo(bar)", "", mock.Anything, mock.Anything, "CloudWatch Logs did not indicate success")
		},
		expected: finished,
	},
	{
		name: "False Success",
		setup: func(t *testing.T, actions *mockActions, logger *mockLogger) {
			success := false
			actions.
				On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{
					QueryId: sp("bar"),
				}).
				Return(&cloudwatchlogs.StopQueryOutput{
					Success: &success,
				}, nil).
				Once()
			logger.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s",
				t.Name(), "failed to stop", "foo(bar)", "", mock.Anything, mock.Anything, "CloudWatch Logs did not indicate success")
		},
		expected: finished,
	},
	{
		name: "True Success",
		setup: func(t *testing.T, actions *mockActions, logger *mockLogger) {
			success := true
			actions.
				On("StopQueryWithContext", anyContext, &cloudwatchlogs.StopQueryInput{
					QueryId: sp("bar"),
				}).
				Return(&cloudwatchlogs.StopQueryOutput{
					Success: &success,
				}, nil).
				Once()
			logger.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)",
				t.Name(), "stopped", "foo(bar)", "", mock.Anything, mock.Anything)
		},
		expected: finished,
	},
}

func TestStopper_manipulate(t *testing.T) {
	for _, testCase := range stopperManipulateCases {
		t.Run(testCase.name, func(t *testing.T) {
			s, actions, logger := newTestableStopper(t, 100_000)
			testCase.setup(t, actions, logger)
			c := &chunk{
				stream:  &stream{},
				chunkID: "foo",
				queryID: "bar",
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
}

func TestStopper_release(t *testing.T) {
	releaseCases := []struct {
		name   string
		create func(t *testing.T) (*stopper, *mockActions, *mockLogger)
	}{
		{
			name: "SetTimerRPS",
			create: func(t *testing.T) (s *stopper, a *mockActions, l *mockLogger) {
				s, a, l = newTestableStopper(t, 1_000)
				s.lastReq = time.Now().Add(500 * time.Microsecond)
				return
			},
		},
		{
			name: "NoSetTimerRPS",
			create: func(t *testing.T) (*stopper, *mockActions, *mockLogger) {
				return newTestableStopper(t, 1_000_000)
			},
		},
	}

	for _, releaseCase := range releaseCases {
		t.Run(releaseCase.name, func(t *testing.T) {
			for _, manipulateCase := range stopperManipulateCases {
				t.Run(manipulateCase.name, func(t *testing.T) {
					s, actions, logger := releaseCase.create(t)
					manipulateCase.setup(t, actions, logger)
					c := &chunk{
						stream:  &stream{},
						chunkID: "foo",
						queryID: "bar",
					}

					before := time.Now()
					s.release(c)
					after := time.Now()

					assert.GreaterOrEqual(t, s.lastReq.Sub(before), time.Duration(0))
					assert.GreaterOrEqual(t, after.Sub(s.lastReq), time.Duration(0))
					actions.AssertExpectations(t)
					logger.AssertExpectations(t)
				})
			}
		})
	}
}

func newTestableStopper(t *testing.T, rps int) (s *stopper, a *mockActions, l *mockLogger) {
	a = newMockActions(t)
	l = newMockLogger(t)
	s = newStopper(&mgr{
		Config: Config{
			Actions: a,
			RPS: map[CloudWatchLogsAction]int{
				StopQuery: rps,
			},
			Logger: l,
			Name:   t.Name(),
		},
		close:  make(chan struct{}),
		stop:   make(chan *chunk),
		update: make(chan *chunk),
	})
	return
}
