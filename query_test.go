// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	t.Run("Nil Context", func(t *testing.T) {
		assert.PanicsWithValue(t, nilContextMsg, func() {
			_, _ = Query(nil, newMockActions(t), QuerySpec{})
		})
	})

	t.Run("Bad Query", func(t *testing.T) {
		// ACT.
		r, err := Query(context.Background(), newMockActions(t), QuerySpec{})

		// ASSERT.
		assert.Nil(t, r)
		assert.EqualError(t, err, "incite: blank query text")
	})

	t.Run("Run to Completion", func(t *testing.T) {
		// ARRANGE.
		actions := newMockActions(t)
		actions.
			On("StartQueryWithContext", anyContext, &cloudwatchlogs.StartQueryInput{
				QueryString:   sp("x"),
				LogGroupNames: []*string{sp("y")},
				StartTime:     startTimeSeconds(defaultStart),
				EndTime:       endTimeSeconds(defaultEnd),
				Limit:         int64p(DefaultLimit),
			}).
			Return(&cloudwatchlogs.StartQueryOutput{QueryId: sp("ham")}, nil).
			Once()
		actions.On("GetQueryResultsWithContext", anyContext, &cloudwatchlogs.GetQueryResultsInput{
			QueryId: sp("ham"),
		}).Return(&cloudwatchlogs.GetQueryResultsOutput{
			Status:  sp(cloudwatchlogs.QueryStatusComplete),
			Results: [][]*cloudwatchlogs.ResultField{},
		}, nil).Once()

		// ACT.
		r, err := Query(context.Background(), actions, QuerySpec{
			Text:   "x",
			Groups: []string{"y"},
			Start:  defaultStart,
			End:    defaultEnd,
		})

		// ASSERT.
		assert.Equal(t, []Result{}, r)
		assert.NoError(t, err)
		actions.AssertExpectations(t)
	})

	t.Run("Context Cancelled", func(t *testing.T) {
		// ARRANGE.
		timer := time.NewTimer(50 * time.Millisecond)
		actions := newMockActions(t)
		actions.
			On("StartQueryWithContext", anyContext, anyStartQueryInput).
			WaitUntil(timer.C).
			Return(&cloudwatchlogs.StartQueryOutput{QueryId: sp("eggs")}).
			Maybe()
		actions.
			On("GetQueryResultsWithContext", anyContext, mock.Anything).
			Return(&cloudwatchlogs.GetQueryResultsOutput{
				Status: sp(cloudwatchlogs.QueryStatusComplete),
			}).
			Maybe()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// ACT.
		r, err := Query(ctx, actions, QuerySpec{
			Text:   "x",
			Groups: []string{"y"},
			Start:  defaultStart,
			End:    defaultEnd,
		})

		// ASSERT.
		assert.Nil(t, r)
		assert.Same(t, context.Canceled, err)
		actions.AssertExpectations(t)
	})
}
