// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
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
			On("StartQuery", anyContext, &cloudwatchlogs.StartQueryInput{
				QueryString:   aws.String("x"),
				LogGroupNames: []string{"y"},
				StartTime:     startTimeMilliseconds(defaultStart),
				EndTime:       endTimeMilliseconds(defaultEnd),
				Limit:         aws.Int32(DefaultLimit),
			}).
			Return(&cloudwatchlogs.StartQueryOutput{QueryId: aws.String("ham")}, nil).
			Once()
		actions.On("GetQueryResults", anyContext, &cloudwatchlogs.GetQueryResultsInput{
			QueryId: aws.String("ham"),
		}).Return(&cloudwatchlogs.GetQueryResultsOutput{
			Status:  types.QueryStatusComplete,
			Results: [][]types.ResultField{},
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
			On("StartQuery", anyContext, anyStartQueryInput).
			WaitUntil(timer.C).
			Return(&cloudwatchlogs.StartQueryOutput{QueryId: aws.String("eggs")}, nil).
			Maybe()
		actions.
			On("GetQueryResults", anyContext, mock.Anything).
			Return(&cloudwatchlogs.GetQueryResultsOutput{
				Status: types.QueryStatusComplete,
			}, nil).
			Maybe()
		trueValue := true
		actions.On("StopQuery", anyContext, mock.Anything).
			Return(&cloudwatchlogs.StopQueryOutput{
				Success: trueValue,
			}, nil).
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
