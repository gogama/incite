// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

func TestQuery(t *testing.T) {
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
	r, err := Query(actions, QuerySpec{
		Text:   "x",
		Groups: []string{"y"},
		Start:  defaultStart,
		End:    defaultEnd,
	})

	// ASSERT.
	assert.Equal(t, []Result{}, r)
	assert.NoError(t, err)
	actions.AssertExpectations(t)
}
