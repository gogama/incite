// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAWSSDKActions(t *testing.T) {
	// The purpose of this test case is to verify and demonstrate the
	// behavior of the CloudWatch Logs client in the AWS SDK for Go v1.
	// In particular, we are asserting that if you pass an "already
	// dead" context to any of the client methods that make up the
	// CloudWatch Logs Insights API, the client will immediately fail
	// with an error that wraps the dead context error. The worker loop
	// relies on this behavior since the chunk context might already be
	// dead at the time the worker invokes the CloudWatch Logs API
	// action.
	contextMakers := []struct {
		name     string
		ctx      func() context.Context
		expected error
	}{
		{
			name: "Canceled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			expected: context.Canceled,
		},
		{
			name: "DeadlineExceeded",
			ctx: func() context.Context {
				ctx, _ := context.WithTimeout(context.Background(), 1)
				return ctx
			},
			expected: context.DeadlineExceeded,
		},
	}
	actors := []struct {
		name string
		act  func(actions CloudWatchLogsActions, ctx context.Context) error
	}{
		{
			name: "StartQueryWithContext",
			act: func(actions CloudWatchLogsActions, ctx context.Context) error {
				_, err := actions.StartQueryWithContext(ctx, &cloudwatchlogs.StartQueryInput{
					QueryString:   sp("foo"),
					StartTime:     int64p(0),
					EndTime:       int64p(1),
					LogGroupNames: []*string{sp("foo")},
				})
				return err
			},
		},
		{
			name: "StopQueryWithContext",
			act: func(actions CloudWatchLogsActions, ctx context.Context) error {
				_, err := actions.StopQueryWithContext(ctx, &cloudwatchlogs.StopQueryInput{
					QueryId: sp("bar"),
				})
				return err
			},
		},
		{
			name: "GetQueryResultsWithContext",
			act: func(actions CloudWatchLogsActions, ctx context.Context) error {
				_, err := actions.GetQueryResultsWithContext(ctx, &cloudwatchlogs.GetQueryResultsInput{
					QueryId: sp("baz"),
				})
				return err
			},
		},
	}

	for _, contextMaker := range contextMakers {
		for _, actor := range actors {
			t.Run(fmt.Sprintf("%s.%s", contextMaker.name, actor.name), func(t *testing.T) {
				s := session.Must(session.NewSession(&aws.Config{
					Credentials: credentials.AnonymousCredentials,
					Region:      sp("us-east-1"),
				}))
				actions := cloudwatchlogs.New(s)
				ctx := contextMaker.ctx()
				<-ctx.Done()

				err := actor.act(actions, ctx)

				x, ok := err.(awserr.Error)
				require.True(t, ok, "expected action %d to return an awserr.Error, but it did not")
				assert.ErrorIs(t, x.OrigErr(), contextMaker.expected)
			})
		}
	}
}

type mockActions struct {
	mock.Mock
	lock sync.RWMutex
}

func newMockActions(t *testing.T) *mockActions {
	m := &mockActions{}
	m.Test(t)
	return m
}

func (m *mockActions) StartQueryWithContext(ctx context.Context, input *cloudwatchlogs.StartQueryInput, _ ...request.Option) (*cloudwatchlogs.StartQueryOutput, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	args := m.Called(ctx, input)
	if output, ok := args.Get(0).(*cloudwatchlogs.StartQueryOutput); ok {
		return output, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockActions) StopQueryWithContext(ctx context.Context, input *cloudwatchlogs.StopQueryInput, _ ...request.Option) (*cloudwatchlogs.StopQueryOutput, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	args := m.Called(ctx, input)
	if output, ok := args.Get(0).(*cloudwatchlogs.StopQueryOutput); ok {
		return output, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockActions) GetQueryResultsWithContext(ctx context.Context, input *cloudwatchlogs.GetQueryResultsInput, _ ...request.Option) (*cloudwatchlogs.GetQueryResultsOutput, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	args := m.Called(ctx, input)
	if output, ok := args.Get(0).(*cloudwatchlogs.GetQueryResultsOutput); ok {
		return output, args.Error(1)
	}
	return nil, args.Error(1)
}
