// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAWSSDKActions(t *testing.T) {
	// The purpose of this test case is to verify and demonstrate the
	// behavior of the CloudWatch Logs client in the AWS SDK for Go v2.
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
			name: "StartQuery",
			act: func(actions CloudWatchLogsActions, ctx context.Context) error {
				_, err := actions.StartQuery(ctx, &cloudwatchlogs.StartQueryInput{
					QueryString:   aws.String("foo"),
					StartTime:     aws.Int64(0),
					EndTime:       aws.Int64(1),
					LogGroupNames: []string{"foo"},
				})
				return err
			},
		},
		{
			name: "StopQuery",
			act: func(actions CloudWatchLogsActions, ctx context.Context) error {
				_, err := actions.StopQuery(ctx, &cloudwatchlogs.StopQueryInput{
					QueryId: aws.String("bar"),
				})
				return err
			},
		},
		{
			name: "GetQueryResults",
			act: func(actions CloudWatchLogsActions, ctx context.Context) error {
				_, err := actions.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{
					QueryId: aws.String("baz"),
				})
				return err
			},
		},
	}

	for _, contextMaker := range contextMakers {
		for _, actor := range actors {
			t.Run(fmt.Sprintf("%s.%s", contextMaker.name, actor.name), func(t *testing.T) {
				cfg, err := config.LoadDefaultConfig(context.TODO(),
					config.WithCredentialsProvider(aws.AnonymousCredentials{}),
					config.WithRegion("us-east-1"),
				)
				require.NoError(t, err)
				actions := cloudwatchlogs.NewFromConfig(cfg)
				ctx := contextMaker.ctx()
				<-ctx.Done()

				err = actor.act(actions, ctx)

				var opErr *smithy.OperationError
				require.True(t, errors.As(err, &opErr), "expected action to return a smithy.OperationError, but it did not")
				assert.ErrorIs(t, opErr.Unwrap(), contextMaker.expected)
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

func (m *mockActions) StartQuery(ctx context.Context, input *cloudwatchlogs.StartQueryInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	args := m.Called(ctx, input)
	if output, ok := args.Get(0).(*cloudwatchlogs.StartQueryOutput); ok {
		return output, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockActions) StopQuery(ctx context.Context, input *cloudwatchlogs.StopQueryInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StopQueryOutput, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	args := m.Called(ctx, input)
	if output, ok := args.Get(0).(*cloudwatchlogs.StopQueryOutput); ok {
		return output, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockActions) GetQueryResults(ctx context.Context, input *cloudwatchlogs.GetQueryResultsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	args := m.Called(ctx, input)
	if output, ok := args.Get(0).(*cloudwatchlogs.GetQueryResultsOutput); ok {
		return output, args.Error(1)
	}
	return nil, args.Error(1)
}
