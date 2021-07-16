package incite

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"

	"github.com/stretchr/testify/mock"
)

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
