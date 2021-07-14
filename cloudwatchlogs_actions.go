package incite

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

// CloudWatchLogsActions provides access to the CloudWatch Logs actions
// which QueryManager needs in order to run Insights queries using the
// CloudWatch Logs service.
//
// This interface is compatible with the AWS SDK for Go (v1)'s
// cloudwatchlogsiface.CloudWatchLogsAPI interface and *cloudwatchlogs.CloudWatchLogs
// type, so you may use either of these AWS SDK for Go types to provide
// the CloudWatch Logs capabilities.
type CloudWatchLogsActions interface {
	CloudWatchLogsQueryStarter
	CloudWatchLogsQueryStopper
	CloudWatchLogsQueryResultsGetter
}

type CloudWatchLogsQueryStarter interface {
	StartQueryWithContext(context.Context, *cloudwatchlogs.StartQueryInput, ...request.Option) (*cloudwatchlogs.StartQueryOutput, error)
}

type CloudWatchLogsQueryStopper interface {
	StopQueryWithContext(context.Context, *cloudwatchlogs.StopQueryInput, ...request.Option) (*cloudwatchlogs.StopQueryOutput, error)
}

type CloudWatchLogsQueryResultsGetter interface {
	GetQueryResultsWithContext(context.Context, *cloudwatchlogs.GetQueryResultsInput, ...request.Option) (*cloudwatchlogs.GetQueryResultsOutput, error)
}

// CloudWatchLogsAction represents a single enumerated CloudWatch Logs
// action.
type CloudWatchLogsAction int

const (
	// StartQuery indicates the CloudWatch Logs StartQuery action.
	StartQuery CloudWatchLogsAction = iota
	// StopQuery indicates the CloudWatch Logs StopQuery action.
	StopQuery
	// GetQueryResults indicates the CloudWatchLogs GetQueryResults action.
	GetQueryResults
	// numActions is the number of CloudWatch Logs actions we need.
	numActions
)

// RPSQuotaLimits contains the CloudWatch Logs service quota limits for
// number of requests per second for each CloudWatch Logs API action
// before the request fails due to a throttling error as documented in
// the AWS service limits system.
//
// The documented service quotas may increase over time, in which case
// the map values should be updated to match the increases.
var RPSQuotaLimits = map[CloudWatchLogsAction]int{
	StartQuery:      5,
	StopQuery:       5,
	GetQueryResults: 5,
}

// DefaultRPS specifies the maximum number of requests per second which
// the QueryManager may make to the CloudWatch Logs web service for each
// CloudWatch Logs action.
var DefaultRPS = map[CloudWatchLogsAction]int{
	StartQuery:      RPSQuotaLimits[StartQuery] - 2,
	StopQuery:       RPSQuotaLimits[StopQuery] - 2,
	GetQueryResults: RPSQuotaLimits[GetQueryResults] - 2,
}
