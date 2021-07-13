package incite

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

// CloudWatchLogsCaps provides the CloudWatch Logs capabilities QueryManager
// needs in order to run queries against CloudWatch Logs Insights.
//
// This interface is compatible with the AWS SDK for Go (v1)'s
// cloudwatchlogsiface.CloudWatchLogsAPI interface and *cloudwatchlogs.CloudWatchLogs
// type, so you may use either of these AWS SDK for Go types to provide
// the CloudWatch Logs capabilities.
type CloudWatchLogsCaps interface {
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
