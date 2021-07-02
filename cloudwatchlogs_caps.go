package incite

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

type CloudWatchLogsCaps interface {
	StartQueryWithContext(context.Context, *cloudwatchlogs.StartQueryInput, ...request.Option) (*cloudwatchlogs.StartQueryOutput, error)
	StopQueryWithContext(context.Context, *cloudwatchlogs.StopQueryInput, ...request.Option) (*cloudwatchlogs.StopQueryOutput, error)
	GetQueryResultsWithContext(context.Context, *cloudwatchlogs.GetQueryResultsInput, ...request.Option) (*cloudwatchlogs.GetQueryResultsOutput, error)
}
