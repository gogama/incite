// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

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
// type, so you may use either of these AWS SDK types to provide the
// CloudWatch Logs act capabilities.
//
// For example:
// 	import (
// 		"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
// 		"github.com/aws/aws-sdk-go/aws/session"
// 	)
//
// 	var myActions incite.CloudWatchLogsActions = cloudwatchlogs.New(session.Must(session.NewSession()))
//
// 	// Now you can use myActions with NewQueryManager to construct a new
// 	// QueryManager.
type CloudWatchLogsActions interface {
	StartQueryWithContext(context.Context, *cloudwatchlogs.StartQueryInput, ...request.Option) (*cloudwatchlogs.StartQueryOutput, error)
	StopQueryWithContext(context.Context, *cloudwatchlogs.StopQueryInput, ...request.Option) (*cloudwatchlogs.StopQueryOutput, error)
	GetQueryResultsWithContext(context.Context, *cloudwatchlogs.GetQueryResultsInput, ...request.Option) (*cloudwatchlogs.GetQueryResultsOutput, error)
}

// CloudWatchLogsAction represents a single enumerated CloudWatch Logs
// act.
type CloudWatchLogsAction int

const (
	// StartQuery indicates the CloudWatch Logs StartQuery act.
	StartQuery CloudWatchLogsAction = iota
	// StopQuery indicates the CloudWatch Logs StopQuery act.
	StopQuery
	// GetQueryResults indicates the CloudWatchLogs GetQueryResults act.
	GetQueryResults
	// numActions is the number of CloudWatch Logs actions supported by
	// CloudWatchLogsActions and required to construct a QueryManager
	// using NewQueryManager.
	numActions
)

// RPSQuotaLimits contains the CloudWatch Logs service quota limits for
// number of requests per second for each CloudWatch Logs API act
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

// RPSDefaults specifies the default maximum number of requests per
// second which the QueryManager will make to the CloudWatch Logs web
// service for each CloudWatch Logs act. These default values are
// operative unless explicitly overwritten in the Config structure
// passed to NewQueryManager.
var RPSDefaults = map[CloudWatchLogsAction]int{
	StartQuery:      RPSQuotaLimits[StartQuery] - 2,
	StopQuery:       RPSQuotaLimits[StopQuery] - 2,
	GetQueryResults: RPSQuotaLimits[GetQueryResults] - 2,
}
