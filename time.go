package incite

// TimeLayout is a Go time layout which documents the format of the time
// values returned in the timestamp fields of CloudWatch Logs Insights
// queries.
//
// TimeLayout defines the format by showing how the Go reference time of
//
// 	Mon Jan 2 15:04:05 -0700 MST 2006
//
// would be formatted if it were the value. TimeLayout can be used with
// time.Parse to parse timestamp fields, such as @timestamp and @ingestionTime,
// which are returned in CloudWatch Logs Insights query results.
const TimeLayout = "2006-01-02 15:04:05.000"
