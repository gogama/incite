// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"io"
	"time"
)

// QuerySpec specifies the parameters for a query operation either
// using the global Query function or a QueryManager's Query method.
type QuerySpec struct {
	// Text contains the actual text of the CloudWatch Insights query.
	//
	// Text must not contain an empty or blank string. Beyond checking
	// for blank text, Incite does not attempt to parse Text and simply
	// forwards it to the CloudWatch Logs service. Care must be taken to
	// specify query text compatible with the Chunk, Preview, and Split
	// fields, or the results may be confusing.
	//
	// To limit the number of results returned by the query, use the
	// Limit field, since the Insights API seems to ignore the `limit`
	// command when specified in the query text. Note that if the
	// QuerySpec specifies a chunked query, then Limit will apply to the
	// results obtained from each chunk, not to the global query.
	//
	// To learn the Insights query syntax, please see the official
	// documentation at:
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html.
	Text string

	// Groups lists the names of the CloudWatch Logs log groups to be
	// queried. It may not be empty.
	Groups []string

	// Start specifies the beginning of the time range to query,
	// inclusive of Start itself.
	//
	// Start must be strictly before End, and must represent a whole
	// number of milliseconds (it cannot have sub-millisecond
	// granularity).
	Start time.Time

	// End specifies the end of the time range to query, exclusive of
	// End itself.
	//
	// End must be strictly after Start, and must represent a whole
	// number of milliseconds (it cannot have sub-millisecond
	// granularity).
	End time.Time

	// Limit optionally specifies the maximum number of results to be
	// returned by the query.
	//
	// If Limit is zero or negative, the value DefaultLimit is used
	// instead. If Limit exceeds MaxLimit, the query operation will fail
	// with an error.
	//
	// In a chunked query, Limit applies to each chunk separately, so
	// up to (n × Limit) final results may be returned, where n is the
	// number of chunks.
	//
	// Note that as of 2021-07-15, the CloudWatch Logs StartQuery API
	// seems to ignore the `limit` command in the query text, so if you
	// want to apply a limit you must use the Limit field.
	Limit int64

	// Chunk optionally requests a chunked query and indicates the chunk
	// size.
	//
	// In a chunked query, the query time range is subdivided into
	// smaller time chunks of duration Chunk, and each chunk is sent to
	// the CloudWatch Logs service as a separate Insights query. This
	// can help large queries avoid the CloudWatch Logs Insights
	// result size limit, MaxLimit; help queries complete before the
	// CloudWatch Logs Insights query timeout of 15 minutes; and can
	// increase performance, because multiple chunks can be run in
	// parallel.
	//
	// If Chunk is zero, negative, or greater than the difference
	// between End and Start, the query is not chunked. If Chunk is
	// positive and less than the difference between End and Start, the
	// query is broken into n or n+1 chunks, where n is
	// (End-Start)/Chunk, rounded up to the nearest integer value. If
	// Chunk is positive, it must represent a whole number of
	// milliseconds (cannot have sub-millisecond granularity).
	//
	// For chunked queries, the following special considerations apply:
	//
	// • In a chunked query, Limit applies separately to each chunk. So
	// a query with 50 chunks and a limit of 1,000 could produce up to
	// 50,000 final results.
	//
	// • If Text contains a sort command, the sort will only apply
	// within each individual chunk. If the QuerySpec is executed by a
	// QueryManager configured with a parallelism factor above 1, then
	// the results may appear to be out of order since the order of
	// chunk completion is not guaranteed.
	//
	// • If Text contains a stats command, the statistical aggregation
	// will be applied separately to each chunk in a chunked query,
	// meaning up to n+1 versions of each aggregate data point may be
	// returned, one per chunk, potentially necessitating further
	// aggregation in your application logic.
	//
	// • In general if you use chunking with query text which implies
	// any kind of server-side post-processing, of which sorting and
	// statistical aggregation are two examples, you may need to perform
	// custom post-processing within your application to put the results
	// into the final form you expect.
	Chunk time.Duration

	// Preview optionally requests preview results from a running query.
	//
	// If Preview is true, intermediate results for the query are
	// sent to the result Stream as soon as they are available. This
	// can improve your application's responsiveness for the end user
	// but requires care since not all intermediate results are valid
	// members of the final result set.
	//
	// When Preview is true, the query result Stream may produce some
	// intermediate results which it later determines are invalid
	// because they shouldn't be final members of the result set. For
	// each such invalid result, an extra dummy Result will be sent to
	// the result Stream with the following structure:
	//
	// 	incite.Result{
	// 		{ Field: "@ptr", Value: "<Unique @ptr of the earlier invalid result>" },
	//		{ Field: "@deleted", Value: "true" },
	// 	}
	//
	// The presence of the "@deleted" field can be used to identify and
	// delete the earlier invalid result sharing the same "@ptr" field.
	//
	// If the results from CloudWatch Logs do not contain an @ptr field,
	// the Preview option does not detect invalidated results and
	// consequently does not create dummy @deleted items. Since the
	// `stats` command creates results that do not contain @ptr,
	// applications should either avoid combining Preview mode with
	// `stats` or apply their own custom logic to eliminate obsolete
	// intermediate results.
	Preview bool

	// Priority optionally allows a query operation to be given a higher
	// or lower priority with regard to other query operations managed
	// by the same QueryManager. This allows your application to ensure
	// its higher priority work runs before lower priority work,
	// making the most efficient work of finite CloudWatch Logs service
	// resources.
	//
	// A lower number indicates a higher priority. The default zero
	// value is appropriate for many cases.
	//
	// The Priority field may be set to any valid int value. A query
	// whose Priority number is lower is allocated CloudWatch Logs
	// query capacity in preference to a query whose Priority number is
	// higher, but only within the same QueryManager.
	Priority int

	// SplitUntil specifies if, and how, the query time range, or the
	// query chunks, will be dynamically split into sub-chunks when
	// they produce the maximum number of results that CloudWatch Logs
	// Insights will return (MaxLimit).
	//
	// If SplitUntil is zero or negative, then splitting is disabled.
	// If positive, then splitting is enabled and SplitUntil must
	// represent a whole number of milliseconds (cannot have
	// sub-millisecond granularity).
	//
	// To use splitting, you must also set Limit to MaxLimit and
	// Preview must be false.
	//
	// When splitting is enabled and, when a time range produces
	// MaxLimit results, the range is split into sub-chunks no smaller
	// than SplitUntil. If a sub-chunk produces MaxLimit results, it is
	// recursively split into smaller sub-sub-chunks again no smaller
	// than SplitUntil. The splitting process continues until either
	// the time range cannot be split into at least two chunks no
	// smaller than SplitUntil or the time range produces fewer than
	// MaxLimit results.
	SplitUntil time.Duration
}

// Stats records metadata about query execution. When returned from a
// Stream, Stats contains metadata about the stream's query. When
// returned from a QueryManager, Stats contains accumulated metadata
// about all queries executed by the query manager.
//
// The Stats structure contains two types of metadata.
//
// The first type of metadata in Stats are returned from the CloudWatch
// Logs Insights web service and consist of metrics about the amount of
// data scanned by the query or queries. These metadata are contained
// within the fields BytesScanned, RecordsMatched, and RecordsScanned.
//
// The second type of metadata in Stats are collected by Incite and
// consist of metrics about the size of the time range or ranges queried
// and how much progress has been on the queries. These metadata can be
// useful, for example, for showing progress bars or other work in
// progress indicators. They are contained within the fields
// RangeRequested, RangeStarted, RangeDone, RangeFailed, and
// RangeMaxed.
type Stats struct {
	// BytesScanned is a metric returned by CloudWatch Logs Insights
	// which represents the total number of bytes of log events
	// scanned.
	BytesScanned float64
	// RecordsMatched is a metric returned by CloudWatch Logs Insights
	// which tallies the number of log events that matched the query or
	// queries.
	RecordsMatched float64
	// RecordsScanned is a metric returned by CloudWatch Logs Insights
	// which tallies the number of log events scanned during the query
	// or queries.
	RecordsScanned float64

	// RangeRequested is a metric collected by Incite which tallies the
	// accumulated time range requested in the query or queries.
	RangeRequested time.Duration
	// RangeStarted is a metric collected by Incite which tallies the
	// aggregate amount of query time for which the query has been
	// initiated in the CloudWatch Logs Insights web service. The value
	// in this field is always less than or equal to RangeRequested, and
	// it never decreases.
	//
	// For a non-chunked query, this field is either zero or the total
	// time range covered by the QuerySpec. For a chunked query, this
	// field reflects the accumulated time of all the chunks which have
	// been started. For a QueryManager, this field represents the
	// accumulated time of all started query chunks from all queries
	// submitted to the query manager.
	RangeStarted time.Duration
	// RangeDone is a metric collected by Incite which tallies the
	// aggregate amount of query time which the CloudWatch Logs Insights
	// service has successfully finished querying so far. The value in
	// this field is always less than or equal to RangeStarted, and it
	// never decreases.
	RangeDone time.Duration
	// RangeFailed is a metric collected by Incite which tallies the
	// aggregate amount of query time which was started in the
	// CloudWatch Logs Insights service but which ended with an error,
	// either because the Stream or QueryManager was closed, or because
	// the CloudWatch Logs service returned a non-retryable error.
	RangeFailed time.Duration
	// RangeMaxed is a metric collected by Incite which tallies the
	// aggregate amount of query time which the CloudWatch Logs Insights
	// service has successfully finished querying so far but for which
	// Insights returned the maximum number of results requested in the
	// QuerySpec Limit field, indicating that more results may be
	// available than were returned. The value in this field is always
	// less than or equal to RangeDone, and it never decreases.
	//
	// If RangeMaxed is zero, this indicates that no query chunks
	// produced the maximum number of results requested.
	//
	// RangeMaxed will usually not be increased by queries that use
	// dynamic chunk splitting. This is because chunk splitting will
	// continue recursively splitting until sub-chunks do not produce
	// the maximum number of results. However, when a chunk which
	// produces MaxLimit results is too small to split further, its
	// duration will be added to RangeMaxed.
	RangeMaxed time.Duration
}

func (s *Stats) add(t *Stats) {
	s.BytesScanned += t.BytesScanned
	s.RecordsMatched += t.RecordsMatched
	s.RecordsScanned += t.RecordsScanned

	s.RangeRequested += t.RangeRequested
	s.RangeStarted += t.RangeStarted
	s.RangeDone += t.RangeDone
	s.RangeFailed += t.RangeFailed
	s.RangeMaxed += t.RangeMaxed
}

// StatsGetter provides access to the Insights query statistics
// returned by the CloudWatch Logs API.
//
// Both Stream and QueryManager contain the StatsGetter interface. Call
// the GetStats method on a Stream to get the query statistics for the
// stream's query. Call the GetStats method on a QueryManager to get the
// query statistics for all queries run within the QueryManager.
type StatsGetter interface {
	GetStats() Stats
}

// QueryManager executes one or more CloudWatch Logs Insights queries,
// optionally executing simultaneous queries in parallel.
//
// QueryManager's job is to hide the complexity of the CloudWatch Logs
// Insights API, taking care of mundane details such as starting and
// polling Insights query jobs in the CloudWatch Logs service, breaking
// queries into smaller time chunks (if desired), de-duplicating and
// providing preview results (if desired), retrying transient request
// failures, and managing resources to try to stay within the
// CloudWatch Logs service quota limits.
//
// Use NewQueryManager to create a QueryManager, and be sure to close it
// when you no longer need its services, since every QueryManager
// consumes a tiny amount of system resources just by existing.
//
// Calling the Query method will return a result Stream from which the
// query results can be read as they become available. Use the
// Unmarshal function to unmarshal the bare results into other
// structured types.
//
// Calling the Close method will immediately cancel all running queries
// started with the QueryManager, as if each query's Stream had been
// explicitly closed.
//
// Calling the GetStats method will return the running sum of all
// statistics for all queries run within the QueryManager since it was
// created.
type QueryManager interface {
	io.Closer
	StatsGetter
	Query(QuerySpec) (Stream, error)
}

// Result represents a single result row from a CloudWatch Logs Insights
// query.
type Result []ResultField

// ResultField represents a single field name/field value pair within a
// Result.
type ResultField struct {
	Field string
	Value string
}

// Stream provides access to the result stream from a query operation
// either using a QueryManager or the global Query function.
//
// Use the Close method if you need to prematurely cancel the query
// operation, releasing the local (in-process) and remote (in the
// CloudWatch Logs service) resources it consumes.
//
// Use the Read method to read query results from the stream. The Read
// method returns io.EOF when the entire results stream has been
// consumed. At this point the query is over and all local and remote
// resources have been released, so it is not necessary to close the
// Stream explicitly.
//
// Use the GetStats method to obtain the Insights statistics pertaining
// to the query. Note that the results from the GetStats method may
// change over time as new results are pulled from the CloudWatch Logs
// web service, but will stop changing after the Read method returns
// io.EOF or any other error. If the query was chunked, the stats will
// be summed across multiple chunks.
type Stream interface {
	io.Closer
	StatsGetter

	// Read reads up to len(p) CloudWatch Logs Insights results into p.
	// It returns the number of results read (0 <= n <= len(p)) and any
	// error encountered. Even if Read returns n < len(p), it may use
	// all of p as scratch space during the call. If some, but fewer
	// than len(p), results are available, Read conventionally
	// returns what is available instead of waiting for more.
	//
	// When Read encounters an error or end-of-file condition after
	// successfully reading n > 0 results, it returns the number of
	// results read. It may return the (non-nil) error from the same
	// call or return the error (and n == 0) from a subsequent call. An
	// instance of this general case is that a Stream returning a
	// non-zero number of results at the end of the input stream may
	// return either err == EOF or err == nil. The next Read should
	// return 0, EOF.
	//
	// Callers should always process the n > 0 results returned before
	// considering the error err. Doing so correctly handles I/O errors
	// that happen after reading some results and also both of the
	// allowed EOF behaviors.
	//
	// Implementations of Read are discouraged from returning a zero
	// result count with a nil error, except when len(p) == 0. Callers
	// should treat a return of 0 and nil as indicating that nothing
	// happened; in particular it does not indicate EOF.
	//
	// Implementations must not retain p.
	//
	// As a convenience, the ReadAll function may be used to read all
	// remaining results available in a Stream.
	//
	// If the query underlying the Stream failed permanently, then err
	// may be one of:
	//
	// • StartQueryError
	// • TerminalQueryStatusError
	// • UnexpectedQueryError
	Read(p []Result) (n int, err error)
}

const (
	// QueryConcurrencyQuotaLimit contains the default CloudWatch Logs
	// query concurrency service limit as documented at
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html.
	//
	// The documented service quota may increase over time, in which
	// case this value should be updated to match the documentation.
	QueryConcurrencyQuotaLimit = 20

	// DefaultParallel is the default maximum number of parallel
	// CloudWatch Logs Insights queries a QueryManager will attempt to
	// run at any one time.
	//
	// The default value is set to slightly less than the service quota
	// limit to leave some concurrency available for other users even if
	// the QueryManager is at maximum capacity.
	DefaultParallel = QueryConcurrencyQuotaLimit - 2

	// DefaultLimit is the default result count limit used if the Limit
	// field of a QuerySpec is zero or negative.
	DefaultLimit = 1000

	// MaxLimit is the maximum value the result count limit field in a
	// QuerySpec may be set to.
	MaxLimit = 10000
)

// Config provides the NewQueryManager function with the information it
// needs to construct a new QueryManager.
type Config struct {
	// Actions provides the CloudWatch Logs capabilities the QueryManager
	// needs to execute Insights queries against the CloudWatch Logs
	// service. If this value is nil then NewQueryManager panics.
	//
	// Normally Actions should be set to the value of an AWS SDK for Go
	// (v1) CloudWatch Logs client: both the cloudwatchlogsiface.CloudWatchLogsAPI
	// interface and the *cloudwatchlogs.CloudWatchLogs type are
	// compatible with the CloudWatchLogsActions interface. Use a
	// properly configured instance of one of these types to set the
	// value of the Actions field.
	Actions CloudWatchLogsActions

	// Parallel optionally specifies the maximum number of parallel
	// CloudWatch Logs Insights queries which the QueryManager may run
	// at one time. The purpose of Parallel is to avoid starving other
	// humans or systems using CloudWatch Logs Insights in the same AWS
	// account and region.
	//
	// If set to a positive number then that exact number is used as the
	// parallelism factor. If set to zero or a negative number then
	// DefaultParallel is used instead.
	//
	// Parallel gives the upper limit on the number of Insights queries
	// the QueryManager may have open at any one time. The actual number
	// of Insights queries may be lower either because of throttling or
	// service limit exceptions from the CloudWatch Logs web service, or
	// because the QueryManager simply doesn't need all the parallel
	// capacity.
	//
	// Do not set Parallel above QueryConcurrencyQuotaLimit unless you
	// have received a query concurrency limit increase from AWS
	// CloudWatch Logs Insights.
	Parallel int

	// RPS optionally specifies the maximum number of requests to the
	// CloudWatch Logs web service which the QueryManager may make in
	// each one-second period for each CloudWatch Logs act. The
	// purpose of RPS is to prevent the QueryManager or other humans or
	// systems using CloudWatch Logs in the same AWS account and region
	// from being throttled by the web service.
	//
	// If RPS has a missing, zero, or negative number for any required
	// CloudWatch Logs act, the value specified in RPSDefaults is
	// used instead. The default behavior should be adequate for many
	// use cases, so you typically will not need to set this field
	// explicitly.
	//
	// The values in RPS should ideally not exceed the corresponding
	// values in RPSQuotaLimits as this will almost certainly result in
	// throttling, worse performance, and your application being a "bad
	// citizen" affecting other users of the same AWS account.
	RPS map[CloudWatchLogsAction]int

	// Logger optionally specifies a logging object to which the
	// QueryManager can send log messages about queries it is managing.
	// This value may be left nil to skip logging altogether.
	Logger Logger

	// Name optionally gives the new QueryManager a friendly name, which
	// will be included in log messages the QueryManager emits to the
	// Logger.
	//
	// Other than being used in logging, this field has no effect on the
	// QueryManager's behavior.
	Name string

	// DisableAdaptation, if true, turns off the QueryManager's adaptive
	// capacity utilization behavior. Most users will want to leave this
	// flag at the zero value, i.e. false.
	//
	// Adaptive capacity utilization makes the QueryManager more
	// resilient to temporary capacity problems reported by the
	// CloudWatch Logs Insights service. The QueryManager achieves this
	// resilience by temporarily reducing its usage of CloudWatch Logs
	// Insights resources in response to detected limit issues. For
	// example, it may temporarily reduce the number of parallel queries
	// in flight below Parallel, or it may temporarily reduce its
	// service requests per second below RPS. Any temporary reductions
	// are phased out when the QueryManager stops getting resource limit
	// errors from the Insights service.
	DisableAdaptation bool
}
