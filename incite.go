// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"container/heap"
	"container/ring"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
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
	// number of seconds (it cannot have sub-second granularity).
	Start time.Time

	// End specifies the end of the time range to query, exclusive of
	// End itself.
	//
	// End must be strictly after Start, and must represent a whole
	// number of seconds (it cannot have sub-second granularity).
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
	// If Chunk is zero, negative, or greater than the difference
	// between End and Start, the query is not chunked. If Chunk is
	// positive and less than the difference between End and Start, the
	// query is broken into n chunks, where n is (End-Start)/Chunk,
	// rounded up to the nearest integer value. If Chunk is positive, it
	// must represent a whole number of seconds (cannot have sub-second
	// granularity).
	//
	// In a chunked query, each chunk is sent to the CloudWatch Logs
	// service as a separate Insights query. This can help large queries
	// complete before the CloudWatch Logs query timeout of 15 minutes,
	// and can increase performance because chunks can be run in parallel.
	// However, the following considerations should be taken into account:
	//
	// • In a chunked query, Limit applies separately to each chunk. So
	// a query with 50 chunks and a limit of 50 could produce up to 2500
	// final results.
	//
	// • If Text contains a sort command, the sort will only apply
	// within each individual chunk. If the QuerySpec is executed by a
	// QueryManager configured with a parallelism factor above 1, then
	// the results may appear be out of order since the order of
	// completion of chunks is not guaranteed.
	//
	// • If Text contains a stats command, the statistical aggregation
	// will be applied to each chunk in a chunked query, meaning up to n
	// versions of each aggregate data point may be returned, one per
	// chunk, necessitating further aggregation in your application
	// logic.
	//
	// • In general if you use chunking with query text which implies
	// any kind of server-side aggregation, you may need to perform
	// custom post-processing on the results.
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
	// represent a whole number of seconds (cannot have sub-second
	// granularity).
	//
	// When splitting is enabled and, when a time range produces
	// MaxLimit results, the range is split into sub-chunks no larger
	// than SplitUntil. If a sub-chunk produces MaxLimit results, it is
	// recursively split into smaller sub-sub-chunks again no larger
	// than SplitUntil. The splitting process continues until either
	// the time range cannot be split into at least two chunks no
	// smaller than SplitUntil or the time range produces fewer than
	// MaxLimit results.
	//
	// To use splitting, you must also set Limit to MaxLimit and
	// Preview must be false.
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
	// Insights returned MaxLimit results, indicating that more results
	// may be available than were returned. The value in this field is
	// always less than or equal to RangeDone, and it never decreases.
	//
	// If RangeMaxed is zero, no query chunks produced MaxLimit results.
	//
	// Queries with dynamic chunk splitting enabled will only increase
	// this field when, after splitting a chunk into the smallest
	// allowable sub-chunks, a sub-chunk still produced MaxLimit
	// results.
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

type mgr struct {
	Config

	// Fields only read/written on construction and in mgr loop goroutine.
	timer     *time.Timer
	ding      bool
	minDelay  map[CloudWatchLogsAction]time.Duration // Used to stay under TPS limit
	lastReq   map[CloudWatchLogsAction]time.Time     // Used to stay under TPS limit
	pq        streamHeap                             // Written by Query, written by mgr loop goroutine
	ready     ring.Ring                              // Circular list of chunks, first item is a sentry
	chunks    ring.Ring                              // Circular list of chunks, first item is a sentry
	numReady  int
	numChunks int
	close     chan struct{} // Receives notification on Close()
	query     chan *stream  // Receives notification of new Query()

	// Fields written by mgr loop and potentially read by any goroutine.
	stats     Stats        // Read by GetStats, written by mgr loop goroutine
	statsLock sync.RWMutex // Controls access to stats
}

const (
	// QueryConcurrencyQuotaLimit contains the CloudWatch Logs Query
	// Concurrency service quota limit as documented at
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html.
	//
	// The documented service quota may increase over time, in which case
	// this value should be updated to match the documentation.
	QueryConcurrencyQuotaLimit = 10

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
	// Note that an Insights query is not necessarily one-to-one with a
	// Query operation on a QueryManager. If the Query operation is
	// chunked, the QueryManager may create Insights multiple queries in
	// the CloudWatch Logs web service to fulfil the chunked Query
	// operation.
	Parallel int

	// RPS optionally specifies the maximum number of requests to the
	// CloudWatch Logs web service which the QueryManager may make in
	// each one-second period for each CloudWatch Logs action. The
	// purpose of RPS is to prevent the QueryManager or other humans or
	// systems using CloudWatch Logs in the same AWS account and region
	// from being throttled by the web service.
	//
	// If RPS has a missing, zero, or negative number for any required
	// CloudWatch Logs action, the value specified in RPSDefaults is
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
}

// NewQueryManager returns a new query manager with the given
// configuration.
func NewQueryManager(cfg Config) QueryManager {
	if cfg.Actions == nil {
		panic(nilActionsMsg)
	}
	if cfg.Parallel <= 0 {
		cfg.Parallel = DefaultParallel
	} else if cfg.Parallel > QueryConcurrencyQuotaLimit {
		cfg.Parallel = QueryConcurrencyQuotaLimit
	}
	minDelay := make(map[CloudWatchLogsAction]time.Duration, numActions)
	for action, defaultRPS := range RPSDefaults {
		rps := cfg.RPS[action]
		if rps <= 0 {
			rps = defaultRPS
		}
		minDelay[action] = time.Second / time.Duration(rps)
	}
	if cfg.Logger == nil {
		cfg.Logger = NopLogger
	}

	m := &mgr{
		Config: cfg,

		timer:    time.NewTimer(1<<63 - 1),
		minDelay: minDelay,
		lastReq:  make(map[CloudWatchLogsAction]time.Time, numActions),

		close: make(chan struct{}),
		query: make(chan *stream),
	}

	if m.Name == "" {
		m.Name = fmt.Sprintf("%p", m)
	}

	go m.loop()

	return m
}

func (m *mgr) loop() {
	defer m.shutdown()

	m.Logger.Printf("incite: QueryManager(%s) started", m.Name)

	for {
		// Start as many next chunks as we have capacity for. We
		// prioritize starting chunks over polling chunks to maximize
		// server-side parallelism.
		x := m.startNextChunks()
		if x == -1 {
			return
		}

		// Poll the next chunk.
		y := m.pollNextChunk()
		if y == -1 {
			return
		}

		// If at least some work was done this iteration, immediately
		// look for more work.
		if x > 0 || y > 0 {
			continue
		}

		// Block until some work is available to do.
		z := m.waitForWork()
		if z == -1 {
			return
		}
	}
}

func (m *mgr) shutdown() {
	// Log start of shutdown process.
	m.Logger.Printf("incite: QueryManager(%s) stopping...", m.Name)

	// On a best effort basis, close all open chunks.
	m.chunks.Do(func(i interface{}) {
		if i != nil {
			m.cancelChunk(i.(*chunk), ErrClosed)
		}
	})

	// Close all open streams.
	for _, s := range m.pq {
		s.setErr(ErrClosed, true, Stats{})
	}

	// Free any other resources.
	m.timer.Stop()
	close(m.close)
	close(m.query)

	// Log a final stop event.
	m.Logger.Printf("incite: QueryManager(%s) stopped", m.Name)
}

func (m *mgr) setTimer(d time.Duration) bool {
	if !m.ding && !m.timer.Stop() {
		<-m.timer.C
	} else {
		m.ding = false
	}

	if d > 0 {
		m.timer.Reset(d)
		return true
	}

	m.timer.Reset(1<<63 - 1)
	return false
}

func (m *mgr) setTimerRPS(action CloudWatchLogsAction) bool {
	minDelay := m.minDelay[action]
	delaySoFar := time.Now().Sub(m.lastReq[action])
	delayRem := minDelay - delaySoFar
	if delayRem <= 0 {
		return false
	}
	return m.setTimer(delayRem)
}

func (m *mgr) addQuery(s *stream) {
	heap.Push(&m.pq, s)
	m.doStats(func(ms *Stats) {
		ms.RangeRequested += s.stats.RangeRequested
	})
}

func (m *mgr) startNextChunks() int {
	var numStarted int
	for len(m.pq)+m.numReady > 0 && m.chunks.Len() <= m.Parallel {
		err := m.startNextChunk()
		if err == errClosing {
			return -1
		} else if err == nil {
			numStarted++
		}
	}

	return numStarted
}

type chunkIDKeyType int

var chunkIDKey = chunkIDKeyType(0)

func (m *mgr) startNextChunk() error {
	// Get the next chunk from the ready list, leaving it there for now.
	r := m.getReadyChunk()
	if r == nil {
		return nil
	}
	c := r.Value.(*chunk)

	// Pause until we have available capacity to start the chunk.
	if m.setTimerRPS(StartQuery) {
		select {
		case <-m.close:
			return errClosing
		case s := <-m.query:
			m.addQuery(s)
			return errInterrupted
		case <-m.timer.C:
			m.ding = true
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}

	// Unlink the next chunk from the ready list.
	m.ready.Unlink(1)
	m.numReady--

	// Get the chunk time range in Insights' format.
	starts := c.start.Unix()
	ends := c.end.Add(-time.Second).Unix() // CWL uses inclusive time ranges at 1 second granularity, we use exclusive ranges.

	// Start the chunk.
	input := cloudwatchlogs.StartQueryInput{
		QueryString:   &c.stream.Text,
		StartTime:     &starts,
		EndTime:       &ends,
		LogGroupNames: c.stream.groups,
		Limit:         &c.stream.Limit,
	}
	output, err := m.Actions.StartQueryWithContext(c.ctx, &input)
	m.lastReq[StartQuery] = time.Now()
	if err != nil {
		if isTemporary(err) {
			m.ready.Prev().Link(r)
			m.numReady++
			m.logChunk(c, "temporary failure to start", err.Error())
		} else {
			err = &StartQueryError{c.stream.Text, c.start, c.end, err}
			s := Stats{
				RangeFailed: c.end.Sub(c.start),
			}
			if c.gen == 0 {
				s.RangeStarted = s.RangeFailed
			}
			c.stream.setErr(err, true, s)
			m.doStats(func(ms *Stats) {
				ms.add(&s)
			})
			m.logChunk(c, "permanent failure to start", "fatal error from CloudWatch Logs: "+err.Error())
		}
		return err
	}

	// Save the current query ID into the chunk.
	queryID := output.QueryId
	if queryID == nil {
		err = &StartQueryError{c.stream.Text, c.start, c.end, errors.New(outputMissingQueryIDMsg)}
		s := Stats{
			RangeFailed: c.end.Sub(c.start),
		}
		if c.gen == 0 {
			s.RangeStarted = s.RangeFailed
		}
		c.stream.setErr(err, true, s)
		m.doStats(func(ms *Stats) {
			ms.add(&s)
		})
		m.logChunk(c, "nil query ID from CloudWatch Logs for", "")
		return err
	}
	c.queryID = *queryID

	// For initial chunks (not resulting from splits), count the chunks'
	// time range toward the range started statistic.
	if c.gen == 0 {
		d := c.end.Sub(c.start)
		m.coStats(c.stream, func(ms, ss *Stats) {
			ms.RangeStarted += d
			ss.RangeStarted += d
		})
	}

	// Put the chunk at the end of the running chunks list.
	m.chunks.Prev().Link(r)
	m.numChunks++

	// Chunk is started successfully.
	m.logChunk(c, "started", "")
	return nil
}

func isTemporary(err error) bool {
	if x, ok := err.(awserr.Error); ok {
		switch x.Code() {
		case cloudwatchlogs.ErrCodeLimitExceededException, cloudwatchlogs.ErrCodeServiceUnavailableException:
			return true
		default:
			// Omit 'e' suffix on 'throttl' to match Throttled and Throttling.
			if strings.Contains(strings.ToLower(x.Code()), "throttl") ||
				strings.Contains(strings.ToLower(x.Message()), "rate exceeded") {
				return true
			}
			return isTemporary(x.OrigErr())
		}
	}

	if errors.Is(err, io.EOF) {
		return true
	}

	var maybeTimeout interface{ Timeout() bool }
	if errors.As(err, &maybeTimeout) && maybeTimeout.Timeout() {
		return true
	}

	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNREFUSED, syscall.ECONNRESET:
			return true
		default:
			return false
		}
	}

	return false
}

func (m *mgr) getReadyChunk() *ring.Ring {
	if m.numReady == 0 {
		s := heap.Pop(&m.pq).(*stream)
		if !s.alive() {
			return nil
		}

		start := s.Start.Add(time.Duration(s.next) * s.Chunk)
		end := start.Add(s.Chunk)
		if end.Before(s.End) {
			heap.Push(&m.pq, s)
		} else {
			end = s.End
		}
		chunkID := strconv.Itoa(int(s.next))
		s.next++ // Only manager loop goroutine modifies next, and we are the manager loop.

		c := &chunk{
			stream:  s,
			ctx:     context.WithValue(s.ctx, chunkIDKey, chunkID),
			chunkID: chunkID,
			start:   start,
			end:     end,
		}
		if s.Preview {
			c.ptr = make(map[string]bool)
		}

		r := ring.New(1)
		r.Value = c
		m.ready.Prev().Link(r)
		m.numReady++
	}

	return m.ready.Next()
}

func (m *mgr) pollNextChunk() int {
	for m.numChunks > 0 {
		r := m.chunks.Next()
		c := r.Value.(*chunk)

		// If the chunk's stream is dead, cancel the chunk and remove it
		// from the ring.
		if !c.stream.alive() {
			m.numChunks--
			m.chunks.Unlink(1)
			m.cancelChunk(c, nil)
			continue
		}

		// If polling the stream failed because the chunk is unhealthy,
		// pass the error on to the stream and remove the chunk from
		// the ring.
		err := m.pollChunk(c)
		if err == errClosing {
			return -1
		} else if err == errInterrupted {
			return 0
		} else if err == errRestartChunk {
			c.chunkID += "R"
			c.queryID = ""
			m.numChunks--
			m.chunks.Unlink(1)
			m.numReady++
			m.ready.Prev().Link(r)
			return 1
		} else if err != nil && err != errEndOfChunk && err != errSplitChunk {
			m.numChunks--
			m.chunks.Unlink(1)
			m.finalizeChunk(c, err)
			m.doStats(func(ms *Stats) {
				ms.add(&c.Stats)
			})
			c.stream.setErr(err, true, c.Stats)
			continue
		}

		// If we successfully polled a chunk, either rotate the ring if
		// the chunk isn't done, or remove the chunk from the ring if it
		// is done.
		m.chunks.Unlink(1)
		if err == errEndOfChunk {
			m.numChunks--
			c.Stats.RangeDone += c.end.Sub(c.start)
			m.coStats(c.stream, func(ms, ss *Stats) {
				ms.add(&c.Stats)
				ss.add(&c.Stats)
			})
			m.logChunk(c, "finished", "end of chunk")
		} else if err == errSplitChunk {
			m.numChunks--
			m.coStats(c.stream, func(ms, ss *Stats) {
				ms.add(&c.Stats)
				ss.add(&c.Stats)
			})
		} else {
			m.chunks.Prev().Link(r)
		}

		// One chunk successfully polled.
		return 1
	}

	// Zero chunks successfully polled.
	return 0
}

func (m *mgr) pollChunk(c *chunk) error {
	if m.setTimerRPS(GetQueryResults) {
		select {
		case <-m.close:
			return errClosing
		case s := <-m.query:
			m.addQuery(s)
			return errInterrupted
		case <-m.timer.C:
			m.ding = true
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}

	input := cloudwatchlogs.GetQueryResultsInput{
		QueryId: &c.queryID,
	}
	output, err := m.Actions.GetQueryResultsWithContext(c.ctx, &input)
	m.lastReq[GetQueryResults] = time.Now()
	if err != nil && isTemporary(err) {
		return nil
	} else if err != nil {
		return &UnexpectedQueryError{c.queryID, c.stream.Text, err}
	}

	status := output.Status
	if status == nil {
		return &UnexpectedQueryError{c.queryID, c.stream.Text, errNilStatus()}
	}

	c.status = *status
	switch c.status {
	case cloudwatchlogs.QueryStatusScheduled, "Unknown":
		return nil
	case cloudwatchlogs.QueryStatusRunning:
		if c.ptr == nil {
			return nil // Ignore non-previewable results.
		}
		return sendChunkBlock(c, output.Results, false)
	case cloudwatchlogs.QueryStatusComplete:
		translateStats(output.Statistics, &c.Stats)
		if m.splitChunk(c, len(output.Results)) {
			return errSplitChunk
		}
		return sendChunkBlock(c, output.Results, true)
	case cloudwatchlogs.QueryStatusFailed:
		if c.ptr == nil {
			translateStats(output.Statistics, &c.Stats)
			return errRestartChunk // Retry transient failures if stream isn't previewable.
		}
		fallthrough
	default:
		translateStats(output.Statistics, &c.Stats)
		return &TerminalQueryStatusError{c.queryID, c.status, c.stream.Text}
	}
}

// splitBits is the number of child chunks into which a parent chunk
// will be split, assuming the parent chunk range is at least splitBits
// seconds long. The minimum chunk size is one second, so a 4-second
// parent chunk will be split into four chunks, but a two-second child
// chunk will only be split into two child chunks.
const splitBits = 4

// maxLimit is an indirect holder for the constant value MaxLimit used
// to facilitate unit testing.
var maxLimit int64 = MaxLimit

func (m *mgr) splitChunk(c *chunk, n int) bool {
	if c.ptr != nil {
		return false // Can't split chunks if previewing is on.
	}
	if int64(n) != maxLimit || c.stream.Limit != maxLimit {
		return false // Don't split unless chunk query overflowed CWL max results.
	}
	d := c.end.Sub(c.start)
	if d <= c.stream.SplitUntil {
		return false // Stop splitting when we reach minimum chunk size.
	}

	splitter := func(parent *chunk, start time.Time, frac time.Duration, n int) *chunk {
		end := start.Add(frac)
		if end.After(parent.end) {
			end = parent.end
		}
		chunkID := fmt.Sprintf("%ss%d", c.chunkID, n)
		return &chunk{
			stream:  parent.stream,
			ctx:     context.WithValue(parent.stream.ctx, chunkIDKey, chunkID),
			gen:     parent.gen + 1,
			chunkID: chunkID,
			start:   start,
			end:     end,
		}
	}

	frac := d / splitBits
	if hasSubSecondD(frac) {
		frac = frac + time.Second/2
		frac = frac.Round(time.Second)
	}

	children := make([]*chunk, 1, splitBits)
	child := splitter(c, c.start, frac, 0)
	children[0] = child
	for child.end.Before(c.end) {
		child = splitter(c, child.end, frac, len(children))
		children = append(children, child)
	}

	var b strings.Builder
	r := ring.New(len(children))
	r.Value = children[0]
	r = r.Next()
	_, _ = fmt.Fprintf(&b, "in %d sub-chunks... ", len(children))
	b.WriteString(children[0].chunkID)
	for i := 1; i < len(children); i++ {
		r.Value = children[i]
		r = r.Next()
		b.WriteString(" / ")
		b.WriteString(children[i].chunkID)
	}

	m.logChunk(c, "split", b.String())
	c.stream.m++
	c.stream.n += int64(len(children))
	m.numReady += len(children)
	m.ready.Prev().Link(r)
	return true
}

func (m *mgr) cancelChunk(c *chunk, err error) {
	if err != nil {
		c.stream.setErr(err, true, Stats{})
	}

	if m.setTimerRPS(StopQuery) {
		<-m.timer.C
		m.ding = true
	}

	output, err := m.Actions.StopQueryWithContext(context.Background(), &cloudwatchlogs.StopQueryInput{
		QueryId: &c.queryID,
	})
	m.lastReq[StopQuery] = time.Now()
	c.Stats.RangeFailed += c.end.Sub(c.start)
	if err != nil {
		m.logChunk(c, "failed to cancel", "error from CloudWatch Logs: "+err.Error())
	} else if output.Success == nil || !*output.Success {
		m.logChunk(c, "failed to cancel", "CloudWatch Logs did not indicate success")
	} else {
		m.logChunk(c, "cancelled", "")
	}
}

func (m *mgr) finalizeChunk(c *chunk, err error) {
	if err == io.EOF {
		c.Stats.RangeDone += c.end.Sub(c.start)
		m.logChunk(c, "finished", "end of stream")
		return
	}
	if terminalErr, ok := err.(*TerminalQueryStatusError); ok {
		switch terminalErr.Status {
		case cloudwatchlogs.QueryStatusFailed, cloudwatchlogs.QueryStatusCancelled, "Timeout":
			c.Stats.RangeFailed += c.end.Sub(c.start)
			m.logChunk(c, "unexpected terminal status", terminalErr.Status)
			return
		}
	}
	m.cancelChunk(c, nil)
}

func (m *mgr) waitForWork() int {
	if m.numChunks == 0 && len(m.pq) == 0 {
		m.setTimer(0)
	} else if !m.setTimerRPS(GetQueryResults) {
		m.setTimer(m.minDelay[GetQueryResults])
	}

	select {
	case <-m.close:
		return -1
	case s := <-m.query:
		m.addQuery(s)
		return 0
	case <-m.timer.C:
		m.ding = true
		return 0
	}
}

func (m *mgr) logChunk(c *chunk, msg, detail string) {
	id := c.chunkID
	if c.queryID != "" {
		id += "(" + c.queryID + ")"
	}
	if detail == "" {
		m.Logger.Printf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", m.Name, msg, id, c.stream.Text, c.start, c.end)
	} else {
		m.Logger.Printf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", m.Name, msg, id, c.stream.Text, c.start, c.end, detail)
	}
}

func (m *mgr) doStats(f func(ms *Stats)) {
	m.statsLock.Lock()
	defer m.statsLock.Unlock()
	f(&m.stats)
}

func (m *mgr) coStats(s *stream, f func(ms, ss *Stats)) {
	m.statsLock.Lock()
	defer m.statsLock.Unlock()
	s.lock.Lock()
	defer s.lock.Unlock()
	f(&m.stats, &s.stats)
}

func sendChunkBlock(c *chunk, results [][]*cloudwatchlogs.ResultField, eof bool) error {
	var block []Result
	var err error

	if c.ptr != nil {
		block, err = translateResultsPreview(c, results)
	} else {
		block, err = translateResultsNoPreview(c, results)
	}

	if !eof && len(block) == 0 && err == nil {
		return nil
	}

	c.stream.lock.Lock()
	defer c.stream.lock.Unlock()

	if len(block) > 0 {
		c.stream.blocks = append(c.stream.blocks, block)
	}
	if eof {
		c.stream.m++
		if c.stream.m == c.stream.n && err == nil {
			err = io.EOF
		} else if err == nil {
			err = errEndOfChunk
		}
	}
	if err == nil {
		c.stream.more.Signal()
	}
	return err
}

func translateStats(in *cloudwatchlogs.QueryStatistics, out *Stats) {
	if in == nil {
		return
	}
	if in.BytesScanned != nil {
		out.BytesScanned += *in.BytesScanned
	}
	if in.RecordsMatched != nil {
		out.RecordsMatched += *in.RecordsMatched
	}
	if in.RecordsScanned != nil {
		out.RecordsScanned += *in.RecordsScanned
	}
}

func translateResultsNoPreview(c *chunk, results [][]*cloudwatchlogs.ResultField) ([]Result, error) {
	var err error
	block := make([]Result, len(results))
	for i, r := range results {
		block[i], err = translateResult(c, r)
		if err != nil {
			return nil, err
		}
	}
	return block, nil
}

func translateResultsPreview(c *chunk, results [][]*cloudwatchlogs.ResultField) ([]Result, error) {
	// Create a slice to contain the block of results.
	block := make([]Result, 0, len(results))
	// Create a map to track which @ptr are new with this batch of results.
	newPtr := make(map[string]bool, len(results))
	// Collect all the results actually returned from CloudWatch Logs.
	for _, r := range results {
		var ptr *string
		for i := range r {
			f := r[i]
			if f == nil {
				continue
			}
			k, v := f.Field, f.Value
			if k != nil && *k == "@ptr" {
				ptr = v
				break
			}
		}
		if ptr != nil {
			newPtr[*ptr] = true
			if c.ptr[*ptr] {
				continue // We've already put this @ptr into the stream.
			}
		}
		rr, err := translateResult(c, r)
		if err != nil {
			return nil, err
		}
		block = append(block, rr)
	}
	// If there were any results delivered in a previous block that had
	// an @ptr that is not present in this block, insert an @deleted
	// result for each such obsolete result.
	for ptr := range c.ptr {
		if !newPtr[ptr] {
			block = append(block, deleteResult(ptr))
			delete(c.ptr, ptr)
		} else {
			delete(newPtr, ptr)
		}
	}
	// Add the @ptr for each result seen in the current batch, but which
	// hasn't been delivered in a previous block, into the chunk's @ptr
	// map.
	for ptr := range newPtr {
		c.ptr[ptr] = true
	}

	// Return the block so it can be sent to the| stream.
	return block, nil
}

func translateResult(c *chunk, r []*cloudwatchlogs.ResultField) (Result, error) {
	rr := make(Result, len(r))
	for i, f := range r {
		if f == nil {
			return Result{}, &UnexpectedQueryError{QueryID: c.queryID, Text: c.stream.Text, Cause: errNilResultField(i)}
		}
		k, v := f.Field, f.Value
		if k == nil {
			return Result{}, &UnexpectedQueryError{QueryID: c.queryID, Text: c.stream.Text, Cause: errNoKey()}
		}
		if v == nil {
			return Result{}, &UnexpectedQueryError{QueryID: c.queryID, Text: c.stream.Text, Cause: errNoValue(*k)}
		}
		rr[i] = ResultField{
			Field: *k,
			Value: *v,
		}
	}
	return rr, nil
}

func deleteResult(ptr string) Result {
	return Result{
		{
			Field: "@ptr",
			Value: ptr,
		},
		{
			Field: "@deleted",
			Value: "true",
		},
	}
}

func (m *mgr) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrClosed
		}
	}()

	m.close <- struct{}{}
	return
}

func (m *mgr) Query(q QuerySpec) (s Stream, err error) {
	// Validation that does not require lock.
	q.Text = strings.TrimSpace(q.Text)
	if q.Text == "" {
		return nil, errors.New(textBlankMsg)
	}

	q.Start = q.Start.UTC()
	if hasSubSecond(q.Start) {
		return nil, errors.New(startSubSecondMsg)
	}
	q.End = q.End.UTC()
	if hasSubSecond(q.End) {
		return nil, errors.New(endSubSecondMsg)
	}
	if !q.End.After(q.Start) {
		return nil, errors.New(endNotBeforeStartMsg)
	}

	if len(q.Groups) == 0 {
		return nil, errors.New(noGroupsMsg)
	}
	groups := make([]*string, len(q.Groups))
	for i := range q.Groups {
		groups[i] = &q.Groups[i]
	}

	d := q.End.Sub(q.Start)
	if q.Chunk <= 0 {
		q.Chunk = d
	} else if hasSubSecondD(q.Chunk) {
		return nil, errors.New(chunkSubSecondMsg)
	} else if q.Chunk > d {
		q.Chunk = d
	}

	n := int64(d / q.Chunk)
	if d%q.Chunk != 0 {
		n++
	}

	if q.Limit <= 0 {
		q.Limit = DefaultLimit
	} else if q.Limit > maxLimit {
		return nil, errors.New(exceededMaxLimitMsg)
	}

	if q.SplitUntil <= 0 {
		q.SplitUntil = q.Chunk
	} else if hasSubSecondD(q.SplitUntil) {
		return nil, errors.New(splitUntilSubSecondMsg)
	} else if q.Preview {
		return nil, errors.New(splitUntilWithPreviewMsg)
	} else if q.Limit < maxLimit {
		return nil, errors.New(splitUntilWithoutMaxLimit)
	}

	ctx, cancel := context.WithCancel(context.Background())

	ss := &stream{
		QuerySpec: q,

		ctx:    ctx,
		cancel: cancel,
		n:      n,
		groups: groups,
		stats: Stats{
			RangeRequested: d,
		},
	}
	ss.more = sync.NewCond(&ss.lock)

	defer func() {
		if r := recover(); r != nil {
			err = ErrClosed
		}
	}()

	m.query <- ss

	return ss, nil
}

func hasSubSecond(t time.Time) bool {
	return t.Nanosecond() != 0
}

func hasSubSecondD(d time.Duration) bool {
	return d%time.Second > 0
}

func (m *mgr) GetStats() Stats {
	m.statsLock.RLock()
	defer m.statsLock.RUnlock()
	return m.stats
}

type streamHeap []*stream

func (h streamHeap) Len() int {
	return len(h)
}

// Less compares two elements in a stream heap.
//
// This function must only be called from a goroutine that owns at least
// the read lock on the manager that owns the stream heap, and may only
// compare stream fields that are either (A) immutable within the stream,
// as in Priority; or (B) are mutable only by the manager and not the
// stream, as in next.
func (h streamHeap) Less(i, j int) bool {
	if h[i].Priority < h[j].Priority {
		return true
	} else if h[j].Priority < h[i].Priority {
		return false
	} else {
		return h[i].next < h[j].next
	}
}

func (h streamHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *streamHeap) Push(x interface{}) {
	*h = append(*h, x.(*stream))
}

func (h *streamHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type stream struct {
	QuerySpec

	// Immutable fields.
	ctx    context.Context    // Stream context used to parent chunk contexts
	cancel context.CancelFunc // Cancels ctx when the stream is closed
	n      int64              // Number of total chunks
	groups []*string          // Preprocessed slice for StartQuery

	// Mutable fields only read/written by mgr loop goroutine.
	next int64 // Next chunk to create
	m    int64 // Number of chunks completed

	// Lock controlling access to the below mutable fields.
	lock sync.RWMutex

	// Mutable fields controlled by stream using lock.
	stats  Stats
	blocks [][]Result
	i, j   int        // Block index and position within block
	more   *sync.Cond // Used to block a Read pending more blocks
	err    error      // Error to return, if any
}

func (s *stream) Close() error {
	if !s.setErr(ErrClosed, true, Stats{}) {
		return ErrClosed
	}

	s.cancel()
	s.blocks, s.i, s.j = nil, 0, 0
	return nil
}

func (s *stream) Read(r []Result) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for {
		n, err := s.read(r)
		if n > 0 || err != nil || len(r) == 0 {
			return n, err
		}
		s.more.Wait()
	}
}

func (s *stream) GetStats() Stats {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.stats
}

func (s *stream) read(r []Result) (int, error) {
	n := 0
	for s.i < len(s.blocks) {
		block := s.blocks[s.i]
		for s.j < len(block) {
			if n == len(r) {
				return n, nil
			}
			r[n] = block[s.j]
			n++
			s.j++
		}
		s.i++
		s.j = 0
	}
	return n, s.err
}

func (s *stream) setErr(err error, lock bool, stats Stats) bool {
	if lock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if s.err == ErrClosed {
		return false
	}

	s.err = err
	s.stats.add(&stats)
	s.more.Signal()
	return true
}

func (s *stream) alive() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.err == nil
}

// A chunk represents a single active CloudWatch Logs Insights query
// owned by a stream. A stream has one or more chunks, depending on
// whether the QuerySpec indicated chunking. A chunk is a passive data
// structure: it owns no goroutines and presents no interface that is
// accessible outside the package.
type chunk struct {
	Stats
	stream  *stream         // Owning stream which receives results of the chunk
	ctx     context.Context // Child of the stream's context owned by this chunk
	gen     int             // Generation, zero if an initial chunk, positive if it came from a split
	chunkID string          // Incite chunk ID
	queryID string          // Insights query ID
	status  string          // Insights query status
	ptr     map[string]bool // Set of already viewed @ptr, nil if QuerySpec not previewable
	start   time.Time       // Start of the chunk's time range (inclusive)
	end     time.Time       // End of the chunk's time range (exclusive)
}

var (
	errClosing      = errors.New("incite: closed")
	errEndOfChunk   = errors.New("incite: end of chunk")
	errInterrupted  = errors.New("incite: timer wait interrupted")
	errSplitChunk   = errors.New("incite: chunk split")
	errRestartChunk = errors.New("incite: transient chunk failure, restart chunk")
)
