package incite

import (
	"container/heap"
	"container/ring"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

// QuerySpec specifies the parameter for a query operation either using
// the global Query function or a QueryManager.
type QuerySpec struct {
	// Text contains the actual text of the CloudWatch Insights query.
	//
	// Text may not contain an empty or blank string. Beyond checking
	// for blank text, Incite does not attempt to parse Text and simply
	// forwards it to the CloudWatch Logs service. Care must be taken to
	// specify query text compatible with the Chunk and Preview fields
	// or the results may be misleading.
	//
	// To limit the number of results returned by the query, use the
	// Limit field, since the Insights API seems to ignore the `limit`
	// command. Note that if the QuerySpec specifies a chunked query,
	// then Limit will apply to the results obtained from each chunk,
	// not to the global query.
	//
	// To learn the Insights query syntax, please see the official
	// documentation at:
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html.
	Text string

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

	// Groups lists the names of the CloudWatch Logs log groups to be
	// queried. It may not be empty.
	Groups []string

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
	// If Chunk is zero, negative, or greater than the difference between
	// End and Start, the query is not chunked. If Chunk is positive and
	// less than the difference between End and Start, the query is
	// broken into n chunks, where n is (End-Start)/Chunk, rounded up to
	// the nearest integer value.
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
	// chunk, necessitating further aggregation on the client side.
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
	// by the same QueryManager. This can help the QueryManager manage
	// finite resources to stay within CloudWatch Logs service quota
	// limits.
	//
	// A lower number indicates a higher priority. The default zero
	// value is appropriate for many cases.
	//
	// The Priority field may be set to any valid int value. A query
	// whose Priority number is lower is allocated CloudWatch Logs
	// query capacity in preference to a query whose Priority number is
	// higher, but only within the same QueryManager.
	Priority int

	// Hint optionally indicates the rough expected size of the result
	// set, which can help the QueryManager do a better job allocating
	// memory needed to manage and hold query results. Leave it zero
	// if you don't know the expected result size or aren't worried
	// about optimizing memory consumption.
	Hint uint16
}

// Stats contains metadata returned by CloudWatch Logs about the amount
// of data scanned and number of result records matched during one or
// more Insights queries.
type Stats struct {
	// BytesScanned represents the total number of bytes of log events
	// scanned.
	BytesScanned float64
	// RecordsMatched counts the number of log events that matched the
	// query or queries.
	RecordsMatched float64
	// RecordsScanned counts the number of log events scanned during the
	// query or queries.
	RecordsScanned float64
}

func (s *Stats) add(t Stats) {
	s.BytesScanned += t.BytesScanned
	s.RecordsMatched += t.RecordsMatched
	s.RecordsScanned += t.RecordsScanned
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
// polling jobs in the CloudWatch Logs Insights service, breaking
// queries into smaller time chunks (if desired), de-duplicating and
// providing preview results (if desired), retrying transient request
// failures, and managing resources to try to stay within the
// CloudWatch Logs service quota limits.
//
// Use NewQueryManager to create a QueryManager, and be sure to close it
// when you no longer need its services, since every QueryManager
// consumes some compute resources just by existing.
//
// Calling the Query method will return a result Stream from which the
// query results can be read as they become available. Use the
// Unmarshal function to unmarshal the bare results into other
// structured types.
//
// Calling the Close method will immediately cancel all running queries
// started with the Query, as if the query's Stream had been explicitly
// closed.
//
// Calling GetStats will return the running sum of all statistics for
// all queries run within the QueryManager since it was created.
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
	// all of p as scratch space during the call. If some data are
	// available but fewer than len(p) results, Read conventionally
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
	chunks    ring.Ring                              // Circular list of chunks, first item is a sentry
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
	// each one second period for each CloudWatch Logs action. The
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

	go m.loop()

	return m
}

func (m *mgr) loop() {
	defer m.shutdown()

	m.Logger.Printf("incite: QueryManager (%p) start", m)

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
	// On a best effort basis, close all open chunks.
	m.chunks.Do(func(i interface{}) {
		if i != nil {
			m.cancelChunk(i.(*chunk))
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
	m.Logger.Printf("incite: QueryManager (%p) stop", m)
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
	return m.timer.Reset(minDelay - delaySoFar)
}

func (m *mgr) startNextChunks() int {
	var numStarted int
	for len(m.pq) > 0 && m.chunks.Len() <= m.Parallel {
		err := m.startNextChunk()
		if err == errClosing {
			return -1
		} else if err == nil {
			numStarted++
		}
	}

	return numStarted
}

type queryIDKeyType int

var queryIDKey = queryIDKeyType(0)

func (m *mgr) startNextChunk() error {
	s := heap.Pop(&m.pq).(*stream)

	if m.setTimerRPS(StartQuery) {
		select {
		case <-m.close:
			return errClosing
		case ss := <-m.query:
			heap.Push(&m.pq, s)
			heap.Push(&m.pq, ss)
			return errInterrupted
		case <-m.timer.C:
			m.ding = true
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}

	next := s.next // Only manager loop goroutine modifies next, and we are the manager loop.
	end := next.Add(s.Chunk)
	if end.After(s.End) {
		end = s.End
	}
	starts := next.Unix()
	ends := end.Add(-time.Second).Unix() // CWL uses inclusive time ranges at 1 second granularity, we use exclusive ranges.

	input := cloudwatchlogs.StartQueryInput{
		QueryString:   &s.Text,
		StartTime:     &starts,
		EndTime:       &ends,
		LogGroupNames: s.groups,
		Limit:         &s.Limit,
	}
	output, err := m.Actions.StartQueryWithContext(s.ctx, &input)
	m.lastReq[StartQuery] = time.Now()
	if err != nil {
		if isTemporary(err) {
			heap.Push(&m.pq, s)
			m.Logger.Printf("incite: QueryManager(%p) temporary failure to start chunk %q [%s..%s): %s",
				m, s.Text, next, end, err.Error())
		} else {
			err = &StartQueryError{s.Text, next, end, err}
			s.setErr(err, true, Stats{})
			m.Logger.Printf("incite: QueryManager(%p) permanent failure to start %q [%s..%s) due to fatal error from CloudWatch Logs: %s",
				m, s.Text, next, end, err.Error())
		}
		return err
	}

	queryID := output.QueryId
	if queryID == nil {
		err = &StartQueryError{s.Text, next, end, errors.New("incite: nil query ID")}
		s.setErr(err, true, Stats{})
		m.Logger.Printf("incite: QueryManager(%p) nil query ID from CloudWatch Logs for %q [%s..%s)", m, s.Text, next, end)
		return err
	}

	if end.Before(s.End) {
		s.next = end // At least one chunk remains.
		heap.Push(&m.pq, s)
	} else {
		s.next = time.Time{} // All chunks are in-flight or finished.
	}

	c := &chunk{
		stream: s,
		ctx:    context.WithValue(s.ctx, queryIDKey, output.QueryId),
		id:     *queryID,
		status: cloudwatchlogs.QueryStatusScheduled,
	}
	if s.Preview {
		c.ptr = make(map[string]bool, s.chunkHint)
	}

	r := ring.New(1)
	r.Value = c
	m.chunks.Prev().Link(r)
	m.numChunks++

	return nil
}

func isTemporary(err error) bool {
	if x, ok := err.(awserr.Error); ok {
		switch x.Code() {
		case cloudwatchlogs.ErrCodeLimitExceededException, cloudwatchlogs.ErrCodeServiceUnavailableException:
			return true
		default:
			// Omit 'e' suffix on 'throttl' to match Throttled and Throttling.
			return strings.Contains(strings.ToLower(x.Code()), "throttl") ||
				strings.Contains(strings.ToLower(x.Message()), "rate exceeded")
		}
	}
	return false
}

func (m *mgr) pollNextChunk() int {
	for m.numChunks > 0 {
		c := m.chunks.Next().Value.(*chunk)

		// If the chunk's stream is dead, cancel the chunk and remove it
		// from the ring.
		if !c.stream.alive() {
			m.numChunks--
			m.chunks.Unlink(1)
			m.cancelChunk(c)
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
		} else if err != nil && err != errEndOfChunk {
			m.numChunks--
			m.chunks.Unlink(1)
			c.stream.setErr(err, true, c.Stats)
			m.statsLock.Lock()
			m.stats.add(c.Stats)
			m.statsLock.Unlock()
			m.cancelChunkMaybe(c, err)
			continue
		}

		// If we successfully polled a chunk, either rotate the ring if
		// the chunk isn't done, or remove the chunk from the ring if it
		// is done.
		r := m.chunks.Unlink(1)
		if err == errEndOfChunk {
			m.numChunks--
			m.statsLock.Lock()
			c.stream.lock.Lock()
			m.stats.add(c.Stats)
			c.stream.stats.add(c.Stats)
			m.statsLock.Unlock()
			c.stream.lock.Unlock()
		} else {
			m.chunks.Prev().Link(r)
		}
		return 1
	}

	return 0
}

func (m *mgr) pollChunk(c *chunk) error {
	if m.setTimerRPS(GetQueryResults) {
		select {
		case <-m.close:
			return errClosing
		case s := <-m.query:
			heap.Push(&m.pq, s)
			return errInterrupted
		case <-m.timer.C:
			m.ding = true
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}

	input := cloudwatchlogs.GetQueryResultsInput{
		QueryId: &c.id,
	}
	output, err := m.Actions.GetQueryResultsWithContext(c.ctx, &input)
	m.lastReq[GetQueryResults] = time.Now()
	if err != nil && isTemporary(err) {
		return nil
	} else if err != nil {
		return &UnexpectedQueryError{c.id, c.stream.Text, err}
	}

	status := output.Status
	if status == nil {
		return &UnexpectedQueryError{c.id, c.stream.Text, errNilStatus()}
	}

	c.status = *status
	switch c.status {
	case cloudwatchlogs.QueryStatusScheduled, "Unknown":
		return nil
	case cloudwatchlogs.QueryStatusRunning:
		if c.ptr == nil {
			return nil // Ignore non-previewable results.
		}
		return sendChunkBlock(c, output.Results, output.Statistics, false)
	case cloudwatchlogs.QueryStatusComplete:
		return sendChunkBlock(c, output.Results, output.Statistics, true)
	default:
		return &TerminalQueryStatusError{c.id, c.status, c.stream.Text}
	}
}

func (m *mgr) cancelChunk(c *chunk) {
	if m.setTimerRPS(StopQuery) {
		<-m.timer.C
		m.ding = true
	}

	_, _ = m.Actions.StopQueryWithContext(context.Background(), &cloudwatchlogs.StopQueryInput{
		QueryId: &c.id,
	})
	m.lastReq[StopQuery] = time.Now()
}

func (m *mgr) cancelChunkMaybe(c *chunk, err error) {
	if err == io.EOF {
		return
	}
	if terminalErr, ok := err.(*TerminalQueryStatusError); ok {
		switch terminalErr.Status {
		case cloudwatchlogs.QueryStatusFailed, cloudwatchlogs.QueryStatusCancelled, "Timeout":
			return
		}
	}
	m.cancelChunk(c)
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
		heap.Push(&m.pq, s)
		return 0
	case <-m.timer.C:
		return 0
	}
}

func sendChunkBlock(c *chunk, results [][]*cloudwatchlogs.ResultField, stats *cloudwatchlogs.QueryStatistics, eof bool) error {
	var block []Result
	var err error

	if stats != nil {
		c.Stats = translateStats(stats)
	}

	if c.ptr != nil {
		block, err = translateResultsPreview(c, results)
	} else {
		block, err = translateResultsNoPreview(c.id, results)
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

func translateStats(stats *cloudwatchlogs.QueryStatistics) (result Stats) {
	if stats.BytesScanned != nil {
		result.BytesScanned = *stats.BytesScanned
	}
	if stats.RecordsMatched != nil {
		result.RecordsMatched = *stats.RecordsMatched
	}
	if stats.RecordsScanned != nil {
		result.RecordsScanned = *stats.RecordsScanned
	}
	return
}

func translateResultsNoPreview(id string, results [][]*cloudwatchlogs.ResultField) ([]Result, error) {
	var err error
	block := make([]Result, len(results))
	for i, r := range results {
		block[i], err = translateResult(id, r)
	}
	return block, err
}

func translateResultsPreview(c *chunk, results [][]*cloudwatchlogs.ResultField) ([]Result, error) {
	// Create a slice to contain the block of results.
	guess := int(c.stream.Hint) - len(c.ptr)
	if guess <= len(results) {
		guess = len(results)
	}
	block := make([]Result, 0, guess)
	// Create a map to track which @ptr are new with this batch of results.
	newPtr := make(map[string]bool, len(results))
	// Collect all the results actually returned from CloudWatch Logs.
	for _, r := range results {
		var ptr *string
		for f := range r {
			k, v := r[f].Field, r[f].Value
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
		rr, err := translateResult(c.id, r)
		if err != nil {
			return nil, &UnexpectedQueryError{c.id, c.stream.Text, err}
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

	// Return the block so it can be sent to the stream.
	return block, nil
}

func translateResult(id string, r []*cloudwatchlogs.ResultField) (Result, error) {
	rr := make(Result, len(r))
	for i, f := range r {
		k, v := f.Field, f.Value
		if k == nil {
			return Result{}, errNoKey(id)
		}
		if v == nil {
			return Result{}, errNoValue(id, *k)
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

const (
	minHint = 100
)

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
	} else if q.Chunk > d {
		q.Chunk = d
	}

	n := int64(d / q.Chunk)
	if d%q.Chunk != 0 {
		n++
	}

	if q.Limit <= 0 {
		q.Limit = DefaultLimit
	} else if q.Limit > MaxLimit {
		return nil, errors.New(exceededMaxLimitMsg)
	}

	if q.Hint < minHint {
		q.Hint = minHint
	}

	chunkHint := int64(q.Hint) / n
	if chunkHint < minHint {
		chunkHint = minHint
	}

	ctx, cancel := context.WithCancel(context.Background())

	ss := &stream{
		QuerySpec: q,

		ctx:       ctx,
		cancel:    cancel,
		n:         n,
		chunkHint: uint16(chunkHint),
		groups:    groups,

		next: q.Start,
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
		di := h[i].next.Sub(h[i].Start)
		dj := h[j].next.Sub(h[j].Start)
		return di < dj
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
	ctx       context.Context    // Stream context used to parent chunk contexts
	cancel    context.CancelFunc // Cancels ctx when the stream is closed
	n         int64              // Number of total chunks
	chunkHint uint16             // Hint divided by number of chunks
	groups    []*string          // Preprocessed slice for StartQuery

	// Mutable fields controlled by mgr using mgr's lock.
	next time.Time // Next chunk start time

	// Lock controlling access to the below mutable fields.
	lock sync.RWMutex

	// Mutable fields controlled by stream using lock.
	stats  Stats
	blocks [][]Result
	m      int64      // Number of chunks completed
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
		n := s.read(r)
		if n > 0 || s.err != nil || len(r) == 0 {
			return n, s.err
		}
		s.more.Wait()
	}
}

func (s *stream) GetStats() Stats {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.stats
}

func (s *stream) read(r []Result) int {
	n := 0
	for s.i < len(s.blocks) {
		block := s.blocks[s.i]
		for s.j < len(block) {
			if n == len(r) {
				return n
			}
			r[n] = block[s.j]
			n++
			s.j++
		}
		s.i++
		s.j = 0
	}
	return n
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
	s.stats.add(stats)
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
	stream *stream         // Owning stream which receives results of the chunk
	ctx    context.Context // Child of the stream's context owned by this chunk
	id     string          // Insights query ID
	status string          // Insights query status
	ptr    map[string]bool // Set of already viewed @ptr, nil if QuerySpec not previewable
}

var (
	errClosing     = errors.New("incite: closed")
	errEndOfChunk  = errors.New("incite: end of chunk")
	errInterrupted = errors.New("incite: timer wait interrupted")
)
