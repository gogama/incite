package incite

import (
	"container/heap"
	"container/ring"
	"context"
	"errors"
	"fmt"
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
	// Text contains the actual text of the CloudWatch Insights query,
	// following the query syntax documented at
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html.
	//
	// To limit the number of results returned by the query, add the
	// `limit` command to your query text. Note that if the QuerySpec
	// specifies a chunked query then the limit will apply to the
	// results obtained from each chunk, not to the global query.
	//
	// Text may not contain an empty or blank string. Beyond checking
	// for blank text, Incite does not attempt to parse Text and simply
	// forwards it to the CloudWatch Logs service. Care must be taken to
	// specify query text compatible with the Chunk and Preview fields
	// or the results may be misleading.
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

	// Chunk optionally specifies a chunk size for chunked queries.
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
	// • If Text contains a sort command, the sort will only apply
	// within each individual chunk. If the QuerySpec is executed by a
	// QueryManager configured with a parallelism factor above 1, then
	// the results may appear be out of order since the order of
	// completion of chunks is not guaranteed.
	//
	// • If Text contains a limit command, the results may be
	// surprising as the limit will be applied separately to each chunk
	// and there may be up to n × limit results.
	//
	// • If Text contains a stats command, the statistical aggregation
	// will be applied to each chunk and each expected
	//
	// • In general if you use chunking with query text which implies
	// any kind of server-side aggregation, you may need to perform
	// custom post-processing on the results.
	Chunk time.Duration

	// Preview optionally requests preview results from a running query.
	//
	// If Preview is true, intermediate results for the query are
	// sent to the results stream as soon as they are available. This
	// can result in increased responsiveness for the end-user of your
	// application but requires care since not all intermediate results
	// are valid members of the final result set.
	//
	// The Preview option should typically be avoided if Text contains a
	// sort or stats command, unless you are prepared to post-process
	// the results.
	Preview bool

	// Priority optionally allows a query operation to be given a higher
	// or lower priority with regard to other query operations managed
	// by the same QueryManager. This can help the QueryManager manage
	// finite resources to stay within CloudWatch Logs service quota
	// limits.
	Priority int

	// Hint optionally indicates the rough expected size of the result
	// set, which can help the QueryManager do a better job allocating
	// memory needed to manage and hold query results. Leave it zero
	// if you don't know the expected result size or aren't worried
	// about optimizing memory consumption.
	Hint uint16
}

// The Queryer interface provides the ability to run a CloudWatch Logs
// Insights query. Use NewQueryManager to create a QueryManager, which
// contains this interface.
type Queryer interface {
	Query(QuerySpec) (Stream, error)
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

func (a *Stats) Add(b Stats) {
	a.BytesScanned += b.BytesScanned
	a.RecordsMatched += b.RecordsMatched
	a.RecordsScanned += b.RecordsScanned
}

// TODO. Document.
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
// Unmarshal to unmarshal the bare results into other structured types.
//
// Calling the Close method will immediately cancel all running queries]
// started with the Query, as if the query's Stream had been explicitly
// closed.
//
// Calling GetStats will return the running sum of all statistics for
// all queries run within the QueryManager since it was created.
type QueryManager interface {
	io.Closer
	Queryer
	StatsGetter
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

// Reader provides a basic Read method to allow reading CloudWatch Logs
// Insights query results as a stream.
//
// Read reads up to len(p) CloudWatch Logs Insights results into p. It
// returns the number of results read (0 <= n <= len(p)) and any error
// encountered. Even if Read returns n < len(p), it may use all of p as
// scratch space during the call. If some data are available but fewer
// than len(p) results, Read conventionally returns what is available
// instead of waiting for more.
//
// When Read encounters an error or end-of-file condition after
// successfully reading n > 0 results, it returns the number of results
// read. It may return the (non-nil) error from the same call or return
// the error (and n == 0) from a subsequent call. An instance of this
// general case is that a Reader returning a non-zero number of results
// at the end of the input stream may return either err == EOF or
// err == nil. The next Read should return 0, EOF.
//
// Callers should always process the n > 0 results returned before
// considering the error err. Doing so correctly handles I/O errors
// that happen after reading some results and also both of the allowed
// EOF behaviors.
//
// Implementations of Read are discouraged from returning a zero result
// count with a nil error, except when len(p) == 0. Callers should treat
// a return of 0 and nil as indicating that nothing happened; in
// particular it does not indicate EOF.
//
// Implementations must not retain p.
//
// As a convenience, the ReadAll function may be used to read all
// remaining results available in a reader.
type Reader interface {
	Read(p []Result) (n int, err error)
}

// Stream provides access to the result stream from a query operation either
// using a QueryManager or the global Query function.
//
// Use the Close method if you need to prematurely cancel the query operation,
// releasing the local (in-process) and remote (in the CloudWatch Logs service)
// resources it consumes.
//
// Use the Read method to read query results from the stream. The Read method
// returns io.EOF when the entire results stream has been consumed. At this
// point the query is over and all local and remote resources have been released,
// so it is not necessary to close the stream explicitly.
//
// Use the GetStats method to obtain the Insights statistics pertaining to the
// query. Note that the results from the GetStats method may change over time
// as new results are pulled from the CloudWatch Logs web service, but will stop
// changing after the Read method returns io.EOF. If the query was chunked, the
// stats will be summed across multiple chunks.
type Stream interface {
	io.Closer
	Reader
	StatsGetter
}

type mgr struct {
	Config

	// Fields only read/written on construction and in mgr loop goroutine.
	timer     *time.Timer
	ding      bool
	minDelay  map[CloudWatchLogsAction]time.Duration // Used to stay under TPS limit
	lastReq   map[CloudWatchLogsAction]time.Time     // Used to stay under TPS limit
	chunks    ring.Ring                              // Circular list of chunks, first item is a sentry
	numChunks int

	// Lock controlling access to the below mutable fields.
	lock sync.RWMutex

	// Mutable fields controlled by mgr using lock.
	stats   Stats      // Read by GetStats, written by mgr loop goroutine
	pq      streamHeap // Written by Query, written by mgr loop goroutine
	close   chan struct{}
	closed  bool
	query   chan struct{}
	waiting bool
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
	// CloudWatch Logs  Insights queries a QueryManager will attempt to
	// run at any one time.
	//
	// The default value is set to slightly less than the service quota
	// limit to leave some concurrency available for other users even if
	// the QueryManager is at maximum capacity.
	DefaultParallel = QueryConcurrencyQuotaLimit - 2
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
	// CloudWatch Logs capability, the value specified in DefaultRPS is
	// used instead.
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
	}
	minDelay := make(map[CloudWatchLogsAction]time.Duration, numActions)
	for action, defaultRPS := range DefaultRPS {
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

		timer:   time.NewTimer(1<<63 - 1),
		lastReq: make(map[CloudWatchLogsAction]time.Time, numActions),
	}

	go m.loop()

	return m
}

func (m *mgr) loop() {
	defer m.shutdown()

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
	m.lock.Lock()
	defer m.lock.Unlock()

	// On a best effort basis, close all open chunks.
	m.chunks.Do(func(i interface{}) {
		if i != nil {
			m.cancelChunk(i.(*chunk))
		}
	})

	// Close all open streams.
	for _, s := range m.pq {
		s.setErr(ErrClosed, true)
		m.stats.Add(s.GetStats())
	}

	// Free any other resources.
	m.timer.Stop()
	close(m.close)
	close(m.query)
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
	m.lock.Lock()
	defer m.lock.Unlock()

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

func (m *mgr) startNextChunk() error { // Assert: Lock is acquired.
	s := heap.Pop(&m.pq).(*stream)

	if m.setTimerRPS(StartQuery) {
		select {
		case <-m.close:
			return errClosing
		case <-m.timer.C:
			m.ding = true
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}

	next := s.next // Only manager modifies next, and we hold the manager lock.
	end := next.Add(s.Chunk)
	if end.After(s.End) {
		end = s.End
	}
	starts := next.Unix()
	ends := end.Add(-time.Second).Unix() // CWL uses inclusive time ranges at 1 second granularity, we use exclusive ranges.
	limit := int64(10000)                // CWL max limit. Use `limit` command in query spec text to sub-limit.

	input := cloudwatchlogs.StartQueryInput{
		QueryString:   &s.Text,
		StartTime:     &starts,
		EndTime:       &ends,
		LogGroupNames: s.groups,
		Limit:         &limit,
	}
	output, err := m.Actions.StartQueryWithContext(s.ctx, &input)
	m.lastReq[StartQuery] = time.Now()
	if err != nil {
		if isTemporary(err) {
			heap.Push(&m.pq, s)
			m.Logger.Printf("incite: temporary failure to start chunk %q [%s..%s): %s",
				s.Text, next, end, err.Error())
		} else {
			s.setErr(wrap(err, "incite: fatal error from CloudWatch Logs for chunk %q [%s..%s)", s.Text, next, end), true)
			m.stats.Add(s.GetStats())
			m.Logger.Printf("incite: permanent failure to start %q [%s..%s) due to fatal error from CloudWatch Logs: %s",
				s.Text, next, end, err.Error())
		}
		return err
	}

	queryID := output.QueryId
	if queryID == nil {
		m.Logger.Printf("incite: nil query ID for %q [%s..%s)", s.Text, next, end)
		return errors.New("incite: nil query ID")
	}

	if end.Before(s.End) {
		s.next = end // At least one chunk remains.
		heap.Push(&m.pq, s)
	} else {
		s.next = time.Time{} // All chunks are in-flight or finished.
		m.stats.Add(s.GetStats())
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

	return nil
}

func isTemporary(err error) bool {
	if x, ok := err.(awserr.RequestFailure); ok {
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
	m.lock.Lock()
	defer m.lock.Unlock()

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
		} else if err != nil && err != io.EOF {
			m.numChunks--
			m.chunks.Unlink(1)
			c.stream.setErr(err, true)
			c.stream.addChunkStats(c.Stats)
			continue
		}

		// If we successfully polled a chunk, either rotate the ring if
		// the chunk isn't done, or remove the chunk from the ring if it
		// is done.
		r := m.chunks.Unlink(1)
		if err != io.EOF {
			m.chunks.Prev().Link(r)
		} else {
			m.numChunks--
		}
		return 1
	}

	return 0
}

func (m *mgr) pollChunk(c *chunk) error {
	m.lock.Unlock()
	defer m.lock.Lock()

	if m.setTimerRPS(GetQueryResults) {
		select {
		case <-m.close:
			return errClosing
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
	if err != nil {
		return wrap(err, "incite: query chunk %q: failed to poll", c.id)
	}

	status := output.Status
	if status == nil {
		return fmt.Errorf("incite: query chunk %q: nil status in GetQueryResults output from CloudWatch Logs", c.id)
	}

	c.status = *status
	switch c.status {
	case cloudwatchlogs.QueryStatusScheduled:
		return nil
	case cloudwatchlogs.QueryStatusRunning:
		return sendChunkBlock(c, output.Results, output.Statistics, false)
	case cloudwatchlogs.QueryStatusComplete:
		return sendChunkBlock(c, output.Results, output.Statistics, true)
	case cloudwatchlogs.QueryStatusCancelled, cloudwatchlogs.QueryStatusFailed, "Timeout":
		return fmt.Errorf("incite: query chunk %q: unexpected terminal status: %s", c.id, c.status)
	default:
		return fmt.Errorf("incite: query chunk %q: unhandled status: %s", c.id, c.status)
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
	c.stream.addChunkStats(c.Stats)
}

func (m *mgr) waitForWork() int {
	m.lock.Lock()
	m.waiting = true
	defer func() {
		m.waiting = false
		m.lock.Unlock()
	}()

	if m.numChunks == 0 && len(m.pq) == 0 {
		m.setTimer(0)
	} else if !m.setTimerRPS(GetQueryResults) {
		m.setTimer(m.minDelay[GetQueryResults])
	}

	select {
	case <-m.close:
		return -1
	case <-m.query:
		return 0
	case <-m.timer.C:
		return 0
	}
}

func sendChunkBlock(c *chunk, results [][]*cloudwatchlogs.ResultField, stats *cloudwatchlogs.QueryStatistics, done bool) error {
	var block []Result
	var err error

	if stats != nil {
		c.Stats = translateStats(stats)
	}

	if c.ptr != nil {
		block, err = translateResultsPart(c, results)
	} else if !done {
		block, err = translateResultsFull(c.id, results)
	}

	if !done && len(block) == 0 && err != nil {
		return nil
	}

	c.stream.lock.Lock()
	defer c.stream.lock.Unlock()

	if len(block) > 0 {
		c.stream.blocks = append(c.stream.blocks, block)
	}
	if c.status == cloudwatchlogs.QueryStatusComplete {
		c.stream.m++
		if c.stream.m == c.stream.n && err == nil {
			err = io.EOF
		}
	}
	if err != nil {
		c.stream.setErr(err, false)
	}
	if c.stream.waiting {
		c.stream.waiting = false
		c.stream.wait <- struct{}{}
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

func translateResultsFull(id string, results [][]*cloudwatchlogs.ResultField) ([]Result, error) {
	var err error
	block := make([]Result, len(results))
	for i, r := range results {
		block[i], err = translateResult(id, r)
	}
	return block, err
}

func translateResultsPart(c *chunk, results [][]*cloudwatchlogs.ResultField) ([]Result, error) {
	guess := int(c.stream.Hint) - len(c.ptr)
	if guess <= len(results) {
		guess = len(results)
	}
	block := make([]Result, 0, guess)
	for _, r := range results {
		var ptr *string
		for _, f := range r {
			k, v := f.Field, f.Value
			if k == nil {
				return nil, errNoKey(c.id)
			}
			if *k != "@ptr" {
				continue
			}
			ptr = v
			break
		}
		if ptr == nil {
			return nil, errNoPtr(c.id)
		}
		if !c.ptr[*ptr] {
			rr, err := translateResult(c.id, r)
			if err != nil {
				return nil, err
			}
			block = append(block, rr)
			c.ptr[*ptr] = true
		}
	}
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

func errNoPtr(id string) error {
	return fmt.Errorf("incite: query chunk %q: no @ptr in result", id)
}

func errNoKey(id string) error {
	return fmt.Errorf("incite: query chunk %q: foo", id)
}

func errNoValue(id, key string) error {
	return fmt.Errorf("incite: query chunk %q: no value for key %q", id, key)
}

func (m *mgr) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.closed {
		return ErrClosed
	}

	m.closed = true
	<-m.close
	return nil
}

const (
	minHint = 100
)

func (m *mgr) Query(q QuerySpec) (Stream, error) {
	// Validation that does not require lock.
	q.Text = strings.TrimSpace(q.Text)
	if q.Text == "" {
		return nil, errors.New("incite: blank query text")
	}

	q.Start = q.Start.UTC()
	if hasSubSecond(q.Start) {
		return nil, errors.New("incite: start has sub-second granularity")
	}
	q.End = q.End.UTC()
	if hasSubSecond(q.End) {
		return nil, errors.New("incite: end has sub-second granularity")
	}
	if !q.End.After(q.Start) {
		return nil, errors.New("incite: end not before start")
	}

	if len(q.Groups) == 0 {
		return nil, errors.New("incite: no log groups")
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
	if int64(d)%n != 0 {
		n++
	}

	if q.Hint < minHint {
		q.Hint = minHint
	}

	chunkHint := int64(q.Hint) / n
	if chunkHint < minHint {
		chunkHint = minHint
	}

	// Acquire lock.
	m.lock.Lock()
	defer m.lock.Unlock()

	// Fail if the manager is already closed.
	if m.closed {
		return nil, ErrClosed
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &stream{
		QuerySpec: q,

		ctx:       ctx,
		cancel:    cancel,
		n:         n,
		chunkHint: uint16(chunkHint),
		groups:    groups,

		next: q.Start,
	}

	heap.Push(&m.pq, s)
	if m.waiting {
		m.waiting = false
		m.query <- struct{}{}
	}

	return s, nil
}

func hasSubSecond(t time.Time) bool {
	return t.Nanosecond() != 0
}

func (m *mgr) GetStats() Stats {
	m.lock.RLock()
	defer m.lock.RUnlock()
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
	lock sync.RWMutex // TODO: Do we ever use read part?

	// Mutable fields controlled by stream using lock.
	stats   Stats
	blocks  [][]Result
	m       int64         // Number of chunks completed
	i, j    int           // Block index and position within block
	wait    chan struct{} // Used to block a Read pending more blocks
	waiting bool          // True if and only if the stream is blocked on wait
	err     error         // Error to return, if any
}

func (s *stream) Close() error {
	if !s.setErr(ErrClosed, true) {
		return ErrClosed
	}

	s.cancel()
	s.blocks, s.i, s.j = nil, 0, 0
	return nil
}

func (s *stream) Read(r []Result) (int, error) {
	for {
		n, err := s.read(r)
		if !s.waiting {
			return n, err
		}

		select {
		case <-s.ctx.Done():
		case <-s.wait:
		}
	}
}

func (s *stream) GetStats() Stats {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.stats
}

func (s *stream) read(r []Result) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

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

	s.waiting = s.err == nil && n == 0 && len(r) > 0
	return n, s.err
}

func (s *stream) setErr(err error, lock bool) bool {
	if lock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if s.err == ErrClosed {
		return false
	}

	s.err = err
	return true
}

func (s *stream) addChunkStats(stats Stats) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.stats.Add(stats)
}

func (s *stream) alive() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.err == nil
}

// A chunk represents a single active CloudWatch Logs Insights query
// owned by a stream. A stream has one or more chunks, depending on
// whether the QuerySpec operation was chunked. A chunk is a passive data
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
	errClosing = errors.New("incite: closed")
)
