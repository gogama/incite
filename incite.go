package incite

import (
	"container/ring"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

type QuerySpec struct {
	Text  string
	Start time.Time
	End   time.Time

	// TODO: Allow Chunk to be zero (no chunks), but QuerySpec() validation step needs
	//       to correct it, in that case, to be the full size of the time range.
	Chunk    time.Duration
	Preview  bool
	Priority int
	Hint     uint16 // Expected result count, used to optimize memory allocation.
}

type Queryer interface {
	Query(q QuerySpec) (Stream, error)
}

type Stats struct {
	BytesScanned   float64
	RecordsMatched float64
	RecordsScanned float64
}

type StatsGetter interface {
	GetStats() Stats
}

// QueryManager manages CloudWatch Insights queries for one underlying
// CloudWatch Logs connection. Use NewQueryManager to create a new query
// manager instance.
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
	Stats // TODO: Put in plumbing to update this.

	timer    *time.Timer
	ding     bool
	minDelay map[CloudWatchLogsAction]time.Duration // Used to stay under TPS limit
	lastReq  map[CloudWatchLogsAction]time.Time     // Used to stay under TPS limit

	chunks    ring.Ring // Circular list of chunks, first item is a sentry
	numChunks int

	lock    sync.RWMutex // TODO: Is anyone using the read capability of this lock?
	pq      streamHeap
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

	// Close all open streams.
	for _, s := range m.pq {
		s.setErr(ErrClosed, true)
	}

	// On a best effort basis, close all open chunks.
	m.chunks.Do(func(i interface{}) {
		if i != nil {
			m.cancelChunk(i.(*chunk))
		}
	})

	// Free any other resources.
	m.timer.Stop()
	close(m.close)
	close(m.query)
}

func (m *mgr) setTimer(d time.Duration) bool {
	// TODO: FIXME: Are they holding the lock here? Yes or no.
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
	// TODO: FIXME: Are they holding the lock here? Yes or no.
	minDelay := m.minDelay[action]
	delaySoFar := time.Now().Sub(m.lastReq[action])
	return m.timer.Reset(minDelay - delaySoFar)
}

func (m *mgr) startNextChunks() int {
	m.lock.Lock()
	defer m.lock.Unlock()

	var numStarted int
	for len(m.pq) > 0 && m.chunks.Len() <= m.Parallel {
		break

		err := m.startNextChunk()
		if err == errClosing {
			return -1
		}

		numStarted++
	}

	return numStarted
}

func (m *mgr) startNextChunk() error {

	// TODO: Take the highest priority stream from the front of the queue.
	// TODO: Wait until we can start the next chunk using the RPS timer (if we
	//       get closed in the process of waiting then return -1).
	// TODO: Start the chunk.
	// TODO: If we got throttled due to too many open queries, (or regular
	//       throttling) just log that and put the stream back into the PQ.
	// TODO: If we got another type of more fatally error, log it, wrap the
	//       error and put it on the stream, and DO NOT replace the stream in
	//       the PQ (it is now dead).
	// TODO: If it was success, create the chunk and put the chunk into the
	//       chunk polling ring.

	return nil
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
	case cloudwatchlogs.QueryStatusComplete:
		return sendChunkBlock(c, output.Results, true)
	case cloudwatchlogs.QueryStatusRunning:
		return sendChunkBlock(c, output.Results, false)
	case cloudwatchlogs.QueryStatusCancelled, cloudwatchlogs.QueryStatusFailed, "Timeout":
		return fmt.Errorf("incite: query chunk %q: unexpected terminal status: %s", c.id, c.status)
	default:
		return fmt.Errorf("incite: query chunk %q: unhandled status: %s", c.id, c.status)
	}
}

func (m *mgr) cancelChunk(c *chunk) { // TODO: Is lock expected to be held here.
	if m.setTimerRPS(StopQuery) {
		<-m.timer.C
		m.ding = true
	}
	_, _ = m.Actions.StopQueryWithContext(context.Background(), &cloudwatchlogs.StopQueryInput{
		QueryId: &c.id,
	})
	m.lastReq[StopQuery] = time.Now()
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

func sendChunkBlock(c *chunk, results [][]*cloudwatchlogs.ResultField, done bool) error {
	var block []Result
	var err error

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
		c.stream.rem--
		if c.stream.rem == 0 && err == nil {
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
	if guess <= 0 {
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

func (m *mgr) Query(q QuerySpec) (Stream, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.closed {
		return nil, ErrClosed
	}

	// TODO: validate and fixup.
	// TODO: part of validation must be to prevent End <= Start or Chunk <= 0.

	ctx, cancel := context.WithCancel(context.Background())
	d := q.End.Sub(q.Start)
	rem := d / q.Chunk
	if d%rem != 0 {
		rem++
	}
	s := &stream{
		QuerySpec: q,
		ctx:       ctx,
		cancel:    cancel,
		next:      q.Start,
		rem:       int64(rem),
	}

	m.pq.Push(s)
	// FIXME: This should get a Read lock on m.lock to correctly follow Go memory model: https://golang.org/ref/mem
	if m.waiting {
		m.waiting = false
		m.query <- struct{}{}
	}

	return s, nil
}

func (m *mgr) GetStats() Stats {
	return m.Stats
}

type streamHeap []*stream

func (h streamHeap) Len() int {
	return len(h)
}

func (h streamHeap) Less(i, j int) bool {
	h[i].lock.RLock()
	defer h[i].lock.RUnlock()
	h[j].lock.RLock()
	defer h[j].lock.RUnlock()

	if h[i].err != nil && h[j].err == nil {
		return true
	} else if h[j].err != nil {
		return false
	} else if h[i].Priority < h[j].Priority {
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
	ctx     context.Context    // stream context used to parent chunk contexts
	cancel  context.CancelFunc // cancels ctx when the stream is closed
	lock    sync.RWMutex       // mgr uses read lock to priority sort the streams
	next    time.Time          // Next chunk start time
	blocks  [][]Result
	rem     int64         // Number of chunks remaining
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
	return Stats{} // TODO.
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
	stream *stream         // Owning stream which receives results of the chunk
	ctx    context.Context // Child of the stream's context owned by this chunk
	id     string          // Insights query ID
	status string          // Insights query status
	ptr    map[string]bool // Set of already viewed @ptr, nil if QuerySpec not previewable
}

var (
	errClosing = errors.New("incite: closed")
)
