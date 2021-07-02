package incite

import (
	"container/ring"
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

var (
	ErrClosed        = errors.New("incite: operation on a closed object")
	ErrManagerClosed = errors.New("incite: query manager was closed")
)

type Config struct {
	Caps     CloudWatchLogsCaps
	Parallel int
	RPS      int
	Logger   log.Logger
}

type Query struct {
	Text    string
	Start   time.Time
	End     time.Time
	Chunk   time.Duration
	Preview bool
	Priority int
	Hint    uint16 // Expected result count, used to optimize memory allocation.
}

type QueryManager interface {
	io.Closer
	Query(q Query) (Stream, error)
}

type ResultField struct {
	Field string
	Value string
}

type Result struct {
	Ptr    string
	Fields []ResultField
}

type Stream interface {
	io.Closer
	Read([]Result) (int, error)
}

func ReadAll(s Stream) (int, error) {
	// foo
}

type mgr struct {
	Config

	timer   *time.Timer
	lastReq time.Time // Used to stay under TPS limit
	ding    bool

	chunks ring.Ring // Circular list of chunks, first item is a sentry
	numChunks int

	lock    sync.RWMutex
	pq      streamHeap
	close   chan struct{}
	closed  bool
	query   chan struct{}
	waiting bool
}

func NewQueryManager(cfg Config) QueryManager {
	// TODO: Validate and update Config here.
	m := &mgr{
		Config: cfg,

		timer: time.NewTimer(1<<63 - 1),
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
	// Close all open streams.
	for _, s := range m.pq {
		s.setErr(ErrManagerClosed, true)
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
	if !m.ding && !m.timer.Stop() {
		<-m.timer.C
	} else {
		m.ding = false
	}

	if d > 0 {
		m.timer.Reset(d)
		return true
	}

	m.timer.Reset(1<<63 -1)
	return false
}

func (m *mgr) setTimerRPS() bool {
	minDelay := time.Duration(m.RPS) / time.Second
	delaySoFar := time.Now().Sub(m.lastReq)
	return m.timer.Reset(minDelay - delaySoFar)
}

func (m *mgr) startNextChunks() (stopping bool) {
	started, stopping := m.startNextChunk()
	for started && !stopping {
		started, stopping = m.startNextChunk()
	}
	return stopping
}

func (m *mgr) startNextChunk() (started bool, stopping bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.canStartChunk() {
		return
	}
}

func (m *mgr) canStartChunk() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.pq) > 0 && m.chunks.Len() <= m.Parallel
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
	if m.setTimerRPS() {
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
	output, err := m.Caps.GetQueryResultsWithContext(c.ctx, &input)
	if err != nil {
		return wrap(err, "incite: failed to poll query chunk %q", c.id)
	}

	status := output.Status
	if status == nil {
		return TODO_MAKE_SUPERBAD_ERROR
	}

	c.status = *status
	switch c.status {
	case cloudwatchlogs.QueryStatusCancelled, cloudwatchlogs.QueryStatusFailed, "Timeout":
		return TODO_MAKE_SUPERBAD_ERROR
	case cloudwatchlogs.QueryStatusComplete, cloudwatchlogs.QueryStatusRunning:
		return sendChunkBlock(c, output.Results)
	}
}

func (m *mgr) cancelChunk(c *chunk) {
	if m.setTimerRPS() {
		<-m.timer.C
		m.ding = true
	}
	_, _ = m.Caps.StopQueryWithContext(context.Background(), &cloudwatchlogs.StopQueryInput{
		QueryId: &c.id,
	})
	m.lastReq = time.Now()
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
	} else if !m.setTimerRPS() {
		m.setTimer(time.Duration(m.RPS)/time.Second)
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

func sendChunkBlock(c *chunk, results [][]*cloudwatchlogs.ResultField) error {
	var block []Result
	var err error

	if c.ptr == nil {
		block, err = translateResultsFull(results)
	} else {
		block, err = translateResultsPart(c, results)
	}

	if c.status != cloudwatchlogs.QueryStatusComplete && len(block) == 0 && err != nil {
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

func translateResultsFull(results [][]*cloudwatchlogs.ResultField) ([]Result, error) {
	var err error
	block := make([]Result, len(results))
	for i, r := range results {
		block[i], err = translateResult(r)
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
				return block, // TODO: return error
			}
			if *k != "@ptr" {
				continue
			}
			ptr = v
			break
		}
		if ptr == nil {
			return block, // TODO: return error
		}
		if !c.ptr[*ptr] {
			rr, err := translateResultPtr(r, *ptr)
			if err != nil {
				return nil, err
			}
			block = append(block, rr)
			c.ptr[*ptr] = true
		}
	}
}

func translateResult(r []*cloudwatchlogs.ResultField) (Result, error) {
	rr := Result{
		Fields: make([]ResultField, len(r)),
	}
	for i, f := range r {
		k, v := f.Field, f.Value
		if k == nil {
			// TODO: return error
		}
		if v == nil {
			// TODO: return error
		}
		if rr.Ptr == "" && *k == "@ptr" {
			rr.Ptr = *k
		}
		rr.Fields[i] = ResultField{
			Field: *k,
			Value: *v,
		}
	}
	return rr, nil
}

func translateResultPtr(r []*cloudwatchlogs.ResultField, ptr string) (Result, error) {
	rr := Result{
		Ptr:    ptr,
		Fields: make([]ResultField, len(r)),
	}
	for i, f := range r {
		k, v := f.Field, f.Value
		if k == nil {
			// TODO: return error
		}
		if v == nil {
			// TODO: return error
		}
		rr.Fields[i] = ResultField{
			Field: *k,
			Value: *v,
		}
	}
	return rr, nil
}

func (m *mgr) Query(q Query) (Stream, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.closed {
		return nil, ErrClosed
	}

	// TODO: validate.
	ctx, cancel := context.WithCancel(context.Background())
	s := &stream{
		Query: q,
		ctx: ctx,
		cancel: cancel,
		next: something,
		rem: somthing else,
	}

	m.pq.Push(s)
	if m.waiting {
		m.waiting = false
		m.query <- struct{}{}
	}

	return s, nil
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
		// TODO: Give priority to a stream which has received less Query duration.
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
	Query
	ctx     context.Context    // stream context used to parent chunk contexts
	cancel  context.CancelFunc // cancels ctx when the stream is closed
	lock    sync.RWMutex       // mgr uses read lock to priority sort the streams
	next    time.Time          // Next chunk start time
	blocks  [][]Result
	rem     int           // Number of chunks remaining
	i, j    int           // Block index and position within block
	wait    chan struct{} // Used to block a Read pending more blocks
	waiting bool          // True if and only if the stream is blocked on wait
	err     error         // Error to return, if any
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

func (s *stream) Close() error {
	if !s.setErr(ErrClosed, true) {
		return ErrClosed
	}

	s.cancel()
	s.blocks, s.i, s.j = nil, 0, 0
	return nil
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
// whether the Query operation was chunked. A chunk is a passive data
// structure: it owns no goroutines and presents no interface that is
// accessible outside the package.
type chunk struct {
	stream *stream         // Owning stream which receives results of the chunk
	ctx    context.Context // Child of the stream's context owned by this chunk
	id     string          // Insights query ID
	status string          // Insights query status
	ptr    map[string]bool // Set of already viewed @ptr, nil if Query not previewable
}

var (
	errClosing = errors.New("incite: closed")
)
