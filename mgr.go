// Copyright 2022 The incite Authors. All rights reserved.
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
	"time"
)

type mgr struct {
	Config

	// Fields owned exclusively by the mgr loop goroutine.
	close       chan struct{} // Receives notification on Close()
	pq          streamHeap    // Written by Query, written by mgr loop goroutine
	ready       ring.Ring     // Chunks ready to start
	numReady    int           // Number of chunks ready to start
	numStarting int           // Number of chunks handed off to starter
	numPolling  int           // Number of chunks handed off to poller
	numStopping int           // Number of chunks handed off to stopper

	// Fields written by arbitrary goroutines.
	query     chan *stream // Receives notification of new Query()
	queryLock sync.Mutex

	// Fields for communicating with workers.
	start  chan *chunk // Sends chunks to starter
	poll   chan *chunk // Sends chunks to poller
	stop   chan *chunk // Sends chunks to stopper
	update chan *chunk // Receives chunks from starter, poller, and stopper

	// Fields written by mgr loop and potentially read by any goroutine.
	stats     Stats        // Read by GetStats, written by mgr loop goroutine
	statsLock sync.RWMutex // Controls access to stats

	// Worker references. Not strictly necessary, and primarily here to
	// make it easier to observe state while debugging tests.
	starter *starter
	poller  *poller
	stopper *stopper
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

		close: make(chan struct{}),
		query: make(chan *stream),

		start: make(chan *chunk, cfg.Parallel),
		poll:  make(chan *chunk, cfg.Parallel),
		stop:  make(chan *chunk, cfg.Parallel),

		// All three workers send back their updates on the update
		// channel. To prevent deadlock, the channel buffer needs
		// to be big enough to receive all possible chunks that all
		// three workers could have in flight at the same time.
		update: make(chan *chunk, 3*cfg.Parallel),
	}

	if m.Name == "" {
		m.Name = fmt.Sprintf("%p", m)
	}

	go m.loop()

	m.starter = newStarter(m)
	go m.starter.loop()
	m.poller = newPoller(m)
	go m.poller.loop()
	m.stopper = newStopper(m)
	go m.stopper.loop()

	return m
}

func (m *mgr) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrClosed
		}
	}()

	m.queryLock.Lock()
	defer m.queryLock.Unlock()
	close(m.close)
	close(m.query)
	return
}

func (m *mgr) GetStats() Stats {
	m.statsLock.RLock()
	defer m.statsLock.RUnlock()
	return m.stats
}

func (m *mgr) Query(q QuerySpec) (s Stream, err error) {
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

	var n int64 = 1
	if q.Chunk != d {
		x := q.Start.Truncate(q.Chunk)
		y := q.End.Add(q.Chunk - 1).Truncate(q.Chunk)
		n = int64(y.Sub(x) / q.Chunk)
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
		return nil, errors.New(splitUntilWithoutMaxLimitMsg)
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

	m.queryLock.Lock()
	defer m.queryLock.Unlock()
	m.query <- ss

	return ss, nil
}

func (m *mgr) loop() {
	defer m.shutdown()

	m.logEvent("", "started")

	for {
		select {
		case s := <-m.query:
			if s == nil {
				return
			}
			m.addQuery(s)
		case c := <-m.update:
			m.handleChunk(c)
		case <-m.close:
			return
		}

		for m.numStarting+m.numPolling+m.numStopping < m.Parallel {
			c := m.getReadyChunk()
			if c == nil {
				break
			}
			c.state = starting
			m.numStarting++
			m.start <- c
		}
	}
}

func (m *mgr) shutdown() {
	// Log start of shutdown process.
	m.logEvent("", "stopping...")

	// Close the start and poll channels, causing starter and poller to
	// shut down.
	close(m.start)
	close(m.poll)

	// Drain all chunks from starter and poller.
	for m.numStarting+m.numPolling > 0 {
		c := <-m.update
		switch c.state {
		case started:
			m.stopChunk(c)
			fallthrough
		case starting:
			m.numStarting--
			c.stream.setErr(ErrClosed, true, Stats{})
		case polling:
			m.stopChunk(c)
			fallthrough
		case complete:
			m.numPolling--
			c.stream.setErr(ErrClosed, true, Stats{})
		case stopping, stopped:
			m.numStopping--
		}
	}

	// Close all open streams.
	for _, s := range m.pq {
		s.setErr(ErrClosed, true, Stats{})
	}

	// Close the stop channel, causing the stopper to shut down.
	close(m.stop)

	// Drain all chunks from stopper.
	for m.numStopping > 0 {
		<-m.update
		m.numStopping--
	}

	// Log a final stop event.
	m.logEvent("", "stopped")
}

func (m *mgr) addQuery(s *stream) {
	heap.Push(&m.pq, s)
	s.lock.RLock()
	defer s.lock.RUnlock()
	m.addStats(&Stats{
		RangeRequested: s.stats.RangeRequested,
	})
}

func (m *mgr) handleChunk(c *chunk) {
	switch c.state {
	case starting:
		m.numStarting--
		c.started()
		m.killStream(c)
	case started:
		m.numStarting--
		m.numPolling++
		c.started()
		c.state = polling
		m.poll <- c
	case polling:
		m.numPolling--
		m.handlePollingError(c)
	case complete:
		m.numPolling--
		m.handleChunkCompletion(c)
	case stopping, stopped:
		m.numStopping--
	default:
		panic("unexpected chunk state")
	}
}

func (m *mgr) makeReady(c *chunk) {
	r := ring.New(1)
	r.Value = c
	m.ready.Prev().Link(r)
	m.numReady++
}

func (m *mgr) handlePollingError(c *chunk) {
	if c.err == errRestartChunk {
		c.chunkID += "R"
		c.err = nil
		m.makeReady(c)
		return
	}

	if c.err == errSplitChunk {
		m.splitChunk(c)
		c.err = nil
		m.handleChunkCompletion(c)
		return
	}

	if c.err == errStopChunk {
		m.logChunk(c, "owning stream died, will stop", "")
		m.stopChunk(c)
		return
	}

	switch err := c.err.(type) {
	case *UnexpectedQueryError:
		m.logChunk(c, "unexpected error, will stop", err.Cause.Error())
		m.stopChunk(c)
	case *TerminalQueryStatusError:
		m.logChunk(c, "terminal status for", err.Status)
	}

	c.Stats.RangeFailed += c.duration()
	m.killStream(c)
}

func (m *mgr) handleChunkCompletion(c *chunk) {
	c.stream.m++
	if c.stream.m == c.stream.n {
		c.err = io.EOF
	}

	m.logChunk(c, "completed", "")
	m.killStream(c)
}

func (m *mgr) killStream(c *chunk) {
	m.statsLock.Lock()
	defer m.statsLock.Unlock()
	c.stream.lock.Lock()
	defer c.stream.lock.Unlock()
	m.stats.add(&c.Stats)
	c.stream.setErr(c.err, false, c.Stats)
}

func (m *mgr) addStats(t *Stats) {
	m.statsLock.Lock()
	defer m.statsLock.Unlock()
	m.stats.add(t)
}

func (m *mgr) getReadyChunk() *chunk {
	for m.numReady == 0 && len(m.pq) > 0 {
		s := heap.Pop(&m.pq).(*stream)
		if !s.alive() {
			continue
		}

		start, end := s.nextChunkRange()
		chunkID := strconv.Itoa(int(s.next))
		s.next++
		if s.next < s.n {
			heap.Push(&m.pq, s)
		}

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

	if m.numReady == 0 {
		return nil
	}

	r := m.ready.Next()
	m.ready.Unlink(1)
	m.numReady--
	return r.Value.(*chunk)
}

func (m *mgr) stopChunk(c *chunk) {
	c.state = stopping
	m.numStopping++
	m.stop <- c
}

// splitBits is the number of child chunks into which a parent chunk
// will be split, assuming the parent chunk range is at least splitBits
// seconds long. The minimum chunk size is one second, so a 4-second
// parent chunk will be split into four chunks, but a two-second child
// chunk will only be split into two child chunks.
const splitBits = 4

func (m *mgr) splitChunk(c *chunk) {
	frac := c.duration() / splitBits
	if frac < c.stream.SplitUntil {
		frac = c.stream.SplitUntil
	} else if hasSubSecondD(frac) {
		frac = frac + time.Second/2
		frac = frac.Round(time.Second)
	}

	children := make([]*chunk, 1, splitBits)
	child := c.split(c.start, frac, 0)
	children[0] = child
	for child.end.Before(c.end) {
		child = c.split(child.end, frac, len(children))
		children = append(children, child)
	}

	var b strings.Builder
	r := ring.New(len(children))
	r.Value = children[0]
	r = r.Next()
	_, _ = fmt.Fprintf(&b, "in %d sub-chunks... ", len(children))
	_, _ = b.WriteString(children[0].chunkID)
	for i := 1; i < len(children); i++ {
		r.Value = children[i]
		r = r.Next()
		b.WriteString(" / ")
		b.WriteString(children[i].chunkID)
	}

	m.logChunk(c, "split", b.String())
	c.stream.n += int64(len(children))
	m.numReady += len(children)
	m.ready.Prev().Link(r)
}

func (m *mgr) logEvent(worker, event string) {
	if worker == "" {
		m.Logger.Printf("incite: QueryManager(%s) %s", m.Name, event)
	} else {
		m.Logger.Printf("incite: QueryManager(%s) %s %s", m.Name, worker, event)
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
