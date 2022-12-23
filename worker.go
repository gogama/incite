// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"container/ring"
	"context"
	"errors"
	"fmt"
	"strconv"
)

// A nextStep is an instruction returned from a manipulator's manipulate
// method which tells the worker what to do next.
type outcome int

const (
	// nothing indicates that the chunk manipulation had no work to do
	// and the chunk should be sent back down the worker's out channel.
	nothing outcome = iota
	// finished indicates that the chunk manipulation is finished and
	// the chunk should be sent back down the worker's out channel.
	finished
	// inconclusive indicates that the manipulation did not obtain a
	// final result and the manipulation should be retried.
	inconclusive
	// throttlingError indicates that the manipulation attempted a
	// service call which was throttled due to service RPS limits and
	// should be retried up to the worker's maximum try limit.
	throttlingError
	// temporaryError indicates that the manipulation encountered a
	// non-throttling temporary error which should be retried up to the
	// worker's maximum try limit.
	temporaryError
)

// A manipulator specializes a generic worker, allowing the worker to
// manipulate chunks.
type manipulator interface {
	context(*chunk) context.Context
	manipulate(*chunk) outcome
	release(*chunk)
}

// A worker maintains a goroutine, loop, which performs work on behalf
// of a mgr. The worker reads chunks from channel in, manipulates them,
// and when the manipulation is successful (or a maximum number of tries
// is reached), the worker sends the chunk back to the mgr on channel
// out.
//
// A worker may have several chunks in progress at once due to rate
// limiting or necessary retries. The worker keeps these in-progress
// chunks in the ring named chunks.
//
// The worker goroutine exits when one of two conditions is met: either
// its regulator's close channel is closed while waiting for the rate
// limiting timer; or the in channel is closed while the worker is
// trying to read the next chunk from it. Before exiting, the worker
// calls the manipulator's release method once for every in-progress
// chunk, and sends the in-progress chunk to channel out.
type worker struct {
	m                 *mgr          // Owning mgr
	regulator                       // Used to rate limit the work loop
	in                <-chan *chunk // Provides chunks to the worker
	out               chan<- *chunk // Receives chunks manipulated or released by the worker
	chunks            ring.Ring     // In-progress chunks
	numChunks         int           // Number of in-progress chunks
	name              string        // Worker name for logging purposes
	maxTemporaryError int           // Maximum number of temporary errors per chunk
	manipulator       manipulator   // Specializes the worker
}

func (w *worker) loop() {
	defer w.shutdown()

	w.m.logEvent(w.name, "started")

	for {
		c := w.pop()
		if c == nil {
			return
		}
		ctx := w.manipulator.context(c)
		err := w.wait(ctx)
		if err == errClosing {
			w.push(c)
			return // mgr is closing, so stop working
		}
		o := w.manipulator.manipulate(c)
		if o == throttlingError {
			var detail string
			if w.throttled() {
				detail = fmt.Sprintf("reduced RPS to %.4f", w.rps.value())
			}
			w.m.logChunk(c, w.name+" throttled", detail)
		} else if o != nothing && w.notThrottled() {
			w.m.logChunk(c, fmt.Sprintf(w.name+" increased RPS to %.4f", w.rps.value()), "")
		}
		switch o {
		case nothing, finished:
			w.out <- c
		case inconclusive:
			w.push(c)
		case temporaryError:
			w.m.logChunk(c, w.name+" temporary error", errors.Unwrap(c.err).Error())
			fallthrough
		case throttlingError:
			c.tmp++
			if c.tmp < w.maxTemporaryError {
				w.push(c)
			} else {
				w.m.logChunk(c, w.name+" exceeded max tries for", strconv.Itoa(c.tmp))
				w.out <- c
			}
		}
	}
}

func (w *worker) shutdown() {
	w.m.logEvent(w.name, "stopping...")

	// Release the chunks we already queued for manipulation.
	w.chunks.Do(func(i interface{}) {
		if i != nil {
			c := i.(*chunk)
			c.err = ErrClosed
			w.manipulator.release(c)
			w.out <- c
		}
	})

	// Release stray chunks in the input channel. These can arise due
	// to a race condition if this worker detected the closure of the
	// close channel before the mgr did, so the mgr had time to cram
	// another chunk down the worker's channel.
	for c := range w.in {
		c.err = ErrClosed
		w.manipulator.release(c)
		w.out <- c
	}

	w.timer.Stop()

	w.m.logEvent(w.name, "stopped")
}

func (w *worker) push(c *chunk) {
	r := ring.New(1)
	r.Value = c
	w.chunks.Prev().Link(r)
	w.numChunks++
}

func (w *worker) pop() *chunk {
	var c *chunk
	var closing bool
	if w.numChunks == 0 {
		c, closing = w.blockPop()
	} else {
		c, closing = w.noBlockPop()
	}
	if closing {
		return nil
	}
	if c != nil {
		c.try = 0
		c.tmp = 0
		w.push(c)
	}
	r := w.chunks.Next()
	c = r.Value.(*chunk)
	w.chunks.Unlink(1)
	w.numChunks--
	c.try++
	return c
}

func (w *worker) blockPop() (*chunk, bool) {
	c := <-w.in
	return c, c == nil
}

func (w *worker) noBlockPop() (*chunk, bool) {
	select {
	case c := <-w.in:
		return c, c == nil
	default:
		return nil, false
	}
}
