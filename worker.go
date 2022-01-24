// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"container/ring"
	"context"
	"strconv"
)

// A manipulator specializes a generic worker, allowing the worker to
// manipulate chunks.
type manipulator interface {
	context(*chunk) context.Context
	manipulate(*chunk) bool
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
	m           *mgr          // Owning mgr
	regulator                 // Used to rate limit the work loop
	in          <-chan *chunk // Provides chunks to the worker
	out         chan<- *chunk // Receives chunks manipulated or released by the worker
	chunks      ring.Ring     // In-progress chunks
	numChunks   int           // Number of in-progress chunks
	name        string        // Worker name for logging purposes
	maxTry      int           // Maximum number of failed manipulations per chunk
	manipulator manipulator   // Specializes the worker
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
		ok := w.manipulator.manipulate(c)
		if !ok && c.try < w.maxTry {
			w.push(c)
		} else if !ok {
			w.m.logChunk(c, "exceeded max tries", strconv.Itoa(w.maxTry))
			w.out <- c
		} else {
			w.out <- c
		}
	}
}

func (w *worker) shutdown() {
	w.m.logEvent(w.name, "stopping...")
	w.chunks.Do(func(i interface{}) {
		if i != nil {
			c := i.(*chunk)
			w.manipulator.release(c)
			w.out <- c
		}
	})
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
