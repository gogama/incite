// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"container/ring"
	"context"
	"strconv"
)

type manipulator interface {
	context(*chunk) context.Context
	manipulate(*chunk) bool
	release(*chunk)
}

type worker struct {
	m *mgr
	regulator
	in          <-chan *chunk
	out         chan<- *chunk
	chunks      ring.Ring
	numChunks   int
	name        string
	maxTry      int
	manipulator manipulator
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
		// TODO: For poll loop, chunk context might be dead. But
		//       manipulator *should* figure this out anyway as GetQueryResultsWithContext
		//       will short-circuit fail, or should anyway. But verify this.
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
	if w.numChunks == 0 {
		return nil
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
