// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"sync"
)

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
