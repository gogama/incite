// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestWorker_LoopAndShutdown(t *testing.T) {
	t.Run("Loop Exits When In Channel Is Closed", func(t *testing.T) {
		w, l, m, in, _, _ := newTestableWorker(t, 1, 1)
		w.expectLoopLogs(l)
		close(in)

		w.loop()

		l.AssertExpectations(t)
		m.AssertExpectations(t)
	})

	t.Run("Chunk Is Pushed When Manipulate Fails and Retries Not Exceeded", func(t *testing.T) {
		w, l, m, in, out, _ := newTestableWorker(t, 100_000, 2)
		out2 := make(chan *chunk)
		w.expectLoopLogs(l)
		c := &chunk{}

		go func() {
			in <- c
		}()
		go func() {
			d := <-out
			out2 <- d
		}()
		m.On("context", c).Return(context.Background()).Once()
		m.On("manipulate", c).
			Run(func(_ mock.Arguments) {
				close(in)
			}).
			Return(temporaryError).
			Once()
		m.On("release", c).Once()

		w.loop()
		d := <-out2

		l.AssertExpectations(t)
		m.AssertExpectations(t)
		assert.Same(t, c, d)
	})

	t.Run("Chunk Is Sent to Out Channel When Manipulate Fails and Retries Exceeded", func(t *testing.T) {
		w, l, m, in, out, _ := newTestableWorker(t, 100_000, 1)
		out2 := make(chan *chunk)
		w.expectLoopLogs(l)
		c := &chunk{
			stream: &stream{},
		}

		go func() {
			in <- c
		}()
		go func() {
			d := <-out
			out2 <- d
		}()
		l.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", w.m.Name)
		m.On("context", c).Return(context.Background()).Once()
		m.On("manipulate", c).
			Run(func(_ mock.Arguments) {
				close(in)
			}).
			Return(temporaryError).
			Once()

		w.loop()
		d := <-out2

		l.AssertExpectations(t)
		m.AssertExpectations(t)
		assert.Same(t, c, d)
	})

	t.Run("Chunk Is Sent to Out Channel When Manipulate Succeeds", func(t *testing.T) {
		w, l, m, in, out, _ := newTestableWorker(t, 100_000, 1)
		out2 := make(chan *chunk)
		w.expectLoopLogs(l)
		c := &chunk{}

		go func() {
			in <- c
		}()
		go func() {
			d := <-out
			out2 <- d
		}()
		m.
			On("context", c).
			Run(func(_ mock.Arguments) {
				close(in)
			}).
			Return(context.Background()).Once()
		m.On("manipulate", c).Return(finished).Once()

		w.loop()
		d := <-out2

		l.AssertExpectations(t)
		m.AssertExpectations(t)
		assert.Same(t, c, d)
	})

	t.Run("Leftover Chunks in Channel are Released In Shutdown", func(t *testing.T) {
		w, l, m, in, out, closer := newTestableWorker(t, 1, 1)
		w.expectLoopLogs(l)
		n := 15
		readFromOut := make(map[int]bool, n)
		manipulated := make(map[int]bool, n)
		released := make(map[int]bool, n)
		var wg sync.WaitGroup
		wg.Add(n)

		go func() {
			for i := 0; i < n; i++ {
				in <- &chunk{
					chunkID: strconv.Itoa(i),
				}
			}
			close(in)
		}()
		go func() {
			for c := range out {
				require.NotNil(t, c)
				i, err := strconv.Atoi(c.chunkID)
				require.NoError(t, err)
				readFromOut[i] = true
				wg.Done()
			}
		}()
		m.
			On("context", mock.Anything).
			Return(context.Background())
		matcher := func(i int, m map[int]bool) interface{} {
			return mock.MatchedBy(func(c *chunk) bool {
				j, err := strconv.Atoi(c.chunkID)
				require.NoError(t, err)
				if j == i {
					m[i] = true
					return true
				}
				return false
			})
		}
		for i := 0; i < n; i++ {
			m.
				On("manipulate", matcher(i, manipulated)).
				Run(func(_ mock.Arguments) {
					w.lastReq = time.Now()
				}).
				Return(finished).
				Maybe()
			m.
				On("release", matcher(i, released)).
				Return().
				Maybe()
		}

		close(closer)
		w.loop()
		wg.Wait()
		close(out)

		l.AssertExpectations(t)
		m.AssertExpectations(t)
		assert.Len(t, readFromOut, n)
		assert.Equal(t, n, len(manipulated)+len(released))
	})
}

func TestWorker_PushAndPop(t *testing.T) {
	t.Run("Empty Pop, Not Closed", func(t *testing.T) {
		w, _, _, in, _, _ := newTestableWorker(t, 1, 1)
		expected := &chunk{}
		go func() {
			in <- expected
		}()

		actual := w.pop()

		assert.Same(t, expected, actual)
	})

	t.Run("Empty Pop, Closed", func(t *testing.T) {
		w, _, _, in, _, _ := newTestableWorker(t, 1, 1)
		close(in)

		actual := w.pop()

		assert.Nil(t, actual)
	})

	t.Run("Single Push, Pop without Receive", func(t *testing.T) {
		w, _, _, _, _, _ := newTestableWorker(t, 1, 1)
		expected := &chunk{}

		t.Run("Push", func(t *testing.T) {
			w.push(expected)

			assert.Equal(t, 1, w.numChunks)
		})

		t.Run("Pop", func(t *testing.T) {
			actual := w.pop()

			assert.Same(t, expected, actual)
			assert.Equal(t, 0, w.numChunks)
		})
	})

	t.Run("Single Push, Pop with Receive, Pop without Receive", func(t *testing.T) {
		w, _, _, in, _, _ := newTestableWorker(t, 1, 1)
		first, second := &chunk{}, &chunk{}
		var x, y *chunk

		t.Run("Push", func(t *testing.T) {
			w.push(first)

			assert.Equal(t, 1, w.numChunks)
		})

		t.Run("Pop with Receive", func(t *testing.T) {
			go func() {
				in <- second
			}()
			x = w.pop()

			assert.True(t, x == first || x == second)
			if x == second {
				assert.Same(t, x, second)
				assert.Equal(t, 1, w.numChunks)
			}
		})

		t.Run("Pop without Receive", func(t *testing.T) {
			y = w.pop()

			if x == first {
				assert.Same(t, second, y)
			} else {
				assert.Same(t, first, y)
			}
			assert.Equal(t, 0, w.numChunks)
		})
	})
}

func newTestableWorker(t *testing.T, rps, tmp int) (w *worker, l *mockLogger, m *mockManipulator, in, out chan *chunk, closer chan struct{}) {
	closer = make(chan struct{})
	l = newMockLogger(t)
	m = newMockManipulator(t)
	in = make(chan *chunk)
	out = make(chan *chunk)
	w = &worker{
		m: &mgr{
			Config: Config{
				Logger: l,
				Name:   "mgr:" + t.Name(),
			},
			close: closer,
		},
		regulator:         makeRegulator(closer, rps, 0),
		in:                in,
		out:               out,
		name:              "worker:" + t.Name(),
		maxTemporaryError: tmp,
		manipulator:       m,
	}
	return
}

type mockManipulator struct {
	mock.Mock
}

func newMockManipulator(t *testing.T) *mockManipulator {
	m := &mockManipulator{}
	m.Test(t)
	return m
}

func (m *mockManipulator) context(c *chunk) context.Context {
	args := m.Called(c)
	return args.Get(0).(context.Context)
}

func (m *mockManipulator) manipulate(c *chunk) outcome {
	args := m.Called(c)
	return args.Get(0).(outcome)
}

func (m *mockManipulator) release(c *chunk) {
	m.Called(c)
}

func (w *worker) expectLoopLogs(m *mockLogger) {
	m.expectPrintf("incite: QueryManager(%s) %s %s", w.m.Name, w.name, "started").Once()
	m.expectPrintf("incite: QueryManager(%s) %s %s", w.m.Name, w.name, "stopping...").Once()
	m.expectPrintf("incite: QueryManager(%s) %s %s", w.m.Name, w.name, "stopped").Once()
}
