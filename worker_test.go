// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"errors"
	"fmt"
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
		w, l, m, in, _, _ := newTestableWorker(t, 1, 1, false)
		w.expectLoopLogs(l)
		close(in)

		w.loop()

		l.AssertExpectations(t)
		m.AssertExpectations(t)
	})

	t.Run("Chunk Is Pushed When Manipulate Has Inconclusive Outcome", func(t *testing.T) {
		w, l, m, in, out, _ := newTestableWorker(t, 500_000, 0, false)
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
			Return(inconclusive).
			Once()
		m.On("release", c).Once()

		w.loop()
		d := <-out2

		l.AssertExpectations(t)
		m.AssertExpectations(t)
		assert.Same(t, c, d)
	})

	t.Run("Chunk Is Pushed When Manipulate Has Temporary Error and Retries Not Exceeded", func(t *testing.T) {
		w, l, m, in, out, _ := newTestableWorker(t, 100_000, 2, false)
		out2 := make(chan *chunk)
		w.expectLoopLogs(l)
		c := &chunk{
			stream:  &stream{},
			chunkID: "foo chunk",
			err:     &UnexpectedQueryError{Cause: errors.New("test error")},
		}

		go func() {
			in <- c
		}()
		go func() {
			d := <-out
			out2 <- d
		}()
		l.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", w.m.Name, "worker:"+t.Name()+" temporary error", "foo chunk", "", time.Time{}, time.Time{}, "test error")
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

	t.Run("Chunk Is Sent to Out Channel When Manipulate Has Temporary Error and Retries Exceeded", func(t *testing.T) {
		w, l, m, in, out, _ := newTestableWorker(t, 100_000, 1, false)
		out2 := make(chan *chunk)
		w.expectLoopLogs(l)
		c := &chunk{
			stream: &stream{},
			err:    &UnexpectedQueryError{Cause: errors.New("a very problematic error")},
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

	t.Run("RPS Are Reduced When Adaptation Is Enabled And Manipulate Has Throttling Error", func(t *testing.T) {
		w, l, m, in, out, _ := newTestableWorker(t, 100_000, 2, true)
		w.expectLoopLogs(l)
		c := &chunk{
			stream: &stream{},
			err:    &UnexpectedQueryError{Cause: errors.New("simmer down there")},
		}

		go func() {
			in <- c
		}()
		go func() {
			<-out
		}()
		l.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", w.m.Name, "worker:"+t.Name()+" throttled", "", "", time.Time{}, time.Time{}, "reduced RPS to 99999.0000")
		m.On("context", c).Return(context.Background()).Once()
		m.On("manipulate", c).
			Run(func(_ mock.Arguments) {
				close(in)
			}).
			Return(throttlingError).
			Once()
		m.On("release", c).Once()

		w.loop()

		l.AssertExpectations(t)
		m.AssertExpectations(t)
		assert.Equal(t, 99_999.0, w.regulator.rps.value())
	})

	t.Run("RPS Are Increased When Adaptation Is Enabled And Manipulate Has a Non-Throttled Outcome", func(t *testing.T) {
		for _, o := range []outcome{finished, inconclusive, temporaryError} {
			t.Run(o.String(), func(t *testing.T) {
				w, l, m, in, out, _ := newTestableWorker(t, 100_000, 2, true)
				w.regulator.rps.decrease() // Decrease RPS by 1 unit to 99,999.
				require.Equal(t, 99_999.0, w.regulator.rps.value())
				lastBefore := standardAdapterLast(t, w.regulator.rps)
				w.expectLoopLogs(l)
				c := &chunk{
					stream: &stream{},
					err:    &UnexpectedQueryError{Cause: errors.New("reduce speed")},
				}

				go func() {
					in <- c
				}()
				go func() {
					<-out
				}()
				if o == temporaryError {
					l.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", w.m.Name)
				}
				l.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", w.m.Name, stringPrefix("worker:"+t.Name()+" increased RPS to 99999."), "", "", time.Time{}, time.Time{})
				m.On("context", c).Return(context.Background()).Once()
				m.On("manipulate", c).
					Run(func(_ mock.Arguments) {
						close(in)
					}).
					Return(o).
					Once()
				if o != finished {
					m.On("release", c).Once()
				}

				w.loop()

				l.AssertExpectations(t)
				m.AssertExpectations(t)
				lastAfter := standardAdapterLast(t, w.regulator.rps)
				assert.False(t, lastAfter.Before(lastBefore))
				expectedRPS := 99_999.0 + float64(lastAfter.Sub(lastBefore))/float64(time.Second)*rpsUpPerS
				assert.Equal(t, expectedRPS, w.regulator.rps.value())
				assert.Equal(t, time.Duration(float64(time.Second)/expectedRPS), w.regulator.minDelay)
			})
		}
	})

	t.Run("Chunk Is Pushed When Manipulate Has Throttling Error and Retries Not Exceeded", func(t *testing.T) {
		w, l, m, in, out, _ := newTestableWorker(t, 100_000, 2, false)
		out2 := make(chan *chunk)
		w.expectLoopLogs(l)
		c := &chunk{
			stream:  &stream{},
			chunkID: "bar chunk",
			err:     &UnexpectedQueryError{Cause: errors.New("throttling error")},
		}

		go func() {
			in <- c
		}()
		go func() {
			d := <-out
			out2 <- d
		}()
		l.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", w.m.Name, "worker:"+t.Name()+" throttled", "bar chunk", "", time.Time{}, time.Time{})
		m.On("context", c).Return(context.Background()).Once()
		m.On("manipulate", c).
			Run(func(_ mock.Arguments) {
				close(in)
			}).
			Return(throttlingError).
			Once()
		m.On("release", c).Once()

		w.loop()
		d := <-out2

		l.AssertExpectations(t)
		m.AssertExpectations(t)
		assert.Same(t, c, d)
		assert.Equal(t, 100_000.0, w.regulator.rps.value())
	})

	t.Run("Chunk Is Sent to Out Channel When Manipulate Has Throttling Error and Retries Exceeded", func(t *testing.T) {
		w, l, m, in, out, _ := newTestableWorker(t, 100_000, 1, false)
		out2 := make(chan *chunk)
		w.expectLoopLogs(l)
		c := &chunk{
			stream: &stream{},
			err:    &UnexpectedQueryError{Cause: errors.New("please slow it right down")},
		}

		go func() {
			in <- c
		}()
		go func() {
			d := <-out
			out2 <- d
		}()
		l.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s)", w.m.Name, "worker:"+t.Name()+" throttled", "", "", time.Time{}, time.Time{})
		l.expectPrintf("incite: QueryManager(%s) %s chunk %s %q [%s..%s): %s", w.m.Name, "worker:"+t.Name()+" exceeded max tries for", "", "", time.Time{}, time.Time{}, "1")
		m.On("context", c).Return(context.Background()).Once()
		m.On("manipulate", c).
			Run(func(_ mock.Arguments) {
				close(in)
			}).
			Return(throttlingError).
			Once()

		w.loop()
		d := <-out2

		l.AssertExpectations(t)
		m.AssertExpectations(t)
		assert.Same(t, c, d)
		assert.Equal(t, 100_000.0, w.regulator.rps.value())
	})

	t.Run("Chunk Is Sent to Out Channel When Manipulate Succeeds", func(t *testing.T) {
		w, l, m, in, out, _ := newTestableWorker(t, 100_000, 1, false)
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
		w, l, m, in, out, closer := newTestableWorker(t, 1, 1, false)
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
		w, _, _, in, _, _ := newTestableWorker(t, 1, 1, false)
		expected := &chunk{}
		go func() {
			in <- expected
		}()

		actual := w.pop()

		assert.Same(t, expected, actual)
	})

	t.Run("Empty Pop, Closed", func(t *testing.T) {
		w, _, _, in, _, _ := newTestableWorker(t, 1, 1, false)
		close(in)

		actual := w.pop()

		assert.Nil(t, actual)
	})

	t.Run("Single Push, Pop without Receive", func(t *testing.T) {
		w, _, _, _, _, _ := newTestableWorker(t, 1, 1, false)
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
		w, _, _, in, _, _ := newTestableWorker(t, 1, 1, false)
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

func newTestableWorker(t *testing.T, rps, tmp int, adapt bool) (w *worker, l *mockLogger, m *mockManipulator, in, out chan *chunk, closer chan struct{}) {
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
		regulator:         makeRegulator(closer, rps, 0, adapt),
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

func (o outcome) String() string {
	switch o {
	case finished:
		return "finished"
	case inconclusive:
		return "inconclusive"
	case throttlingError:
		return "throttlingError"
	case temporaryError:
		return "temporaryError"
	default:
		panic(fmt.Sprintf("unknown outcome: %d", int(o)))
	}
}
