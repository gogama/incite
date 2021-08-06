// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"
)

func TestReadAll(t *testing.T) {
	t.Run("Panic", func(t *testing.T) {
		assert.PanicsWithValue(t, nilStream, func() {
			_, _ = ReadAll(nil)
		})
	})

	t.Run("Error", func(t *testing.T) {
		expectedErr := errors.New("foo")
		m := newMockStream(t)
		m.
			On("Read", matchResultsSliceLen(1, maxLen)).Return(0, expectedErr).
			Once()

		rs, actualErr := ReadAll(m)

		m.AssertExpectations(t)
		assert.Error(t, actualErr)
		assert.Same(t, expectedErr, actualErr)
		assert.Equal(t, []Result{}, rs)
	})

	t.Run("Success", func(t *testing.T) {
		t.Run("ZeroEOF", func(t *testing.T) {
			m := newMockStream(t)
			m.
				On("Read", matchResultsSliceLen(1, maxLen)).
				Return(0, io.EOF).
				Once()

			rs, err := ReadAll(m)

			m.AssertExpectations(t)
			assert.NoError(t, err)
			assert.Equal(t, []Result{}, rs)
		})

		t.Run("PositiveEOF", func(t *testing.T) {
			var expectedData []Result
			m := newMockStream(t)
			m.On("Read", matchResultsSliceLen(1, maxLen)).
				Run(fillResultSlice(&expectedData, func(_ int) int { return 1 })).
				Return(1, io.EOF).
				Once()

			rs, err := ReadAll(m)

			m.AssertExpectations(t)
			assert.NoError(t, err)
			assert.Equal(t, []Result{r("@ptr", "0")}, rs)
		})

		t.Run("NothingHappened", func(t *testing.T) {
			m := newMockStream(t)
			m.
				On("Read", matchResultsSliceLen(1, maxLen)).
				Return(0, nil).
				Once()
			m.
				On("Read", matchResultsSliceLen(1, maxLen)).
				Return(0, io.EOF).
				Once()

			rs, err := ReadAll(m)

			m.AssertExpectations(t)
			assert.NoError(t, err)
			assert.Equal(t, []Result{}, rs)
		})

		t.Run("GotLessThanInitialCap", func(t *testing.T) {
			var expectedData []Result
			m := newMockStream(t)
			m.On("Read", matchResultsSliceLen(1, maxLen)).
				Run(fillResultSlice(&expectedData, func(n int) int { return n - 1 })).
				Return(lenFunc(func(n int) int { return n - 1 }), io.EOF).
				Once()

			rs, err := ReadAll(m)

			m.AssertExpectations(t)
			assert.NoError(t, err)
			assert.NotEmpty(t, rs)
			for i := range rs {
				assert.Equal(t, r("@ptr", strconv.Itoa(i)), rs[i], "mismatch at index %d", i)
			}
		})

		t.Run("GotExactlyInitialCap", func(t *testing.T) {
			var expectedData []Result
			m := newMockStream(t)
			m.On("Read", matchResultsSliceLen(1, maxLen)).
				Run(fillResultSlice(&expectedData, func(n int) int { return n })).
				Return(lenFunc(func(n int) int { return n }), io.EOF).
				Once()

			rs, err := ReadAll(m)

			m.AssertExpectations(t)
			assert.NoError(t, err)
			assert.NotEmpty(t, rs)
			for i := range rs {
				assert.Equal(t, r("@ptr", strconv.Itoa(i)), rs[i], "mismatch at index %d", i)
			}
		})

		t.Run("GotMoreThanInitialCap", func(t *testing.T) {
			var expectedData1, expectedData2 []Result
			m := newMockStream(t)
			m.On("Read", matchResultsSliceLen(1, maxLen)).
				Run(fillResultSlice(&expectedData1, func(n int) int { return n })).
				Return(lenFunc(func(n int) int { return n }), nil).
				Once()
			m.On("Read", matchResultsSliceLen(1, maxLen)).
				Run(fillResultSlice(&expectedData2, func(n int) int { return 1 })).
				Return(lenFunc(func(n int) int { return 1 }), io.EOF).
				Once()

			rs, err := ReadAll(m)

			m.AssertExpectations(t)
			assert.NoError(t, err)
			assert.NotEmpty(t, rs)
			assert.Len(t, rs, len(expectedData1)+1)
			for i := range expectedData1 {
				assert.Equal(t, r("@ptr", strconv.Itoa(i)), rs[i], "mismatch at index %d", i)
			}
			assert.Equal(t, r("@ptr", "0"), rs[len(expectedData1)], "mismatch at index %d", len(expectedData1))
		})
	})
}

type mockStream struct {
	mock.Mock
	Stream
}

func newMockStream(t *testing.T) *mockStream {
	m := &mockStream{}
	m.Test(t)
	return m
}

type lenFunc func(n int) int

func (m *mockStream) Read(r []Result) (int, error) {
	args := m.Called(r)
	var n int
	if f, ok := args.Get(0).(lenFunc); ok {
		n = f(len(r))
	} else {
		n = args.Int(0)
	}
	return n, args.Error(1)
}

const maxLen = int(^uint(0) >> 1)

func matchResultsSliceLen(min, max int) interface{} {
	return mock.MatchedBy(func(r []Result) bool {
		return len(r) >= min && len(r) <= max
	})
}

func fillResultSlice(expected *[]Result, lenFunc lenFunc) func(args mock.Arguments) {
	return func(args mock.Arguments) {
		buffer := args[0].([]Result)
		*expected = make([]Result, lenFunc(len(buffer)))
		for i := range *expected {
			ri := r("@ptr", strconv.Itoa(i))
			buffer[i] = ri
			(*expected)[i] = ri
		}
	}
}
