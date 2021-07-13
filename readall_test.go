package incite

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"
)

func TestReadAll(t *testing.T) {
	t.Run("Panic", func(t *testing.T) {
		assert.PanicsWithValue(t, nilReaderMsg, func() {
			ReadAll(nil)
		})
	})

	t.Run("Error", func(t *testing.T) {
		expectedErr := errors.New("foo")
		m := newMockReader(t)
		m.On("Read", mock.Anything).Return(0, expectedErr)

		r, actualErr := ReadAll(m)

		assert.Error(t, actualErr)
		assert.Same(t, expectedErr, actualErr)
		assert.Equal(t, []Result{}, r)
	})

	t.Run("Success", func(t *testing.T) {
		// TODO: Add test cases here.
		// FIXME: The most important part is unfinished!
	})
}

type mockReader struct {
	mock.Mock
	Reader
}

func newMockReader(t *testing.T) *mockReader {
	m := &mockReader{}
	m.Test(t)
	return m
}

func (m *mockReader) Read(r []Result) (int, error) {
	args := m.Called(r)
	return args.Int(0), args.Error(1)
}
