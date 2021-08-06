// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
)

func TestNopLogger_Printf(t *testing.T) {
	t.Run("Empty Format", func(t *testing.T) {
		NopLogger.Printf("foo")
	})

	t.Run("One Argument", func(t *testing.T) {
		NopLogger.Printf("hello, %q", "world")
	})

	t.Run("Two Arguments", func(t *testing.T) {
		NopLogger.Printf("the two numbers are %d and %d", 1, 2)
	})
}

type mockLogger struct {
	mock.Mock
}

func newMockLogger(t *testing.T) *mockLogger {
	m := &mockLogger{}
	m.Test(t)
	return m
}

func (m *mockLogger) Printf(format string, v ...interface{}) {
	m.Called(format, v)
}

func (m *mockLogger) ExpectPrintf(format string) *mock.Call {
	nPct := strings.Count(format, "%")
	return m.On("Printf", format, mock.MatchedBy(func(v []interface{}) bool {
		return len(v) == nPct
	})).Return()
}
