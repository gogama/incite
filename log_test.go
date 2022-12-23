// Copyright 2022 The incite Authors. All rights reserved.
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

func (m *mockLogger) expectPrintf(format string, v ...interface{}) *mock.Call {
	nPct := strings.Count(format, "%")
	if len(v) > nPct {
		panic("more arguments given than percent signs in the format string!")
	}
	w := make([]interface{}, len(v))
	copy(w, v)
	return m.On("Printf", format, mock.MatchedBy(func(x []interface{}) bool {
		if len(x) != nPct {
			return false
		}
		for i := range w {
			if x[i] != w[i] && w[i] != mock.Anything {
				return false
			}
		}
		return true
	})).Return()
}
