// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

// A Logger represents a logging object which can receive log messages
// from a QueryManager and send them to an output sink.
//
// Logger is compatible with *log.Logger. Therefore you may, for
// example, use log.DefaultLogger(), or any other *log.Logger value, as
// the Logger field of a Config structure when constructing a new
// QueryManager using the NewQueryManager function.
type Logger interface {
	// Printf sends a line of output to the logger. Arguments are
	// handled in the manner of fmt.Printf. If the formatted string
	// does not end in a newline, the logger implementation should
	// append a final newline.
	Printf(format string, v ...interface{})
}

type nopLogger int

func (nop nopLogger) Printf(_ string, _ ...interface{}) {}

// NopLogger is a Logger that ignores any messages sent to it.
var NopLogger = nopLogger(0)
