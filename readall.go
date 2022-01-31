// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"io"
)

// ReadAll reads from s until an error or EOF and returns the data it
// read. A successful call returns err == nil, not err == EOF. Because
// ReadAll is defined to read from s until EOF, it does not treat an EOF
// from Read as an error to be reported.
func ReadAll(s Stream) ([]Result, error) {
	if s == nil {
		panic(nilStreamMsg)
	}

	// For interests' sake, this implementation is borrowed almost
	// char-for-char from io.ReadAll.

	x := make([]Result, 0, 512)
	for {
		if len(x) == cap(x) {
			// Add more capacity (let append pick now much).
			x = append(x, Result{})[:len(x)]
		}
		n, err := s.Read(x[len(x):cap(x)])
		x = x[:len(x)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return x, err
		}
	}
}
