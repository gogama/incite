package incite

import (
	"io"
)

// ReadAll reads from s until an error or EOF and returns the data it
// read. A successful call returns err == nil, nor err == EOF. Because
// ReadAll is defined to read from r until EOF, it does not treat an EOF
// from Read as an error to be reported.
func ReadAll(r Reader) ([]Result, error) {
	if r == nil {
		panic(nilReaderMsg)
	}

	// For interests' sake, this implementation is borrowed almost
	// char-for-char from io.ReadAll.

	x := make([]Result, 0, 512)
	for {
		if len(x) == cap(x) {
			// Add more capacity (let append pick now much).
			x = append(x, Result{})[:len(x)]
		}
		n, err := r.Read(x[len(x):cap(x)])
		x = x[:len(x)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return x, err
		}
	}
}
