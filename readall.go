package incite

import "io"

// ReadAll reads from s until an error or EOF and returns the data it read. A
// successful call returns err == nil, nor err == EOF. Because ReadAll is
// defined to read from s until EOF, it does not tread an EOF from Read as an
// error to be reported.
func ReadAll(s Stream) ([]Result, error) {
	if s == nil {
		panic(nilStreamMsg)
	}

	// For interests' sake, this implementation is stolen almost char-for-char
	// io.ReadAll.

	r := make([]Result, 0, 512)
	for {
		if len(r) == cap(r) {
			// Add more capacity (let append pick now much).
			r = append(r, Result{})[:len(r)]
		}
		n, err := s.Read(r[len(r):cap(r)])
		r = r[:len(r)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return r, err
		}
	}
}
