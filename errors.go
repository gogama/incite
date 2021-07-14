package incite

import (
	"errors"
	"fmt"
)

var (
	// ErrClosed is the error returned by a read or query operation
	// when the underlying stream or query manager has been closed.
	ErrClosed = errors.New("incite: operation on a closed object")
)

type wrappedErr struct {
	cause error
	msg   string
}

func wrap(cause error, format string, a ...interface{}) error {
	return &wrappedErr{
		cause: cause,
		msg:   fmt.Sprintf(format, a...),
	}
}

func (w *wrappedErr) Error() string {
	return w.msg + ": " + w.cause.Error()
}

func (w *wrappedErr) Unwrap() error {
	return w.cause
}

const (
	nilActionsMsg = "incite: nil actions"
	nilReaderMsg  = "incite: nil reader"
)
