package incite

import "fmt"

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
	nilReaderMsg = "incite: nil reader"
)
