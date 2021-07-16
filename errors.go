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

func errNoKey(id string) error {
	return fmt.Errorf("incite: query chunk %q: foo", id)
}

func errNoValue(id, key string) error {
	return fmt.Errorf("incite: query chunk %q: no value for key %q", id, key)
}

const (
	nilActionsMsg = "incite: nil actions"
	nilReaderMsg  = "incite: nil reader"

	textBlankMsg         = "incite: blank query text"
	startSubSecondMsg    = "incite: start has sub-second granularity"
	endSubSecondMsg      = "incite: end has sub-second granularity"
	endNotBeforeStartMsg = "incite: end not before start"
	noGroupsMsg          = "incite: no log groups"
	exceededMaxLimitMsg  = "incite: exceeded MaxLimit"
)
