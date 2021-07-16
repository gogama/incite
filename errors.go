package incite

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrClosed is the error returned by a read or query operation
	// when the underlying stream or query manager has been closed.
	ErrClosed = errors.New("incite: operation on a closed object")
)

type StartQueryError struct {
	Text  string
	Start time.Time
	End   time.Time
	Cause error
}

func (err *StartQueryError) Error() string {
	return fmt.Sprintf("incite: CloudWatch Logs failed to start query for chunk %q [%s..%s): %s", err.Text, err.Start, err.End, err.Cause)
}

func (err *StartQueryError) Unwrap() error {
	return err.Cause
}

type TerminalQueryStatusError struct {
	QueryID string
	Status  string
	Text    string
}

func (err *TerminalQueryStatusError) Error() string {
	return fmt.Sprintf("incite: query %q has terminal status %q (text %q)", err.QueryID, err.Status, err.Text)
}

type UnexpectedQueryError struct {
	QueryID string
	Text    string
	Cause   error
}

func (err *UnexpectedQueryError) Error() string {
	return fmt.Sprintf("incite: query %q had unexpected error (text %q): %s", err.QueryID, err.Text, err.Cause)
}

func (err *UnexpectedQueryError) Unwrap() error {
	return err.Cause
}

func errNoKey(id string) error {
	return fmt.Errorf("incite: query chunk %q: foo", id)
}

func errNoValue(id, key string) error {
	return fmt.Errorf("incite: query chunk %q: no value for key %q", id, key)
}

func errNilStatus() error {
	return errors.New("incite: nil status in GetQueryResults output from CloudWatch Logs")
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
