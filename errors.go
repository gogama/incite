// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

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

// StartQueryError is returned by Stream.Read to indicate that the
// CloudWatch Logs service API returned a fatal error when attempting
// to start a chunk of the stream's query.
//
// When StartQueryError is returned by Stream.Read, the stream's query
// is considered failed and all subsequent reads on the stream will
// return an error.
type StartQueryError struct {
	// Text is the text of the query that could not be started.
	Text string
	// Start is the start time of the query chunk that could not be
	// started. If the query has more than one chunk, this could differ
	// from the value originally set in the QuerySpec.
	Start time.Time
	// End is the end time of the query chunk that could not be started
	// If the query has more than one chunk, this could differ from the
	// value originally set in the QuerySpec.
	End time.Time
	// Cause is the causing error, which will typically be an AWS SDK
	// for Go error type.
	Cause error
}

func (err *StartQueryError) Error() string {
	return fmt.Sprintf("incite: CloudWatch Logs failed to start query for chunk %q [%s..%s): %s", err.Text, err.Start, err.End, err.Cause)
}

func (err *StartQueryError) Unwrap() error {
	return err.Cause
}

// TerminalQueryStatusError is returned by Stream.Read when CloudWatch
// Logs Insights indicated that a chunk of the stream's query is in a
// failed status, such as Cancelled, Failed, or Timeout.
//
// When TerminalQueryStatusError is returned by Stream.Read, the
// stream's query is considered failed and all subsequent reads on the
// stream will return an error.
type TerminalQueryStatusError struct {
	// QueryID is the CloudWatch Logs Insights query ID of the chunk
	// that was reported in a terminal status.
	QueryID string
	// Status is the status string returned by CloudWatch Logs via the
	// GetQueryResults API action.
	Status string
	// Text is the text of the query that was reported in terminal
	// status.
	Text string
}

func (err *TerminalQueryStatusError) Error() string {
	return fmt.Sprintf("incite: query ID %q has terminal status %q [query text %q]", err.QueryID, err.Status, err.Text)
}

// UnexpectedQueryError is returned by Stream.Read when the CloudWatch
// Logs Insights API behaved unexpectedly while Incite was polling a
// chunk status via the CloudWatch Logs GetQueryResults API action.
//
// When UnexpectedQueryError is returned by Stream.Read, the stream's
// query is considered failed and all subsequent reads on the stream
// will return an error.
type UnexpectedQueryError struct {
	// QueryID is the CloudWatch Logs Insights query ID of the chunk
	// that experienced an unexpected event.
	QueryID string
	// Text is the text of the query for the chunk.
	Text string
	// Cause is the causing error.
	Cause error
}

func (err *UnexpectedQueryError) Error() string {
	return fmt.Sprintf("incite: query ID %q had unexpected error [query text %q]: %s", err.QueryID, err.Text, err.Cause)
}

func (err *UnexpectedQueryError) Unwrap() error {
	return err.Cause
}

func errNilStatus() error {
	return errors.New(outputMissingStatusMsg)
}

func errNilResultField(i int) error {
	return fmt.Errorf("incite: result field [%d] is nil", i)
}

func errNoKey() error {
	return errors.New(fieldMissingKeyMsg)
}

func errNoValue(key string) error {
	return fmt.Errorf("incite: result field missing value for key %q", key)
}

const (
	nilActionsMsg = "incite: nil actions"
	nilStreamMsg  = "incite: nil stream"
	nilContextMsg = "incite: nil context"

	textBlankMsg             = "incite: blank query text"
	startSubSecondMsg        = "incite: start has sub-second granularity"
	endSubSecondMsg          = "incite: end has sub-second granularity"
	endNotBeforeStartMsg     = "incite: end not before start"
	noGroupsMsg              = "incite: no log groups"
	exceededMaxLimitMsg      = "incite: exceeded MaxLimit"
	chunkSubSecondMsg        = "incite: chunk has sub-second granularity"
	splitUntilSubSecondMsg   = "incite: split-until has sub-second granularity"
	splitUntilWithPreviewMsg = "incite: split-until incompatible with preview"

	outputMissingQueryIDMsg = "incite: nil query ID in StartQuery output from CloudWatch Logs"
	outputMissingStatusMsg  = "incite: nil status in GetQueryResults output from CloudWatch Logs"
	fieldMissingKeyMsg      = "incite: result field missing key"
)
