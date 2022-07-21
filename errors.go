// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
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
	// GetQueryResults API act.
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
// chunk status via the CloudWatch Logs GetQueryResults API act.
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

func isTemporary(err error) bool {
	if x, ok := err.(awserr.Error); ok {
		// Short-circuit if the HTTP status code indicates retryability.
		if f, ok := err.(awserr.RequestFailure); ok {
			status := f.StatusCode()
			if status == 429 || status == 502 || status == 503 || status == 504 {
				return true
			}
		}

		// Check for known CloudWatch Logs retryability codes.
		switch x.Code() {
		case cloudwatchlogs.ErrCodeLimitExceededException, cloudwatchlogs.ErrCodeServiceUnavailableException:
			return true
		}

		// Check for throttling using common AWS service patterns for indicating
		// throttling via exception. Omit 'e' suffix on 'throttl' to match
		// Throttled and Throttling.
		if strings.Contains(strings.ToLower(x.Code()), "throttl") ||
			strings.Contains(strings.ToLower(x.Message()), "rate exceeded") {
			return true
		}

		// Recursively examine the cause error, if any.
		return isTemporary(x.OrigErr())
	}

	if errors.Is(err, io.EOF) {
		return true
	}

	var maybeTimeout interface{ Timeout() bool }
	if errors.As(err, &maybeTimeout) && maybeTimeout.Timeout() {
		return true
	}

	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNREFUSED, syscall.ECONNRESET:
			return true
		default:
			return false
		}
	}

	return false
}

const (
	nilActionsMsg = "incite: nil actions"
	nilStreamMsg  = "incite: nil stream"
	nilContextMsg = "incite: nil context"

	textBlankMsg                 = "incite: blank query text"
	startSubMillisecondMsg       = "incite: start has sub-millisecond granularity"
	endSubMillisecondMsg         = "incite: end has sub-millisecond granularity"
	endNotBeforeStartMsg         = "incite: end not before start"
	noGroupsMsg                  = "incite: no log groups"
	exceededMaxLimitMsg          = "incite: exceeded MaxLimit"
	chunkSubMillisecondMsg       = "incite: chunk has sub-millisecond granularity"
	splitUntilSubMillisecondMsg  = "incite: split-until has sub-millisecond granularity"
	splitUntilWithPreviewMsg     = "incite: split-until incompatible with preview"
	splitUntilWithoutMaxLimitMsg = "incite: split-until requires MaxLimit"

	outputMissingQueryIDMsg = "incite: nil query ID in StartQuery output from CloudWatch Logs"
	outputMissingStatusMsg  = "incite: nil status in GetQueryResults output from CloudWatch Logs"
	fieldMissingKeyMsg      = "incite: result field missing key"
)

var (
	errClosing      = errors.New("incite: closing")
	errStopChunk    = errors.New("incite: owning stream died, cancel chunk")
	errRestartChunk = errors.New("incite: transient chunk failure, restart chunk")
	errSplitChunk   = errors.New("incite: chunk maxed, split chunk")
)
