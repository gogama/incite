// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/private/protocol/restjson"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
)

func TestStartQueryError_Error(t *testing.T) {
	err := &StartQueryError{
		Text:  "foo",
		Start: defaultStart,
		End:   defaultEnd,
		Cause: errors.New(`an annoyance`),
	}

	s := err.Error()

	assert.Equal(t, `incite: CloudWatch Logs failed to start query for chunk "foo" [2020-08-25 03:30:00 +0000 UTC..2020-08-25 03:35:00 +0000 UTC): an annoyance`, s)
}

func TestStartQueryError_Unwrap(t *testing.T) {
	cause := errors.New(`an annoyance`)
	err := &StartQueryError{
		Text:  "foo",
		Start: defaultStart,
		End:   defaultEnd,
		Cause: cause,
	}

	assert.Same(t, cause, err.Unwrap())
}

func TestTerminalQueryStatusError_Error(t *testing.T) {
	err := &TerminalQueryStatusError{
		QueryID: "Sweet",
		Status:  "Home",
		Text:    "Chicago",
	}

	s := err.Error()

	assert.Equal(t, `incite: query ID "Sweet" has terminal status "Home" [query text "Chicago"]`, s)
}

func TestUnexpectedQueryError_Error(t *testing.T) {
	cause := errors.New(`an annoyance`)
	err := &UnexpectedQueryError{
		QueryID: "ham",
		Text:    "eggs",
		Cause:   cause,
	}

	s := err.Error()

	assert.Equal(t, `incite: query ID "ham" had unexpected error [query text "eggs"]: an annoyance`, s)
}

func TestUnexpectedQueryError_Unwrap(t *testing.T) {
	cause := errors.New(`an annoyance`)
	err := &UnexpectedQueryError{
		QueryID: "ham",
		Text:    "eggs",
		Cause:   cause,
	}

	assert.Same(t, cause, err.Unwrap())
}

func TestErrNilStatus(t *testing.T) {
	err := errNilStatus()

	assert.EqualError(t, err, outputMissingStatusMsg)
}

func TestErrNilResultField(t *testing.T) {
	err := errNilResultField(13)

	assert.EqualError(t, err, `incite: result field [13] is nil`)
}

func TestErrNoKey(t *testing.T) {
	err := errNoKey()

	assert.EqualError(t, err, fieldMissingKeyMsg)
}

func TestErrNoValue(t *testing.T) {
	err := errNoValue("foo")

	assert.EqualError(t, err, `incite: result field missing value for key "foo"`)
}

func TestClassifyError(t *testing.T) {
	t.Run("Permanent Errors", func(t *testing.T) {
		permanentCases := []error{
			nil,
			errors.New("bif"),
			awserr.NewRequestFailure(awserr.New("a", "b", nil), 400, "very, very, bad request"),
			awserr.NewRequestFailure(awserr.New("a", "b", nil), 500, "internal server error"),
			cwlErr(cloudwatchlogs.ErrCodeInvalidOperationException, "foo"),
			cwlErr(cloudwatchlogs.ErrCodeInvalidParameterException, "bar"),
			cwlErr(cloudwatchlogs.ErrCodeMalformedQueryException, "baz"),
			cwlErr(cloudwatchlogs.ErrCodeResourceNotFoundException, "ham"),
			cwlErr(cloudwatchlogs.ErrCodeUnrecognizedClientException, "eggs"),
			syscall.ENETDOWN,
			wrapErr{syscall.ENETDOWN},
			cwlErr("Ain't no network", "It's down", syscall.ENETDOWN),
		}
		for i, permanentCase := range permanentCases {
			t.Run(fmt.Sprintf("permanentCase[%d]=%s", i, permanentCase), func(t *testing.T) {
				assert.Equal(t, permanentClass, classifyError(permanentCase))
			})
		}
	})

	t.Run("Throttling Cases", func(t *testing.T) {
		throttlingCases := []error{
			awserr.NewRequestFailure(awserr.New("a", "b", nil), 429, "too-many-requests"),
			cwlErr("tttthroTTLED!", "simmer down"),
		}
		for i, throttlingCase := range throttlingCases {
			t.Run(fmt.Sprintf("throttlingCase[%d]=%s", i, throttlingCase), func(t *testing.T) {
				assert.Equal(t, throttlingClass, classifyError(throttlingCase))
			})
		}
	})

	t.Run("Limit Exceeded Cases", func(t *testing.T) {
		limitExceededCases := []error{
			cwlErr(cloudwatchlogs.ErrCodeLimitExceededException, "stay under that limit"),
		}
		for i, limitExceededCase := range limitExceededCases {
			t.Run(fmt.Sprintf("limitExceededCase[%d]=%s", i, limitExceededCase), func(t *testing.T) {
				assert.Equal(t, limitExceededClass, classifyError(limitExceededCase))
			})
		}
	})

	t.Run("Temporary Cases", func(t *testing.T) {
		temporaryCases := []error{
			awserr.NewRequestFailure(awserr.New("a", "b", nil), 502, "bad-gateway"),
			awserr.NewRequestFailure(awserr.New("a", "b", nil), 503, "service-unavailable"),
			awserr.NewRequestFailure(awserr.New("a", "b", nil), 504, "gateway-timeout"),
			awserr.New("there was no availability of service", "unavailable", awserr.NewRequestFailure(awserr.New("a", "b", nil), 503, "my request")),
			cwlErr(cloudwatchlogs.ErrCodeServiceUnavailableException, "stand by for more great service"),
			io.EOF,
			wrapErr{io.EOF},
			cwlErr("i am at the end of my file", "the end I say", io.EOF),
			syscall.ETIMEDOUT,
			wrapErr{syscall.ETIMEDOUT},
			cwlErr("my time has run expected", "the end I say", syscall.ETIMEDOUT),
			syscall.ECONNREFUSED,
			wrapErr{syscall.ECONNREFUSED},
			cwlErr("let there be no connection", "for it has been refused", syscall.ECONNREFUSED),
			syscall.ECONNRESET,
			wrapErr{syscall.ECONNRESET},
			cwlErr("Reset that conn!", "Reset, reset!", syscall.ECONNRESET),
		}
		for i, temporaryCase := range temporaryCases {
			t.Run(fmt.Sprintf("temporaryCase[%d]=%s", i, temporaryCase), func(t *testing.T) {
				assert.Equal(t, temporaryClass, classifyError(temporaryCase))
			})
		}
	})

	t.Run("Special Cases", func(t *testing.T) {
		t.Run("Issue #13 - Retry API calls when the CWL API response payload can't be deserialized", func(t *testing.T) {
			// Regression test for: https://github.com/gogama/incite/issues/13
			assert.Equal(t, temporaryClass, classifyError(issue13Error(t.Name(), 503)))
		})
	})
}

// issue13Error returns an error of the type that triggered issue #13,
// https://github.com/gogama/incite/issues/13.
func issue13Error(requestID string, statusCode int) error {
	r := request.Request{
		RequestID: requestID,
		HTTPResponse: &http.Response{
			StatusCode: statusCode,
			Body:       ioutil.NopCloser(strings.NewReader("")),
		},
	}

	// Construct the problem error.
	restjson.UnmarshalError(&r)

	return r.Error
}

func cwlErr(code, message string, cause ...error) error {
	var origErr error
	if len(cause) == 1 {
		origErr = cause[0]
	} else if len(cause) > 1 {
		panic("only one cause allowed")
	}
	return awserr.New(code, message, origErr)
}

type wrapErr struct {
	cause error
}

func (err wrapErr) Error() string {
	return fmt.Sprintf("wrapped: %s", err.cause)
}

func (err wrapErr) Unwrap() error {
	return err.cause
}
