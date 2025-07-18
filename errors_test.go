// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
			&smithy.GenericAPIError{Code: "BadRequest", Message: "very, very, bad request"},
			&smithy.GenericAPIError{Code: "InternalServerError", Message: "internal server error"},
			&types.InvalidOperationException{Message: aws.String("foo")},
			&types.InvalidParameterException{Message: aws.String("bar")},
			&types.MalformedQueryException{Message: aws.String("baz")},
			&types.ResourceNotFoundException{Message: aws.String("ham")},
			&types.UnrecognizedClientException{Message: aws.String("eggs")},
			syscall.ENETDOWN,
			wrapErr{syscall.ENETDOWN},
			&smithy.GenericAPIError{Code: "Ain't no network", Message: "It's down"},
		}
		for i, permanentCase := range permanentCases {
			t.Run(fmt.Sprintf("permanentCase[%d]=%s", i, permanentCase), func(t *testing.T) {
				assert.Equal(t, permanentClass, classifyError(permanentCase))
			})
		}
	})

	t.Run("Throttling Cases", func(t *testing.T) {
		throttlingCases := []error{
			&types.ThrottlingException{Message: aws.String("too-many-requests")},
			&types.ThrottlingException{Message: aws.String("simmer down")},
			&types.ThrottlingException{Message: aws.String("slow down")},
		}
		for i, throttlingCase := range throttlingCases {
			t.Run(fmt.Sprintf("throttlingCase[%d]=%s", i, throttlingCase), func(t *testing.T) {
				assert.Equal(t, throttlingClass, classifyError(throttlingCase))
			})
		}
	})

	t.Run("Limit Exceeded Cases", func(t *testing.T) {
		limitExceededCases := []error{
			&types.LimitExceededException{Message: aws.String("stay under that limit")},
		}
		for i, limitExceededCase := range limitExceededCases {
			t.Run(fmt.Sprintf("limitExceededCase[%d]=%s", i, limitExceededCase), func(t *testing.T) {
				assert.Equal(t, limitExceededClass, classifyError(limitExceededCase))
			})
		}
	})

	t.Run("Temporary Cases", func(t *testing.T) {
		temporaryCases := []error{
			&types.ServiceUnavailableException{Message: aws.String("stand by for more great service")},
			io.EOF,
			wrapErr{io.EOF},
			wrapErr{&types.ServiceUnavailableException{Message: aws.String("i am at the end of my file")}},
			syscall.ETIMEDOUT,
			wrapErr{syscall.ETIMEDOUT},
			wrapErr{&types.ServiceUnavailableException{Message: aws.String("my time has run expected")}},
			syscall.ECONNREFUSED,
			wrapErr{syscall.ECONNREFUSED},
			wrapErr{&types.ServiceUnavailableException{Message: aws.String("let there be no connection")}},
			syscall.ECONNRESET,
			wrapErr{syscall.ECONNRESET},
			wrapErr{&types.ServiceUnavailableException{Message: aws.String("Reset that conn!")}},
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
	httpResponse := &smithyhttp.Response{
		Response: &http.Response{
			StatusCode: statusCode,
			Body:       io.NopCloser(strings.NewReader("")),
		},
	}

	deserializeErr := &smithy.DeserializationError{
		Err: fmt.Errorf("failed to deserialize response for request %s", requestID),
	}

	httpResponseError := &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: httpResponse,
			Err:      deserializeErr,
		},
		RequestID: requestID,
	}

	return httpResponseError
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
