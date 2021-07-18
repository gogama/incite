package incite

import (
	"errors"
	"testing"

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
