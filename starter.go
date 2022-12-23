// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

type starter struct {
	worker
}

const maxTempStartingErrs = 10

func newStarter(m *mgr) *starter {
	s := &starter{
		worker: worker{
			m:                 m,
			regulator:         makeRegulator(m.close, m.RPS[StartQuery], RPSDefaults[StartQuery], !m.DisableAdaptation),
			in:                m.start,
			out:               m.update,
			name:              "starter",
			maxTemporaryError: maxTempStartingErrs,
		},
	}
	s.manipulator = s
	return s
}

func (s *starter) context(c *chunk) context.Context {
	return c.ctx
}

func (s *starter) manipulate(c *chunk) outcome {
	// Discard chunk if the owning stream is dead.
	if !c.stream.alive() {
		return nothing
	}

	// Get the chunk time range in Insights' format.
	starts := epochMillisecond(c.start)
	ends := epochMillisecond(c.end.Add(-time.Millisecond)) // CWL uses inclusive time ranges, we use exclusive ranges.

	// Start the chunk.
	input := cloudwatchlogs.StartQueryInput{
		QueryString:   &c.stream.Text,
		StartTime:     &starts,
		EndTime:       &ends,
		LogGroupNames: c.stream.groups,
		Limit:         &c.stream.Limit,
	}
	output, err := s.m.Actions.StartQueryWithContext(c.ctx, &input, request.WithAppendUserAgent(version()))
	s.lastReq = time.Now()
	if err != nil {
		c.err = &StartQueryError{c.stream.Text, c.start, c.end, err}
		switch classifyError(err) {
		case throttlingClass:
			return throttlingError
		case limitExceededClass:
			// TODO: Pass this information up to mgr.
			// TODO: Log.
			fallthrough
		case temporaryClass:
			return temporaryError
		default:
			s.m.logChunk(c, "permanent failure to start", "fatal error from CloudWatch Logs: "+err.Error())
			return finished
		}
	}

	// Save the current query ID into the chunk.
	queryID := output.QueryId
	if queryID == nil {
		c.err = &StartQueryError{c.stream.Text, c.start, c.end, errors.New(outputMissingQueryIDMsg)}
		s.m.logChunk(c, "nil query ID from CloudWatch Logs for", "")
		return finished
	}
	c.queryID = *queryID

	// Chunk is started successfully.
	c.state = started
	c.err = nil
	s.m.logChunk(c, "started", "")
	return finished
}

func (s *starter) release(c *chunk) {
	s.m.logChunk(c, "releasing startable", "")
}
