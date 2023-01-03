// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

type stopper struct {
	worker
}

func newStopper(m *mgr) *stopper {
	s := &stopper{
		worker: worker{
			m:                 m,
			regulator:         makeRegulator(m.close, m.RPS[StopQuery], RPSDefaults[StopQuery], !m.DisableAdaptation),
			in:                m.stop,
			out:               m.update,
			name:              "stopper",
			maxTemporaryError: 3,
		},
	}
	s.manipulator = s
	return s
}

func (s *stopper) context(_ *chunk) context.Context {
	return context.Background()
}

func (s *stopper) manipulate(c *chunk) outcome {
	output, err := s.m.Actions.StopQueryWithContext(context.Background(), &cloudwatchlogs.StopQueryInput{
		QueryId: &c.queryID,
	}, request.WithAppendUserAgent(version()))
	s.lastReq = time.Now()
	if err != nil {
		switch classifyError(err) {
		case throttlingClass:
			return throttlingError
		case temporaryClass, limitExceededClass:
			return temporaryError
		default:
			s.m.logChunk(c, "failed to stop", "error from CloudWatch Logs: "+err.Error())
			return finished
		}
	} else if output.Success == nil || !*output.Success {
		s.m.logChunk(c, "failed to stop", "CloudWatch Logs did not indicate success")
		return finished
	} else {
		s.m.logChunk(c, "stopped", "")
		return finished
	}
}

func (s *stopper) release(c *chunk) {
	if s.setTimerRPS() {
		<-s.timer.C
		s.ding = true
	}

	_ = s.manipulate(c)
}
