// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

type poller struct {
	worker
}

func newPoller(m *mgr) *poller {
	p := &poller{
		worker: worker{
			m:         m,
			regulator: makeRegulator(m.close, m.RPS[GetQueryResults], RPSDefaults[GetQueryResults]),
			in:        m.poll,
			out:       m.update,
			name:      "poller",
			maxTry:    10,
		},
	}
	p.manipulator = p
	return p
}

func (p *poller) context(c *chunk) context.Context {
	return c.ctx
}

func (p *poller) manipulate(c *chunk) bool {
	// If the owning stream has died, send chunk back for cancellation.
	if !c.stream.alive() {
		c.err = errStopChunk
		return true
	}

	// Poll the chunk.
	input := cloudwatchlogs.GetQueryResultsInput{
		QueryId: &c.queryID,
	}
	output, err := p.m.Actions.GetQueryResultsWithContext(c.ctx, &input)
	p.lastReq = time.Now()

	if err != nil && isTemporary(err) {
		return false
	} else if err != nil {
		c.err = &UnexpectedQueryError{c.queryID, c.stream.Text, err}
		return true
	}

	if output.Status == nil {
		c.err = &UnexpectedQueryError{c.queryID, c.stream.Text, errNilStatus()}
		return true
	}

	status := *output.Status
	switch status {
	case cloudwatchlogs.QueryStatusScheduled, "Unknown":
		return false
	case cloudwatchlogs.QueryStatusRunning:
		if c.ptr == nil {
			return false // Ignore non-previewable results.
		}
		return !sendChunkBlock(c, output.Results)
	case cloudwatchlogs.QueryStatusComplete:
		translateStats(output.Statistics, &c.Stats)
		if p.splitChunk(c, len(output.Results)) {
			c.state = complete
			return true
		}
		c.Stats.RangeDone += c.duration()
		if sendChunkBlock(c, output.Results) {
			c.state = complete
		}
		return true
	case cloudwatchlogs.QueryStatusFailed:
		if c.ptr == nil && c.restart < maxRestart {
			c.restart++
			c.err = errRestartChunk
			return true // Retry transient failures if stream isn't previewable.
		}
		fallthrough
	default:
		translateStats(output.Statistics, &c.Stats)
		c.err = &TerminalQueryStatusError{c.queryID, status, c.stream.Text}
		return true
	}
}

func (p *poller) release(c *chunk) {
	p.m.logChunk(c, "releasing pollable", "")
}

func sendChunkBlock(c *chunk, results [][]*cloudwatchlogs.ResultField) bool {
	var block []Result
	var err error

	if c.ptr != nil {
		block, err = translateResultsPreview(c, results)
	} else {
		block, err = translateResultsNoPreview(c, results)
	}

	if err != nil {
		c.err = err
		return false
	}

	if len(block) == 0 {
		return true
	}

	c.stream.lock.Lock()
	defer c.stream.lock.Unlock()

	c.stream.blocks = append(c.stream.blocks, block)
	c.stream.more.Signal()
	return true
}

func translateStats(in *cloudwatchlogs.QueryStatistics, out *Stats) {
	if in == nil {
		return
	}
	if in.BytesScanned != nil {
		out.BytesScanned += *in.BytesScanned
	}
	if in.RecordsMatched != nil {
		out.RecordsMatched += *in.RecordsMatched
	}
	if in.RecordsScanned != nil {
		out.RecordsScanned += *in.RecordsScanned
	}
}

func translateResultsNoPreview(c *chunk, results [][]*cloudwatchlogs.ResultField) ([]Result, error) {
	var err error
	block := make([]Result, len(results))
	for i, r := range results {
		block[i], err = translateResult(c, r)
		if err != nil {
			return nil, err
		}
	}
	return block, nil
}

func translateResultsPreview(c *chunk, results [][]*cloudwatchlogs.ResultField) ([]Result, error) {
	// Create a slice to contain the block of results.
	block := make([]Result, 0, len(results))
	// Create a map to track which @ptr are new with this batch of results.
	newPtr := make(map[string]bool, len(results))
	// Collect all the results actually returned from CloudWatch Logs.
	for _, r := range results {
		var ptr *string
		for i := range r {
			f := r[i]
			if f == nil {
				continue
			}
			k, v := f.Field, f.Value
			if k != nil && *k == "@ptr" {
				ptr = v
				break
			}
		}
		if ptr != nil {
			newPtr[*ptr] = true
			if c.ptr[*ptr] {
				continue // We've already put this @ptr into the stream.
			}
		}
		rr, err := translateResult(c, r)
		if err != nil {
			return nil, err
		}
		block = append(block, rr)
	}
	// If there were any results delivered in a previous block that had
	// an @ptr that is not present in this block, insert an @deleted
	// result for each such obsolete result.
	for ptr := range c.ptr {
		if !newPtr[ptr] {
			block = append(block, deleteResult(ptr))
			delete(c.ptr, ptr)
		} else {
			delete(newPtr, ptr)
		}
	}
	// Add the @ptr for each result seen in the current batch, but which
	// hasn't been delivered in a previous block, into the chunk's @ptr
	// map.
	for ptr := range newPtr {
		c.ptr[ptr] = true
	}

	// Return the block so it can be sent to the stream.
	return block, nil
}

func translateResult(c *chunk, r []*cloudwatchlogs.ResultField) (Result, error) {
	rr := make(Result, len(r))
	for i, f := range r {
		if f == nil {
			return Result{}, &UnexpectedQueryError{QueryID: c.queryID, Text: c.stream.Text, Cause: errNilResultField(i)}
		}
		k, v := f.Field, f.Value
		if k == nil {
			return Result{}, &UnexpectedQueryError{QueryID: c.queryID, Text: c.stream.Text, Cause: errNoKey()}
		}
		if v == nil {
			return Result{}, &UnexpectedQueryError{QueryID: c.queryID, Text: c.stream.Text, Cause: errNoValue(*k)}
		}
		rr[i] = ResultField{
			Field: *k,
			Value: *v,
		}
	}
	return rr, nil
}

func deleteResult(ptr string) Result {
	return Result{
		{
			Field: "@ptr",
			Value: ptr,
		},
		{
			Field: "@deleted",
			Value: "true",
		},
	}
}

// splitBits is the number of child chunks into which a parent chunk
// will be split, assuming the parent chunk range is at least splitBits
// seconds long. The minimum chunk size is one second, so a 4-second
// parent chunk will be split into four chunks, but a two-second child
// chunk will only be split into two child chunks.
const splitBits = 4

// maxLimit is an indirect holder for the constant value MaxLimit used
// to facilitate unit testing.
var maxLimit int64 = MaxLimit

// TODO: Refactor this to live in mgr.
func (p *poller) splitChunk(c *chunk, n int) bool {
	// Short circuit if the chunk isn't maxed out.
	if int64(n) < c.stream.Limit {
		return false
	}

	// This chunk is maxed out so record that.
	c.Stats.RangeMaxed += c.duration()

	// Short circuit if splitting isn't required.
	if c.ptr != nil {
		return false // Can't split chunks if previewing is on.
	}
	if int64(n) < maxLimit {
		return false // Don't split unless chunk query overflowed CWL max results.
	}
	if c.duration() <= c.stream.SplitUntil {
		return false // Stop splitting when we reach minimum chunk size.
	}

	// At this point we know this chunk will be split. Thus, we should
	// stop counting it as maxed out. If the sub-chunks are later
	// determined to be maxed out that will be recorded later.
	c.Stats.RangeMaxed -= c.duration()

	splitter := func(parent *chunk, start time.Time, frac time.Duration, n int) *chunk {
		end := start.Add(frac)
		if end.After(parent.end) {
			end = parent.end
		}
		chunkID := fmt.Sprintf("%ss%d", c.chunkID, n)
		return &chunk{
			stream:  parent.stream,
			ctx:     context.WithValue(parent.stream.ctx, chunkIDKey, chunkID),
			gen:     parent.gen + 1,
			chunkID: chunkID,
			start:   start,
			end:     end,
		}
	}

	frac := c.duration() / splitBits
	if hasSubSecondD(frac) {
		frac = frac + time.Second/2
		frac = frac.Round(time.Second)
	}

	children := make([]*chunk, 1, splitBits)
	child := splitter(c, c.start, frac, 0)
	children[0] = child
	for child.end.Before(c.end) {
		child = splitter(c, child.end, frac, len(children))
		children = append(children, child)
	}

	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "in %d sub-chunks... ", len(children))
	b.WriteString(children[0].chunkID)
	for i := 1; i < len(children); i++ {
		b.WriteString(" / ")
		b.WriteString(children[i].chunkID)
	}

	p.m.logChunk(c, "split", b.String())
	c.stream.n += int64(len(children))
	for i := range children {
		p.out <- children[i]
	}
	return true
}

// TODO: Maybe move this constant block elsewhere and/or merge with others.
const (
	maxRestart = 2
)
