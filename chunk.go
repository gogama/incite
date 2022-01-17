// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"context"
	"fmt"
	"time"
)

// A chunk represents a single active CloudWatch Logs Insights query
// owned by a stream. A stream has one or more chunks, depending on
// whether the QuerySpec indicated chunking. A chunk is a passive data
// structure: it owns no goroutines and presents no interface that is
// accessible outside the package.
type chunk struct {
	Stats
	stream  *stream         // Owning stream which receives results of the chunk
	ctx     context.Context // Child of the stream's context owned by this chunk
	gen     int             // Generation, zero if an initial chunk, positive if it came from a split
	chunkID string          // Incite chunk ID
	queryID string          // Insights query ID
	ptr     map[string]bool // Set of already viewed @ptr, nil if QuerySpec not previewable
	start   time.Time       // Start of the chunk's time range (inclusive)
	end     time.Time       // End of the chunk's time range (exclusive)
	state   state           // Chunk state consumed by mgr loop
	err     error           // Chunk error
	try     int             // Local attempt number within worker loop
	restart int             // Number of times a chunk is restarted after CW Insights "Failed" status
}

// A state contains the current status of a chunk. This is used by the
// mgr loop to direct traffic returned by its worker goroutines on the
// update channel.
type state int

const (
	// The created state indicates that the chunk is brand new and ready
	// to run.
	created state = iota
	// The starting state indicates that the chunk has been sent to the
	// starter but is not yet started.
	starting
	// The started state indicates that the chunk has been started but
	// has not yet been sent to the poller.
	started
	// The polling state indicates that the chunk has been sent to the
	// poller but is not yet polled.
	polling
	// The complete state indicates that the chunk has finished polling.
	complete
	// The stopping state indicates that the chunk has been sent to the
	// stopper but is not yet stopped.
	stopping
	// The stopped state indicates that the chunk has been stopped, or
	// has reached its final stop attempt within the stopper.
	stopped
)

func (c *chunk) duration() time.Duration {
	return c.end.Sub(c.start)
}

func (c *chunk) started() {
	if c.gen == 0 {
		c.RangeStarted += c.duration()
	}
	if c.err != nil {
		c.RangeFailed += c.duration()
	}
}

func (c *chunk) split(start time.Time, frac time.Duration, n int) *chunk {
	end := start.Add(frac)
	if end.After(c.end) {
		end = c.end
	}
	chunkID := fmt.Sprintf("%ss%d", c.chunkID, n)
	return &chunk{
		stream:  c.stream,
		ctx:     context.WithValue(c.stream.ctx, chunkIDKey, chunkID),
		gen:     c.gen + 1,
		chunkID: chunkID,
		start:   start,
		end:     end,
	}
}

type chunkIDKeyType int

var chunkIDKey = chunkIDKeyType(0)
