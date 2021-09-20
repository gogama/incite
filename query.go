// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import "context"

// Query is sweet sweet sugar to perform a synchronous CloudWatch Logs
// Insights query and get back all the results without needing to
// construct a QueryManager. Query runs the query indicated by q, using
// the CloudWatch Logs actions provided by a, and returns all the
// query results (or the error if the query failed).
//
// The context ctx controls the lifetime of the query. If ctx expires or
// is cancelled, the query is cancelled and an error is returned. If
// you don't need the ability to set a timeout or cancel the request,
// use context.Background().
//
// Unlike NewQueryManager, supports a configurable level of parallel
// query execution, Query uses a parallelism factor of 1. This means
// that if q represents a chunked query, then the chunks will be run
// serially, in ascending order of time.
//
// This function is intended for quick prototyping and simple scripting
// and command-line interface use cases. More complex applications,
// especially applications running concurrent queries against the same
// region from multiple goroutines, should construct and configure a
// QueryManager explicitly.
func Query(ctx context.Context, a CloudWatchLogsActions, q QuerySpec) ([]Result, error) {
	if ctx == nil {
		panic(nilContextMsg)
	}
	m := NewQueryManager(Config{
		Actions:  a,
		Parallel: 1,
	})
	defer func() {
		_ = m.Close()
	}()
	c := make(chan struct {
		r   []Result
		err error
	})
	go func() {
		var s Stream
		var p struct {
			r   []Result
			err error
		}
		s, p.err = m.Query(q)
		if p.err == nil {
			p.r, p.err = ReadAll(s)
		}
		c <- p
	}()
	select {
	case p := <-c:
		return p.r, p.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
