// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

// Query is sweet sweet sugar to perform a synchronous CloudWatch Logs
// Insights query and get back all the results without needing to
// construct a QueryManager.
//
// Unlike NewQueryManager, which defaults to DefaultParallel, Query uses
// a parallelism factor of 1. This means that if q represents a chunked
// query, then the chunks will be run serially.
//
// This function is intended for quick prototyping and simple scripting
// and command-line interface use cases. More complex applications,
// especially applications running concurrent queries against the same
// region from multiple goroutines, should construct and configure a
// QueryManager explicitly.
func Query(a CloudWatchLogsActions, q QuerySpec) ([]Result, error) {
	m := NewQueryManager(Config{
		Actions:  a,
		Parallel: 1,
	})
	s, err := m.Query(q)
	if err != nil {
		return nil, err
	}
	return ReadAll(s)
}
