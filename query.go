package incite

import "fmt"

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
//
// The input query may be either a QuerySpec or a bare string containing
// the text of an Insights query. If the query is a bare string, then it
// is treated like a zero-value QuerySpec which has had its Text member
// set to the string.
func Query(a CloudWatchLogsActions, q interface{}) ([]Result, error) {
	m := NewQueryManager(Config{
		Actions:  a,
		Parallel: 1,
	})
	var qs QuerySpec
	switch q2 := q.(type) {
	case QuerySpec:
		qs = q2
	case string:
		// FIXME: TODO: This won't work because Start, End, and Groups
		// are all mandatory and missing. This idea could work if we
		// support varargs and treat subsequent strings as the log
		// groups, but even then we need some reasonable zero value
		// behavior for the Start/End times.
		qs.Text = q2
	default:
		return nil, fmt.Errorf("incite: invalid query type: must be string or QuerySpec")
	}
	s, err := m.Query(qs)
	if err != nil {
		return nil, err
	}
	return ReadAll(s)
}
