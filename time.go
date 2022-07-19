// Copyright 2022 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import "time"

// TimeLayout is a Go time layout which documents the format of the time
// values returned within the timestamp fields of CloudWatch Logs
// Insights queries.
//
// TimeLayout defines the format by showing how the Go reference time of
//
// 	Mon Jan 2 15:04:05 -0700 MST 2006
//
// would be formatted if it were the value. TimeLayout can be used with
// time.Parse to parse timestamp fields, such as @timestamp and @ingestionTime,
// which are returned within CloudWatch Logs Insights query results.
const TimeLayout = "2006-01-02 15:04:05.000"

func hasSubSecond(t time.Time) bool {
	return t.Nanosecond() != 0
}

func hasSubSecondD(d time.Duration) bool {
	return d%time.Second > 0
}

func epochMillisecond(t time.Time) int64 {
	return t.Unix()*1000 + int64(t.Nanosecond())/int64(time.Millisecond)
}
