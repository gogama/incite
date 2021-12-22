Incite! - CloudWatch Insights queries made (very) easy
======================================================

[![Build Status](https://travis-ci.org/gogama/incite.svg)](https://travis-ci.com/gogama/incite) [![Go Report Card](https://goreportcard.com/badge/github.com/gogama/incite)](https://goreportcard.com/report/github.com/gogama/incite) [![PkgGoDev](https://pkg.go.dev/badge/github.com/gogama/incite)](https://pkg.go.dev/github.com/gogama/incite)

What is Incite!?
================

With AWS CloudWatch Logs's Insights service, you can query your application logs
like a database, allowing you to discover rich debugging, operational, and
business  insights from your massive troves of log data distributed across the
cloud!

Incite is a native GoLang library that streamlines your interactions with the
CloudWatch Logs Insights API using minimalist, native Go, paradigms. Incite gets
the API out of the way so you can focus on app development.

Features
========

- **Streaming**. The CloudWatch Logs Insights API makes you poll your queries
  until they are done, requiring boilerplate code that is hard to write efficiently.
  Incite does the polling for you and gives you your query results as a stream!
- **Auto-Chunking**. Each AWS CloudWatch Logs Insights query is limited to 10,000
  results and AWS recommends you chunk your queries into smaller time ranges if
  your query exceeds 10K results. Incite does this chunking automatically and
  merges the results of all chunks into one convenient stream.
- **Dynamic Splitting**. Since v1.2.0, Incite can dynamically detect when a query
  chunk exceeds the 10K result limit, split that chunk into sub-chunks, and
  re-query the chunks, all automatically and without intervention.
- **Multiplexing**. Incite efficiently runs multiple queries at the same time
  and is smart enough to do this without getting throttled or going over your
  CloudWatch Logs service quota limits.
- **Previewing**. AWS CloudWatch Logs Insights can give you intermediate results
  before the query is done. Incite supports an optional previewing mode to give
  you these early results as soon as they are available, increasing your app's
  responsiveness.
- **Unmarshalling**. The CloudWatch Logs Insights API can only give you
  unstructured key/value string pairs, so you have to write more boilerplate code
  to put your results into a useful structure for analysis. Incite lets you
  unmarshal your results into maps or structs using a single function call.
  Incite supports tag-based field mapping just like `encoding/json`. (And it
  supports`json:"..."` tags as well as its native `incite:"..."` tags, right
  out of the box!)
- **Go Native**. Incite gives you a more Go-friendly coding experience than the
  AWS SDK for Go, including getting rid of unnecessary pointers and using
  standard types like `time.Time`.
- **Optional Logging**. If your app needs to provide real-time diagnostic
  information about how Incite is interacting with CloudWatch Logs, Incite lets
  you plug in a logger to listen for interesting events.

Getting Started
===============

## Get the code

```
$ go get github.com/gogama/incite
```

## Concepts

- For quick prototyping and scripting type work, simplify your life using the
  global[`Query`](https://pkg.go.dev/github.com/gogama/incite#Query) function.
- When you need finer control over what your app is doing, create a new
  `QueryManager` using
  [`NewQueryManager`](https://pkg.go.dev/github.com/gogama/incite#NewQueryManager)
  and query it using its `Query` method.
- To read all the results from a stream, use the global
  [`ReadAll`](https://pkg.go.dev/github.com/gogama/incite#ReadAll) function.
- To unmarshal the results into a structure of your choice, use the global
  [`Unmarshal`](https://pkg.go.dev/github.com/gogama/incite#Unmarshal) function.

## A simple app

```go
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/gogama/incite"
)

func main() {
	// Use the AWS SDK for Go to get the CloudWatch API actions Incite needs.
	// For simplicity we assume that the correct AWS region and credentials are
	// already set in the environment.
	a := cloudwatchlogs.New(session.Must(session.NewSession()))

	// Create a QueryManager. An alternative to using a QueryManager is just
	// using the global scope Query function.
	m := incite.NewQueryManager(incite.Config{Actions: a})
	defer func() {
		_ = m.Close()
	}()

	// Look at the last 15 minutes.
	end := time.Now().Truncate(time.Second)
	start := end.Add(-15*time.Minute)

	// Query the results.
	s, err := m.Query(incite.QuerySpec{
		Text:   "fields @timestamp, @message | filter @message =~ /foo/ | sort @timestamp desc",
		Start:  start,
		End:    end,
		Groups: []string{"/my/log/group"},
		Limit:  100,
	})
	if err != nil {
		return
	}
	data, err := incite.ReadAll(s)
	if err != nil {
		return
	}

	// Unpack the results into a structured format.
	var v []struct{
		Timestamp time.Time `incite:"@timestamp"`
		Message string      `incite:"@message"`
	}
	err = incite.Unmarshal(data, &v)
	if err != nil {
		return
	}

	// Print the results!
	fmt.Println(v)
}
```

Compatibility
=============

Works with all Go versions 1.14 and up, and [AWS SDK for Go V1](https://github.com/aws/aws-sdk-go)
versions 1.21.6 and up.

Related
=======

Official AWS documentation: [Analyzing log data with CloudWatch Logs Insights](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/AnalyzingLogData.html).
Find Insights' query syntax documentation [here](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html)
and the API reference [here](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_Operations.html) (look
for `StartQuery`, `GetQueryResults`, and `StopQuery`).

Acknowledgements
================

Developer happiness on this project was embiggened by JetBrains, which
generously donated an [open source license](https://www.jetbrains.com/opensource/)
for their lovely GoLand IDE. Thanks JetBrains!
