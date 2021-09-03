[1mdiff --git a/README.md b/README.md[m
[1mindex 9f04531..a6b4d2b 100644[m
[1m--- a/README.md[m
[1m+++ b/README.md[m
[36m@@ -8,16 +8,15 @@[m [mIncite! - CloudWatch Insights queries made (very) easy[m
 [m
 TODO list in order:[m
 [m
[31m-1. Remove hint support as it overcomplicates to little benefit.[m
[31m-2. Audit the logging code to ensure we log enough worthwhile status info.[m
[31m-3. To behave like encoding/json, make Unmarshal keep trying after encountering error. [m
[31m-4. Write README.[m
[32m+[m[32m1. Audit the logging code to ensure we log enough worthwhile status info.[m
[32m+[m[32m2. To behave like encoding/json, make Unmarshal keep trying after encountering error.[m[41m [m
[32m+[m[32m3. Write README.[m
 [m
 [m
 [m
 Acknowledgements[m
 ================[m
 [m
[31m-Developer happiness on this project was enhanced by JetBrains, which[m
[32m+[m[32mDeveloper happiness on this project was embiggened by JetBrains, which[m
 generously donated an [open source license](https://www.jetbrains.com/opensource/)[m
 for their lovely GoLand IDE. Thanks JetBrains![m
[1mdiff --git a/incite.go b/incite.go[m
[1mindex 3f189a6..9280ce2 100644[m
[1m--- a/incite.go[m
[1m+++ b/incite.go[m
[36m@@ -155,13 +155,6 @@[m [mtype QuerySpec struct {[m
 	// query capacity in preference to a query whose Priority number is[m
 	// higher, but only within the same QueryManager.[m
 	Priority int[m
[31m-[m
[31m-	// Hint optionally indicates the rough expected size of the result[m
[31m-	// set, which can help the QueryManager do a better job allocating[m
[31m-	// memory needed to manage and hold query results. Leave it zero[m
[31m-	// if you don't know the expected result size or aren't worried[m
[31m-	// about optimizing memory consumption.[m
[31m-	Hint uint16[m
 }[m
 [m
 // Stats contains metadata returned by CloudWatch Logs about the amount[m
[36m@@ -622,7 +615,7 @@[m [mfunc (m *mgr) startNextChunk() error {[m
 		status: cloudwatchlogs.QueryStatusScheduled,[m
 	}[m
 	if s.Preview {[m
[31m-		c.ptr = make(map[string]bool, s.chunkHint)[m
[32m+[m		[32mc.ptr = make(map[string]bool)[m
 	}[m
 [m
 	r := ring.New(1)[m
[36m@@ -860,11 +853,7 @@[m [mfunc translateResultsNoPreview(c *chunk, results [][]*cloudwatchlogs.ResultField[m
 [m
 func translateResultsPreview(c *chunk, results [][]*cloudwatchlogs.ResultField) ([]Result, error) {[m
 	// Create a slice to contain the block of results.[m
[31m-	guess := len(results) - len(c.ptr)[m
[31m-	if guess < int(c.stream.chunkHint) {[m
[31m-		guess = min(int(c.stream.chunkHint), len(results))[m
[31m-	}[m
[31m-	block := make([]Result, 0, guess)[m
[32m+[m	[32mblock := make([]Result, 0, len(results))[m
 	// Create a map to track which @ptr are new with this batch of results.[m
 	newPtr := make(map[string]bool, len(results))[m
 	// Collect all the results actually returned from CloudWatch Logs.[m
[36m@@ -960,10 +949,6 @@[m [mfunc (m *mgr) Close() (err error) {[m
 	return[m
 }[m
 [m
[31m-const ([m
[31m-	minHint = 100[m
[31m-)[m
[31m-[m
 func (m *mgr) Query(q QuerySpec) (s Stream, err error) {[m
 	// Validation that does not require lock.[m
 	q.Text = strings.TrimSpace(q.Text)[m
[36m@@ -1009,25 +994,15 @@[m [mfunc (m *mgr) Query(q QuerySpec) (s Stream, err error) {[m
 		return nil, errors.New(exceededMaxLimitMsg)[m
 	}[m
 [m
[31m-	if q.Hint < minHint {[m
[31m-		q.Hint = minHint[m
[31m-	}[m
[31m-[m
[31m-	chunkHint := int64(q.Hint) / n[m
[31m-	if chunkHint < minHint {[m
[31m-		chunkHint = minHint[m
[31m-	}[m
[31m-[m
 	ctx, cancel := context.WithCancel(context.Background())[m
 [m
 	ss := &stream{[m
 		QuerySpec: q,[m
 [m
[31m-		ctx:       ctx,[m
[31m-		cancel:    cancel,[m
[31m-		n:         n,[m
[31m-		chunkHint: uint16(chunkHint),[m
[31m-		groups:    groups,[m
[32m+[m		[32mctx:    ctx,[m
[32m+[m		[32mcancel: cancel,[m
[32m+[m		[32mn:      n,[m
[32m+[m		[32mgroups: groups,[m
 [m
 		next: q.Start,[m
 	}[m
[36m@@ -1097,11 +1072,10 @@[m [mtype stream struct {[m
 	QuerySpec[m
 [m
 	// Immutable fields.[m
[31m-	ctx       context.Context    // Stream context used to parent chunk contexts[m
[31m-	cancel    context.CancelFunc // Cancels ctx when the stream is closed[m
[31m-	n         int64              // Number of total chunks[m
[31m-	chunkHint uint16             // Hint divided by number of chunks[m
[31m-	groups    []*string          // Preprocessed slice for StartQuery[m
[32m+[m	[32mctx    context.Context    // Stream context used to parent chunk contexts[m
[32m+[m	[32mcancel context.CancelFunc // Cancels ctx when the stream is closed[m
[32m+[m	[32mn      int64              // Number of total chunks[m
[32m+[m	[32mgroups []*string          // Preprocessed slice for StartQuery[m
 [m
 	// Mutable fields controlled by mgr using mgr's lock.[m
 	next time.Time // Next chunk start time[m
[36m@@ -1189,13 +1163,6 @@[m [mfunc (s *stream) alive() bool {[m
 	return s.err == nil[m
 }[m
 [m
[31m-func min(a, b int) int {[m
[31m-	if a < b {[m
[31m-		return a[m
[31m-	}[m
[31m-	return b[m
[31m-}[m
[31m-[m
 // A chunk represents a single active CloudWatch Logs Insights query[m
 // owned by a stream. A stream has one or more chunks, depending on[m
 // whether the QuerySpec indicated chunking. A chunk is a passive data[m
[1mdiff --git a/incite_test.go b/incite_test.go[m
[1mindex 15bb793..047c08f 100644[m
[1m--- a/incite_test.go[m
[1m+++ b/incite_test.go[m
[36m@@ -691,16 +691,15 @@[m [mfunc TestQueryManager_Query(t *testing.T) {[m
 		causeErr := errors.New("super fatal error")[m
 [m
 		testCases := []struct {[m
[31m-			name              string[m
[31m-			before            QuerySpec[m
[31m-			after             QuerySpec[m
[31m-			startQueryOutput  *cloudwatchlogs.StartQueryOutput[m
[31m-			startQueryErr     error[m
[31m-			expectedN         int64[m
[31m-			expectedChunkHint uint16[m
[31m-			expectedGroups    []*string[m
[31m-			expectedNext      time.Time[m
[31m-			expectedCauseErr  error[m
[32m+[m			[32mname             string[m
[32m+[m			[32mbefore           QuerySpec[m
[32m+[m			[32mafter            QuerySpec[m
[32m+[m			[32mstartQueryOutput *cloudwatchlogs.StartQueryOutput[m
[32m+[m			[32mstartQueryErr    error[m
[32m+[m			[32mexpectedN        int64[m
[32m+[m			[32mexpectedGroups   []*string[m
[32m+[m			[32mexpectedNext     time.Time[m
[32m+[m			[32mexpectedCauseErr error[m
 		}{[m
 			{[m
 				name: "Zero",[m
[36m@@ -717,14 +716,12 @@[m [mfunc TestQueryManager_Query(t *testing.T) {[m
 					Groups: []string{"bar", "Baz"},[m
 					Limit:  DefaultLimit,[m
 					Chunk:  5 * time.Minute,[m
[31m-					Hint:   minHint,[m
[31m-				},[m
[31m-				startQueryErr:     causeErr,[m
[31m-				expectedN:         1,[m
[31m-				expectedChunkHint: minHint,[m
[31m-				expectedGroups:    []*string{sp("bar"), sp("Baz")},[m
[31m-				expectedNext:      defaultStart,[m
[31m-				expectedCauseErr:  causeErr,[m
[32m+[m				[32m},[m
[32m+[m				[32mstartQueryErr:    causeErr,[m
[32m+[m				[32mexpectedN:        1,[m
[32m+[m				[32mexpectedGroups:   []*string{sp("bar"), sp("Baz")},[m
[32m+[m				[32mexpectedNext:     defaultStart,[m
[32m+[m				[32mexpectedCauseErr: causeErr,[m
 			},[m
 			{[m
 				name: "ChunkExceedsRange",[m
[36m@@ -742,14 +739,12 @@[m [mfunc TestQueryManager_Query(t *testing.T) {[m
 					Groups: []string{"bar", "Baz"},[m
 					Limit:  DefaultLimit,[m
 					Chunk:  5 * time.Minute,[m
[31m-					Hint:   minHint,[m
[31m-				},[m
[31m-				startQueryErr:     causeErr,[m
[31m-				expectedN:         1,[m
[31m-				expectedChunkHint: minHint,[m
[31m-				expectedGroups:    []*string{sp("bar"), sp("Baz")},[m
[31m-				expectedNext:      defaultStart,[m
[31m-				expectedCauseErr:  causeErr,[m
[32m+[m				[32m},[m
[32m+[m				[32mstartQueryErr:    causeErr,[m
[32m+[m				[32mexpectedN:        1,[m
[32m+[m				[32mexpectedGroups:   []*string{sp("bar"), sp("Baz")},[m
[32m+[m				[32mexpectedNext:     defaultStart,[m
[32m+[m				[32mexpectedCauseErr: causeErr,[m
 			},[m
 			{[m
 				name: "PartialChunk",[m
[36m@@ -767,14 +762,12 @@[m [mfunc TestQueryManager_Query(t *testing.T) {[m
 					Groups: []string{"bar", "Baz"},[m
 					Limit:  DefaultLimit,[m
 					Chunk:  4 * time.Minute,[m
[31m-					Hint:   minHint,[m
[31m-				},[m
[31m-				startQueryErr:     causeErr,[m
[31m-				expectedN:         2,[m
[31m-				expectedChunkHint: minHint,[m
[31m-				expectedGroups:    []*string{sp("bar"), sp("Baz")},[m
[31m-				expectedNext:      defaultStart,[m
[31m-				expectedCauseErr:  causeErr,[m
[32m+[m				[32m},[m
[32m+[m				[32mstartQueryErr:    causeErr,[m
[32m+[m				[32mexpectedN:        2,[m
[32m+[m				[32mexpectedGroups:   []*string{sp("bar"), sp("Baz")},[m
[32m+[m				[32mexpectedNext:     defaultStart,[m
[32m+[m				[32mexpectedCauseErr: causeErr,[m
 			},[m
 			{[m
 				name: "MissingQueryID",[m
[36m@@ -791,14 +784,12 @@[m [mfunc TestQueryManager_Query(t *testing.T) {[m
 					Groups: []string{"eggs", "Spam"},[m
 					Limit:  DefaultLimit,[m
 					Chunk:  5 * time.Minute,[m
[31m-					Hint:   minHint,[m
[31m-				},[m
[31m-				startQueryOutput:  &cloudwatchlogs.StartQueryOutput{},[m
[31m-				expectedN:         1,[m
[31m-				expectedChunkHint: minHint,[m
[31m-				expectedGroups:    []*string{sp("eggs"), sp("Spam")},[m
[31m-				expectedNext:      defaultStart,[m
[31m-				expectedCauseErr:  errors.New(outputMissingQueryIDMsg),[m
[32m+[m				[32m},[m
[32m+[m				[32mstartQueryOutput: &cloudwatchlogs.StartQueryOutput{},[m
[32m+[m				[32mexpectedN:        1,[m
[32m+[m				[32mexpectedGroups:   []*string{sp("eggs"), sp("Spam")},[m
[32m+[m				[32mexpectedNext:     defaultStart,[m
[32m+[m				[32mexpectedCauseErr: errors.New(outputMissingQueryIDMsg),[m
 			},[m
 		}[m
 [m
[36m@@ -825,7 +816,6 @@[m [mfunc TestQueryManager_Query(t *testing.T) {[m
 				s2 := s.(*stream)[m
 				assert.Equal(t, testCase.after, s2.QuerySpec)[m
 				assert.Equal(t, testCase.expectedN, s2.n)[m
[31m-				assert.Equal(t, testCase.expectedChunkHint, s2.chunkHint)[m
 				assert.Equal(t, testCase.expectedGroups, s2.groups)[m
 				assert.Equal(t, testCase.expectedNext, s2.next)[m
 				r := make([]Result, 1)[m
[36m@@ -1403,7 +1393,6 @@[m [mvar scenarios = []queryScenario{[m
 			Start:  defaultStart,[m
 			End:    defaultEnd,[m
 			Groups: []string{"/my/empty/group"},[m
[31m-			Hint:   ^uint16(0),[m
 		},[m
 		chunks: []chunkPlan{[m
 			{[m
[36m@@ -1700,7 +1689,6 @@[m [mvar scenarios = []queryScenario{[m
 			Groups:  []string{"/normal/log/group"},[m
 			Limit:   MaxLimit,[m
 			Preview: true,[m
[31m-			Hint:    5,[m
 		},[m
 		chunks: []chunkPlan{[m
 			{[m
[36m@@ -1788,7 +1776,6 @@[m [mvar scenarios = []queryScenario{[m
 			End:     defaultEnd.Add(time.Hour),[m
 			Groups:  []string{"/trove/of/data"},[m
 			Preview: true,[m
[31m-			Hint:    1,[m
 		},[m
 		chunks: []chunkPlan{[m
 			{[m
