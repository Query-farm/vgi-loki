package loki

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func col(name string) *expr            { return &expr{K: "col", S: name} }
func strLit(s string) *expr            { return &expr{K: "str", S: s} }
func call(fn string, a ...*expr) *expr { return &expr{K: "call", S: fn, Args: a} }

func TestNewWorkerBuildsEveryFunction(t *testing.T) {
	// Registration derives every argument spec; a bad vgi tag panics here.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("NewWorker panicked: %v", r)
		}
	}()
	_ = NewWorker()
	for _, f := range []*LogsFunc{{}, {NoSelector: true}} {
		_ = f.ArgumentSpecs()
	}
	_ = (&QueryRangeFunc{}).ArgumentSpecs()
	_ = (&QueryFunc{}).ArgumentSpecs()
}

func TestQueryKind(t *testing.T) {
	for q, want := range map[string]bool{
		`{app="x"}`: true,
		`  ( {app="x"} | json | line_format "{{.a}}" )`: true,
		`# comment\n{app="x"} |= "a"`:                   true,
		`rate({app="x"}[5m])`:                           false,
		`sum by (a) (count_over_time({a="b"}[1m]))`:     false,
		`vector(1)`:                false,
		`(sum(rate({a="b"}[1m])))`: false,
	} {
		li, err := analyzeLogQL(strings.ReplaceAll(q, `\n`, "\n"))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if li.IsLog != want {
			t.Errorf("%s: IsLog=%v, want %v", q, li.IsLog, want)
		}
	}
	for _, bad := range []string{``, `{app="x"`, `{app="x}`, `rate({a="b"}[5m]`, "`raw", `{a="b"})`} {
		if _, err := analyzeLogQL(bad); err == nil {
			t.Errorf("%q: want a lex error", bad)
		}
	}
}

func TestSelectorsSkipStringsAndComments(t *testing.T) {
	q := `sum(rate({app="a\"}"} | line_format "{{.x}}" [5m])) / sum(rate({app=` + "`b`" + `}[5m])) # {c="d"}`
	li, err := analyzeLogQL(q)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, s := range li.Selectors {
		got = append(got, li.selectorText(q, s))
	}
	want := []string{`{app="a\"}"}`, "{app=`b`}"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("selectors = %q, want %q", got, want)
	}
	ms, err := li.matchers(li.Selectors[0])
	if err != nil || len(ms) != 1 || ms[0].Value != `a"}` {
		t.Fatalf("matchers = %+v, %v", ms, err)
	}
}

func TestMergeQuery(t *testing.T) {
	extra := []matcher{{"app", "=", `x"y`}}
	lines := []string{`|= "a\\b"`}
	for in, want := range map[string]string{
		`{}`:                                  `{app="x\"y"} |= "a\\b"`,
		`{ns="shop"}`:                         `{ns="shop", app="x\"y"} |= "a\\b"`,
		`{ns="shop"} | json # trailing`:       `{ns="shop", app="x\"y"} | json |= "a\\b" # trailing`,
		`( {ns="shop"} | logfmt )`:            `( {ns="shop", app="x\"y"} | logfmt |= "a\\b" )`,
		`{ns="s"} | (status >= 400 or x="y")`: `{ns="s", app="x\"y"} | (status >= 400 or x="y") |= "a\\b"`,
	} {
		li, err := analyzeLogQL(in)
		if err != nil {
			t.Fatal(err)
		}
		if got := mergeQuery(in, li, extra, lines); got != want {
			t.Errorf("mergeQuery(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestRequiredMatcher(t *testing.T) {
	cases := []struct {
		ms   []matcher
		want bool
	}{
		{[]matcher{{"app", "=", "x"}}, true},
		{[]matcher{{"app", "=", ""}}, false},
		{[]matcher{{"app", "!=", "x"}}, false},
		{[]matcher{{"app", "=~", ".*"}}, false},
		{[]matcher{{"app", "=~", ".+"}}, true},
		{[]matcher{{"app", "!~", ""}}, true},
		{nil, false},
	}
	for _, c := range cases {
		if got := hasRequiredMatcher(c.ms); got != c.want {
			t.Errorf("%+v: got %v", c.ms, got)
		}
	}
}

func TestQuoteRoundTrips(t *testing.T) {
	for _, s := range []string{`plain`, `say "hi"`, `C:\temp`, "tab\there", "caf\u00e9 \u2603", "nl\nx", "\x00"} {
		q := quoteLogQL(s)
		toks, err := lexLogQL(q)
		if err != nil || len(toks) != 1 || toks[0].Str != s {
			t.Errorf("quote(%q) = %s does not lex back: %+v %v", s, q, toks, err)
		}
	}
}

func TestPlanPushdown(t *testing.T) {
	cols := columnsByName(logColumns([]string{"cluster"}))
	preds := []*expr{
		call("=", col("app"), strLit("checkout")),
		call("in", col("pod"), strLit("a"), strLit("b.1")),
		call("contains", col("line"), strLit(`say "hi"`)),
		{K: "not", Args: []*expr{call("contains", col("line"), strLit("GET"))}},
		call("~~", col("line"), strLit("%a_b%c")),
		call("!~~", col("line"), strLit("%refund%")),
		call(">=", col("ts"), &expr{K: "ts", I: 1_767_225_600_000_000}),
		call("<", col("ts_ns"), &expr{K: "int", I: 1_767_229_200_000_000_123}),
		call("=", col("cluster"), strLit("eu")),
	}
	plan := planPushdown(preds, cols, true)
	var ms []string
	for _, m := range plan.Matchers {
		ms = append(ms, m.String())
	}
	if got, want := strings.Join(ms, ","), `app="checkout",pod=~"a|b\\.1",cluster="eu"`; got != want {
		t.Errorf("matchers = %s, want %s", got, want)
	}
	if got, want := strings.Join(plan.LineFilters, " "), `|= "say \"hi\"" != "GET" |= "a" |= "b" |= "c" != "refund"`; got != want {
		t.Errorf("line filters = %s, want %s", got, want)
	}
	if *plan.Window.Lo != 1_767_225_600_000_000_000 || *plan.Window.Hi != 1_767_229_200_000_000_123 {
		t.Errorf("window = %d..%d", *plan.Window.Lo, *plan.Window.Hi)
	}
	// Label pushdown off (pipeline rewrites labels): only line and time remain.
	plan = planPushdown(preds, cols, false)
	if len(plan.Matchers) != 0 || len(plan.LineFilters) != 6 {
		t.Errorf("pushLabels=false: %+v", plan)
	}
}

func TestTsBoundsAreExact(t *testing.T) {
	cols := columnsByName(logColumns(nil))
	us := int64(1_767_225_600_000_000)
	for op, want := range map[string][2]int64{
		">=": {us * 1000, -1}, ">": {(us + 1) * 1000, -1},
		"<": {-1, us * 1000}, "<=": {-1, (us + 1) * 1000}, "=": {us * 1000, (us + 1) * 1000},
	} {
		var w timeWindow
		if !tsBound(call(op, col("ts"), &expr{K: "ts", I: us}), cols, &w) {
			t.Fatalf("%s not pushed", op)
		}
		got := [2]int64{-1, -1}
		if w.Lo != nil {
			got[0] = *w.Lo
		}
		if w.Hi != nil {
			got[1] = *w.Hi
		}
		if got != want {
			t.Errorf("ts %s: window %v, want %v", op, got, want)
		}
	}
}

func TestFiltersEvaluateLogRows(t *testing.T) {
	cols := columnsByName(logColumns(nil))
	r := row{Labels: map[string]string{"app": "x"}, TsNs: 1_767_225_600_000_000_500, Line: "hello world"}
	env := &rowEnv{cols: cols, r: &r}
	for _, c := range []struct {
		p    *expr
		want bool
	}{
		{call("=", col("app"), strLit("x")), true},
		{call("=", col("pod"), strLit("x")), false},
		{call("isnull", col("pod")), true},
		{call("contains", col("line"), strLit("lo wo")), true},
		{call("~~", col("line"), strLit("h_llo%")), true},
		{call("=", col("ts_ns"), &expr{K: "int", I: 1_767_225_600_000_000_500}), true},
		{call(">", col("ts_ns"), &expr{K: "int", I: 1_767_225_600_000_000_500}), false},
		{call("=", col("ts"), &expr{K: "ts", I: 1_767_225_600_000_000}), true},
	} {
		got, err := matchAll([]*expr{c.p}, env)
		if err != nil || got != c.want {
			t.Errorf("%+v: got %v, %v", c.p, got, err)
		}
	}
}

func TestDecodeStreamsAndMetric(t *testing.T) {
	data := json.RawMessage(`{"resultType":"streams","result":[{"stream":{"app":"a"},"values":[
		["1767225600000000001","l1",{"structuredMetadata":{"trace_id":"t"},"parsed":{"k":"v"}}],["1767225600000000002","l2"]]}]}`)
	rows, err := decodeStreams(data)
	if err != nil || len(rows) != 2 || rows[0].Structured["trace_id"] != "t" || rows[0].Parsed["k"] != "v" || rows[1].TsNs != 1767225600000000002 {
		t.Fatalf("streams: %+v %v", rows, err)
	}
	data = json.RawMessage(`{"resultType":"matrix","result":[{"metric":{"level":"info"},"values":[[1767225600.5,"3"],[1767225601,"NaN"]]}]}`)
	rows, err = decodeMetric(data)
	if err != nil || len(rows) != 2 || rows[0].TsNs != 1767225600_500_000_000 || rows[0].Value != 3 || rows[1].Value == rows[1].Value {
		t.Fatalf("matrix: %+v %v", rows, err)
	}
}

// fakeLoki serves query_range over a fixed entry set with Loki's semantics:
// [start, end), limit, direction, and the 3.7 quirk of skipping entries that
// sit exactly on the query start at a chunk's end.
func fakeLoki(t *testing.T, entries []int64, quirk bool) (*httptest.Server, *int) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		q := r.URL.Query()
		start, _ := strconv.ParseInt(q.Get("start"), 10, 64)
		end, _ := strconv.ParseInt(q.Get("end"), 10, 64)
		limit, _ := strconv.Atoi(q.Get("limit"))
		backward := q.Get("direction") != "forward"
		last := entries[len(entries)-1]
		var sel []int
		for i, ts := range entries {
			skipped := quirk && ts == start && ts == last
			if ts >= start && ts < end && !skipped {
				sel = append(sel, i)
			}
		}
		sort.SliceStable(sel, func(a, b int) bool {
			if entries[sel[a]] != entries[sel[b]] {
				return (entries[sel[a]] > entries[sel[b]]) == backward
			}
			return sel[a] < sel[b]
		})
		if len(sel) > limit {
			sel = sel[:limit]
		}
		vals := [][]string{}
		for _, i := range sel {
			vals = append(vals, []string{strconv.FormatInt(entries[i], 10), fmt.Sprintf("line-%03d", i)})
		}
		body, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{
			"resultType": "streams", "result": []any{map[string]any{"stream": map[string]string{"app": "t"}, "values": vals}}}})
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, &requests
}

func scanAll(t *testing.T, s *LogScan, c *conn) ([]string, error) {
	var lines []string
	for i := 0; !s.Done; i++ {
		if i > 10000 {
			t.Fatal("scan does not terminate")
		}
		rows, err := s.fetchPage(context.Background(), c, 0)
		if err != nil {
			return lines, err
		}
		for _, r := range rows {
			lines = append(lines, r.Line)
		}
	}
	return lines, nil
}

func TestLogScanPagesExactlyOnce(t *testing.T) {
	base := int64(1_767_225_600_000_000_000)
	var entries []int64
	for i := 0; i < 50; i++ {
		entries = append(entries, base+int64(i/3)*1_000_000_000) // three entries per second
	}
	srv, _ := fakeLoki(t, entries, true)
	c := &conn{BaseURL: srv.URL, Headers: http.Header{}}
	for _, backward := range []bool{true, false} {
		for _, page := range []int64{4, 5, 7, 100} {
			for _, chunk := range []int64{3, 3600} {
				s := &LogScan{Query: `{app="t"}`, StartNs: base, EndNs: base + 3600*1_000_000_000, Backward: backward,
					PageSize: page, ChunkNs: chunk * 1_000_000_000}
				lines, err := scanAll(t, s, c)
				if err != nil {
					t.Fatalf("backward=%v page=%d chunk=%d: %v", backward, page, chunk, err)
				}
				seen := map[string]bool{}
				for _, l := range lines {
					if seen[l] {
						t.Fatalf("backward=%v page=%d chunk=%d: duplicate %s", backward, page, chunk, l)
					}
					seen[l] = true
				}
				if len(lines) != len(entries) {
					t.Fatalf("backward=%v page=%d chunk=%d: %d lines, want %d", backward, page, chunk, len(lines), len(entries))
				}
				// Timestamps (line index / 3) must be monotonic in scan direction.
				sec := func(l string) int { n, _ := strconv.Atoi(strings.TrimPrefix(l, "line-")); return n / 3 }
				sorted := true
				for i := 1; i < len(lines); i++ {
					if a, b := sec(lines[i-1]), sec(lines[i]); a != b && (a > b) != backward {
						sorted = false
					}
				}
				if !sorted {
					t.Errorf("backward=%v page=%d: not in scan order", backward, page)
				}
			}
		}
	}
}

func TestLogScanTooManyTies(t *testing.T) {
	base := int64(1_767_225_600_000_000_000)
	entries := []int64{base, base, base, base, base}
	srv, _ := fakeLoki(t, entries, false)
	c := &conn{BaseURL: srv.URL, Headers: http.Header{}}
	s := &LogScan{Query: `{app="t"}`, StartNs: base, EndNs: base + 1, Backward: true, PageSize: 3, ChunkNs: int64(time.Hour)}
	if _, err := scanAll(t, s, c); err == nil || !strings.Contains(err.Error(), "share the timestamp") {
		t.Fatalf("want a ties error, got %v", err)
	}
}

func TestLogScanMaxLinesAndEmptyWindow(t *testing.T) {
	base := int64(1_767_225_600_000_000_000)
	srv, reqs := fakeLoki(t, []int64{base, base + 1, base + 2}, false)
	c := &conn{BaseURL: srv.URL, Headers: http.Header{}}
	s := &LogScan{Query: `{app="t"}`, StartNs: base, EndNs: base, PageSize: 10, ChunkNs: int64(time.Hour)}
	if lines, err := scanAll(t, s, c); err != nil || len(lines) != 0 || *reqs != 0 {
		t.Fatalf("empty window: %v %v requests=%d", lines, err, *reqs)
	}
}

func TestDefaultStep(t *testing.T) {
	if got := defaultStepMs(0, 7_200_000); got != 28_000 {
		t.Errorf("2h default step = %d ms", got)
	}
	if got := defaultStepMs(0, 60_000); got != 1000 {
		t.Errorf("1m default step = %d ms", got)
	}
}
