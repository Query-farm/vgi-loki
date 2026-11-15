// Offline LogQL tools: scalars that run the worker's own LogQL lexer
// (logql.go) with no server and no secret. They carry the worker's
// executable examples.
//
//	SELECT loki.main.logql_quote('say "hi"');                        -- "say \"hi\""
//	SELECT loki.main.query_type('sum(rate({app="x"}[5m]))');          -- metric
//	SELECT loki.main.stream_selectors('{a="1"} / {b="2"}');           -- [{a="1"}, {b="2"}]
//	SELECT loki.main.selector_labels('{app="x", env=~"p.*"} | json'); -- [app, env]
//
// Full validation needs Loki's parser, which is AGPL-3.0 and is not embedded;
// format_query() asks the server instead.

package loki

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// textArgs is the single text column every tool takes.
type textArgs struct {
	Text *array.String `vgi:"pos=0,const=false,doc=LogQL query text (for logql_quote: any string). NULL gives NULL."`
}

// executableExample is one vgi.executable_examples entry.
type executableExample struct {
	Description string  `json:"description"`
	SQL         string  `json:"sql"`
	Expected    [][]any `json:"expected_result"`
}

// logqlScalar is a vectorized one-column scalar over text.
type logqlScalar struct {
	name       string
	docs       docs
	desc       string
	examples   []vgi.CatalogExample
	executable []executableExample
	result     arrow.DataType
	eval       func(b array.Builder, s string)
}

func (f *logqlScalar) Name() string { return f.name }

func (f *logqlScalar) Metadata() vgi.FunctionMetadata {
	tags := withExamples(objectTags(f.docs), f.examples)
	tags["vgi.executable_examples"] = mustJSON(f.executable)
	return vgi.FunctionMetadata{
		Description: f.desc,
		Stability:   vgi.StabilityConsistent,
		Tags:        tags,
		Examples:    f.examples,
	}
}

func (f *logqlScalar) OnBindTyped(*textArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(f.result)
}

func (f *logqlScalar) ProcessTyped(_ context.Context, _ *textArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	b := array.NewBuilder(memory.NewGoAllocator(), f.result)
	defer b.Release()
	for i := 0; i < int(batch.NumRows()); i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		f.eval(b, vgi.GetStringValue(col, i))
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: f.result, Nullable: true}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, batch.NumRows()), nil
}

const toolsDocSuffix = " Runs locally in the worker: no server, no secret, no network. It lexes LogQL (strings, " +
	"comments, brackets, stream selectors) but is not Loki's full parser; use format_query() for server-side validation."

func appendStrings(b array.Builder, items []string) {
	lb := b.(*array.ListBuilder)
	lb.Append(true)
	vb := lb.ValueBuilder().(*array.StringBuilder)
	for _, s := range items {
		vb.Append(s)
	}
}

func newLogqlQuote() *logqlScalar {
	return &logqlScalar{
		name: "logql_quote",
		desc: "Quote a string as a LogQL string literal, escaping quotes, backslashes and control characters",
		docs: docs{Title: "Quote A LogQL String", Category: "logql",
			Keywords: "logql, quote, escape, string literal, line filter, label matcher, injection",
			DocLLM: "logql_quote(s) returns s as a double-quoted LogQL string literal with \\\" \\\\ and control characters escaped, " +
				"so the server reads back exactly s. Use it to build LogQL from SQL values safely, e.g. " +
				"'{app=' || loki.main.logql_quote(app) || '} |= ' || loki.main.logql_quote(needle). NULL gives NULL." + toolsDocSuffix,
			DocMD: "## logql_quote(s)\n\n`say \"hi\"` → `\"say \\\"hi\\\"\"`. The escaping the worker itself uses for pushed filters."},
		examples: []vgi.CatalogExample{{
			SQL:         "SELECT '{app=' || loki.main.logql_quote('checkout') || '} |= ' || loki.main.logql_quote('error=\"payment declined\"') AS logql",
			Description: "Build a LogQL query from SQL strings with correct escaping"}},
		executable: []executableExample{{Description: "Quote strings with quotes, backslashes and a tab",
			SQL:      "SELECT s, loki.main.logql_quote(s) AS literal FROM (VALUES ('plain'), ('say \"hi\"'), ('C:\\temp'), ('a' || chr(9) || 'b')) t(s) ORDER BY s",
			Expected: [][]any{{"C:\\temp", "\"C:\\\\temp\""}, {"a\tb", "\"a\\tb\""}, {"plain", "\"plain\""}, {"say \"hi\"", "\"say \\\"hi\\\"\""}}}},
		result: arrow.BinaryTypes.String,
		eval: func(b array.Builder, s string) {
			b.(*array.StringBuilder).Append(quoteLogQL(s))
		},
	}
}

func newQueryType() *logqlScalar {
	return &logqlScalar{
		name: "query_type",
		desc: "'log' for a LogQL log query, 'metric' for a metric query, NULL when the text does not lex",
		docs: docs{Title: "Classify A LogQL Query", Category: "logql",
			Keywords: "logql, query type, log query, metric query, classify, lint",
			DocLLM: "query_type(logql) returns 'log' when the query selects log lines (it starts with a stream selector, " +
				"optionally in parentheses: '{app=\"x\"} | json'), 'metric' when it computes samples (count_over_time, rate, " +
				"sum by, vector, ...), and NULL when the text is lexically broken (unterminated string, unbalanced brackets). " +
				"query_range() uses the same rule to choose its output columns: log rows or metric rows." + toolsDocSuffix,
			DocMD: "## query_type(logql)\n\n`log`, `metric`, or `NULL` when the text does not lex."},
		examples: []vgi.CatalogExample{{
			SQL:         "SELECT loki.main.query_type(query) AS kind, count(*) AS rules FROM loki.main.rules() GROUP BY kind ORDER BY kind",
			Description: "How many ruler rules are metric queries"}},
		executable: []executableExample{{Description: "Classify three queries",
			SQL:      "SELECT q, loki.main.query_type(q) AS kind FROM (VALUES ('{app=\"x\"} |= \"err\"'), ('sum by (app) (rate({app=\"x\"}[5m]))'), ('{app=\"x\"')) t(q) ORDER BY q",
			Expected: [][]any{{"sum by (app) (rate({app=\"x\"}[5m]))", "metric"}, {"{app=\"x\"", nil}, {"{app=\"x\"} |= \"err\"", "log"}}}},
		result: arrow.BinaryTypes.String,
		eval: func(b array.Builder, s string) {
			li, err := analyzeLogQL(s)
			if err != nil {
				b.AppendNull()
				return
			}
			if li.IsLog {
				b.(*array.StringBuilder).Append("log")
			} else {
				b.(*array.StringBuilder).Append("metric")
			}
		},
	}
}

func newStreamSelectors() *logqlScalar {
	return &logqlScalar{
		name: "stream_selectors",
		desc: "Every stream selector ({...}) in a LogQL query, in order (NULL when the text does not lex)",
		docs: docs{Title: "Stream Selectors In LogQL", Category: "logql",
			Keywords: "logql, stream selector, extract, audit, rules, dashboards",
			DocLLM: "stream_selectors(logql) returns the text of every stream selector in the query, in order, as `VARCHAR[]` " +
				"(a binary metric expression has one per side). Braces inside string literals (line_format templates) are not " +
				"selectors. NULL when the text does not lex. Pass a selector to series() or index_stats() to see what a query " +
				"will read." + toolsDocSuffix,
			DocMD: "## stream_selectors(logql)\n\nThe `{...}` selectors of a query, as written."},
		examples: []vgi.CatalogExample{{
			SQL:         "SELECT rule_name, unnest(loki.main.stream_selectors(query)) AS selector FROM loki.main.rules() ORDER BY rule_name",
			Description: "The stream selectors every ruler rule reads"}},
		executable: []executableExample{{Description: "Selectors of a ratio query; braces in a template string are skipped",
			SQL:      "SELECT loki.main.stream_selectors('sum(rate({app=\"api\", level=\"error\"} | line_format \"{{.msg}}\" [5m])) / sum(rate({app=\"api\"}[5m]))') AS selectors",
			Expected: [][]any{{[]any{"{app=\"api\", level=\"error\"}", "{app=\"api\"}"}}}}},
		result: arrow.ListOf(arrow.BinaryTypes.String),
		eval: func(b array.Builder, s string) {
			li, err := analyzeLogQL(s)
			if err != nil {
				b.AppendNull()
				return
			}
			out := make([]string, len(li.Selectors))
			for i, sel := range li.Selectors {
				out[i] = li.selectorText(s, sel)
			}
			appendStrings(b, out)
		},
	}
}

func newSelectorLabels() *logqlScalar {
	return &logqlScalar{
		name: "selector_labels",
		desc: "Sorted distinct stream label names a LogQL query's selectors match on (NULL when the text does not lex)",
		docs: docs{Title: "Labels Used By LogQL Selectors", Category: "logql",
			Keywords: "logql, labels, stream labels, dependencies, audit, cardinality",
			DocLLM: "selector_labels(logql) returns the sorted distinct label names used by the matchers of every stream " +
				"selector in the query, as `VARCHAR[]`. Labels created by parsers later in the pipeline are not included. NULL " +
				"when the text does not lex or a selector is malformed. Use it to find which rules or dashboards depend on a " +
				"stream label before renaming or dropping it." + toolsDocSuffix,
			DocMD: "## selector_labels(logql)\n\nSorted, distinct label names in the query's stream selectors (`VARCHAR[]`)."},
		examples: []vgi.CatalogExample{{
			SQL:         "SELECT l AS label, count(*) AS rules FROM loki.main.rules(), unnest(loki.main.selector_labels(query)) u(l) GROUP BY l ORDER BY rules DESC",
			Description: "Which stream labels the most rules depend on"}},
		executable: []executableExample{{Description: "Labels of a two-selector query",
			SQL:      "SELECT loki.main.selector_labels('sum(count_over_time({namespace=\"shop\", app=~\"check.*\"} | json [5m])) / sum(count_over_time({namespace=\"shop\"}[5m]))') AS labels",
			Expected: [][]any{{[]any{"app", "namespace"}}}}},
		result: arrow.ListOf(arrow.BinaryTypes.String),
		eval: func(b array.Builder, s string) {
			li, err := analyzeLogQL(s)
			if err != nil {
				b.AppendNull()
				return
			}
			labels, err := li.selectorLabels()
			if err != nil {
				b.AppendNull()
				return
			}
			appendStrings(b, labels)
		},
	}
}

func toolScalars() []*logqlScalar {
	return []*logqlScalar{newLogqlQuote(), newQueryType(), newStreamSelectors(), newSelectorLabels()}
}

// toolsExamples are the LogQL tools' catalog examples.
func toolsExamples() [][]vgi.CatalogExample {
	var out [][]vgi.CatalogExample
	for _, f := range toolScalars() {
		out = append(out, f.examples)
	}
	return out
}

func registerTools(w *vgi.Worker) {
	for _, f := range toolScalars() {
		w.RegisterScalar(vgi.AsScalarFunction[textArgs](f))
	}
}
