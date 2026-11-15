// query and query_range: run any LogQL on the server.

package loki

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// DefaultMaxPoints is Loki's per-series point limit for one range query
// ((end-start)/step must not exceed 11,000), the same as Prometheus.
const DefaultMaxPoints = 11000

// ---------------------------------------------------------------- query

type queryArgs struct {
	Logql      string     `vgi:"pos=0,doc=LogQL metric expression, e.g. 'sum by (app) (count_over_time({namespace=\"shop\"}[5m]))'. Log queries need query_range or logs."`
	AtTime     *time.Time `vgi:"name=time,doc=Evaluation time. Omit or NULL for the server's current time."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist; default: the unscoped one."`
}

func (a queryArgs) secret() string { return deref(a.SecretName) }

// QueryState is the instant-query cursor.
type QueryState struct {
	Logql      string
	HasAt      bool
	AtNs       int64
	SecretName string
	Done       bool
}

// QueryFunc implements query(logql, time := ...).
type QueryFunc struct{}

var _ vgi.TypedTableFunc[QueryState] = (*QueryFunc)(nil)

var queryExamples = []vgi.CatalogExample{
	{SQL: "SELECT labels['level'] AS level, value AS lines FROM loki.main.query('sum by (level) (count_over_time({app=\"checkout\"}[1h]))', time := TIMESTAMPTZ '2026-01-01 01:00:00+00') ORDER BY level",
		Description: "Log lines per level for the checkout app in the hour before 01:00 UTC"},
	{SQL: "SELECT labels['app'] AS app, round(value, 3) AS lines_per_second FROM loki.main.query('sum by (app) (rate({namespace=\"shop\"}[5m]))') ORDER BY lines_per_second DESC",
		Description: "Current log volume per app in one namespace"},
}

// Name is the SQL name.
func (*QueryFunc) Name() string { return "query" }

// Metadata describes the function.
func (*QueryFunc) Metadata() vgi.FunctionMetadata {
	tags := objectTags(docs{
		Title:    "LogQL Instant Metric Query",
		Category: "query",
		Keywords: "logql, instant query, metric query, count_over_time, rate, loki, /loki/api/v1/query",
		DocLLM: "Evaluate a LogQL metric expression at one instant (GET /loki/api/v1/query) and return one row per result series. " +
			"Positional arg: the LogQL text, which must be a metric query (count_over_time, rate, sum by, ...); Loki rejects " +
			"log queries here, so use query_range or logs for log lines. Named args: time (`TIMESTAMPTZ`; omitted = now on the " +
			"server), secret_name. Columns are exactly vgi-prometheus query()'s: result_type, metric (always NULL), job, instance, " +
			"labels `MAP(VARCHAR,VARCHAR)`, ts `TIMESTAMPTZ`, value `DOUBLE`, histogram (always NULL), string_value, so Loki and " +
			"Prometheus results UNION ALL. Read grouping labels with labels['name']. Needs a loki secret.",
		DocMD: "## query(logql, time := NULL)\n\n" +
			"Instant LogQL metric evaluation (`/loki/api/v1/query`). Same columns as vgi-prometheus `query`.",
	})
	tags["vgi.result_columns_schema"] = mustJSON(resultColumnsOf(metricColumns))
	return vgi.FunctionMetadata{
		Description:     "Evaluate a LogQL metric expression at one instant and return the result series as rows",
		Stability:       vgi.StabilityVolatile,
		Tags:            withExamples(tags, queryExamples),
		RequiredSecrets: secretRequirements,
		Examples:        queryExamples,
	}
}

// ArgumentSpecs derives the signature.
func (*QueryFunc) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(queryArgs{}) }

// OnBind checks the query kind and declares the output schema.
func (*QueryFunc) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var a queryArgs
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	li, err := analyzeLogQL(a.Logql)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	if li.IsLog {
		return nil, fmt.Errorf("query: %q is a log query; Loki only evaluates metric queries at an instant. Use loki.main.query_range(logql, start, end) or loki.main.logs(selector) for log lines", a.Logql)
	}
	return bindNamedSecret(params, a.secret(), schemaOf(metricColumns))
}

// NewState copies the arguments.
func (*QueryFunc) NewState(params *vgi.ProcessParams) (*QueryState, error) {
	var args queryArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	st := &QueryState{Logql: args.Logql, SecretName: deref(args.SecretName)}
	if args.AtTime != nil {
		st.HasAt, st.AtNs = true, args.AtTime.UnixNano()
	}
	if _, err := resolveConn(params.Secrets, st.SecretName); err != nil {
		return nil, err
	}
	return st, nil
}

// Process runs the query once.
func (*QueryFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *QueryState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	c, err := resolveConn(params.Secrets, st.SecretName)
	if err != nil {
		return err
	}
	p := url.Values{"query": {st.Logql}}
	if st.HasAt {
		p.Set("time", nsParam(st.AtNs))
	}
	data, err := c.data(ctx, apiPath+"query", p, true)
	if err != nil {
		return err
	}
	rows, err := decodeMetric(data)
	if err != nil {
		return err
	}
	st.Done = true
	return emitRows(out, metricColumns, rows)
}

// ---------------------------------------------------------------- query_range

type queryRangeArgs struct {
	Logql             string    `vgi:"pos=0,doc=LogQL log query (e.g. '{app=\"checkout\"} |= \"error\"') or metric query (e.g. 'sum by (level) (count_over_time({app=\"checkout\"}[5m]))')."`
	StartDate         time.Time `vgi:"pos=1,doc=Start of the range (inclusive)."`
	EndDate           time.Time `vgi:"pos=2,doc=End of the range: exclusive for log lines, the last evaluation time for metric queries."`
	MaxLines          *int64    `vgi:"name=max_lines,ge=1,doc=Log queries: stop after this many lines (Loki's limit). NULL (default) pages through every line in the range."`
	Direction         *string   `vgi:"name=direction,choices='backward,forward',doc=Log queries: 'backward' (newest first; the default) or 'forward' (oldest first). When omitted, ORDER BY ts [DESC] LIMIT n picks it."`
	StepSeconds       *float64  `vgi:"name=step,gt=0,doc=Metric queries: resolution step in seconds. NULL uses Loki's default, max(1, floor(range_seconds / 250))."`
	PageSize          int64     `vgi:"name=page_size,default=5000,ge=1,le=100000,doc=Log queries: entries per request; must not exceed the server's max_entries_limit_per_query (5000 by default)."`
	ChunkSeconds      int64     `vgi:"name=chunk_seconds,default=3600,ge=1,doc=Log queries: the range is read in windows of this many seconds, aligned to the epoch like Loki's split_queries_by_interval."`
	MaxPointsPerChunk int64     `vgi:"name=max_points_per_chunk,default=11000,ge=1,le=11000,doc=Metric queries: evaluation points per request (Loki rejects more than 11000)."`
	SecretName        *string   `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a queryRangeArgs) secret() string { return deref(a.SecretName) }

// RangeState is the externalized cursor of query_range: a log scan or a
// chunked metric evaluation.
type RangeState struct {
	IsLog bool
	Log   LogScan
	// Metric mode.
	Logql       string
	StartMs     int64
	EndMs       int64
	StepMs      int64
	ChunkPoints int64
	NextMs      int64
	Chunks      int64
	SecretName  string
}

// QueryRangeFunc implements query_range(logql, start, end, ...).
type QueryRangeFunc struct{}

var _ vgi.TypedTableFunc[RangeState] = (*QueryRangeFunc)(nil)

var queryRangeExamples = []vgi.CatalogExample{
	{SQL: "SELECT ts, pod, line FROM loki.main.query_range('{app=\"checkout\", level=\"error\"} |= \"timeout\"', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 02:00:00+00') ORDER BY ts",
		Description: "Checkout error lines mentioning a timeout in a two-hour window"},
	{SQL: "SELECT labels['level'] AS level, sum(value) AS lines FROM loki.main.query_range('sum by (level) (count_over_time({app=\"checkout\"}[30m]))', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 02:00:00+00', step := 1800) GROUP BY level ORDER BY level",
		Description: "Checkout log lines per level in half-hour buckets, summed"},
}

// Name is the SQL name.
func (*QueryRangeFunc) Name() string { return "query_range" }

// Metadata describes the function.
func (*QueryRangeFunc) Metadata() vgi.FunctionMetadata {
	tags := objectTags(docs{
		Title:    "LogQL Range Query",
		Category: "query",
		Keywords: "logql, range query, log lines, streams, metric query, matrix, /loki/api/v1/query_range, paging, chunking",
		DocLLM: "Run any LogQL over a time range (/loki/api/v1/query_range). Positional args: logql, start (`TIMESTAMPTZ`, inclusive), end. " +
			"The output schema depends on the query kind, decided when the query is bound: a LOG query (starts with a stream " +
			"selector, e.g. '{app=\"x\"} |= \"err\" | json') returns one row per log line: ts `TIMESTAMPTZ`, ts_ns `BIGINT` (exact " +
			"nanoseconds), job, namespace, pod, app, level (stream labels, NULL when absent), labels `MAP` (all stream labels), line, " +
			"structured_metadata `MAP`, parsed `MAP` (labels extracted by | json, | logfmt, ... in your query); end is exclusive. " +
			"A METRIC query (count_over_time, rate, sum by, ...) returns vgi-prometheus's long layout: result_type, metric (NULL), " +
			"job, instance, labels, ts, value `DOUBLE`, histogram (NULL), string_value, so it UNIONs with Prometheus data; end is " +
			"the last evaluation time. Named args: max_lines (stop after N log lines; default all), direction ('backward' newest " +
			"first, default, or 'forward'), step (metric step seconds; default Loki's), page_size (default 5000, the server limit), " +
			"chunk_seconds (default 3600), max_points_per_chunk (default 11000), secret_name. Log pages advance from the last " +
			"timestamp with de-duplication, so every line is returned exactly once. Put parsing and label filters in the LogQL; " +
			"use logs() to have SQL WHERE filters pushed into the query instead.",
		DocMD: "## query_range(logql, start, end, max_lines := NULL, direction := 'backward', step := NULL, ...)\n\n" +
			"Log queries return log rows (paged past the server's 5000-line limit); metric queries return the vgi-prometheus " +
			"long layout (chunked under the 11,000-point limit). The cursor is resumable over HTTP.",
	})
	tags["vgi.result_dynamic_columns_md"] = "The columns depend on the LogQL kind.\n\n" +
		"**Log query** (`{...}` with an optional pipeline):\n\n" + columnsMD(logColumns(nil)) +
		"\n\n**Metric query** (same as vgi-prometheus `query_range`):\n\n" + columnsMD(metricColumns)
	return vgi.FunctionMetadata{
		Description:     "Run a LogQL log or metric query over a time range: log lines paged past the server limit, or metric series as long rows",
		Stability:       vgi.StabilityVolatile,
		Tags:            withExamples(tags, queryRangeExamples),
		RequiredSecrets: secretRequirements,
		Examples:        queryRangeExamples,
	}
}

// ArgumentSpecs derives the signature.
func (*QueryRangeFunc) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(queryRangeArgs{}) }

// OnBind classifies the query and declares the schema for its kind.
func (*QueryRangeFunc) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var a queryRangeArgs
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	li, err := analyzeLogQL(a.Logql)
	if err != nil {
		return nil, fmt.Errorf("query_range: %w", err)
	}
	cols := metricColumns
	if li.IsLog {
		cols = logColumns(nil)
	}
	return bindNamedSecret(params, a.secret(), schemaOf(cols))
}

// stepMillis converts a step in seconds to whole milliseconds.
func stepMillis(sec float64) (int64, error) {
	ms := int64(sec*1000 + 0.5)
	if ms < 1 {
		return 0, fmt.Errorf("step must be at least 0.001 seconds")
	}
	return ms, nil
}

// defaultStepMs is Loki's default step: max(1, floor(range_seconds / 250)) seconds.
func defaultStepMs(startMs, endMs int64) int64 {
	return max(1, (endMs-startMs)/1000/250) * 1000
}

// applyOrderHint lets ORDER BY ts [DESC] LIMIT n pick the direction and cap
// the scan. Only used when the direction was not given explicitly.
func applyOrderHint(params *vgi.ProcessParams, s *LogScan, directionGiven bool) {
	h := params.OrderByHint
	if h == nil || (h.ColumnName != "ts" && h.ColumnName != "ts_ns") {
		return
	}
	backward := h.Direction == vgi.OrderByDescending
	if directionGiven && backward != s.Backward {
		return
	}
	s.Backward = backward
	if h.RowLimit > 0 && (s.MaxLines == 0 || h.RowLimit < s.MaxLines) {
		s.MaxLines = h.RowLimit
	}
	slog.Debug("loki: order-by hint", "column", h.ColumnName, "backward", backward, "limit", h.RowLimit)
}

// NewState validates the range and starts the cursor.
func (*QueryRangeFunc) NewState(params *vgi.ProcessParams) (*RangeState, error) {
	var a queryRangeArgs
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	li, err := analyzeLogQL(a.Logql)
	if err != nil {
		return nil, fmt.Errorf("query_range: %w", err)
	}
	if a.EndDate.Before(a.StartDate) {
		return nil, fmt.Errorf("query_range: end (%s) is before start (%s)", a.EndDate.UTC().Format(time.RFC3339), a.StartDate.UTC().Format(time.RFC3339))
	}
	st := &RangeState{IsLog: li.IsLog, SecretName: deref(a.SecretName)}
	if li.IsLog {
		backward, err := parseDirection(deref(a.Direction))
		if err != nil {
			return nil, err
		}
		st.Log = LogScan{Query: a.Logql, StartNs: a.StartDate.UnixNano(), EndNs: a.EndDate.UnixNano(), Backward: backward,
			PageSize: a.PageSize, ChunkNs: a.ChunkSeconds * 1_000_000_000}
		if a.MaxLines != nil {
			st.Log.MaxLines = *a.MaxLines
		}
		applyOrderHint(params, &st.Log, a.Direction != nil)
	} else {
		st.Logql = a.Logql
		st.StartMs, st.EndMs = a.StartDate.UnixMilli(), a.EndDate.UnixMilli()
		if a.StepSeconds != nil {
			if st.StepMs, err = stepMillis(*a.StepSeconds); err != nil {
				return nil, err
			}
		} else {
			st.StepMs = defaultStepMs(st.StartMs, st.EndMs)
		}
		st.ChunkPoints = a.MaxPointsPerChunk
		st.NextMs = st.StartMs
	}
	if _, err := resolveConn(params.Secrets, st.SecretName); err != nil {
		return nil, err
	}
	return st, nil
}

// chunkEnd is the last evaluation time of the chunk starting at next.
func chunkEnd(next, end, step, points int64) int64 {
	return min(end, next+(points-1)*step)
}

// Process fetches one page (log) or one chunk (metric) per call.
func (*QueryRangeFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *RangeState, out *vgirpc.OutputCollector) error {
	if st.IsLog {
		if st.Log.Done {
			return out.Finish()
		}
		c, err := resolveConn(params.Secrets, st.SecretName)
		if err != nil {
			return err
		}
		return emitLogPage(ctx, c, &st.Log, logColumns(nil), nil, out)
	}
	if st.NextMs > st.EndMs {
		return out.Finish()
	}
	c, err := resolveConn(params.Secrets, st.SecretName)
	if err != nil {
		return err
	}
	last := chunkEnd(st.NextMs, st.EndMs, st.StepMs, st.ChunkPoints)
	data, err := c.data(ctx, apiPath+"query_range", url.Values{
		"query": {st.Logql}, "start": {nsParam(st.NextMs * 1_000_000)}, "end": {nsParam(last * 1_000_000)}, "step": {formatStep(st.StepMs)},
	}, true)
	if err != nil {
		return fmt.Errorf("%w (chunk %d starting %s)", err, st.Chunks+1, time.UnixMilli(st.NextMs).UTC().Format(time.RFC3339))
	}
	rows, err := decodeMetric(data)
	if err != nil {
		return err
	}
	st.NextMs = last + st.StepMs
	st.Chunks++
	return emitRows(out, metricColumns, rows)
}

// ---------------------------------------------------------------- helpers

// resultColumnsOf documents a column layout for vgi.result_columns_schema.
func resultColumnsOf(cols []colSpec) []resultColumn {
	out := make([]resultColumn, len(cols))
	for i, c := range cols {
		f := c.field()
		desc, _ := f.Metadata.GetValue("comment")
		out[i] = resultColumn{Name: c.Name, Type: sqlTypeOf(c), Description: desc}
	}
	return out
}

// columnsMD renders a column layout as a Markdown table.
func columnsMD(cols []colSpec) string {
	var b strings.Builder
	b.WriteString("| Name | Type | Description |\n|---|---|---|\n")
	for _, c := range cols {
		f := c.field()
		desc, _ := f.Metadata.GetValue("comment")
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", c.Name, sqlTypeOf(c), desc)
	}
	return strings.TrimRight(b.String(), "\n")
}
