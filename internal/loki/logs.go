// logs: typed log rows with SQL filters pushed into LogQL.
//
//	SELECT ts, pod, line
//	FROM loki.main.logs('{namespace="shop"} | logfmt')
//	WHERE app = 'checkout' AND level = 'error' AND line LIKE '%timeout%'
//	  AND ts >= TIMESTAMPTZ '2026-01-01 00:00:00+00' AND ts < TIMESTAMPTZ '2026-01-01 02:00:00+00'
//	LIMIT 100;
//
// becomes the request
//
//	{namespace="shop", app="checkout", level="error"} | logfmt |= "timeout"
//
// over [00:00, 02:00), paged and chunked by logscan.go. Stream-label
// predicates compile into the selector, line predicates into line filters
// appended after the user's pipeline, ts/ts_ns ranges into the window, and
// ORDER BY ts [DESC] LIMIT n into the direction and line limit (selector.go).
// Every pushed predicate is then re-applied exactly (filters.go).

package loki

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

type logsArgs struct {
	Selector string `vgi:"pos=0,doc=LogQL log query: a stream selector with an optional pipeline, e.g. '{namespace=\"shop\"}' or '{app=\"api\"} | json'. Omit it (or pass '{}') to build the selector entirely from WHERE filters on label columns."`

	LabelColumns string     `vgi:"name=label_columns,default=,doc=Comma-separated extra stream labels to expose as their own VARCHAR columns (after job/namespace/pod/app/level) so WHERE filters on them push into the selector, e.g. 'cluster,service_name'."`
	StartDate    *time.Time `vgi:"name=start_date,doc=Window start (inclusive) when not given by WHERE ts >= ...; default: one hour before the end, like Loki."`
	EndDate      *time.Time `vgi:"name=end_date,doc=Window end (exclusive) when not given by WHERE ts < ...; default: now."`
	MaxLines     *int64     `vgi:"name=max_lines,ge=1,doc=Stop after this many matching lines. NULL (default) reads every line; a SQL LIMIT also stops the scan early."`
	Direction    *string    `vgi:"name=direction,choices='backward,forward',doc=Either 'backward' (newest first; the default) or 'forward' (oldest first). When omitted, ORDER BY ts [DESC] LIMIT n picks it."`
	PageSize     int64      `vgi:"name=page_size,default=5000,ge=1,le=100000,doc=Entries per request; must not exceed the server's max_entries_limit_per_query (5000 by default)."`
	ChunkSeconds int64      `vgi:"name=chunk_seconds,default=3600,ge=1,doc=The window is read in epoch-aligned chunks of this many seconds (matching Loki's split_queries_by_interval)."`
	SecretName   *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

// logsNoSelectorArgs are the arguments of the logs(...) overload without a selector.
type logsNoSelectorArgs struct {
	LabelColumns string     `vgi:"name=label_columns,default=,doc=Comma-separated extra stream labels to expose as their own VARCHAR columns (after job/namespace/pod/app/level) so WHERE filters on them push into the selector, e.g. 'cluster,service_name'."`
	StartDate    *time.Time `vgi:"name=start_date,doc=Window start (inclusive) when not given by WHERE ts >= ...; default: one hour before the end, like Loki."`
	EndDate      *time.Time `vgi:"name=end_date,doc=Window end (exclusive) when not given by WHERE ts < ...; default: now."`
	MaxLines     *int64     `vgi:"name=max_lines,ge=1,doc=Stop after this many matching lines. NULL (default) reads every line; a SQL LIMIT also stops the scan early."`
	Direction    *string    `vgi:"name=direction,choices='backward,forward',doc=Either 'backward' (newest first; the default) or 'forward' (oldest first). When omitted, ORDER BY ts [DESC] LIMIT n picks it."`
	PageSize     int64      `vgi:"name=page_size,default=5000,ge=1,le=100000,doc=Entries per request; must not exceed the server's max_entries_limit_per_query (5000 by default)."`
	ChunkSeconds int64      `vgi:"name=chunk_seconds,default=3600,ge=1,doc=The window is read in epoch-aligned chunks of this many seconds (matching Loki's split_queries_by_interval)."`
	SecretName   *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a logsArgs) secret() string { return deref(a.SecretName) }

// LogsState is the scan cursor (gob-encoded into the HTTP continuation token).
type LogsState struct {
	Scan        LogScan
	Columns     []colSpec
	FiltersJSON string
	SecretName  string
}

// LogsFunc implements logs(selector, ...); NoSelector is the logs(...)
// overload, which builds the whole selector from filters.
type LogsFunc struct{ NoSelector bool }

var _ vgi.TypedTableFunc[LogsState] = (*LogsFunc)(nil)

var logsExamples = []vgi.CatalogExample{
	{SQL: "SELECT pod, count(*) AS errors FROM loki.main.logs('{namespace=\"shop\"}') " +
		"WHERE app = 'checkout' AND level = 'error' AND line LIKE '%declined%' " +
		"AND ts >= TIMESTAMPTZ '2026-01-01 00:00:00+00' AND ts < TIMESTAMPTZ '2026-01-01 02:00:00+00' GROUP BY pod ORDER BY pod",
		Description: "Declined-payment errors per pod; app and level become stream matchers, the LIKE a |= line filter and the ts range the request window"},
	{SQL: "SELECT ts, parsed['status'] AS status, line FROM loki.main.logs('{app=\"checkout\"} | logfmt', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 02:00:00+00') " +
		"ORDER BY ts DESC LIMIT 5",
		Description: "The five newest checkout lines with a logfmt-parsed status; ORDER BY ts DESC LIMIT 5 becomes a backward query with limit 5"},
}

// Name is the SQL name.
func (*LogsFunc) Name() string { return "logs" }

// Metadata describes the function.
func (*LogsFunc) Metadata() vgi.FunctionMetadata {
	tags := objectTags(docs{
		Title:    "Log Lines With LogQL Pushdown",
		Category: "logs",
		Keywords: "logs, log lines, loki, logql, filter pushdown, stream selector, line filter, grep, structured metadata, search",
		DocLLM: "Read log lines as typed rows with SQL WHERE filters pushed into LogQL. Positional arg (optional): a LogQL log query " +
			"(stream selector plus optional pipeline such as '| json' or '| logfmt'); omit it to build the selector from filters. " +
			"Named: label_columns (extra stream labels as `VARCHAR` columns, e.g. 'cluster'), start_date/end_date (window, default " +
			"the last hour; end exclusive), max_lines, direction ('backward' default or 'forward'), page_size (default 5000), " +
			"chunk_seconds (default 3600), secret_name. Columns: ts `TIMESTAMPTZ`, ts_ns `BIGINT`, job, namespace, pod, app, level, " +
			"label_columns..., labels `MAP` (all stream labels), line `VARCHAR`, structured_metadata `MAP`, parsed `MAP` (fields from " +
			"parsers in the query). Pushdown: =, <>, IN, IS [NOT] NULL, LIKE, NOT LIKE, prefix/suffix/contains on label columns become " +
			"stream matchers; contains(line,'x'), line LIKE '%x%', prefix/suffix/= on line become '|= \"x\"' line filters and NOT " +
			"contains / NOT LIKE '%x%' become '!= \"x\"'; ts and ts_ns comparisons become the request window; ORDER BY ts [DESC] " +
			"LIMIT n sets the direction and limit. Every filter is re-applied exactly, so results always match the SQL. Loki " +
			"needs at least one non-empty stream matcher (e.g. app = 'x' or a selector argument). Filters on labels['x'], " +
			"parsed[...] or structured_metadata[...] run in DuckDB; list a stream label in label_columns to push it, or write " +
			"parser filters into the LogQL argument (e.g. '{app=\"api\"} | json | status >= 500').",
		DocMD: "## logs(selector := '{}', label_columns := '', start_date, end_date, max_lines, direction, ...)\n\n" +
			"Typed log rows. SQL filters on stream-label columns become stream matchers, filters on `line` become LogQL line " +
			"filters, `ts` ranges become the request window, and `ORDER BY ts DESC LIMIT n` becomes a backward query with a limit. " +
			"Every filter is also re-applied exactly by the worker.",
	})
	tags["vgi.result_dynamic_columns_md"] = "The result has `ts`, `ts_ns`, `job`, `namespace`, `pod`, `app`, `level`, then one " +
		"`VARCHAR` column per name in `label_columns` (in the order given), then `labels`, `line`, `structured_metadata` and `parsed`.\n\n" +
		columnsMD(logColumns([]string{"cluster"}))
	return vgi.FunctionMetadata{
		Description:     "Log lines as typed rows; label, line and time filters push down into the LogQL query",
		Stability:       vgi.StabilityVolatile,
		FilterPushdown:  true,
		Tags:            withExamples(tags, logsExamples),
		RequiredSecrets: secretRequirements,
		Examples:        logsExamples,
	}
}

// ArgumentSpecs derives the signature.
func (f *LogsFunc) ArgumentSpecs() []vgi.ArgSpec {
	if f.NoSelector {
		return vgi.DeriveArgSpecs(logsNoSelectorArgs{})
	}
	return vgi.DeriveArgSpecs(logsArgs{})
}

// args decodes either overload's arguments.
func (f *LogsFunc) args(raw *vgi.Arguments) (logsArgs, error) {
	if f.NoSelector {
		var a logsNoSelectorArgs
		err := vgi.BindArgs(raw, &a)
		return logsArgs{Selector: "{}", LabelColumns: a.LabelColumns, StartDate: a.StartDate, EndDate: a.EndDate,
			MaxLines: a.MaxLines, Direction: a.Direction, PageSize: a.PageSize, ChunkSeconds: a.ChunkSeconds,
			SecretName: a.SecretName}, err
	}
	var a logsArgs
	err := vgi.BindArgs(raw, &a)
	return a, err
}

// reservedColumns cannot be used as label column names.
var reservedColumns = map[string]bool{"ts": true, "ts_ns": true, "labels": true, "line": true,
	"structured_metadata": true, "parsed": true}

func init() {
	for _, l := range hoistedLabels {
		reservedColumns[l] = true
	}
}

// parseLabelColumns validates the label_columns argument.
func parseLabelColumns(s string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !legacyLabelName.MatchString(p) || strings.HasPrefix(p, "__") {
			return nil, fmt.Errorf("label_columns: %q is not a valid Loki label name", p)
		}
		if reservedColumns[p] {
			return nil, fmt.Errorf("label_columns: %q is already a column; read it directly", p)
		}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out, nil
}

// checkLogSelector lexes the selector argument and checks it is a log query.
func checkLogSelector(sel string) (*logqlInfo, error) {
	li, err := analyzeLogQL(sel)
	if err != nil {
		return nil, fmt.Errorf("logs: %w", err)
	}
	if !li.IsLog || len(li.Selectors) == 0 {
		return nil, fmt.Errorf("logs: %q is not a log query; pass a stream selector such as '{app=\"api\"}' (use query_range for metric queries)", sel)
	}
	return li, nil
}

// OnBind declares the layout, which depends on label_columns.
func (f *LogsFunc) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	a, err := f.args(params.Args)
	if err != nil {
		return nil, err
	}
	extra, err := parseLabelColumns(a.LabelColumns)
	if err != nil {
		return nil, err
	}
	if _, err := checkLogSelector(a.Selector); err != nil {
		return nil, err
	}
	return bindNamedSecret(params, a.secret(), schemaOf(logColumns(extra)))
}

// NewState compiles the pushed filters and plans the scan.
func (f *LogsFunc) NewState(params *vgi.ProcessParams) (*LogsState, error) {
	a, err := f.args(params.Args)
	if err != nil {
		return nil, err
	}
	extra, err := parseLabelColumns(a.LabelColumns)
	if err != nil {
		return nil, err
	}
	li, err := checkLogSelector(a.Selector)
	if err != nil {
		return nil, err
	}
	userMatchers, err := li.matchers(li.Selectors[0])
	if err != nil {
		return nil, fmt.Errorf("logs: %w", err)
	}
	cols := logColumns(extra)
	preds, err := compileFilters(params.CurrentPushdownFilters, schemaOf(cols))
	if err != nil {
		return nil, err
	}
	filtersJSON, err := json.Marshal(preds)
	if err != nil {
		return nil, err
	}
	plan := planPushdown(preds, columnsByName(cols), !li.rewritesLabels())
	if !hasRequiredMatcher(append(append([]matcher{}, userMatchers...), plan.Matchers...)) {
		return nil, fmt.Errorf("logs: Loki needs at least one stream matcher that cannot match an empty value: pass a selector such as '{app=\"api\"}' or filter a label column (WHERE app = 'api')")
	}
	backward, err := parseDirection(deref(a.Direction))
	if err != nil {
		return nil, err
	}
	w := plan.Window
	if a.StartDate != nil {
		w.raiseLo(a.StartDate.UnixNano())
	}
	if a.EndDate != nil {
		w.lowerHi(a.EndDate.UnixNano())
	}
	end := time.Now().UnixNano()
	if w.Hi != nil {
		end = *w.Hi
	}
	start := end - int64(time.Hour)
	if w.Lo != nil {
		start = *w.Lo
	}
	st := &LogsState{
		Scan: LogScan{Query: mergeQuery(a.Selector, li, plan.Matchers, plan.LineFilters), StartNs: start, EndNs: end,
			Backward: backward, PageSize: a.PageSize, ChunkNs: a.ChunkSeconds * 1_000_000_000},
		Columns:     cols,
		FiltersJSON: string(filtersJSON),
		SecretName:  deref(a.SecretName),
	}
	if a.MaxLines != nil {
		st.Scan.MaxLines = *a.MaxLines
	}
	applyOrderHint(params, &st.Scan, a.Direction != nil)
	slog.Debug("logs pushdown", "query", st.Scan.Query, "start_ns", start, "end_ns", end, "backward", st.Scan.Backward,
		"max_lines", st.Scan.MaxLines, "filters", st.FiltersJSON)
	if _, err := resolveConn(params.Secrets, st.SecretName); err != nil {
		return nil, err
	}
	return st, nil
}

// Process fetches one page per call and emits the rows that pass the filters.
func (*LogsFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *LogsState, out *vgirpc.OutputCollector) error {
	if st.Scan.Done {
		return out.Finish()
	}
	c, err := resolveConn(params.Secrets, st.SecretName)
	if err != nil {
		return err
	}
	preds, err := predsFromJSON(st.FiltersJSON)
	if err != nil {
		return err
	}
	return emitLogPage(ctx, c, &st.Scan, st.Columns, preds, out)
}
