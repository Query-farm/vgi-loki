// Metadata and status functions. Each makes one request.
//
//	labels, label_values, series       stream index (/loki/api/v1/labels, label/<n>/values, series)
//	index_stats, volume                index statistics (/loki/api/v1/index/stats, index/volume)
//	detected_fields, detected_labels   field/label discovery (Loki 3.x; 404 on older servers)
//	patterns                           pattern-ingester patterns (needs pattern_ingester.enabled)
//	rules, alerts                      the ruler's Prometheus-compatible API (/prometheus/api/v1/...)
//	build_info, format_query           server version and server-side LogQL validation

package loki

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

var (
	str = arrow.BinaryTypes.String
	i64 = arrow.PrimitiveTypes.Int64
	f64 = arrow.PrimitiveTypes.Float64
)

// timeRangeParams adds optional start/end parameters (Unix nanoseconds).
func timeRangeParams(p url.Values, start, end *time.Time) {
	if start != nil {
		p.Set("start", nsParam(start.UnixNano()))
	}
	if end != nil {
		p.Set("end", nsParam(end.UnixNano()))
	}
}

func meta(title, category, keywords, docLLM, docMD, desc string, cols []arrow.Field, examples []vgi.CatalogExample) func() vgi.FunctionMetadata {
	return func() vgi.FunctionMetadata {
		tags := objectTags(docs{Title: title, Category: category, Keywords: keywords, DocLLM: docLLM, DocMD: docMD})
		rc := make([]resultColumn, len(cols))
		for i, f := range cols {
			d, _ := f.Metadata.GetValue("comment")
			rc[i] = resultColumn{Name: f.Name, Type: duckType(f.Type), Description: d}
		}
		tags["vgi.result_columns_schema"] = mustJSON(rc)
		return vgi.FunctionMetadata{
			Description:     desc,
			Stability:       vgi.StabilityVolatile,
			Tags:            withExamples(tags, examples),
			RequiredSecrets: secretRequirements,
			Examples:        examples,
		}
	}
}

func duckType(t arrow.DataType) string {
	switch t.ID() {
	case arrow.STRING:
		return "VARCHAR"
	case arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.TIMESTAMP:
		return "TIMESTAMP WITH TIME ZONE"
	case arrow.MAP:
		return "MAP(VARCHAR, VARCHAR)"
	case arrow.LIST:
		return "VARCHAR[]"
	}
	return t.String()
}

// notFoundHint adds what a 404 means for an optional API.
func notFoundHint(err error, hint string) error {
	var ae *apiError
	if errors.As(err, &ae) && ae.Status == http.StatusNotFound {
		return fmt.Errorf("%w; %s", err, hint)
	}
	return err
}

// ---------------------------------------------------------------- labels

type labelsArgs struct {
	Match      *string    `vgi:"name=match,doc=Optional stream selector restricting which streams' labels are listed, e.g. '{namespace=\"shop\"}'."`
	StartDate  *time.Time `vgi:"name=start_date,doc=Window start; default: the server's (6 hours before end)."`
	EndDate    *time.Time `vgi:"name=end_date,doc=Window end; default: now."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a labelsArgs) secret() string { return deref(a.SecretName) }

var labelsColumns = []arrow.Field{column("label", str, "A stream label name present in the window.", false)}

var labelsExamples = []vgi.CatalogExample{
	{SQL: "SELECT label FROM loki.main.labels(match := '{namespace=\"shop\"}', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 02:00:00+00') ORDER BY label",
		Description: "Which stream labels the shop namespace uses"},
	{SQL: "SELECT count(*) AS label_names FROM loki.main.labels()",
		Description: "How many distinct stream label names were seen in the last six hours"},
}

func newLabels() vgi.TableFunction {
	return newSimple(&simpleFunc[labelsArgs]{
		name:    "labels",
		columns: labelsColumns,
		meta: meta("Stream Label Names", "metadata", "labels, label names, stream labels, /loki/api/v1/labels, schema discovery",
			"List stream label names (/loki/api/v1/labels). Named args: match (optional stream selector, sent as Loki's query "+
				"parameter), start_date, end_date (`TIMESTAMPTZ`; default the last 6 hours), secret_name. One column: label. "+
				"These are index labels; fields inside lines come from detected_fields().",
			"## labels(match := NULL, start_date := NULL, end_date := NULL)\n\nEvery stream label name in the window.",
			"List the stream label names known to Loki, optionally for matching streams", labelsColumns, labelsExamples),
		fetch: func(ctx context.Context, c *conn, a labelsArgs) ([][]any, error) {
			p := url.Values{}
			if a.Match != nil {
				p.Set("query", *a.Match)
			}
			timeRangeParams(p, a.StartDate, a.EndDate)
			data, err := c.data(ctx, apiPath+"labels", p, false)
			if err != nil {
				return nil, err
			}
			return stringRows(data)
		},
	})
}

func stringRows(data json.RawMessage) ([][]any, error) {
	var names []string
	if len(data) > 0 && string(data) != "null" {
		if err := json.Unmarshal(data, &names); err != nil {
			return nil, fmt.Errorf("decoding names: %w", err)
		}
	}
	rows := make([][]any, len(names))
	for i, n := range names {
		rows[i] = []any{n}
	}
	return rows, nil
}

// ---------------------------------------------------------------- label_values

type labelValuesArgs struct {
	Label      string     `vgi:"pos=0,pattern=^[a-zA-Z_][a-zA-Z0-9_]*$,doc=Stream label name, e.g. 'app' or 'namespace'."`
	Match      *string    `vgi:"name=match,doc=Optional stream selector restricting which streams' values are listed."`
	StartDate  *time.Time `vgi:"name=start_date,doc=Window start; default: the server's (6 hours before end)."`
	EndDate    *time.Time `vgi:"name=end_date,doc=Window end; default: now."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a labelValuesArgs) secret() string { return deref(a.SecretName) }

var labelValuesColumns = []arrow.Field{column("value", str, "A value of the label.", false)}

var labelValuesExamples = []vgi.CatalogExample{
	{SQL: "SELECT value AS app FROM loki.main.label_values('app', match := '{namespace=\"shop\"}', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 02:00:00+00') ORDER BY app",
		Description: "Apps logging in the shop namespace"},
	{SQL: "SELECT count(*) AS namespaces FROM loki.main.label_values('namespace')",
		Description: "How many namespaces logged in the last six hours"},
}

func newLabelValues() vgi.TableFunction {
	return newSimple(&simpleFunc[labelValuesArgs]{
		name:    "label_values",
		columns: labelValuesColumns,
		meta: meta("Stream Label Values", "metadata", "label values, stream labels, /loki/api/v1/label/values, discovery, cardinality",
			"List the values of one stream label (/loki/api/v1/label/<name>/values). Positional: label name. Named: match (optional "+
				"stream selector), start_date, end_date (default the last 6 hours), secret_name. One column: value.",
			"## label_values(label, match := NULL, start_date := NULL, end_date := NULL)\n\nEvery value of one stream label in the window.",
			"List the values of one stream label, optionally for matching streams", labelValuesColumns, labelValuesExamples),
		fetch: func(ctx context.Context, c *conn, a labelValuesArgs) ([][]any, error) {
			p := url.Values{}
			if a.Match != nil {
				p.Set("query", *a.Match)
			}
			timeRangeParams(p, a.StartDate, a.EndDate)
			data, err := c.data(ctx, apiPath+"label/"+url.PathEscape(a.Label)+"/values", p, false)
			if err != nil {
				return nil, err
			}
			return stringRows(data)
		},
	})
}

// ---------------------------------------------------------------- series

type seriesArgs struct {
	Match      string     `vgi:"pos=0,doc=Stream selector, e.g. '{namespace=\"shop\"}'."`
	StartDate  *time.Time `vgi:"name=start_date,doc=Window start; default: the server's (6 hours before end)."`
	EndDate    *time.Time `vgi:"name=end_date,doc=Window end; default: now."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a seriesArgs) secret() string { return deref(a.SecretName) }

func hoistedFields(mapDoc string) []arrow.Field {
	var out []arrow.Field
	for _, l := range hoistedLabels {
		out = append(out, column(l, str, "The "+l+" stream label; NULL when absent.", true))
	}
	return append(out, column("labels", labelsType, mapDoc, false))
}

func hoisted(m map[string]string) []any {
	if m == nil {
		m = map[string]string{}
	}
	out := make([]any, 0, len(hoistedLabels)+1)
	for _, l := range hoistedLabels {
		out = append(out, optString(m[l]))
	}
	return append(out, m)
}

var seriesColumns = hoistedFields("Every label of the stream.")

var seriesExamples = []vgi.CatalogExample{
	{SQL: "SELECT app, count(*) AS streams FROM loki.main.series('{namespace=\"shop\"}', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 02:00:00+00') GROUP BY app ORDER BY streams DESC, app",
		Description: "Stream cardinality per app in one namespace"},
	{SQL: "SELECT labels['pod'] AS pod, level FROM loki.main.series('{app=\"checkout\"}', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 02:00:00+00') ORDER BY pod",
		Description: "Which pods and levels the checkout app has streams for"},
}

func newSeries() vgi.TableFunction {
	return newSimple(&simpleFunc[seriesArgs]{
		name:    "series",
		columns: seriesColumns,
		meta: meta("Streams Matching A Selector", "metadata", "series, streams, cardinality, label sets, /loki/api/v1/series, selector",
			"List every stream (label set) matching a selector (/loki/api/v1/series). Positional: the selector. Named: start_date, "+
				"end_date (default the last 6 hours), secret_name. Columns: job, namespace, pod, app, level (NULL when absent) and labels `MAP`. "+
				"Use it for stream-cardinality audits without reading lines.",
			"## series(match, start_date := NULL, end_date := NULL)\n\nEvery stream matching a selector, one row per label set.",
			"List the streams (label sets) that match a selector in a time window", seriesColumns, seriesExamples),
		fetch: func(ctx context.Context, c *conn, a seriesArgs) ([][]any, error) {
			p := url.Values{"match[]": {a.Match}}
			timeRangeParams(p, a.StartDate, a.EndDate)
			data, err := c.data(ctx, apiPath+"series", p, true)
			if err != nil {
				return nil, err
			}
			var sets []map[string]string
			if len(data) > 0 && string(data) != "null" {
				if err := json.Unmarshal(data, &sets); err != nil {
					return nil, fmt.Errorf("decoding series: %w", err)
				}
			}
			rows := make([][]any, len(sets))
			for i, s := range sets {
				rows[i] = hoisted(s)
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- index_stats

type statsArgs struct {
	Query      string     `vgi:"pos=0,doc=Stream selector, e.g. '{namespace=\"shop\"}'."`
	StartDate  *time.Time `vgi:"name=start_date,doc=Window start; default: one hour before end."`
	EndDate    *time.Time `vgi:"name=end_date,doc=Window end; default: now."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a statsArgs) secret() string { return deref(a.SecretName) }

var statsColumns = []arrow.Field{
	column("streams", i64, "Streams matching the selector.", false),
	column("chunks", i64, "Chunks those streams have in the window.", false),
	column("entries", i64, "Log entries (lines) in those chunks.", false),
	column("bytes", i64, "Uncompressed bytes of those chunks.", false),
}

var statsExamples = []vgi.CatalogExample{
	{SQL: "SELECT streams, entries, bytes FROM loki.main.index_stats('{namespace=\"shop\"}', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 02:00:00+00')",
		Description: "How much data a selector would read before running the query"},
	{SQL: "SELECT round(bytes / 1e6, 1) AS megabytes FROM loki.main.index_stats('{app=\"bulk\"}')",
		Description: "Size of one app's logs over the last hour"},
}

func newIndexStats() vgi.TableFunction {
	return newSimple(&simpleFunc[statsArgs]{
		name:    "index_stats",
		columns: statsColumns,
		meta: meta("Index Statistics For A Selector", "metadata", "index stats, cost estimate, query size, bytes, entries, chunks, /loki/api/v1/index/stats",
			"Index statistics for a stream selector (/loki/api/v1/index/stats): streams, chunks, entries and bytes in the window. "+
				"Positional: selector. Named: start_date, end_date (default the last hour), secret_name. One row. Use it to estimate "+
				"how much a logs() or query_range() call would read. Counts come from chunk metadata, so they can over-count data "+
				"still in ingesters.",
			"## index_stats(query, start_date := NULL, end_date := NULL)\n\nStreams, chunks, entries and bytes for a selector.",
			"Streams, chunks, entries and bytes a stream selector covers in a window", statsColumns, statsExamples),
		fetch: func(ctx context.Context, c *conn, a statsArgs) ([][]any, error) {
			p := url.Values{"query": {a.Query}}
			timeRangeParams(p, a.StartDate, a.EndDate)
			body, err := c.raw(ctx, apiPath+"index/stats", p, false)
			if err != nil {
				return nil, notFoundHint(err, "index/stats needs Loki 2.8+ with a TSDB index")
			}
			var s struct {
				Streams, Chunks, Entries, Bytes int64
			}
			if err := json.Unmarshal(body, &s); err != nil {
				return nil, fmt.Errorf("decoding index stats: %w", err)
			}
			return [][]any{{s.Streams, s.Chunks, s.Entries, s.Bytes}}, nil
		},
	})
}

// ---------------------------------------------------------------- volume

type volumeArgs struct {
	Query        string     `vgi:"pos=0,doc=Stream selector, e.g. '{namespace=~\".+\"}'."`
	TargetLabels *string    `vgi:"name=target_labels,doc=Comma-separated labels to aggregate by, e.g. 'namespace,app'; default: the selector's labels."`
	AggregateBy  string     `vgi:"name=aggregate_by,default=series,choices='series,labels',doc=Either 'series' (one row per label combination) or 'labels' (one row per label name)."`
	TopN         int64      `vgi:"name=top_n,default=100,ge=1,le=10000,doc=Largest entries to return (the API's limit)."`
	StartDate    *time.Time `vgi:"name=start_date,doc=Window start; default: one hour before end."`
	EndDate      *time.Time `vgi:"name=end_date,doc=Window end; default: now."`
	SecretName   *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a volumeArgs) secret() string { return deref(a.SecretName) }

var volumeColumns = []arrow.Field{
	column("labels", labelsType, "The aggregated label set (or, with aggregate_by 'labels', the label name).", false),
	column("bytes", i64, "Bytes ingested for that label set in the window.", false),
}

var volumeExamples = []vgi.CatalogExample{
	{SQL: "SELECT labels['namespace'] AS namespace, bytes FROM loki.main.volume('{namespace=~\".+\"}', target_labels := 'namespace', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 02:00:00+00') ORDER BY bytes DESC, namespace",
		Description: "Log volume per namespace, largest first"},
	{SQL: "SELECT labels['app'] AS app, bytes FROM loki.main.volume('{namespace=\"shop\"}', target_labels := 'app', top_n := 5) ORDER BY bytes DESC",
		Description: "The five noisiest apps in a namespace over the last hour"},
}

func newVolume() vgi.TableFunction {
	return newSimple(&simpleFunc[volumeArgs]{
		name:    "volume",
		columns: volumeColumns,
		meta: meta("Log Volume By Label", "metadata", "volume, bytes, ingestion, cost, noisy streams, /loki/api/v1/index/volume",
			"Ingested bytes per label set (/loki/api/v1/index/volume). Positional: selector. Named: target_labels (comma-separated, e.g. "+
				"'namespace,app'), aggregate_by ('series' or 'labels'), top_n (default 100), start_date, end_date (default the last hour), "+
				"secret_name. Columns: labels `MAP`, bytes `BIGINT`. Needs volume_enabled on the server.",
			"## volume(query, target_labels := NULL, aggregate_by := 'series', top_n := 100, ...)\n\nBytes per label set, largest first.",
			"Bytes ingested per label set for a selector, largest first", volumeColumns, volumeExamples),
		fetch: func(ctx context.Context, c *conn, a volumeArgs) ([][]any, error) {
			p := url.Values{"query": {a.Query}, "limit": {strconv.FormatInt(a.TopN, 10)}, "aggregateBy": {a.AggregateBy}}
			if a.TargetLabels != nil {
				p.Set("targetLabels", *a.TargetLabels)
			}
			timeRangeParams(p, a.StartDate, a.EndDate)
			data, err := c.data(ctx, apiPath+"index/volume", p, false)
			if err != nil {
				return nil, notFoundHint(err, "index/volume needs Loki 2.9+ with volume_enabled: true")
			}
			rows, err := decodeMetric(data)
			if err != nil {
				return nil, err
			}
			out := make([][]any, len(rows))
			for i, r := range rows {
				out[i] = []any{r.Labels, int64(r.Value)}
			}
			return out, nil
		},
	})
}

// ---------------------------------------------------------------- detected_fields

type detectedArgs struct {
	Query      string     `vgi:"pos=0,doc=LogQL log query whose lines are sampled, e.g. '{app=\"payments\"}'."`
	StartDate  *time.Time `vgi:"name=start_date,doc=Window start; default: one hour before end."`
	EndDate    *time.Time `vgi:"name=end_date,doc=Window end; default: now."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a detectedArgs) secret() string { return deref(a.SecretName) }

var detectedFieldsColumns = []arrow.Field{
	column("field", str, "Field name, as a parser would extract it.", false),
	column("type", str, "Inferred type: string, int, float, boolean, duration or bytes.", true),
	column("cardinality", i64, "Approximate distinct values in the sampled lines.", true),
	column("parsers", arrow.ListOf(str), "Parsers that extract it (json, logfmt); empty for structured metadata.", false),
	column("json_path", arrow.ListOf(str), "Path inside a JSON line; empty for non-JSON fields.", false),
}

var detectedFieldsExamples = []vgi.CatalogExample{
	{SQL: "SELECT field, type, cardinality, parsers FROM loki.main.detected_fields('{app=\"payments\"}', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 02:00:00+00') ORDER BY field",
		Description: "Fields inside the payments app's JSON lines, with types and parsers"},
	{SQL: "SELECT field FROM loki.main.detected_fields('{namespace=\"shop\"}') WHERE list_contains(parsers, 'logfmt') ORDER BY cardinality DESC",
		Description: "logfmt fields in a namespace's recent lines, most distinct first"},
}

func newDetectedFields() vgi.TableFunction {
	return newSimple(&simpleFunc[detectedArgs]{
		name:    "detected_fields",
		columns: detectedFieldsColumns,
		meta: meta("Detected Fields In Log Lines", "metadata", "detected fields, schema discovery, json fields, logfmt keys, /loki/api/v1/detected_fields",
			"Fields Loki detects inside sampled log lines (/loki/api/v1/detected_fields, Loki 3.x): field, type, cardinality, parsers "+
				"`VARCHAR[]`, json_path `VARCHAR[]`. Positional: a log query. Named: start_date, end_date (default the last hour), "+
				"secret_name. Use it to learn what to write after '| json' or '| logfmt' in a logs() or query_range() query. Servers "+
				"before 3.0 answer 404.",
			"## detected_fields(query, start_date := NULL, end_date := NULL)\n\nParsable fields, their types and cardinality.",
			"Fields Loki detects in the lines of a log query, with type, cardinality and parser", detectedFieldsColumns, detectedFieldsExamples),
		fetch: func(ctx context.Context, c *conn, a detectedArgs) ([][]any, error) {
			p := url.Values{"query": {a.Query}}
			timeRangeParams(p, a.StartDate, a.EndDate)
			body, err := c.raw(ctx, apiPath+"detected_fields", p, false)
			if err != nil {
				return nil, notFoundHint(err, "detected_fields needs Loki 3.0+")
			}
			var resp struct {
				Fields []struct {
					Label       string   `json:"label"`
					Type        string   `json:"type"`
					Cardinality *int64   `json:"cardinality"`
					Parsers     []string `json:"parsers"`
					JSONPath    []string `json:"jsonPath"`
				} `json:"fields"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				return nil, fmt.Errorf("decoding detected fields: %w", err)
			}
			rows := make([][]any, 0, len(resp.Fields))
			for _, f := range resp.Fields {
				var card any
				if f.Cardinality != nil {
					card = *f.Cardinality
				}
				parsers, path := f.Parsers, f.JSONPath
				if parsers == nil {
					parsers = []string{}
				}
				if path == nil {
					path = []string{}
				}
				rows = append(rows, []any{f.Label, optString(f.Type), card, parsers, path})
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- detected_labels

type detectedLabelsArgs struct {
	Query      *string    `vgi:"name=query,doc=Optional stream selector, e.g. '{namespace=\"shop\"}'."`
	StartDate  *time.Time `vgi:"name=start_date,doc=Window start; default: one hour before end."`
	EndDate    *time.Time `vgi:"name=end_date,doc=Window end; default: now."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a detectedLabelsArgs) secret() string { return deref(a.SecretName) }

var detectedLabelsColumns = []arrow.Field{
	column("label", str, "Stream label name.", false),
	column("cardinality", i64, "Distinct values of the label in the window.", true),
}

var detectedLabelsExamples = []vgi.CatalogExample{
	{SQL: "SELECT label, cardinality FROM loki.main.detected_labels(query := '{namespace=\"shop\"}', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 02:00:00+00') ORDER BY cardinality DESC, label",
		Description: "Stream labels of one namespace by cardinality"},
	{SQL: "SELECT label FROM loki.main.detected_labels() WHERE cardinality > 100 ORDER BY label",
		Description: "High-cardinality stream labels over the last hour"},
}

func newDetectedLabels() vgi.TableFunction {
	return newSimple(&simpleFunc[detectedLabelsArgs]{
		name:    "detected_labels",
		columns: detectedLabelsColumns,
		meta: meta("Detected Stream Labels", "metadata", "detected labels, cardinality, stream labels, /loki/api/v1/detected_labels",
			"Stream labels with their cardinality (/loki/api/v1/detected_labels, Loki 3.x). Named: query (optional selector), "+
				"start_date, end_date (default the last hour), secret_name. Columns: label, cardinality `BIGINT`. Servers before 3.0 answer 404.",
			"## detected_labels(query := NULL, start_date := NULL, end_date := NULL)\n\nStream labels and their cardinality.",
			"Stream labels with their cardinality in a window", detectedLabelsColumns, detectedLabelsExamples),
		fetch: func(ctx context.Context, c *conn, a detectedLabelsArgs) ([][]any, error) {
			p := url.Values{}
			if a.Query != nil {
				p.Set("query", *a.Query)
			}
			timeRangeParams(p, a.StartDate, a.EndDate)
			body, err := c.raw(ctx, apiPath+"detected_labels", p, false)
			if err != nil {
				return nil, notFoundHint(err, "detected_labels needs Loki 3.0+")
			}
			var resp struct {
				Labels []struct {
					Label       string `json:"label"`
					Cardinality *int64 `json:"cardinality"`
				} `json:"detectedLabels"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				return nil, fmt.Errorf("decoding detected labels: %w", err)
			}
			rows := make([][]any, 0, len(resp.Labels))
			for _, l := range resp.Labels {
				var card any
				if l.Cardinality != nil {
					card = *l.Cardinality
				}
				rows = append(rows, []any{l.Label, card})
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- patterns

type patternsArgs struct {
	Query       string     `vgi:"pos=0,doc=Stream selector, e.g. '{app=\"bulk\"}'."`
	StepSeconds *float64   `vgi:"name=step,gt=0,doc=Bucket width in seconds for the sample counts; default: the server's."`
	StartDate   *time.Time `vgi:"name=start_date,doc=Window start; default: one hour before end."`
	EndDate     *time.Time `vgi:"name=end_date,doc=Window end; default: now."`
	SecretName  *string    `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a patternsArgs) secret() string { return deref(a.SecretName) }

var patternsColumns = []arrow.Field{
	column("pattern", str, "Detected log pattern; <_> marks a variable token.", false),
	column("level", str, "Log level of the pattern's lines, when the server reports it.", true),
	column("ts", tsType, "Start of the sample bucket.", false),
	column("count", i64, "Lines matching the pattern in the bucket.", false),
}

var patternsExamples = []vgi.CatalogExample{
	{SQL: "SELECT pattern, sum(count) AS lines FROM loki.main.patterns('{app=\"bulk\"}', start_date := TIMESTAMPTZ '2026-01-01 01:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 02:00:00+00') GROUP BY pattern ORDER BY lines DESC",
		Description: "The most frequent line shapes of one app"},
	{SQL: "SELECT pattern, count(*) AS buckets FROM loki.main.patterns('{namespace=\"shop\"}', step := 300) GROUP BY pattern ORDER BY buckets DESC",
		Description: "Which patterns appear in the most five-minute buckets over the last hour"},
}

func newPatterns() vgi.TableFunction {
	return newSimple(&simpleFunc[patternsArgs]{
		name:    "patterns",
		columns: patternsColumns,
		meta: meta("Log Patterns", "metadata", "patterns, log patterns, clustering, templates, pattern ingester, /loki/api/v1/patterns",
			"Log line patterns detected by Loki's pattern ingester (/loki/api/v1/patterns) in long format: one row per pattern per "+
				"time bucket (pattern, level, ts, count). Positional: selector. Named: step (bucket seconds), start_date, end_date "+
				"(default the last hour), secret_name. Needs pattern_ingester.enabled on the server, which only tracks recent data "+
				"(about the last three hours); a server without it answers 404 or no rows.",
			"## patterns(query, step := NULL, start_date := NULL, end_date := NULL)\n\nPattern counts per time bucket.",
			"Detected log-line patterns and their counts per time bucket", patternsColumns, patternsExamples),
		fetch: func(ctx context.Context, c *conn, a patternsArgs) ([][]any, error) {
			p := url.Values{"query": {a.Query}}
			if a.StepSeconds != nil {
				ms, err := stepMillis(*a.StepSeconds)
				if err != nil {
					return nil, err
				}
				p.Set("step", formatStep(ms))
			}
			timeRangeParams(p, a.StartDate, a.EndDate)
			data, err := c.data(ctx, apiPath+"patterns", p, false)
			if err != nil {
				return nil, notFoundHint(err, "patterns needs Loki 3.0+ with pattern_ingester.enabled: true")
			}
			var pats []struct {
				Pattern string            `json:"pattern"`
				Level   string            `json:"level"`
				Samples []json.RawMessage `json:"samples"`
			}
			if len(data) > 0 && string(data) != "null" {
				if err := json.Unmarshal(data, &pats); err != nil {
					return nil, fmt.Errorf("decoding patterns: %w", err)
				}
			}
			var rows [][]any
			for _, pt := range pats {
				for _, s := range pt.Samples {
					var pair []json.Number
					if err := json.Unmarshal(s, &pair); err != nil || len(pair) != 2 {
						return nil, fmt.Errorf("bad pattern sample %s", truncate(string(s)))
					}
					ts, err := parseTimestamp(pair[0])
					if err != nil {
						return nil, err
					}
					n, err := pair[1].Int64()
					if err != nil {
						return nil, fmt.Errorf("bad pattern sample count %s", pair[1])
					}
					rows = append(rows, []any{pt.Pattern, optString(pt.Level), time.Unix(0, ts).UTC(), n})
				}
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- rules

type rulesArgs struct {
	RuleType   string  `vgi:"name=rule_type,default=any,choices='any,alert,record',doc=Which rules: 'alert', 'record' or 'any'."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a rulesArgs) secret() string { return deref(a.SecretName) }

var rulesColumns = []arrow.Field{
	column("group_name", str, "Rule group name.", false),
	column("group_file", str, "Namespace (file) the group belongs to.", true),
	column("group_interval_seconds", f64, "Group evaluation interval in seconds.", true),
	column("rule_name", str, "Alert name or recorded metric name.", false),
	column("rule_type", str, "'alerting' or 'recording'.", false),
	column("query", str, "The rule's LogQL expression.", false),
	column("duration_seconds", f64, "Alerting rules: the 'for' duration in seconds; NULL for recording rules.", true),
	column("labels", labelsType, "Labels the rule adds.", false),
	column("annotations", labelsType, "Alerting rules: annotations; empty for recording rules.", false),
	column("health", str, "'ok', 'err' or 'unknown'.", true),
	column("last_error", str, "Last evaluation error; NULL when none.", true),
	column("state", str, "Alerting rules: 'firing', 'pending' or 'inactive'; NULL for recording rules.", true),
	column("active_alerts", i64, "Alerting rules: number of active (pending or firing) alerts; NULL for recording rules.", true),
	column("last_evaluation", tsType, "Time of the rule's last evaluation; NULL if never evaluated.", true),
	column("evaluation_time_seconds", f64, "Duration of the last evaluation in seconds.", true),
}

var rulesExamples = []vgi.CatalogExample{
	{SQL: "SELECT group_name, rule_name, state, active_alerts FROM loki.main.rules(rule_type := 'alert') ORDER BY group_name, rule_name",
		Description: "Every LogQL alerting rule with its state"},
	{SQL: "SELECT rule_name, loki.main.selector_labels(query) AS labels_used FROM loki.main.rules() ORDER BY rule_name",
		Description: "Which stream labels each rule's LogQL selects on"},
}

func ptrFloat(p *float64, missing any) any {
	if p == nil {
		return missing
	}
	return *p
}

func newRules() vgi.TableFunction {
	return newSimple(&simpleFunc[rulesArgs]{
		name:    "rules",
		columns: rulesColumns,
		meta: meta("LogQL Recording And Alerting Rules", "rules", "rules, alerting rules, recording rules, ruler, rule groups, health, /prometheus/api/v1/rules",
			"Recording and alerting rules from Loki's ruler (Prometheus-compatible /prometheus/api/v1/rules), one row per rule: group, "+
				"LogQL query, for-duration, labels, annotations, health, last error, state and active alert count. Named: rule_type "+
				"('any', 'alert', 'record'), secret_name. Needs the ruler (ruler.enable_api).",
			"## rules(rule_type := 'any')\n\nEvery ruler rule with health and evaluation stats.",
			"List the ruler's LogQL recording and alerting rules with their health and state", rulesColumns, rulesExamples),
		fetch: func(ctx context.Context, c *conn, a rulesArgs) ([][]any, error) {
			p := url.Values{}
			if a.RuleType != "any" {
				p.Set("type", a.RuleType)
			}
			data, err := c.data(ctx, "prometheus/api/v1/rules", p, false)
			if err != nil {
				return nil, notFoundHint(err, "the ruler API needs ruler.enable_api: true")
			}
			var resp struct {
				Groups []struct {
					Name     string   `json:"name"`
					File     string   `json:"file"`
					Interval *float64 `json:"interval"`
					Rules    []struct {
						Name           string            `json:"name"`
						Query          string            `json:"query"`
						Type           string            `json:"type"`
						Duration       *float64          `json:"duration"`
						Labels         map[string]string `json:"labels"`
						Annotations    map[string]string `json:"annotations"`
						Health         string            `json:"health"`
						LastError      string            `json:"lastError"`
						State          string            `json:"state"`
						Alerts         []json.RawMessage `json:"alerts"`
						LastEvaluation string            `json:"lastEvaluation"`
						EvaluationTime *float64          `json:"evaluationTime"`
					} `json:"rules"`
				} `json:"groups"`
			}
			if err := json.Unmarshal(data, &resp); err != nil {
				return nil, fmt.Errorf("decoding rules: %w", err)
			}
			var rows [][]any
			for _, g := range resp.Groups {
				for _, r := range g.Rules {
					labels, ann := r.Labels, r.Annotations
					if labels == nil {
						labels = map[string]string{}
					}
					if ann == nil {
						ann = map[string]string{}
					}
					var dur, state, active any
					if r.Type == "alerting" {
						dur, state, active = ptrFloat(r.Duration, 0.0), optString(r.State), int64(len(r.Alerts))
					}
					rows = append(rows, []any{g.Name, optString(g.File), ptrFloat(g.Interval, nil), r.Name, r.Type, r.Query,
						dur, labels, ann, optString(r.Health), optString(r.LastError), state, active,
						optTime(r.LastEvaluation), ptrFloat(r.EvaluationTime, nil)})
				}
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- alerts

type noArgs struct {
	SecretName *string `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a noArgs) secret() string { return deref(a.SecretName) }

var alertsColumns = []arrow.Field{
	column("alert_name", str, "The alertname label.", true),
	column("state", str, "'pending' or 'firing'.", false),
	column("active_at", tsType, "When the alert became active (pending).", true),
	column("value", f64, "Value of the alert expression when last evaluated.", true),
	column("labels", labelsType, "Alert labels.", false),
	column("annotations", labelsType, "Alert annotations.", false),
}

var alertsExamples = []vgi.CatalogExample{
	{SQL: "SELECT alert_name, state, labels['severity'] AS severity FROM loki.main.alerts() ORDER BY alert_name",
		Description: "Active LogQL alerts with severity"},
	{SQL: "SELECT labels['severity'] AS severity, count(*) AS alerts FROM loki.main.alerts() WHERE state = 'firing' GROUP BY severity ORDER BY alerts DESC",
		Description: "Firing alerts per severity"},
}

func newAlerts() vgi.TableFunction {
	return newSimple(&simpleFunc[noArgs]{
		name:    "alerts",
		columns: alertsColumns,
		meta: meta("Active Ruler Alerts", "rules", "alerts, firing, pending, ruler, alerting, /prometheus/api/v1/alerts",
			"Active (pending or firing) alerts from Loki's ruler (/prometheus/api/v1/alerts) with labels, annotations, active_at and "+
				"value. Named: secret_name. Join rules() on alert_name = rule_name for the LogQL definition.",
			"## alerts()\n\nEvery pending and firing ruler alert.",
			"List the ruler's currently pending and firing alerts", alertsColumns, alertsExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs) ([][]any, error) {
			data, err := c.data(ctx, "prometheus/api/v1/alerts", url.Values{}, false)
			if err != nil {
				return nil, notFoundHint(err, "the ruler API needs ruler.enable_api: true")
			}
			var resp struct {
				Alerts []struct {
					Labels      map[string]string `json:"labels"`
					Annotations map[string]string `json:"annotations"`
					State       string            `json:"state"`
					ActiveAt    string            `json:"activeAt"`
					Value       string            `json:"value"`
				} `json:"alerts"`
			}
			if err := json.Unmarshal(data, &resp); err != nil {
				return nil, fmt.Errorf("decoding alerts: %w", err)
			}
			rows := make([][]any, 0, len(resp.Alerts))
			for _, a := range resp.Alerts {
				labels, ann := a.Labels, a.Annotations
				if labels == nil {
					labels = map[string]string{}
				}
				if ann == nil {
					ann = map[string]string{}
				}
				var v any
				if f, err := strconv.ParseFloat(a.Value, 64); err == nil {
					v = f
				}
				rows = append(rows, []any{optString(labels["alertname"]), a.State, optTime(a.ActiveAt), v, labels, ann})
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- build_info

var buildInfoColumns = []arrow.Field{
	column("version", str, "Server version, e.g. '3.7.0'.", true),
	column("revision", str, "Source revision.", true),
	column("branch", str, "Source branch.", true),
	column("build_user", str, "Who built it.", true),
	column("build_date", str, "Build date as reported.", true),
	column("go_version", str, "Go toolchain version; NULL when not reported.", true),
}

var buildInfoExamples = []vgi.CatalogExample{
	{SQL: "SELECT version, revision FROM loki.main.build_info()",
		Description: "Which Loki version the server runs"},
	{SQL: "SELECT version >= '3.0' AS has_detected_fields FROM loki.main.build_info()",
		Description: "Whether the server is new enough for detected_fields()"},
}

func newBuildInfo() vgi.TableFunction {
	return newSimple(&simpleFunc[noArgs]{
		name:    "build_info",
		columns: buildInfoColumns,
		meta: meta("Server Build Info", "status", "build info, version, revision, /loki/api/v1/status/buildinfo",
			"The server's version and build details (/loki/api/v1/status/buildinfo): version, revision, branch, build_user, "+
				"build_date, go_version. Named: secret_name. One row. A cheap connectivity and credentials check.",
			"## build_info()\n\nServer version and build details.",
			"Loki server version and build details", buildInfoColumns, buildInfoExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs) ([][]any, error) {
			body, err := c.raw(ctx, apiPath+"status/buildinfo", url.Values{}, false)
			if err != nil {
				return nil, err
			}
			var b struct {
				Version, Revision, Branch, BuildUser, BuildDate, GoVersion string
			}
			if err := json.Unmarshal(body, &b); err != nil {
				return nil, fmt.Errorf("decoding build info: %w", err)
			}
			return [][]any{{optString(b.Version), optString(b.Revision), optString(b.Branch), optString(b.BuildUser),
				optString(b.BuildDate), optString(b.GoVersion)}}, nil
		},
	})
}

// ---------------------------------------------------------------- format_query

type formatArgs struct {
	Logql      string  `vgi:"pos=0,doc=LogQL text to validate and format on the server."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the loki secret to use when several exist."`
}

func (a formatArgs) secret() string { return deref(a.SecretName) }

var formatColumns = []arrow.Field{
	column("valid", arrow.FixedWidthTypes.Boolean, "Whether the server's LogQL parser accepts the query.", false),
	column("formatted", str, "The query in the server's canonical form; NULL when invalid.", true),
	column("error", str, "The parser's message (with line and column) when invalid; NULL when valid.", true),
}

var formatExamples = []vgi.CatalogExample{
	{SQL: "SELECT valid, formatted FROM loki.main.format_query('sum(rate({app=\"x\"}|=\"a\"[5m]))by(b)')",
		Description: "Canonical form of a LogQL metric query"},
	{SQL: "SELECT valid, error FROM loki.main.format_query('{app=\"x\" |= \"a\"')",
		Description: "The parser's message for an unclosed stream selector"},
}

func newFormatQuery() vgi.TableFunction {
	return newSimple(&simpleFunc[formatArgs]{
		name:    "format_query",
		columns: formatColumns,
		meta: meta("Validate And Format LogQL", "logql", "format, validate, lint, logql, parse error, canonical, /loki/api/v1/format_query",
			"Validate and canonicalize LogQL with the server's own parser (/loki/api/v1/format_query). Positional: the LogQL text. "+
				"Named: secret_name. One row: valid `BOOLEAN`, formatted (canonical text, NULL when invalid), error (parser message, "+
				"NULL when valid). An invalid query is a row, not an error, so it can lint stored queries. The offline scalars "+
				"(query_type, stream_selectors, selector_labels) need no server but do not fully parse.",
			"## format_query(logql)\n\nServer-side LogQL validation and formatting.",
			"Validate a LogQL query with the server's parser and return its canonical form", formatColumns, formatExamples),
		fetch: func(ctx context.Context, c *conn, a formatArgs) ([][]any, error) {
			data, err := c.data(ctx, apiPath+"format_query", url.Values{"query": {a.Logql}}, true)
			if err != nil {
				var ae *apiError
				if errors.As(err, &ae) && ae.Status == http.StatusBadRequest {
					return [][]any{{false, nil, ae.Message}}, nil
				}
				return nil, notFoundHint(err, "format_query needs Loki 2.9+")
			}
			var s string
			if err := json.Unmarshal(data, &s); err != nil {
				return nil, fmt.Errorf("decoding formatted query: %w", err)
			}
			return [][]any{{true, s, nil}}, nil
		},
	})
}
