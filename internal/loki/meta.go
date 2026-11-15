// Discovery metadata: the one file to edit when you rename or extend the worker.
//
// DuckDB, coding agents and the vgi-lint metadata checker all read these tags.
// Keep every object documented and `make lint-explain` stays at 100.
//
//   - Catalog-level tags (title, license, author, links, long description)
//     live in CatalogTags below.
//   - Every function carries objectTags(...): a title, a category from
//     SchemaCategories, search keywords, and LLM- and human-facing docs.

package loki

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogName is the name DuckDB attaches the worker under.
const CatalogName = "loki"

// SourceURL is where this worker's code lives.
const SourceURL = "https://github.com/Query-farm/vgi-loki"

// category is one entry of the schema's vgi.categories list.
type category struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// SchemaCategories group functions in docs and agent tools. Every function's
// vgi.category must name one of these.
var SchemaCategories = []category{
	{Name: "query", Title: "LogQL Queries", Description: "Run arbitrary LogQL: log queries as log rows, metric queries as long metric rows."},
	{Name: "logs", Title: "Log Lines", Description: "Typed log rows with SQL filter pushdown into LogQL selectors, line filters and the time window."},
	{Name: "metadata", Title: "Stream Metadata", Description: "Labels, label values, streams, index statistics, volume, detected fields and labels, patterns."},
	{Name: "rules", Title: "Ruler", Description: "LogQL recording and alerting rules and active alerts from Loki's ruler."},
	{Name: "logql", Title: "LogQL Tools", Description: "Quote, classify and inspect LogQL locally; validate and format it on the server."},
	{Name: "status", Title: "Server Status", Description: "Server version and build information."},
}

// mustJSON marshals v, panicking on failure (only static data is passed).
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// keywordsArray turns "a, b, c" into the JSON array string vgi.keywords requires.
func keywordsArray(keywords string) string {
	out := []string{}
	for _, k := range strings.Split(keywords, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return mustJSON(out)
}

// docs is the set of standard per-object discovery fields.
type docs struct {
	Title    string
	Category string
	Keywords string
	DocLLM   string
	DocMD    string
}

// objectTags builds the standard per-object discovery tags.
func objectTags(d docs) map[string]string {
	known := false
	for _, c := range SchemaCategories {
		known = known || c.Name == d.Category
	}
	if !known {
		panic(fmt.Sprintf("unknown category %q; add it to SchemaCategories", d.Category))
	}
	return map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": d.Category,
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}
}

// resultColumn describes one output column for vgi.result_columns_schema.
type resultColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// column builds an Arrow field whose comment shows up in DuckDB's DESCRIBE and agent tools.
func column(name string, typ arrow.DataType, comment string, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: typ, Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{"comment"}, []string{comment})}
}

// exampleQuery is one entry of vgi.example_queries.
type exampleQuery struct {
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

// exampleQueries converts catalog examples to vgi.example_queries entries.
func exampleQueries(lists ...[]vgi.CatalogExample) []exampleQuery {
	out := []exampleQuery{}
	for _, examples := range lists {
		for _, ex := range examples {
			out = append(out, exampleQuery{Description: ex.Description, SQL: ex.SQL})
		}
	}
	return out
}

// withExamples publishes examples (with descriptions) as vgi.example_queries.
// Overloads share one SQL name, so pass every overload's examples.
func withExamples(tags map[string]string, lists ...[]vgi.CatalogExample) map[string]string {
	tags["vgi.example_queries"] = mustJSON(exampleQueries(lists...))
	return tags
}

const catalogDocLLM = "Query Grafana Loki (self-hosted, or Grafana Cloud Logs with basic auth) from SQL, read-only. Every " +
	"server function needs a DuckDB secret: CREATE SECRET (TYPE loki, BASE_URL 'http://host:3100', BEARER_TOKEN or " +
	"USERNAME/PASSWORD, optional TENANT_ID sent as X-Scope-OrgID, optional EXTRA_HEADERS as a JSON object). " +
	"query_range(logql, start, end) runs any LogQL: a log query returns one row per line (ts, ts_ns, job, namespace, pod, " +
	"app, level, labels map, line, structured_metadata map, parsed map), paged past the server's 5000-line limit with " +
	"de-duplication; a metric query returns vgi-prometheus's long layout (labels map, ts, value, ...) so it UNIONs with " +
	"Prometheus data. query(logql, time := ...) is an instant metric query. logs(selector) returns the same log rows with " +
	"SQL WHERE filters pushed down: label columns become stream matchers, line LIKE/contains become |= line filters, ts " +
	"ranges become the window, ORDER BY ts DESC LIMIT n becomes a backward limited query. labels, label_values, series, " +
	"index_stats, volume, detected_fields, detected_labels and patterns read stream metadata; rules and alerts read the " +
	"ruler; build_info and format_query (server-side LogQL validation) are utilities. logql_quote, query_type, " +
	"stream_selectors and selector_labels run locally with no server."

// CatalogDocMD is the catalog's comment: one short paragraph for directories.
const CatalogDocMD = "Query Grafana Loki and Grafana Cloud Logs from DuckDB: LogQL log and metric queries as typed rows, " +
	"log lines with SQL filters pushed into LogQL, stream metadata, ruler rules and alerts, and offline LogQL tools."

const catalogDocMDLong = "# Loki in DuckDB\n\n" +
	"Run LogQL against Grafana Loki and get typed rows back: log lines with `TIMESTAMPTZ` and exact nanosecond " +
	"timestamps, stream labels hoisted into columns and kept as a `MAP`, structured metadata and parsed fields as maps; " +
	"metric queries in the same long layout as vgi-prometheus, so logs-derived and Prometheus metrics UNION.\n\n" +
	"- **query_range / query**: arbitrary LogQL; log results are paged past the 5000-line limit, metric ranges chunked under 11,000 points.\n" +
	"- **logs**: SQL `WHERE` filters become stream matchers, line filters and the time window; every filter is re-applied exactly.\n" +
	"- **Metadata**: labels, values, streams, index stats, volume, detected fields and labels, patterns.\n" +
	"- **Ruler**: LogQL rules and active alerts.\n" +
	"- **LogQL tools**: quote, classify and inspect LogQL offline; validate on the server.\n\n" +
	"Credentials come from a `loki` DuckDB secret (bearer token or basic auth, optional tenant header)."

const schemaDocLLM = "The main schema holds every function. Table functions that call the server take an optional " +
	"secret_name named argument (default: the loki secret whose scope matches). Window arguments are start_date/end_date " +
	"(query_range takes start and end positionally); log windows are [start, end). Log rows: read a stream label with " +
	"labels['name'], a parsed field with parsed['name'] (add | json or | logfmt to the LogQL), structured metadata with " +
	"structured_metadata['name']. In logs(), list a stream label in label_columns to get a pushdown column. Scalars " +
	"(logql_quote, query_type, stream_selectors, selector_labels) take one `VARCHAR` column and need no secret."

// SchemaDocMD is the main schema's human-facing description (also its comment).
const SchemaDocMD = "## main\n\n" +
	"Everything this worker exposes lives here. Table functions call the Loki HTTP API named by a `loki` secret; " +
	"scalars inspect LogQL locally.\n\n" +
	"Reach for `logs` when you want SQL filters pushed into LogQL, and for `query_range` when you need arbitrary " +
	"LogQL such as parsers, `line_format` or metric aggregations."

// agentTask is one public analyst task for `vgi-lint simulate`.
type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

// agentTestTasks are graded privately by vgi-agent-tests.yaml. The server
// tasks assume a loki secret pointing at the replay fixture (test/fixture_server).
var agentTestTasks = []agentTask{
	{Name: "quote_literal", Prompt: "Using the loki worker, write the string say \"hello\" to C:\\temp as a LogQL string literal."},
	{Name: "classify_queries", Prompt: "Using the loki worker, which of these LogQL queries are metric queries: '{app=\"x\"} |= \"err\"', " +
		"'rate({app=\"x\"}[5m])', 'sum by (pod) (count_over_time({app=\"x\"}[1m]))'?"},
	{Name: "rule_labels", Prompt: "Using the loki worker, which stream labels does the LogQL " +
		"'sum(count_over_time({namespace=\"shop\", app=~\"check.*\"}[5m])) / sum(count_over_time({namespace=\"shop\"}[5m]))' select on?"},
	{Name: "checkout_errors", Prompt: "How many error-level log lines did the checkout app write between 2026-01-01 00:00 and 02:00 UTC, per pod?"},
	{Name: "declined_payments", Prompt: "How many checkout error lines between 2026-01-01 00:00 and 02:00 UTC mention 'payment declined'?"},
	{Name: "bulk_failures", Prompt: "List the bulk app's lines with status=failed between 2026-01-01 01:00 and 02:00 UTC, oldest first."},
	{Name: "levels_per_half_hour", Prompt: "Count checkout log lines per level in 30-minute buckets between 2026-01-01 00:00 and 02:00 UTC."},
	{Name: "shop_apps", Prompt: "Which apps logged in the shop namespace between 2026-01-01 00:00 and 02:00 UTC?"},
	{Name: "payments_fields", Prompt: "Which fields does Loki detect in the payments app's lines between 2026-01-01 00:00 and 02:00 UTC?"},
	{Name: "shop_volume", Prompt: "How many entries and bytes does the shop namespace have between 2026-01-01 00:00 and 02:00 UTC?"},
	{Name: "firing_alerts", Prompt: "Which Loki ruler alerts are currently firing?"},
	{Name: "server_version", Prompt: "Which Loki version is the server running?"},
	{Name: "instant_levels", Prompt: "How many checkout log lines per level were written in the two hours before 2026-01-01 02:00 UTC? Use one instant metric query."},
	{Name: "shop_labels", Prompt: "Which stream label names does the shop namespace use between 2026-01-01 00:00 and 02:00 UTC?"},
	{Name: "shop_streams", Prompt: "List the shop namespace's streams (app, pod, level) between 2026-01-01 00:00 and 02:00 UTC."},
	{Name: "namespace_volume", Prompt: "Which namespaces ingested any bytes between 2026-01-01 00:00 and 02:00 UTC?"},
	{Name: "shop_label_cardinality", Prompt: "What is the cardinality of each stream label Loki detects in the shop namespace between 2026-01-01 00:00 and 02:00 UTC?"},
	{Name: "bulk_patterns", Prompt: "Which log patterns does the pattern ingester report for the bulk app between 2026-01-01 01:00 and 02:00 UTC?"},
	{Name: "alert_rules", Prompt: "List the Loki ruler's alerting rules with their 'for' duration in seconds."},
	{Name: "canonical_logql", Prompt: "What is the server's canonical form of the LogQL 'sum(rate({app=\"x\"}|=\"a\"[5m]))by(b)'?"},
	{Name: "query_selectors", Prompt: "Using the loki worker, list the stream selectors in 'sum(rate({app=\"api\", level=\"error\"}[5m])) / sum(rate({app=\"api\"}[5m]))'."},
}

// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "VGI Loki Worker",
	"vgi.subtitle":           "LogQL log and metric queries, log lines with pushdown, stream metadata and ruler state from Grafana Loki.",
	"vgi.doc_llm":            catalogDocLLM,
	"vgi.doc_md":             catalogDocMDLong,
	"vgi.source_url":         SourceURL,
	"vgi.icon_url":           "https://query.farm/media-kit/products/vgi/vgi-logo.png",
	"vgi.license":            "0BSD",
	"vgi.author":             "Query.Farm",
	"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
	"vgi.keywords":           keywordsArray("loki, logql, logs, log analytics, observability, grafana, grafana cloud logs, structured metadata, monitoring"),
	"vgi.support_contact":    SourceURL + "/issues",
	"vgi.support_policy_url": SourceURL + "#support",
	// Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
	"vgi.agent_test_tasks": mustJSON(agentTestTasks),
}

// schemaTags builds the main schema's tags; allExamples feeds vgi.example_queries.
func schemaTags(allExamples []exampleQuery) map[string]string {
	return map[string]string{
		"vgi.title":           "Loki Functions",
		"domain":              "observability",
		"vgi.keywords":        keywordsArray("logql, query_range, logs, labels, series, patterns, rules, alerts"),
		"vgi.doc_llm":         schemaDocLLM,
		"vgi.doc_md":          SchemaDocMD,
		"vgi.categories":      mustJSON(SchemaCategories),
		"vgi.example_queries": mustJSON(allExamples),
	}
}
