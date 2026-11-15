<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Grafana Loki Logs With LogQL and SQL in DuckDB

> **vgi-loki** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-loki/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-loki/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Loki answers LogQL, but the answer is a JSON tree that Grafana draws and nothing else can join.
This worker turns the **Grafana Loki HTTP API** into SQL tables in DuckDB: run LogQL and get
typed log rows or metric rows, filter log lines with a `WHERE` clause that becomes a LogQL
stream selector and line filters, audit streams, volume, fields and ruler state, and join it
all with your Parquet, CSV or warehouse tables.

It works with self-hosted **Loki** (single binary or behind the query frontend), **Grafana
Cloud Logs** (basic auth) and anything that serves the Loki API behind a bearer token. It is
**read-only**: no push, no deletes, no admin endpoints.

- `query_range` runs any LogQL. A **log query** returns one row per line: `ts TIMESTAMPTZ`,
  `ts_ns BIGINT` (Loki's full nanosecond precision), hoisted stream labels `job`, `namespace`,
  `pod`, `app`, `level`, every stream label in a `MAP(VARCHAR, VARCHAR)`, `line`,
  `structured_metadata` and `parsed` maps. Results are paged past the server's 5,000-line limit
  with exact de-duplication at page boundaries. A **metric query** returns exactly
  vgi-prometheus's long layout (`labels`, `ts`, `value`, ...) so Loki-derived and Prometheus
  metrics `UNION ALL`.
- `logs` returns the same log rows with SQL filters **pushed into LogQL** (stream matchers,
  `|=` / `!=` line filters, the time window, `ORDER BY ts DESC LIMIT n`), then re-applied exactly.
- Stream metadata, index statistics, volume, detected fields and labels, patterns, ruler rules
  and alerts, and LogQL tools that run with no server.

## Quick start

```sql
INSTALL vgi FROM community;     -- Haybarn 2.0; on DuckDB 2.0 use the query_farm repository
LOAD vgi;
ATTACH 'loki' (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-loki@v0.1.0');

CREATE SECRET (TYPE loki, BASE_URL 'http://localhost:3100');

-- Error lines from one app in the last hour, newest first.
SELECT ts, pod, line
FROM loki.main.logs('{namespace="shop"}')
WHERE app = 'checkout' AND level = 'error' AND line LIKE '%timeout%'
  AND ts >= now() - INTERVAL 1 HOUR
ORDER BY ts DESC LIMIT 50;
```

The secret type is registered by the worker, so run `CREATE SECRET` after `ATTACH`.

<details><summary>Other ways to run it</summary>

- **DuckDB 2.0**:
  `SET allow_extension_repositories = 'allowed'; CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm'; INSTALL vgi FROM query_farm; LOAD vgi FROM query_farm;`
  then attach as above.
- **Docker** (HTTP): `docker run -p 8000:8000 ghcr.io/query-farm/vgi-loki:0.1.0`, then
  `ATTACH 'loki' (TYPE vgi, LOCATION 'http://localhost:8000')`.
- **Go**: `go install github.com/Query-farm/vgi-loki/cmd/vgi-loki-worker@v0.1.0`,
  then attach the binary by absolute path.
- **From source**: `make build` (Go 1.26+, pure Go, no cgo), then
  `ATTACH 'loki' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-loki-worker')`.

v0.1.0 is not published yet. Until it is, build from source.
</details>

## Secrets and configuration

Every function that calls a server reads a DuckDB secret of type `loki`:

| Field | Meaning |
|---|---|
| `BASE_URL` | Server root; `/loki/api/v1/...` (and `/prometheus/api/v1/...` for the ruler) is appended. `http://loki:3100`, `http://loki-gateway.monitoring`, `https://logs-prod-012.grafana.net`. |
| `BEARER_TOKEN` | Sent as `Authorization: Bearer …`. Redacted. |
| `USERNAME` / `PASSWORD` | Basic auth (Grafana Cloud Logs: the numeric instance id and an access-policy token with `logs:read`). Password redacted. |
| `TENANT_ID` | Sent as `X-Scope-OrgID` (multi-tenant Loki). |
| `EXTRA_HEADERS` | JSON object of extra request headers, e.g. `'{"X-Custom": "v"}'`. Redacted. |

```sql
-- Grafana Cloud Logs
CREATE SECRET grafana (TYPE loki, BASE_URL 'https://logs-prod-012.grafana.net',
                       USERNAME '123456', PASSWORD 'glc_...');
-- Self-hosted, one tenant
CREATE SECRET team_a (TYPE loki, BASE_URL 'http://loki-gateway:80', TENANT_ID 'team-a');

SELECT * FROM loki.main.build_info(secret_name := 'team_a');
```

With several secrets, pick one per call with `secret_name := '...'` (the worker asks DuckDB for
that secret by name at bind time). Credentials never enter table-function state or logs.
Requests that get 429, 502, 503 or 504 are retried with exponential backoff, honouring
`Retry-After`; other errors carry the HTTP status and Loki's message. Every request asks for
`X-Loki-Response-Encoding-Flags: categorize-labels`, so stream labels, structured metadata and
parsed labels come back separately instead of merged.

## What's inside

All objects are in `loki.main`.

| Function | What it returns |
|---|---|
| `query_range(logql, start, end, max_lines := NULL, direction := 'backward', step := NULL, page_size := 5000, chunk_seconds := 3600, max_points_per_chunk := 11000)` | Log rows for a log query, metric rows for a metric query (`/loki/api/v1/query_range`). |
| `query(logql, time := …)` | Instant metric query (`/loki/api/v1/query`); metric rows. Loki rejects log queries here. |
| `logs(selector, label_columns := '', start_date, end_date, max_lines, direction, page_size, chunk_seconds)` | Log rows with filter pushdown. `logs()` without a selector builds it from `WHERE`. |
| `labels(match := …, start_date, end_date)` | Stream label names. |
| `label_values(label, match := …, start_date, end_date)` | Values of one stream label. |
| `series(match, start_date, end_date)` | Every stream (label set) matching a selector, labels hoisted. |
| `index_stats(query, start_date, end_date)` | `streams`, `chunks`, `entries`, `bytes` a selector covers: cost before you query. |
| `volume(query, target_labels := …, aggregate_by := 'series', top_n := 100, …)` | Ingested bytes per label set (`index/volume`). |
| `detected_fields(query, start_date, end_date)` | Fields inside lines with type, cardinality, parsers and JSON path (Loki 3.x). |
| `detected_labels(query := …, start_date, end_date)` | Stream labels with cardinality (Loki 3.x). |
| `patterns(query, step := …, start_date, end_date)` | Pattern-ingester patterns, one row per pattern per bucket. |
| `rules(rule_type := 'any')` | Ruler recording and alerting rules (LogQL) with health and state. |
| `alerts()` | Active ruler alerts. |
| `build_info()` | Server version and build. |
| `format_query(logql)` | Server-side validation: `valid`, canonical `formatted` text, parser `error`. |
| `logql_quote(s)` | `s` as an escaped LogQL string literal. |
| `query_type(logql)` | `'log'`, `'metric'`, or `NULL` when it does not lex. |
| `stream_selectors(logql)` | `VARCHAR[]` of every `{...}` selector in the query. |
| `selector_labels(logql)` | `VARCHAR[]` of the label names those selectors match on. |

**Log rows** (`query_range` with a log query, `logs`): `ts`, `ts_ns`, `job`, `namespace`, `pod`,
`app`, `level` (stream labels, `NULL` when absent), any `label_columns`, `labels`, `line`,
`structured_metadata` (e.g. `trace_id`, Loki's `detected_level`) and `parsed` (fields from
`| json`, `| logfmt`, `| regexp`, `| pattern` in your LogQL). Parsing happens in LogQL, not in
the worker: `logs('{app="api"} | json')` then `parsed['status']`.

**Metric rows** (`query`, `query_range` with a metric query): `result_type`, `metric` (always
`NULL`), `job`, `instance`, `labels`, `ts`, `value` (`NaN`/`±Inf` kept), `histogram` (always
`NULL`), `string_value`: the same columns and types as vgi-prometheus `query_range`.

```sql
-- Log-derived and Prometheus metrics side by side.
SELECT 'loki' AS source, labels['app'] AS app, ts, value FROM loki.main.query_range(
  'sum by (app) (rate({namespace="shop"} |= "error" [5m]))', now() - INTERVAL 1 HOUR, now(), step := 60)
UNION ALL BY NAME
SELECT 'prometheus', labels['app'], ts, value FROM prometheus.main.query_range(
  'sum by (app) (rate(http_requests_total{code=~"5.."}[5m]))', now() - INTERVAL 1 HOUR, now(), 60);
```

### Paging and chunking

Loki caps each request at `max_entries_limit_per_query` (5,000 by default). A log scan reads the
window in epoch-aligned `chunk_seconds` pieces (matching the query frontend's
`split_queries_by_interval`, so its results cache is reused), newest first for `backward`, and
pages inside each piece: when a page comes back full, the next request ends just after
(backward) or starts at (forward) the timestamp of the last entry, and entries at that timestamp
that were already returned are skipped. If `page_size` entries share one timestamp the scan
stops with an error rather than lose lines (raise `page_size`). The cursor is part of the
table-function state, so over the HTTP transport each page can be served by a different request.

Metric range queries are split into requests of at most `max_points_per_chunk` steps (Loki
rejects more than 11,000 points per series); the step is never changed. Without `step :=` the
worker uses Loki's own default, `max(1, floor(range_seconds / 250))`, and sends it explicitly.

## What pushes down

`logs` is the pushdown surface. Its `WHERE` clause compiles into the request:

| SQL on `logs(...)` | Becomes |
|---|---|
| `app = 'api'`, `namespace <> 'qa'` (hoisted or `label_columns` column) | `app="api"`, `namespace!="qa"` in the stream selector |
| `pod IN ('a', 'b')`, `pod = 'a' OR pod = 'b'` | `pod=~"a|b"` |
| `col LIKE 'p%'`, `prefix(col, 'p')`, `contains(col, 'x')`, `col NOT LIKE '…'` | `col=~"…"` / `col!~"…"` |
| `col IS NULL` / `col IS NOT NULL` | `col=""` / `col!=""` |
| `contains(line, 'x')`, `line LIKE '%x%'`, `prefix(line, 'x')`, `line = 'x'` | `|= "x"` (each literal run of a `LIKE` pattern becomes one `|=`) |
| `NOT contains(line, 'x')`, `line NOT LIKE '%x%'` | `!= "x"` |
| `ts >= …`, `ts < …`, `ts BETWEEN …`, `ts_ns >= …` | the request window `[start, end)` |
| `ORDER BY ts DESC LIMIT n` (or `ASC`) | `direction=backward` (`forward`) with the scan stopped after `n` lines |

Line filters are appended **after** your pipeline, so they test the line exactly as returned
(after any `line_format`); values are escaped as LogQL strings. Every pushed predicate is also
re-applied by the worker, so results always match the SQL. Not pushed: filters on `labels['x']`,
`parsed[...]` or `structured_metadata[...]` (DuckDB evaluates them; list a stream label in
`label_columns` to push it, or write the filter into the LogQL, e.g. `| json | status >= 500`),
and DuckDB does not push `regexp_matches` or `ILIKE` to table functions (use `|~` in the
selector argument). When your pipeline contains `label_format`, `drop` or `keep`, label filters
are not pushed, because the returned labels are no longer the stream's. Loki requires one
stream matcher that cannot match the empty string; `logs()` says so before calling the server.
The window defaults to the last hour, like Loki.

## Examples

```sql
-- How much would this query read?
SELECT streams, entries, round(bytes / 1e9, 2) AS gb
FROM loki.main.index_stats('{namespace="prod"}', start_date := now() - INTERVAL 1 DAY);

-- Noisiest apps by ingested bytes.
SELECT labels['app'] AS app, bytes FROM loki.main.volume('{namespace="prod"}', target_labels := 'app')
ORDER BY bytes DESC LIMIT 10;

-- 5xx responses per route from JSON logs, parsed by LogQL.
SELECT parsed['route'] AS route, count(*) AS errors
FROM loki.main.logs('{app="api"} | json | status >= 500')
WHERE ts >= now() - INTERVAL 6 HOUR
GROUP BY route ORDER BY errors DESC;

-- Join log lines to a trace table through structured metadata.
SELECT l.ts, l.line, t.duration_ms
FROM loki.main.logs('{app="checkout"}') l JOIN 'traces.parquet' t
  ON t.trace_id = l.structured_metadata['trace_id']
WHERE l.level = 'error' AND l.ts >= now() - INTERVAL 1 HOUR;

-- Which stream labels do the ruler's rules depend on?
SELECT l AS label, count(*) AS rules
FROM loki.main.rules(), unnest(loki.main.selector_labels(query)) u(l)
GROUP BY l ORDER BY rules DESC;

-- Export a day of one app's logs to Parquet.
COPY (SELECT * FROM loki.main.logs('{app="api"}', start_date := now() - INTERVAL 1 DAY)) TO 'api.parquet';
```

## How it is tested

`make check` runs gofmt, go vet, golangci-lint, `go test` (lexer, pushdown planning, exact filter
evaluation, decoding, and the paging engine against a fake server that models Loki's limit,
direction and boundary behaviour), the SQL suite (`test/sql/loki.test`, ~85 statements) over the
stdio, launch and HTTP transports, and vgi-lint at `fail_on = "info"`.

**CI needs no Docker and no credentials.** The SQL suite talks to
`test/fixture_server/server.py`, which replays **recorded responses** from
`test/fixture_server/recorded.json` (keyed by path and sorted query/form parameters) plus a few
hand-written ones in `handwritten.json` for shapes a stock server will not produce on demand
(a 503, a warning, pattern-ingester output, a 404 from an older Loki). The runner starts two
copies: one answers every request with `429 + Retry-After: 0` once (so retries are always
exercised), the other demands a bearer token or basic auth plus `X-Scope-OrgID: tenant-a`.

**The recordings come from a real Loki.** `test/fixture/run-loki.sh start` runs
`grafana/loki:3.7.0` in monolithic mode with filesystem storage (about 150 MB RAM, capped at
768 MB), pushes deterministic streams from `test/fixture/gen_push.py` through
`/loki/api/v1/push` (2026-01-01 00:00–02:00 UTC: logfmt and JSON lines, structured metadata, a
multi-line entry, 6,200 lines in one stream to force paging, entries sharing timestamps, and
quotes, backslashes, tabs and Unicode in labels and lines), flushes the ingester and loads the
ruler rules in `test/fixture/rules`. To re-record after changing the tests:

```bash
test/fixture/run-loki.sh start            # prints http://127.0.0.1:13100
rm test/fixture_server/recorded.json
LOKI_RECORD_UPSTREAM=http://127.0.0.1:13100 make test-sql
test/fixture/run-loki.sh stop
```

Point the worker at either fixture through the secret's `BASE_URL`; there are no test-only
code paths. Recorded bodies have Loki's per-query `stats` block removed.

## Known limitations

- **Time zone.** vgi-go 0.42.0 cannot decode a pushed `TIMESTAMPTZ` filter in a session whose
  `TimeZone` is not UTC. Use `SET TimeZone = 'UTC'` with `WHERE ts …` on `logs`, or pass
  `start_date :=` / `end_date :=` (or filter `ts_ns`), which work in any time zone.
- **No embedded LogQL parser.** Loki's parser (`pkg/logql/syntax`) is AGPL-3.0 and drags in etcd,
  Kubernetes, Redis and a forked memberlist, so the worker has its own small lexer. It decides
  log vs metric (`query_type`), finds selectors and merges pushed filters, but it does not
  validate a query: `format_query()` asks the server.
- **A Loki 3.7 boundary quirk.** Loki skips a stored chunk whose last entry sits exactly on the
  query's `start`. The worker therefore always asks from one nanosecond earlier and discards
  what falls before the window; without that, forward paging and `WHERE ts >= …` could silently
  drop lines.
- **Old data and ingesters.** A stock Loki only asks ingesters about the last
  `query_ingesters_within` (3 h); older entries are served from the store once flushed.
- `query` and `query_range` do not parse LogQL to push SQL filters; filter in the LogQL, or
  use `logs`.
- `regexp_matches` / `ILIKE` are not pushed by DuckDB; `LIMIT` without `ORDER BY ts` is not sent
  to Loki (DuckDB still stops the scan after the rows it needs).
- No live tail (`/loki/api/v1/tail`), no push or delete APIs (read-only by design), no TLS
  client certificates (put a proxy in front, or use a bearer token).
- `patterns()` only sees what the pattern ingester holds (recent data, when enabled).

## Development

See [AGENTS.md](AGENTS.md) for the layout, the recipe for adding a function and the sharp
edges. `make help` lists the targets; `make template-diff` shows upstream starter changes
(this worker started from vgi-starter-go v1.3.0).

## Support

Open an issue on this repository. For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). Grafana Loki is a Grafana Labs
project; this worker is not affiliated with or endorsed by Grafana Labs. It talks to Loki over
its HTTP API and embeds no Loki code.
