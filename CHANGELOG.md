# Changelog

## 0.1.1

First complete release (v0.1.0 shipped macOS and Windows binaries only: the Linux builds
failed a release-workflow glibc check that wrongly rejected statically linked Go binaries).

- Catalog and schema descriptions rewritten for vgi-lint 2.8 (no object lists; distinct comments).

## 0.1.0

Initial Loki worker: query / query_range (log rows with paging and boundary de-duplication; metric rows in
the vgi-prometheus layout), logs() with LogQL pushdown, stream metadata (labels, label_values, series,
index_stats, volume, detected_fields, detected_labels, patterns), ruler rules and alerts, build_info,
format_query and offline LogQL tools (logql_quote, query_type, stream_selectors, selector_labels).

Targets: vgi-go 0.42 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
