// Package loki exposes the Grafana Loki HTTP API (and LogQL tools) as a VGI catalog.
//
// To add a function: write it (see query.go, logs.go, catalog_funcs.go or
// compute.go), then register it in NewWorker below.
//
// Run it:
//
//	vgi-loki-worker                      # stdio: what DuckDB launches by default
//	vgi-loki-worker --http --port 8000   # HTTP: for Docker / hosted deploys
//	vgi-loki-worker --help
//
// Attach it from DuckDB:
//
//	ATTACH 'loki' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-loki-worker');
package loki

import (
	"github.com/Query-farm/vgi-go/vgi"
)

// allExamples lists every function's examples for the schema-level tag.
func allExamples() []exampleQuery {
	lists := [][]vgi.CatalogExample{queryExamples, queryRangeExamples, logsExamples, labelsExamples, labelValuesExamples,
		seriesExamples, statsExamples, volumeExamples, detectedFieldsExamples, detectedLabelsExamples, patternsExamples,
		rulesExamples, alertsExamples, buildInfoExamples, formatExamples}
	return exampleQueries(append(lists, toolsExamples()...)...)
}

// NewWorker builds the worker: one catalog, one main schema. Extra options
// (logging, storage) are appended after the catalog options.
func NewWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	sourceURL, version := SourceURL, Version
	base := []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment(CatalogDocMD),
		vgi.WithCatalogTags(CatalogTags),
		vgi.WithCatalogInfo(vgi.CatalogInfo{SourceURL: &sourceURL, ImplementationVersion: &version}),
		vgi.WithSchemaComments(map[string]string{"main": SchemaDocMD}),
		vgi.WithSchemaTags(map[string]map[string]string{"main": schemaTags(allExamples())}),
		vgi.WithSecretTypes(APISecretType),
	}
	w := vgi.NewWorker(append(base, opts...)...)
	registerEvaluator()
	w.RegisterTable(vgi.AsTableFunction[QueryState](&QueryFunc{}))
	w.RegisterTable(vgi.AsTableFunction[RangeState](&QueryRangeFunc{}))
	w.RegisterTable(vgi.AsTableFunction[LogsState](&LogsFunc{}))
	w.RegisterTable(vgi.AsTableFunction[LogsState](&LogsFunc{NoSelector: true}))
	for _, f := range []vgi.TableFunction{newLabels(), newLabelValues(), newSeries(), newIndexStats(), newVolume(),
		newDetectedFields(), newDetectedLabels(), newPatterns(), newRules(), newAlerts(), newBuildInfo(), newFormatQuery()} {
		w.RegisterTable(f)
	}
	registerTools(w)
	return w
}
