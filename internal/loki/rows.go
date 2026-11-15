// Arrow layouts of log rows and metric rows.

package loki

import (
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// colKind says how a column is filled from a row.
type colKind int

const (
	kindResultType colKind = iota
	kindLabel              // one stream/series label's value
	kindLabels             // every stream/series label as MAP(VARCHAR, VARCHAR)
	kindTs                 // TIMESTAMPTZ (microseconds)
	kindValue              // DOUBLE sample value
	kindHistogram          // always NULL: keeps the vgi-prometheus layout
	kindString             // string result value
	kindTsNs               // BIGINT nanoseconds
	kindLine               // log line
	kindStructured         // structured metadata MAP
	kindParsed             // parser-extracted labels MAP
)

// colSpec is one output column. Exported fields: it is kept in scan state.
type colSpec struct {
	Name  string
	Kind  colKind
	Label string // for kindLabel
}

var (
	labelsType = arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	tsType     = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	bucketType = arrow.StructOf(
		arrow.Field{Name: "lower", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "upper", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "lower_inclusive", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		arrow.Field{Name: "upper_inclusive", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	)
	// histogramType matches vgi-prometheus so metric rows UNION with it.
	histogramType = arrow.StructOf(
		arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "sum", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "buckets", Type: arrow.ListOf(bucketType), Nullable: false},
	)
)

const histogramSQLType = "STRUCT(count DOUBLE, sum DOUBLE, buckets STRUCT(lower DOUBLE, upper DOUBLE, count DOUBLE, lower_inclusive BOOLEAN, upper_inclusive BOOLEAN)[])"

// field returns the Arrow field for a column spec.
func (c colSpec) field() arrow.Field {
	switch c.Kind {
	case kindResultType:
		return column(c.Name, arrow.BinaryTypes.String, "LogQL result type: vector, matrix or scalar.", false)
	case kindLabel:
		if c.Label == "__name__" {
			return column(c.Name, arrow.BinaryTypes.String, "The __name__ label; NULL for Loki results (kept so metric rows UNION with vgi-prometheus).", true)
		}
		return column(c.Name, arrow.BinaryTypes.String, "Value of the '"+c.Label+"' label; NULL when absent.", true)
	case kindLabels:
		return column(c.Name, labelsType, "Every label of the stream or series as MAP(VARCHAR, VARCHAR). Read one with labels['name'].", false)
	case kindTs:
		return column(c.Name, tsType, "Entry or sample timestamp (UTC, microsecond precision; ts_ns keeps nanoseconds).", false)
	case kindValue:
		return column(c.Name, arrow.PrimitiveTypes.Float64, "Sample value; NaN, +Inf and -Inf are kept.", true)
	case kindHistogram:
		return column(c.Name, histogramType, "Always NULL for Loki; present so metric rows UNION with vgi-prometheus query_range.", true)
	case kindString:
		return column(c.Name, arrow.BinaryTypes.String, "Value of a string result; NULL otherwise.", true)
	case kindTsNs:
		return column(c.Name, arrow.PrimitiveTypes.Int64, "Entry timestamp in Unix nanoseconds (Loki's full precision).", false)
	case kindLine:
		return column(c.Name, arrow.BinaryTypes.String, "The log line as returned (after any line_format in the query).", false)
	case kindStructured:
		return column(c.Name, labelsType, "Structured metadata of the entry (e.g. trace_id, detected_level); empty map when none.", false)
	case kindParsed:
		return column(c.Name, labelsType, "Labels extracted by parsers in the query (| json, | logfmt, | regexp, | pattern); empty map when none.", false)
	}
	panic("unknown column kind")
}

func schemaOf(cols []colSpec) *arrow.Schema {
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i] = c.field()
	}
	return arrow.NewSchema(fields, nil)
}

// metricColumns is the layout of metric results: identical to vgi-prometheus
// query/query_range, so the two UNION ALL (by position or by name).
var metricColumns = []colSpec{
	{Name: "result_type", Kind: kindResultType},
	{Name: "metric", Kind: kindLabel, Label: "__name__"},
	{Name: "job", Kind: kindLabel, Label: "job"},
	{Name: "instance", Kind: kindLabel, Label: "instance"},
	{Name: "labels", Kind: kindLabels},
	{Name: "ts", Kind: kindTs},
	{Name: "value", Kind: kindValue},
	{Name: "histogram", Kind: kindHistogram},
	{Name: "string_value", Kind: kindString},
}

// hoistedLabels are the stream labels every log row carries as columns.
var hoistedLabels = []string{"job", "namespace", "pod", "app", "level"}

// logColumns is the log-row layout with optional extra label columns.
func logColumns(extra []string) []colSpec {
	cols := []colSpec{{Name: "ts", Kind: kindTs}, {Name: "ts_ns", Kind: kindTsNs}}
	for _, l := range hoistedLabels {
		cols = append(cols, colSpec{Name: l, Kind: kindLabel, Label: l})
	}
	for _, l := range extra {
		cols = append(cols, colSpec{Name: l, Kind: kindLabel, Label: l})
	}
	return append(cols,
		colSpec{Name: "labels", Kind: kindLabels},
		colSpec{Name: "line", Kind: kindLine},
		colSpec{Name: "structured_metadata", Kind: kindStructured},
		colSpec{Name: "parsed", Kind: kindParsed},
	)
}

// buildRows converts rows to Arrow arrays in cols order.
func buildRows(cols []colSpec, rows []row) []arrow.Array {
	mem := memory.NewGoAllocator()
	out := make([]arrow.Array, len(cols))
	for i, c := range cols {
		out[i] = buildColumn(mem, c, rows)
	}
	return out
}

func appendMap(b *array.MapBuilder, m map[string]string) {
	b.Append(true)
	kb := b.KeyBuilder().(*array.StringBuilder)
	vb := b.ItemBuilder().(*array.StringBuilder)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		kb.Append(k)
		vb.Append(m[k])
	}
}

func buildColumn(mem memory.Allocator, c colSpec, rows []row) arrow.Array {
	switch c.Kind {
	case kindResultType, kindLabel, kindString, kindLine:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for i := range rows {
			r := &rows[i]
			switch c.Kind {
			case kindResultType:
				b.Append(r.ResultType)
			case kindLine:
				b.Append(r.Line)
			case kindLabel:
				if v, ok := r.label(c.Label); ok {
					b.Append(v)
				} else {
					b.AppendNull()
				}
			default:
				if r.Str != nil {
					b.Append(*r.Str)
				} else {
					b.AppendNull()
				}
			}
		}
		return b.NewArray()
	case kindLabels, kindStructured, kindParsed:
		b := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
		defer b.Release()
		for i := range rows {
			switch c.Kind {
			case kindLabels:
				appendMap(b, rows[i].Labels)
			case kindStructured:
				appendMap(b, rows[i].Structured)
			default:
				appendMap(b, rows[i].Parsed)
			}
		}
		return b.NewArray()
	case kindTs:
		b := array.NewTimestampBuilder(mem, tsType)
		defer b.Release()
		for i := range rows {
			b.Append(arrow.Timestamp(rows[i].tsMicros()))
		}
		return b.NewArray()
	case kindTsNs:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for i := range rows {
			b.Append(rows[i].TsNs)
		}
		return b.NewArray()
	case kindValue:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for i := range rows {
			if rows[i].HasValue {
				b.Append(rows[i].Value)
			} else {
				b.AppendNull()
			}
		}
		return b.NewArray()
	case kindHistogram:
		return array.MakeArrayOfNull(mem, histogramType, len(rows))
	}
	panic("unknown column kind")
}

// sqlTypeOf is a column's DuckDB type name for documentation.
func sqlTypeOf(c colSpec) string {
	switch c.Kind {
	case kindLabels, kindStructured, kindParsed:
		return "MAP(VARCHAR, VARCHAR)"
	case kindTs:
		return "TIMESTAMP WITH TIME ZONE"
	case kindValue:
		return "DOUBLE"
	case kindHistogram:
		return histogramSQLType
	case kindTsNs:
		return "BIGINT"
	}
	return "VARCHAR"
}

func columnsByName(cols []colSpec) map[string]colSpec {
	m := make(map[string]colSpec, len(cols))
	for _, c := range cols {
		m[c.Name] = c
	}
	return m
}
