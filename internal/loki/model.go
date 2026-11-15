// Decoding Loki query results into rows.
//
//   - streams (log queries): one row per log entry. With the
//     categorize-labels encoding flag (always requested) each entry is
//     ["<ns>", "line", {"structuredMetadata": {...}, "parsed": {...}}] and the
//     stream object holds only the stream labels.
//   - matrix / vector / scalar (metric queries): long rows exactly like
//     vgi-prometheus: one per series per timestamp, values as strings with
//     NaN/±Inf kept.

package loki

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// row is one output row (a log entry or a metric sample).
type row struct {
	ResultType string
	Labels     map[string]string
	TsNs       int64
	Value      float64
	HasValue   bool
	Str        *string
	Line       string
	Structured map[string]string
	Parsed     map[string]string
}

// label returns a stream/series label, ok=false when absent (SQL NULL).
func (r *row) label(name string) (string, bool) {
	v, ok := r.Labels[name]
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// tsMicros is the TIMESTAMPTZ value: nanoseconds floored to microseconds.
func (r *row) tsMicros() int64 { return floorDiv(r.TsNs, 1000) }

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// parseTimestamp converts the API's float seconds (e.g. 1767225600.015) to
// Unix nanoseconds without float rounding.
func parseTimestamp(n json.Number) (int64, error) {
	s := n.String()
	if strings.ContainsAny(s, "eE") {
		f, err := n.Float64()
		if err != nil {
			return 0, err
		}
		return int64(math.Round(f * 1e9)), nil
	}
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	whole, frac, _ := strings.Cut(s, ".")
	sec, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad timestamp %q", n)
	}
	frac = (frac + "000000000")[:9]
	ns, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad timestamp %q", n)
	}
	v := sec*1_000_000_000 + ns
	if neg {
		v = -v
	}
	return v, nil
}

// parseValue converts the API's string sample value, including NaN/±Inf.
func parseValue(s string) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("bad sample value %q", s)
	}
	return v, nil
}

// decodePoint decodes a [ts, "value"] pair.
func decodePoint(raw json.RawMessage) (int64, string, error) {
	var pair []json.RawMessage
	if err := json.Unmarshal(raw, &pair); err != nil || len(pair) != 2 {
		return 0, "", fmt.Errorf("bad sample %s", truncate(string(raw)))
	}
	var n json.Number
	dec := json.NewDecoder(bytes.NewReader(pair[0]))
	dec.UseNumber()
	if err := dec.Decode(&n); err != nil {
		return 0, "", fmt.Errorf("bad sample timestamp %s", truncate(string(pair[0])))
	}
	ts, err := parseTimestamp(n)
	if err != nil {
		return 0, "", err
	}
	var v string
	if err := json.Unmarshal(pair[1], &v); err != nil {
		return 0, "", fmt.Errorf("bad sample value %s", truncate(string(pair[1])))
	}
	return ts, v, nil
}

type rawSeries struct {
	Metric map[string]string `json:"metric"`
	Value  json.RawMessage   `json:"value"`
	Values []json.RawMessage `json:"values"`
}

type rawStream struct {
	Stream map[string]string   `json:"stream"`
	Values [][]json.RawMessage `json:"values"`
}

type rawResult struct {
	ResultType string          `json:"resultType"`
	Result     json.RawMessage `json:"result"`
}

// resultType peeks at a query result's type.
func resultType(data json.RawMessage) (string, error) {
	var rr rawResult
	if err := json.Unmarshal(data, &rr); err != nil {
		return "", fmt.Errorf("decoding query result: %w", err)
	}
	return rr.ResultType, nil
}

// decodeMetric turns a matrix/vector/scalar result into long rows.
func decodeMetric(data json.RawMessage) ([]row, error) {
	var rr rawResult
	if err := json.Unmarshal(data, &rr); err != nil {
		return nil, fmt.Errorf("decoding query result: %w", err)
	}
	switch rr.ResultType {
	case "scalar", "string":
		ts, v, err := decodePoint(rr.Result)
		if err != nil {
			return nil, err
		}
		r := row{ResultType: rr.ResultType, Labels: map[string]string{}, TsNs: ts}
		if rr.ResultType == "string" {
			r.Str = &v
		} else {
			if r.Value, err = parseValue(v); err != nil {
				return nil, err
			}
			r.HasValue = true
		}
		return []row{r}, nil
	case "vector", "matrix":
	case "streams":
		return nil, fmt.Errorf("the server returned log lines for a query the worker classified as a metric query")
	default:
		return nil, fmt.Errorf("unsupported result type %q", rr.ResultType)
	}
	var series []rawSeries
	if err := json.Unmarshal(rr.Result, &series); err != nil {
		return nil, fmt.Errorf("decoding %s result: %w", rr.ResultType, err)
	}
	var out []row
	for _, s := range series {
		labels := s.Metric
		if labels == nil {
			labels = map[string]string{}
		}
		add := func(raw json.RawMessage) error {
			ts, v, err := decodePoint(raw)
			if err != nil {
				return err
			}
			f, err := parseValue(v)
			if err != nil {
				return err
			}
			out = append(out, row{ResultType: rr.ResultType, Labels: labels, TsNs: ts, Value: f, HasValue: true})
			return nil
		}
		if len(s.Value) > 0 && string(s.Value) != "null" {
			if err := add(s.Value); err != nil {
				return nil, err
			}
		}
		for _, p := range s.Values {
			if err := add(p); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

type entryMeta struct {
	StructuredMetadata map[string]string `json:"structuredMetadata"`
	Parsed             map[string]string `json:"parsed"`
}

// decodeStreams turns a streams result into log rows (stream order).
func decodeStreams(data json.RawMessage) ([]row, error) {
	var rr rawResult
	if err := json.Unmarshal(data, &rr); err != nil {
		return nil, fmt.Errorf("decoding query result: %w", err)
	}
	if rr.ResultType != "streams" {
		return nil, fmt.Errorf("the server returned a %s result for a query the worker classified as a log query", rr.ResultType)
	}
	var streams []rawStream
	if err := json.Unmarshal(rr.Result, &streams); err != nil {
		return nil, fmt.Errorf("decoding streams: %w", err)
	}
	var out []row
	for _, s := range streams {
		labels := s.Stream
		if labels == nil {
			labels = map[string]string{}
		}
		for _, v := range s.Values {
			if len(v) < 2 {
				return nil, fmt.Errorf("bad log entry %s", truncate(fmt.Sprint(v)))
			}
			var tsText, line string
			if err := json.Unmarshal(v[0], &tsText); err != nil {
				return nil, fmt.Errorf("bad log entry timestamp %s", truncate(string(v[0])))
			}
			ts, err := strconv.ParseInt(tsText, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("bad log entry timestamp %q", tsText)
			}
			if err := json.Unmarshal(v[1], &line); err != nil {
				return nil, fmt.Errorf("bad log line %s", truncate(string(v[1])))
			}
			r := row{ResultType: "streams", Labels: labels, TsNs: ts, Line: line}
			if len(v) > 2 && string(v[2]) != "null" {
				var m entryMeta
				if err := json.Unmarshal(v[2], &m); err != nil {
					return nil, fmt.Errorf("bad log entry metadata %s", truncate(string(v[2])))
				}
				r.Structured, r.Parsed = m.StructuredMetadata, m.Parsed
			}
			out = append(out, r)
		}
	}
	return out, nil
}

func truncate(s string) string {
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}
