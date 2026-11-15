// Compiling pushed SQL predicates into a LogQL stream selector, line
// filters and a time window.
//
// The query only has to return a superset of the matching rows: every pushed
// predicate is re-evaluated exactly in filters.go. Each rule below is chosen
// so that anything the SQL predicate accepts, the LogQL also accepts:
//
//	label = 'v'                      -> label="v"
//	label <> 'v'                     -> label!="v"
//	label IN ('a','b')               -> label=~"a|b"
//	label IS NULL / IS NOT NULL      -> label="" / label!=""
//	prefix/suffix/contains(label,…)  -> label=~"…"
//	label LIKE 'p%' / NOT LIKE       -> label=~"…" / label!~"…"
//	a OR b on one label              -> one =~ alternation
//	contains(line, 'x'), line LIKE '%x%' -> |= "x"   (each literal fragment
//	  of a LIKE / prefix / suffix / = pattern becomes one |= filter)
//	NOT contains(line, 'x'), line NOT LIKE '%x%' -> != "x"
//	regexp_matches(line, 're')       -> |~ "re"
//	ts / ts_ns comparisons           -> the request window [start, end)
//
// Label columns are stream labels (job, namespace, pod, app, level and every
// label_columns entry). When the query's pipeline rewrites labels
// (label_format, drop, keep) no label predicate is pushed.

package loki

import (
	"regexp"
	"strconv"
	"strings"
)

// matcher is one PromQL label matcher.
type matcher struct {
	Label string
	Op    string // =, !=, =~, !~
	Value string
}

func (m matcher) String() string {
	name := m.Label
	if !legacyLabelName.MatchString(name) {
		name = strconv.Quote(name)
	}
	return name + m.Op + strconv.Quote(m.Value)
}

var legacyLabelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// timeWindow is a nanosecond range [Lo, Hi); nil ends are unbounded.
type timeWindow struct {
	Lo, Hi *int64
}

func (w *timeWindow) raiseLo(ns int64) {
	if w.Lo == nil || ns > *w.Lo {
		w.Lo = &ns
	}
}

func (w *timeWindow) lowerHi(ns int64) {
	if w.Hi == nil || ns < *w.Hi {
		w.Hi = &ns
	}
}

// pushdownPlan is what the pushed predicates compile to.
type pushdownPlan struct {
	Matchers    []matcher
	LineFilters []string
	Window      timeWindow
}

// planPushdown walks the top-level conjunction of preds. pushLabels=false
// keeps label predicates in the worker.
func planPushdown(preds []*expr, cols map[string]colSpec, pushLabels bool) pushdownPlan {
	var plan pushdownPlan
	var walk func(e *expr)
	walk = func(e *expr) {
		if e.K == "and" {
			if m, ok := prefixRange(e, cols); ok {
				if pushLabels {
					plan.Matchers = append(plan.Matchers, m)
				}
				return
			}
			if f, ok := linePrefixRange(e, cols); ok {
				plan.LineFilters = append(plan.LineFilters, f)
				return
			}
			for _, a := range e.Args {
				walk(a)
			}
			return
		}
		if tsBound(e, cols, &plan.Window) {
			return
		}
		if fs, ok := lineFilter(e, cols); ok {
			plan.LineFilters = append(plan.LineFilters, fs...)
			return
		}
		if !pushLabels {
			return
		}
		if m, ok := labelMatcher(e, cols); ok {
			plan.Matchers = append(plan.Matchers, m)
		}
	}
	for _, p := range preds {
		walk(p)
	}
	return plan
}

// colLabel returns the label a column reference reads, if any.
func colLabel(e *expr, cols map[string]colSpec) (string, bool) {
	if e == nil || e.K != "col" {
		return "", false
	}
	c, ok := cols[e.S]
	if !ok || c.Kind != kindLabel {
		return "", false
	}
	return c.Label, true
}

func colKindOf(e *expr, cols map[string]colSpec) (colKind, bool) {
	if e == nil || e.K != "col" {
		return 0, false
	}
	c, ok := cols[e.S]
	return c.Kind, ok
}

// tsBound narrows w for a `ts|ts_ns <op> literal` comparison.
func tsBound(e *expr, cols map[string]colSpec, w *timeWindow) bool {
	if e.K != "call" || len(e.Args) != 2 {
		return false
	}
	op, a, b := e.S, e.Args[0], e.Args[1]
	flip := map[string]string{"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "="}
	if k, ok := colKindOf(a, cols); !ok || (k != kindTs && k != kindTsNs) {
		a, b = b, a
		op = flip[op]
	}
	kind, ok := colKindOf(a, cols)
	if !ok {
		return false
	}
	// lo/hi: [lo, hi) in nanoseconds for "column >= v" / "column <= v".
	var geLo, gtLo, ltHi, leHi int64
	switch {
	case kind == kindTs && b.K == "ts":
		// ts is the entry's nanoseconds floored to microseconds. A literal
		// with a sub-microsecond part (F > 0) lies between v and v+1.
		us := b.I
		geLo, gtLo = us*1000, (us+1)*1000
		if b.F > 0 {
			geLo = (us + 1) * 1000
			ltHi = (us + 1) * 1000
		} else {
			ltHi = us * 1000
		}
		leHi = (us + 1) * 1000
	case kind == kindTsNs && b.K == "int":
		geLo, gtLo, ltHi, leHi = b.I, b.I+1, b.I, b.I+1
	default:
		return false
	}
	switch op {
	case ">=":
		w.raiseLo(geLo)
	case ">":
		w.raiseLo(gtLo)
	case "<":
		w.lowerHi(ltHi)
	case "<=":
		w.lowerHi(leHi)
	case "=":
		w.raiseLo(geLo)
		w.lowerHi(leHi)
	default:
		return false
	}
	return true
}

func isLineCol(e *expr, cols map[string]colSpec) bool {
	k, ok := colKindOf(e, cols)
	return ok && k == kindLine
}

// likeFragments returns the literal runs of a LIKE pattern (split on % and _).
func likeFragments(p string) []string {
	var out []string
	for _, f := range strings.FieldsFunc(p, func(r rune) bool { return r == '%' || r == '_' }) {
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

func containsFilters(frags []string) []string {
	out := make([]string, len(frags))
	for i, f := range frags {
		out[i] = "|= " + quoteLogQL(f)
	}
	return out
}

// lineFilter compiles one predicate on the line column to LogQL line filters.
func lineFilter(e *expr, cols map[string]colSpec) ([]string, bool) {
	if e.K == "not" && len(e.Args) == 1 {
		in := e.Args[0]
		if in.K == "call" && in.S == "contains" && len(in.Args) == 2 && isLineCol(in.Args[0], cols) && in.Args[1].K == "str" && in.Args[1].S != "" {
			return []string{"!= " + quoteLogQL(in.Args[1].S)}, true
		}
		return nil, false
	}
	if e.K != "call" || len(e.Args) < 2 || !isLineCol(e.Args[0], cols) || e.Args[1].K != "str" {
		return nil, false
	}
	lit := e.Args[1].S
	switch e.S {
	case "contains", "prefix", "starts_with", "suffix", "ends_with", "=":
		if lit == "" {
			return nil, false
		}
		return []string{"|= " + quoteLogQL(lit)}, true
	case "~~", "like":
		frags := likeFragments(lit)
		return containsFilters(frags), len(frags) > 0
	case "!~~", "not_like":
		if len(lit) >= 2 && strings.HasPrefix(lit, "%") && strings.HasSuffix(lit, "%") {
			inner := lit[1 : len(lit)-1]
			if inner != "" && !strings.ContainsAny(inner, "%_") {
				return []string{"!= " + quoteLogQL(inner)}, true
			}
		}
		return nil, false
	case "regexp_matches":
		if len(e.Args) != 2 {
			return nil, false
		}
		if _, err := regexp.Compile(lit); err != nil {
			return nil, false
		}
		return []string{"|~ " + quoteLogQL(lit)}, true
	}
	return nil, false
}

// linePrefixRange recognises DuckDB's rewrite of prefix(line, 'p') into
// `line >= 'p' AND line < succ('p')`.
func linePrefixRange(e *expr, cols map[string]colSpec) (string, bool) {
	if len(e.Args) != 2 {
		return "", false
	}
	var lo, hi string
	for _, a := range e.Args {
		if a.K != "call" || len(a.Args) != 2 || a.Args[1].K != "str" || !isLineCol(a.Args[0], cols) {
			return "", false
		}
		switch a.S {
		case ">=":
			lo = a.Args[1].S
		case "<":
			hi = a.Args[1].S
		default:
			return "", false
		}
	}
	if lo == "" || hi == "" || prefixSuccessor(lo) != hi {
		return "", false
	}
	return "|= " + quoteLogQL(lo), true
}

// labelRegex returns an RE2 pattern (fully anchored, Prometheus style) that
// accepts every label value the predicate accepts, for positive predicates.
func labelRegex(e *expr, cols map[string]colSpec) (string, string, bool) {
	if e.K == "or" {
		var label string
		var alts []string
		for _, a := range e.Args {
			l, re, ok := labelRegex(a, cols)
			if !ok || (label != "" && l != label) {
				return "", "", false
			}
			label = l
			alts = append(alts, re)
		}
		return label, strings.Join(alts, "|"), label != ""
	}
	if e.K != "call" || len(e.Args) < 2 {
		return "", "", false
	}
	label, ok := colLabel(e.Args[0], cols)
	if !ok {
		return "", "", false
	}
	var lits []string
	for _, a := range e.Args[1:] {
		if a.K != "str" {
			return "", "", false
		}
		lits = append(lits, a.S)
	}
	switch e.S {
	case "=":
		return label, regexp.QuoteMeta(lits[0]), true
	case "in":
		q := make([]string, len(lits))
		for i, l := range lits {
			q[i] = regexp.QuoteMeta(l)
		}
		return label, strings.Join(q, "|"), true
	case "prefix", "starts_with":
		return label, regexp.QuoteMeta(lits[0]) + "(?s:.*)", true
	case "suffix", "ends_with":
		return label, "(?s:.*)" + regexp.QuoteMeta(lits[0]), true
	case "contains":
		return label, "(?s:.*)" + regexp.QuoteMeta(lits[0]) + "(?s:.*)", true
	case "~~", "like":
		return label, "(?:" + likeToRegex(lits[0], false, false) + ")", true
	case "~~*", "ilike":
		return label, "(?:" + likeToRegex(lits[0], true, false) + ")", true
	case "regexp_matches", "regexp_full_match":
		flags := ""
		if len(lits) > 1 {
			f, err := regexFlags(lits[1])
			if err != nil {
				return "", "", false
			}
			flags = f
		}
		if _, err := regexp.Compile(flags + lits[0]); err != nil {
			return "", "", false
		}
		if e.S == "regexp_full_match" {
			return label, "(?:" + flags + "(?:" + lits[0] + "))", true
		}
		return label, "(?s:.*)(?:" + flags + "(?:" + lits[0] + "))(?s:.*)", true
	}
	return "", "", false
}

// prefixSuccessor is the smallest string greater than every string starting
// with p ("" when there is none).
func prefixSuccessor(p string) string {
	b := []byte(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

// prefixRange recognises DuckDB's rewrite of prefix(col, 'p') / col LIKE 'p%'
// into `col >= 'p' AND col < succ('p')` and turns it back into a prefix regex.
func prefixRange(e *expr, cols map[string]colSpec) (matcher, bool) {
	if len(e.Args) != 2 {
		return matcher{}, false
	}
	var label, lo, hi string
	for _, a := range e.Args {
		if a.K != "call" || len(a.Args) != 2 || a.Args[1].K != "str" {
			return matcher{}, false
		}
		l, ok := colLabel(a.Args[0], cols)
		if !ok || (label != "" && l != label) {
			return matcher{}, false
		}
		label = l
		switch a.S {
		case ">=":
			lo = a.Args[1].S
		case "<":
			hi = a.Args[1].S
		default:
			return matcher{}, false
		}
	}
	if lo == "" || hi == "" || prefixSuccessor(lo) != hi {
		return matcher{}, false
	}
	return matcher{label, "=~", regexp.QuoteMeta(lo) + "(?s:.*)"}, true
}

// labelMatcher compiles one predicate to a matcher.
func labelMatcher(e *expr, cols map[string]colSpec) (matcher, bool) {
	if e.K == "not" && len(e.Args) == 1 {
		// NOT p(label) is true only for non-NULL labels that fail p, so the
		// negated regex (which also admits absent labels) is a superset.
		if label, re, ok := labelRegex(e.Args[0], cols); ok {
			if _, err := regexp.Compile("^(?:" + re + ")$"); err == nil {
				return matcher{label, "!~", re}, true
			}
		}
		return matcher{}, false
	}
	if e.K == "call" {
		switch e.S {
		case "isnull", "isnotnull":
			label, ok := colLabel(e.Args[0], cols)
			if !ok {
				return matcher{}, false
			}
			if e.S == "isnull" {
				return matcher{label, "=", ""}, true
			}
			return matcher{label, "!=", ""}, true
		case "=":
			if label, ok := colLabel(e.Args[0], cols); ok && e.Args[1].K == "str" {
				return matcher{label, "=", e.Args[1].S}, true
			}
			if label, ok := colLabel(e.Args[1], cols); ok && e.Args[0].K == "str" {
				return matcher{label, "=", e.Args[0].S}, true
			}
			return matcher{}, false
		case "!=", "<>":
			if label, ok := colLabel(e.Args[0], cols); ok && e.Args[1].K == "str" {
				return matcher{label, "!=", e.Args[1].S}, true
			}
			return matcher{}, false
		case "!~~", "not_like", "!~~*", "not_ilike":
			label, ok := colLabel(e.Args[0], cols)
			if !ok || len(e.Args) != 2 || e.Args[1].K != "str" {
				return matcher{}, false
			}
			ci := strings.Contains(e.S, "*") || strings.Contains(e.S, "ilike")
			return matcher{label, "!~", likeToRegex(e.Args[1].S, ci, false)}, true
		}
	}
	label, re, ok := labelRegex(e, cols)
	if !ok {
		return matcher{}, false
	}
	if _, err := regexp.Compile("^(?:" + re + ")$"); err != nil {
		return matcher{}, false
	}
	return matcher{label, "=~", re}, true
}
