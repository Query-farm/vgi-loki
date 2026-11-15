// A small LogQL lexer, written for this worker.
//
// The worker does not embed Loki's own LogQL parser: it lives in the
// AGPL-3.0 grafana/loki module and pulls etcd, Kubernetes, Redis and a forked
// memberlist into the binary. Full validation is available from the server
// (format_query()). What the worker needs locally is much smaller, and this
// lexer provides exactly that:
//
//   - the query kind (log or metric), which fixes query_range's output schema
//     at bind time: a log query is a stream selector, optionally wrapped in
//     parentheses, so its first significant token is '{';
//   - the stream selectors ({...} outside string literals) and their
//     matchers, so pushed SQL filters can be merged into the user's selector;
//   - whether the pipeline rewrites labels (label_format, drop, keep), which
//     turns label pushdown off.
//
// Strings follow LogQL (Go) syntax: "..." with backslash escapes and `...`
// raw strings. Comments run from '#' to the end of the line.

package loki

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type lqTok struct {
	Kind  byte // 'i' identifier, 's' string, 'n' number/duration, 'o' operator, 'p' punctuation
	Text  string
	Start int // byte offset of the token
	End   int // byte offset after the token
	Str   string
}

// lexLogQL splits a LogQL query into tokens.
func lexLogQL(q string) ([]lqTok, error) {
	var out []lqTok
	i := 0
	for i < len(q) {
		c := q[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#':
			for i < len(q) && q[i] != '\n' {
				i++
			}
		case c == '"':
			j := i + 1
			for j < len(q) && q[j] != '"' {
				if q[j] == '\\' {
					j++
				}
				if j < len(q) && q[j] == '\n' {
					return nil, fmt.Errorf("newline in string literal at offset %d", i)
				}
				j++
			}
			if j >= len(q) {
				return nil, fmt.Errorf("unterminated string literal at offset %d", i)
			}
			s, err := strconv.Unquote(q[i : j+1])
			if err != nil {
				return nil, fmt.Errorf("bad string literal %s: %v", q[i:j+1], err)
			}
			out = append(out, lqTok{Kind: 's', Text: q[i : j+1], Start: i, End: j + 1, Str: s})
			i = j + 1
		case c == '`':
			j := strings.IndexByte(q[i+1:], '`')
			if j < 0 {
				return nil, fmt.Errorf("unterminated raw string at offset %d", i)
			}
			end := i + 1 + j + 1
			out = append(out, lqTok{Kind: 's', Text: q[i:end], Start: i, End: end, Str: q[i+1 : end-1]})
			i = end
		case c == '{' || c == '}' || c == '(' || c == ')' || c == '[' || c == ']' || c == ',':
			out = append(out, lqTok{Kind: 'p', Text: string(c), Start: i, End: i + 1})
			i++
		case isIdentStart(c):
			j := i + 1
			for j < len(q) && isIdentPart(q[j]) {
				j++
			}
			out = append(out, lqTok{Kind: 'i', Text: q[i:j], Start: i, End: j})
			i = j
		case c >= '0' && c <= '9' || c == '.':
			j := i + 1
			for j < len(q) && (isIdentPart(q[j]) || q[j] == '.' ||
				((q[j] == '+' || q[j] == '-') && (q[j-1] == 'e' || q[j-1] == 'E'))) {
				j++
			}
			out = append(out, lqTok{Kind: 'n', Text: q[i:j], Start: i, End: j})
			i = j
		default:
			op := ""
			for _, cand := range []string{"|=", "|~", "|>", "!=", "!~", "!>", "=~", "==", "<=", ">=", "|", "=", "<", ">", "+", "-", "*", "/", "%", "^", "!"} {
				if strings.HasPrefix(q[i:], cand) {
					op = cand
					break
				}
			}
			if op == "" {
				return nil, fmt.Errorf("unexpected character %q at offset %d", c, i)
			}
			out = append(out, lqTok{Kind: 'o', Text: op, Start: i, End: i + len(op)})
			i += len(op)
		}
	}
	return out, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == ':'
}

// selectorSpan is one stream selector: token indexes of its braces.
type selectorSpan struct {
	Open, Close int // token indexes of '{' and '}'
}

// logqlInfo is what the worker learns from a query without a full parse.
type logqlInfo struct {
	Tokens    []lqTok
	Selectors []selectorSpan
	IsLog     bool
}

// analyzeLogQL lexes q, checks bracket balance and finds the selectors.
func analyzeLogQL(q string) (*logqlInfo, error) {
	toks, err := lexLogQL(q)
	if err != nil {
		return nil, err
	}
	if len(toks) == 0 {
		return nil, errors.New("empty LogQL query")
	}
	info := &logqlInfo{Tokens: toks}
	var stack []int
	pairs := map[string]string{")": "(", "]": "[", "}": "{"}
	for i, t := range toks {
		if t.Kind != 'p' || t.Text == "," {
			continue
		}
		switch t.Text {
		case "(", "[", "{":
			if t.Text == "{" && len(stack) > 0 && toks[stack[len(stack)-1]].Text == "{" {
				return nil, fmt.Errorf("nested '{' at offset %d", t.Start)
			}
			stack = append(stack, i)
		default:
			if len(stack) == 0 || toks[stack[len(stack)-1]].Text != pairs[t.Text] {
				return nil, fmt.Errorf("unbalanced %q at offset %d", t.Text, t.Start)
			}
			open := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if t.Text == "}" {
				info.Selectors = append(info.Selectors, selectorSpan{Open: open, Close: i})
			}
		}
	}
	if len(stack) > 0 {
		return nil, fmt.Errorf("unclosed %q at offset %d", toks[stack[len(stack)-1]].Text, toks[stack[len(stack)-1]].Start)
	}
	first := 0
	for first < len(toks) && toks[first].Text == "(" && toks[first].Kind == 'p' {
		first++
	}
	info.IsLog = first < len(toks) && toks[first].Kind == 'p' && toks[first].Text == "{"
	return info, nil
}

// selectorText returns the source text of selector s.
func (li *logqlInfo) selectorText(q string, s selectorSpan) string {
	return q[li.Tokens[s.Open].Start:li.Tokens[s.Close].End]
}

// matchers parses the matchers of selector s.
func (li *logqlInfo) matchers(s selectorSpan) ([]matcher, error) {
	toks := li.Tokens[s.Open+1 : s.Close]
	var out []matcher
	for i := 0; i < len(toks); {
		if len(toks)-i < 3 {
			return nil, fmt.Errorf("incomplete matcher in stream selector at offset %d", toks[i].Start)
		}
		name, op, val := toks[i], toks[i+1], toks[i+2]
		if name.Kind != 'i' || op.Kind != 'o' || val.Kind != 's' {
			return nil, fmt.Errorf("bad matcher in stream selector at offset %d: want label=\"value\"", name.Start)
		}
		switch op.Text {
		case "=", "!=", "=~", "!~":
		default:
			return nil, fmt.Errorf("bad matcher operator %q at offset %d", op.Text, op.Start)
		}
		out = append(out, matcher{Label: name.Text, Op: op.Text, Value: val.Str})
		i += 3
		if i < len(toks) {
			if toks[i].Text != "," {
				return nil, fmt.Errorf("expected ',' in stream selector at offset %d", toks[i].Start)
			}
			i++
		}
	}
	return out, nil
}

// rewritesLabels reports whether a pipeline stage can change stream labels,
// in which case stream-label filters cannot be pushed into the selector.
func (li *logqlInfo) rewritesLabels() bool {
	for i, t := range li.Tokens {
		if t.Kind == 'i' && i > 0 && li.Tokens[i-1].Text == "|" {
			switch t.Text {
			case "label_format", "drop", "keep":
				return true
			}
		}
	}
	return false
}

// matchesEmpty reports whether a matcher accepts the empty value (Loki
// requires at least one matcher that does not).
func (m matcher) matchesEmpty() bool {
	switch m.Op {
	case "=":
		return m.Value == ""
	case "!=":
		return m.Value != ""
	case "=~", "!~":
		re, err := regexp.Compile("^(?:" + m.Value + ")$")
		if err != nil {
			return true
		}
		return re.MatchString("") == (m.Op == "=~")
	}
	return true
}

// hasRequiredMatcher reports whether Loki will accept ms as a selector.
func hasRequiredMatcher(ms []matcher) bool {
	for _, m := range ms {
		if !m.matchesEmpty() {
			return true
		}
	}
	return false
}

// selectorLabels returns the sorted distinct label names used by every selector.
func (li *logqlInfo) selectorLabels() ([]string, error) {
	seen := map[string]bool{}
	for _, s := range li.Selectors {
		ms, err := li.matchers(s)
		if err != nil {
			return nil, err
		}
		for _, m := range ms {
			seen[m.Label] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// quoteLogQL renders s as a LogQL string literal ("..."), escaped so the
// server reads back exactly s.
func quoteLogQL(s string) string { return strconv.Quote(s) }

// mergeQuery inserts extra matchers into the first stream selector of a log
// query and appends line filters at the end of its pipeline (inside any
// parentheses wrapping the log expression). Line filters go last so they
// test the line exactly as returned, after any line_format stage.
func mergeQuery(q string, li *logqlInfo, extra []matcher, lineFilters []string) string {
	if len(li.Selectors) == 0 {
		return q
	}
	sel := li.Selectors[0]
	toks := li.Tokens
	closeAt := toks[sel.Close].Start
	var b strings.Builder
	b.WriteString(q[:closeAt])
	if len(extra) > 0 {
		if sel.Close > sel.Open+1 {
			b.WriteString(", ")
		}
		for i, m := range extra {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(m.String())
		}
	}
	if len(lineFilters) == 0 {
		b.WriteString(q[closeAt:])
		return b.String()
	}
	// The log expression is wrapped in exactly sel.Open parentheses.
	insertTok := len(toks) - 1 - sel.Open
	at := toks[insertTok].End
	b.WriteString(q[closeAt:at])
	for _, f := range lineFilters {
		b.WriteString(" ")
		b.WriteString(f)
	}
	b.WriteString(q[at:])
	return b.String()
}
