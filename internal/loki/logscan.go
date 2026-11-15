// The paged, chunked log scan shared by query_range (log queries) and logs().
//
// Loki returns at most `limit` entries per request (and rejects limits above
// the server's max_entries_limit_per_query, 5000 by default), newest first
// for direction=backward and oldest first for forward. The scan:
//
//  1. splits [start, end) into chunks of ChunkNs aligned to multiples of
//     ChunkNs since the epoch (so they line up with the query frontend's
//     split_queries_by_interval and its results cache), walked newest-first
//     for backward and oldest-first for forward;
//  2. pages inside a chunk: when a page comes back full, the next request
//     ends just after (backward) or starts at (forward) the timestamp T of the
//     last entry returned, because more entries may share T; entries at T
//     already emitted (same stream, timestamp and line: Loki itself
//     de-duplicates those) are skipped. If PageSize entries share one
//     timestamp the scan cannot make progress and fails, like logcli;
//  3. stops after MaxLines rows (0 = unlimited).
//
// All of it lives in LogScan, which is part of the table-function state and
// therefore travels in the HTTP continuation token: each page can be served
// by a different request.

package loki

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// LogScan is the externalized cursor of a log scan.
type LogScan struct {
	Query    string
	StartNs  int64 // inclusive
	EndNs    int64 // exclusive
	Backward bool
	PageSize int64
	MaxLines int64 // 0 = unlimited
	ChunkNs  int64
	// Cursor.
	ChunkLo, ChunkHi int64 // current chunk [lo, hi)
	PageLo, PageHi   int64 // next request [lo, hi) inside the chunk
	Started          bool
	Done             bool
	BoundaryTs       int64
	BoundarySeen     []uint64
	Emitted          int64
	Requests         int64
}

// chunkFor returns the aligned chunk containing ns, clipped to the scan.
func (s *LogScan) chunkFor(ns int64) (int64, int64) {
	lo := floorDiv(ns, s.ChunkNs) * s.ChunkNs
	return max(lo, s.StartNs), min(lo+s.ChunkNs, s.EndNs)
}

// begin positions the cursor on the first chunk.
func (s *LogScan) begin() {
	s.Started = true
	if s.EndNs <= s.StartNs || s.ChunkNs <= 0 {
		s.Done = true
		return
	}
	if s.Backward {
		s.ChunkLo, s.ChunkHi = s.chunkFor(s.EndNs - 1)
	} else {
		s.ChunkLo, s.ChunkHi = s.chunkFor(s.StartNs)
	}
	s.PageLo, s.PageHi = s.ChunkLo, s.ChunkHi
	s.BoundarySeen = nil
}

// nextChunk moves to the following chunk, or finishes.
func (s *LogScan) nextChunk() {
	s.BoundarySeen = nil
	if s.Backward {
		if s.ChunkLo <= s.StartNs {
			s.Done = true
			return
		}
		s.ChunkLo, s.ChunkHi = s.chunkFor(s.ChunkLo - 1)
	} else {
		if s.ChunkHi >= s.EndNs {
			s.Done = true
			return
		}
		s.ChunkLo, s.ChunkHi = s.chunkFor(s.ChunkHi)
	}
	s.PageLo, s.PageHi = s.ChunkLo, s.ChunkHi
}

// entryKey identifies one entry for boundary de-duplication.
func entryKey(r *row) uint64 {
	h := fnv.New64a()
	keys := make([]string, 0, len(r.Labels))
	for k := range r.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(r.Labels[k]))
		_, _ = h.Write([]byte{1})
	}
	_, _ = h.Write([]byte(strconv.FormatInt(r.TsNs, 10)))
	_, _ = h.Write([]byte{2})
	_, _ = h.Write([]byte(r.Line))
	return h.Sum64()
}

// sortEntries orders entries in scan direction, ties broken deterministically.
func sortEntries(rows []row, backward bool) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := &rows[i], &rows[j]
		if a.TsNs != b.TsNs {
			return (a.TsNs > b.TsNs) == backward
		}
		return a.Line < b.Line
	})
}

func (s *LogScan) seen(k uint64) bool {
	for _, x := range s.BoundarySeen {
		if x == k {
			return true
		}
	}
	return false
}

// fetchPage makes one request and returns the new rows (already de-duplicated,
// in scan direction), advancing the cursor.
// want > 0 lowers the request limit when only that many more rows are needed.
func (s *LogScan) fetchPage(ctx context.Context, c *conn, want int64) ([]row, error) {
	if !s.Started {
		s.begin()
	}
	if s.Done {
		return nil, nil
	}
	limit := s.PageSize
	if want > 0 {
		limit = min(limit, want+int64(len(s.BoundarySeen)))
	}
	if int64(len(s.BoundarySeen)) >= limit {
		limit = s.PageSize
	}
	dir := "forward"
	if s.Backward {
		dir = "backward"
	}
	// Ask from one nanosecond early: Loki 3.7 skips a chunk whose last entry
	// sits exactly on the query start (seen against the store), which would
	// silently drop entries at a forward page boundary or a WHERE ts >= bound.
	// Entries before PageLo are discarded below.
	data, err := c.data(ctx, apiPath+"query_range", url.Values{
		"query": {s.Query}, "start": {nsParam(s.PageLo - 1)}, "end": {nsParam(s.PageHi)},
		"limit": {strconv.FormatInt(limit, 10)}, "direction": {dir},
	}, true)
	if err != nil {
		return nil, fmt.Errorf("%w (request %d, window %s to %s)", err, s.Requests+1, fmtNs(s.PageLo), fmtNs(s.PageHi))
	}
	s.Requests++
	rows, err := decodeStreams(data)
	if err != nil {
		return nil, err
	}
	full := int64(len(rows)) >= limit
	inWindow := rows[:0]
	for i := range rows {
		if rows[i].TsNs >= s.PageLo && rows[i].TsNs < s.PageHi {
			inWindow = append(inWindow, rows[i])
		}
	}
	if full && len(inWindow) == 0 {
		return nil, fmt.Errorf("loki: a full page of %d entries fell just before %s; raise page_size", limit, fmtNs(s.PageLo))
	}
	rows = inWindow
	sortEntries(rows, s.Backward)
	fresh := rows[:0:0]
	for i := range rows {
		r := &rows[i]
		if r.TsNs == s.BoundaryTs && len(s.BoundarySeen) > 0 && s.seen(entryKey(r)) {
			continue
		}
		fresh = append(fresh, *r)
	}
	if !full || len(rows) == 0 {
		s.nextChunk()
	} else {
		t := rows[len(rows)-1].TsNs
		if t != s.BoundaryTs {
			s.BoundaryTs, s.BoundarySeen = t, nil
		}
		for i := range fresh {
			if fresh[i].TsNs == t {
				s.BoundarySeen = append(s.BoundarySeen, entryKey(&fresh[i]))
			}
		}
		if int64(len(s.BoundarySeen)) >= s.PageSize {
			return nil, fmt.Errorf("loki: at least %d entries share the timestamp %s, so paging with page_size %d cannot make progress; raise page_size (up to the server's max_entries_limit_per_query)",
				len(s.BoundarySeen), fmtNs(t), s.PageSize)
		}
		if s.Backward {
			s.PageHi = t + 1
		} else {
			s.PageLo = t
		}
	}
	return fresh, nil
}

func fmtNs(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339Nano) }

// emitLogPage runs one page and emits the rows that pass preds.
func emitLogPage(ctx context.Context, c *conn, s *LogScan, cols []colSpec, preds []*expr, out *vgirpc.OutputCollector) error {
	if s.Done {
		return out.Finish()
	}
	// MaxLines counts rows that pass the filters; without filters every
	// fetched row counts, so the request limit can shrink to what is left.
	var want int64
	if s.MaxLines > 0 && len(preds) == 0 {
		want = s.MaxLines - s.Emitted
	}
	rows, err := s.fetchPage(ctx, c, want)
	if err != nil {
		return err
	}
	if len(preds) > 0 {
		env := &rowEnv{cols: columnsByName(cols)}
		kept := rows[:0]
		for i := range rows {
			env.r = &rows[i]
			ok, err := matchAll(preds, env)
			if err != nil {
				return err
			}
			if ok {
				kept = append(kept, rows[i])
			}
		}
		rows = kept
	}
	if s.MaxLines > 0 && s.Emitted+int64(len(rows)) >= s.MaxLines {
		rows = rows[:s.MaxLines-s.Emitted]
		s.Done = true
	}
	s.Emitted += int64(len(rows))
	return emitRows(out, cols, rows)
}

// emitRows emits rows as one batch (an empty batch is fine: the scan continues).
func emitRows(out *vgirpc.OutputCollector, cols []colSpec, rows []row) error {
	arrs := buildRows(cols, rows)
	for _, a := range arrs {
		defer a.Release()
	}
	return out.EmitArrays(arrs, int64(len(rows)))
}

// parseDirection validates a direction argument.
func parseDirection(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "backward":
		return true, nil
	case "forward":
		return false, nil
	}
	return false, fmt.Errorf("direction must be 'backward' or 'forward', got %q", s)
}

// predsFromJSON decodes the filters kept in state.
func predsFromJSON(s string) ([]*expr, error) {
	if s == "" {
		return nil, nil
	}
	var preds []*expr
	err := json.Unmarshal([]byte(s), &preds)
	return preds, err
}
