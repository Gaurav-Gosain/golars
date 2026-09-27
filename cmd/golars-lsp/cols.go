package main

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
)

// Column discovery. We read headers from the files a script loads
// so completion can offer real column names after `filter`, `sort`,
// `select`, `drop`, and `groupby`.
//
// Supported formats:
//   - .csv / .tsv: first row, parsed with encoding/csv
//   - .parquet / .pq / .arrow / .ipc / .json / .ndjson: not yet (would
//     pull golars' io/parquet + io/json into the LSP; left as future
//     work: the LSP silently skips unsupported formats)
//
// A mtime-keyed cache keeps the LSP from re-reading the same file
// on every keystroke.

type headerEntry struct {
	mtime int64
	cols  []string
	rows  int // -1 when unknown (file too large or unsupported format)
}

var headerCache sync.Map // abs path → headerEntry

// maxRowCountBytes caps file size for row-count inlay hints. Counting
// lines in a multi-GiB CSV on every didChange would stall the LSP;
// past this threshold we report `?` as the row count.
const maxRowCountBytes int64 = 8 * 1024 * 1024

// readFileStats returns (columns, row count) for a loaded file,
// reading from disk at most once per mtime. rows is -1 for
// unsupported formats or files above maxRowCountBytes.
func readFileStats(absPath string) headerEntry {
	info, err := os.Stat(absPath)
	if err != nil || !info.Mode().IsRegular() {
		return headerEntry{rows: -1}
	}
	mtime := info.ModTime().UnixNano()
	if v, ok := headerCache.Load(absPath); ok {
		e := v.(headerEntry)
		if e.mtime == mtime {
			return e
		}
	}
	cols := readHeaderFromDisk(absPath)
	rows := -1
	ext := strings.ToLower(filepath.Ext(absPath))
	if (ext == ".csv" || ext == ".tsv") && info.Size() <= maxRowCountBytes {
		rows = countCSVRows(absPath)
	}
	e := headerEntry{mtime: mtime, cols: cols, rows: rows}
	headerCache.Store(absPath, e)
	return e
}

func countCSVRows(absPath string) int {
	f, err := os.Open(absPath)
	if err != nil {
		return -1
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxRowCountBytes {
		return -1
	}
	limited := &io.LimitedReader{R: f, N: maxRowCountBytes + 1}
	r := csv.NewReader(limited)
	r.ReuseRecord = true
	if strings.EqualFold(filepath.Ext(absPath), ".tsv") {
		r.Comma = '\t'
	}
	records := 0
	for {
		_, err := r.Read()
		if limited.N == 0 {
			return -1
		}
		if err == io.EOF {
			return max(0, records-1)
		}
		if err != nil {
			return -1
		}
		records++
	}
}

func readHeaderFromDisk(absPath string) []string {
	ext := strings.ToLower(filepath.Ext(absPath))
	switch ext {
	case ".csv", ".tsv":
		return readDelimitedHeader(absPath, ext == ".tsv")
	}
	return nil
}

func readDelimitedHeader(absPath string, isTSV bool) []string {
	f, err := os.Open(absPath)
	if err != nil {
		return nil
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	limited := &io.LimitedReader{R: f, N: maxRowCountBytes + 1}
	r := csv.NewReader(limited)
	r.FieldsPerRecord = -1
	if isTSV {
		r.Comma = '\t'
	}
	row, err := r.Read()
	if err != nil || limited.N == 0 {
		return nil
	}
	out := make([]string, len(row))
	for i, v := range row {
		out[i] = strings.TrimSpace(v)
	}
	return out
}

// frameShape pairs a frame's column list with its row count. rows
// is -1 when the count can't be inferred symbolically: after a
// filter, an inner join, or a groupby we know the schema but not
// the exact cardinality.
type frameShape struct {
	cols []string
	rows int
}

// rowsUnknown is our sentinel for "can't infer statically".
const rowsUnknown = -1

// frameState captures what a reader of the document knows about
// loaded frames at a given cursor line. The focus is the currently-
// targeted frame (polars' equivalent of `df`); staged holds frames
// brought in via `load PATH as NAME`.
type frameState struct {
	focusName string                // "" = anonymous
	focus     frameShape            // shape of the focus
	staged    map[string]frameShape // NAME → shape
}

// framesAtLine walks the document up to (but not including) stopLine
// and returns the frame state as it would be at that cursor position.
// Every pipeline statement is replayed symbolically to keep (rows,
// cols) in sync: this mirrors golars' runtime semantics closely
// enough for inlay hints to be useful.
func framesAtLine(d *document, stopLine int) frameState {
	st := frameState{staged: make(map[string]frameShape)}
	dir := docDir(d)
	for li := 0; li < stopLine && li < len(d.lines); li++ {
		stmt := script.Normalize(d.lines[li])
		if stmt == "" {
			continue
		}
		applyStmt(&st, dir, stmt)
	}
	return st
}

// applyStmt advances the state machine by one statement. Split out
// so the inlay-hint walker can invoke the same logic while also
// capturing the post-statement shape for each line.
func applyStmt(st *frameState, dir, stmt string) {
	parts := strings.Fields(stmt)
	if len(parts) == 0 {
		return
	}
	cmd := canonicalCommand(parts[0])
	switch cmd {
	case "load",
		"scan_csv", "scan_parquet", "scan_ipc", "scan_json", "scan_ndjson", "scan_auto":
		// Lazy scans carry the same shape semantics as eager load for
		// the purposes of inlay hints. The executor decides whether
		// the file actually opens at Collect time; the LSP just needs
		// the static column/row shape.
		if len(parts) < 2 {
			return
		}
		abs := resolvePath(parts[1], dir)
		stats := readFileStats(abs)
		shape := frameShape{cols: stats.cols, rows: stats.rows}
		if len(parts) >= 4 && strings.EqualFold(parts[2], "as") {
			st.staged[parts[3]] = shape
		} else {
			st.focusName = ""
			st.focus = shape
		}
	case "use":
		// Copy-on-promote: focus becomes a clone of staged[NAME]; the
		// staged entry stays so repeated `use NAME` can branch off the
		// same base. Prior focus is discarded (no auto-stash).
		if len(parts) < 2 {
			return
		}
		name := parts[1]
		staged, ok := st.staged[name]
		if !ok {
			return
		}
		st.focusName = name
		st.focus = staged
	case "stash":
		// Snapshot the focus under NAME. Focus continues with the same
		// materialised shape so downstream statements operate on the
		// snapshot.
		if len(parts) < 2 {
			return
		}
		st.staged[parts[1]] = st.focus
	case "drop_frame":
		if len(parts) >= 2 {
			delete(st.staged, parts[1])
		}
	case "filter":
		// Row count drops to an unknown upper bound; schema unchanged.
		st.focus.rows = rowsUnknown
	case "sort":
		// Shape-preserving.
	case "limit", "sample", "top_k", "bottom_k":
		// limit N and sample N keep at most N rows; top_k/bottom_k K
		// keep at most K.
		if len(parts) >= 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				st.focus.rows = clampRows(st.focus.rows, n)
			}
		}
	case "unique", "drop_null":
		st.focus.rows = rowsUnknown
	case "rename":
		// rename OLD as NEW
		if len(parts) == 4 {
			cols := append([]string(nil), st.focus.cols...)
			for i, c := range cols {
				if c == parts[1] {
					cols[i] = parts[3]
				}
			}
			st.focus.cols = cols
		}
	case "with_row_index":
		if len(parts) >= 2 {
			st.focus.cols = append([]string{parts[1]}, st.focus.cols...)
		}
	case "sum_horizontal", "mean_horizontal", "min_horizontal",
		"max_horizontal", "all_horizontal", "any_horizontal":
		if len(parts) >= 2 {
			st.focus.cols = append(append([]string(nil), st.focus.cols...), parts[1])
		}
	case "unpivot":
		// unpivot IDS [VALS] -> IDS..., variable, value
		if len(parts) >= 2 {
			st.focus.cols = append(commaList(stmt, parts[:2]), "variable", "value")
			st.focus.rows = rowsUnknown
		}
	case "transpose", "pivot":
		st.focus.cols = nil
		st.focus.rows = rowsUnknown
	case "select":
		st.focus.cols = selectColumns(stmt, parts)
	case "with":
		// `with NAME = EXPR` adds (or replaces) one column; the row
		// count is unchanged.
		rest := strings.TrimSpace(strings.TrimPrefix(stmt, parts[0]))
		if name, _, found := strings.Cut(rest, "="); found {
			if name = strings.TrimSpace(name); name != "" && !slices.Contains(st.focus.cols, name) {
				st.focus.cols = append(append([]string(nil), st.focus.cols...), name)
			}
		}
	case "drop":
		st.focus.cols = dropFromList(st.focus.cols, commaList(stmt, parts))
	case "groupby":
		// groupby <keys> <agg>...
		args := script.SplitTopLevel(strings.TrimSpace(strings.TrimPrefix(stmt, parts[0])), ' ')
		if len(args) < 1 {
			return
		}
		out := commaList(stmt, []string{"", args[0]})
		for _, spec := range args[1:] {
			out = append(out, aggSpecAlias(spec))
		}
		st.focus.cols = out
		st.focus.rows = rowsUnknown
	case "group_by_dynamic":
		// group_by_dynamic TIME every DUR [opt VAL]... AGG...: the by
		// keys come first, then the window start, then the aggregates.
		args := script.SplitTopLevel(strings.TrimSpace(strings.TrimPrefix(stmt, parts[0])), ' ')
		if len(args) < 1 {
			return
		}
		var keys, aggs []string
		i := 1
		for ; i+1 < len(args); i += 2 {
			opt := strings.ToLower(args[i])
			if !slices.Contains([]string{"every", "period", "offset", "by", "closed", "label", "start_by"}, opt) {
				break
			}
			if opt == "by" {
				keys = commaList(stmt, []string{"", args[i+1]})
			}
		}
		for _, spec := range args[i:] {
			aggs = append(aggs, aggSpecAlias(spec))
		}
		st.focus.cols = append(append(keys, args[0]), aggs...)
		st.focus.rows = rowsUnknown
	case "join_asof":
		// join_asof NAME on KEY [by COLS] ...: every left row stays; the
		// right key and by columns are dropped, collisions get _right.
		if len(parts) < 4 || !strings.EqualFold(parts[2], "on") {
			return
		}
		drop := []string{parts[3]}
		for i := 4; i+1 < len(parts); i++ {
			if strings.EqualFold(parts[i], "by") {
				drop = append(drop, commaList(stmt, []string{"", parts[i+1]})...)
			}
		}
		right := joinTargetShape(st, dir, parts[1])
		cols := append([]string(nil), st.focus.cols...)
		for _, c := range right.cols {
			switch {
			case slices.Contains(drop, c):
			case slices.Contains(st.focus.cols, c):
				cols = append(cols, c+"_right")
			default:
				cols = append(cols, c)
			}
		}
		st.focus.cols = cols
	case "to_dummies":
		// The indicator columns depend on the data.
		st.focus.cols = nil
	case "join":
		if len(parts) < 4 || !strings.EqualFold(parts[2], "on") {
			return
		}
		target := parts[1]
		key := parts[3]
		how := "inner"
		if len(parts) >= 5 {
			how = strings.ToLower(parts[4])
		}
		right := joinTargetShape(st, dir, target)
		leftRows := st.focus.rows
		st.focus.cols = joinedCols(st.focus.cols, right.cols, key, how)
		switch how {
		case "left":
			// Left join preserves left rows.
			st.focus.rows = leftRows
		case "cross":
			if leftRows >= 0 && right.rows >= 0 {
				st.focus.rows = leftRows * right.rows
			} else {
				st.focus.rows = rowsUnknown
			}
		default: // inner
			st.focus.rows = rowsUnknown
		}
	case "unnest":
		// Replaces the named column with its struct fields. We don't
		// know the field count without type info, so widen to unknown.
		st.focus.cols = nil
	case "explode":
		// Row count grows to an unknown upper bound; schema unchanged.
		st.focus.rows = rowsUnknown
	case "upsample":
		// Row count becomes an unknown upper bound; schema unchanged.
		st.focus.rows = rowsUnknown
	default:
		// Everything else either prints, persists, or keeps the shape
		// (head, tail, show, sort, reverse, shuffle, cast, fills, ...).
	}
}

// canonicalCommand maps a typed command (with or without the leading
// dot, possibly an alias) to its canonical spec name.
func canonicalCommand(tok string) string {
	tok = strings.TrimPrefix(tok, ".")
	if spec := script.FindCommand(tok); spec != nil {
		return spec.Name
	}
	return tok
}

// clampRows returns the smaller positive bound between prev and n;
// -1 means unknown-so-effectively-infinity on the prev side.
func clampRows(prev, n int) int {
	if n < 0 {
		return prev
	}
	if prev < 0 {
		return n
	}
	if n < prev {
		return n
	}
	return prev
}

// commaList normalises a whitespace-or-comma separated column list
// that may span multiple args, e.g. "a, b , c" or "a,b,c" or
// "a b c". parts[0] is assumed to be the command token.
func commaList(_ string, parts []string) []string {
	if len(parts) < 2 {
		return nil
	}
	joined := strings.Join(parts[1:], " ")
	raw := strings.Split(joined, ",")
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		for tok := range strings.FieldsSeq(r) {
			tok = strings.TrimSpace(tok)
			if tok != "" {
				out = append(out, tok)
			}
		}
	}
	return out
}

// selectColumns returns the output names of a select statement: the
// plain column list, or for expression items the `name =` label or
// the expression's output name.
func selectColumns(stmt string, parts []string) []string {
	rest := strings.TrimSpace(strings.TrimPrefix(stmt, parts[0]))
	if !strings.ContainsAny(rest, "(=\"'") {
		return commaList(stmt, parts)
	}
	var out []string
	for _, item := range script.SplitTopLevel(rest, ',') {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if name, _, found := strings.Cut(item, "="); found && !strings.ContainsAny(name, "(\"'!<>") &&
			!strings.HasPrefix(item[len(name):], "==") {
			out = append(out, strings.TrimSpace(name))
			continue
		}
		if e, err := exprparse.Parse(item); err == nil {
			out = append(out, expr.OutputName(e))
			continue
		}
		out = append(out, item)
	}
	return out
}

// dropFromList removes each name in drop from cols, preserving
// order. Case-sensitive to match golars' column-name semantics.
func dropFromList(cols, drop []string) []string {
	if len(drop) == 0 {
		return cols
	}
	dropSet := make(map[string]struct{}, len(drop))
	for _, d := range drop {
		dropSet[d] = struct{}{}
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, skip := dropSet[c]; skip {
			continue
		}
		out = append(out, c)
	}
	return out
}

// aggSpecAlias extracts the output column name from a groupby agg
// spec "col:op[:alias]". Falls back to "col" when no alias is
// given (matches cmd/golars' dispatcher).
func aggSpecAlias(spec string) string {
	if name, _, found := strings.Cut(spec, "="); found && !strings.Contains(name, "(") {
		return strings.TrimSpace(name)
	}
	pp := strings.Split(spec, ":")
	switch len(pp) {
	case 0:
		return ""
	case 1:
		return pp[0]
	case 2:
		return pp[0]
	default:
		return pp[2]
	}
}

// joinTargetShape resolves the right-hand frame by name (preferred)
// or by path, returning whatever shape info is available. Rows are
// often unknown for file-path joins because we don't read the full
// file here: the LSP's row cache handles that upstream.
func joinTargetShape(st *frameState, dir, target string) frameShape {
	if s, ok := st.staged[target]; ok {
		return s
	}
	abs := resolvePath(target, dir)
	stats := readFileStats(abs)
	return frameShape{cols: stats.cols, rows: stats.rows}
}

// joinedCols computes the output column list: left's columns, then
// right's minus the join key (for inner/left). Cross keeps every
// column of both sides.
func joinedCols(left, right []string, key, how string) []string {
	out := make([]string, 0, len(left)+len(right))
	out = append(out, left...)
	for _, rc := range right {
		if how != "cross" && rc == key {
			continue
		}
		out = append(out, rc)
	}
	return out
}

// resolvePath turns a script path into an absolute filesystem path,
// resolving relative paths against the directory of the .glr file.
// resolvePath turns a script path into an absolute filesystem path.
// Runtime resolution in `golars run` uses CWD, but the LSP has no
// well-defined CWD relative to the edited script. To cover the common
// cases we try, in order:
//
//  1. absolute path as given
//  2. docDir/<path>            (script sits next to its data)
//  3. walk up docDir ancestors (script is in a subdir of the repo
//     and the data sits at the repo root: "examples/script/foo.glr"
//     referencing "examples/script/foo.csv")
//  4. $PWD/<path>              (user launched nvim from the project root)
//
// First path that exists on disk wins; if none exist, return the
// doc-dir candidate so downstream error messages still point at a
// sensible location.
func resolvePath(scriptPath, docDir string) string {
	if filepath.IsAbs(scriptPath) {
		return scriptPath
	}
	var candidates []string
	if docDir != "" {
		dir := docDir
		for {
			candidates = append(candidates, filepath.Join(dir, scriptPath))
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, scriptPath))
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	if len(candidates) > 0 {
		return candidates[0]
	}
	abs, _ := filepath.Abs(scriptPath)
	return abs
}
