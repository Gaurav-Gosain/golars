package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/Gaurav-Gosain/golars/script"
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
	if err != nil {
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

// countCSVRows returns the number of data rows in a CSV/TSV file
// (excluding the header). Returns -1 on any error. We scan line-by-
// line rather than parsing records because lsp-side counts don't
// have to be perfect on multi-line-quoted values: the inlay hint
// is an approximation.
func countCSVRows(absPath string) int {
	f, err := os.Open(absPath)
	if err != nil {
		return -1
	}
	defer f.Close()
	buf := make([]byte, 64*1024)
	lines := 0
	for {
		n, err := f.Read(buf)
		for _, b := range buf[:n] {
			if b == '\n' {
				lines++
			}
		}
		if err != nil {
			break
		}
	}
	// Header row → subtract one. Guard against empty file.
	if lines <= 0 {
		return 0
	}
	return lines - 1
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
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	if isTSV {
		r.Comma = '\t'
	}
	row, err := r.Read()
	if err != nil {
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
	cmd := strings.TrimPrefix(parts[0], ".")
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
	case "limit", "head", "tail":
		if len(parts) >= 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				st.focus.rows = clampRows(st.focus.rows, n)
			}
		} else if cmd != "limit" {
			// head/tail default to 10.
			st.focus.rows = clampRows(st.focus.rows, 10)
		}
	case "select":
		st.focus.cols = commaList(stmt, parts)
	case "with":
		// `with NAME = EXPR` appends a single column. The new name
		// is the first token after `with`, up to the `=`. Row count
		// stays constant; the schema grows by one.
		if len(parts) >= 2 {
			st.focus.cols = append(append([]string(nil), st.focus.cols...), parts[1])
		}
	case "drop":
		st.focus.cols = dropFromList(st.focus.cols, commaList(stmt, parts))
	case "groupby":
		// groupby <keys> <agg>...
		if len(parts) < 2 {
			return
		}
		keys := commaList(stmt, parts[:2])
		var out []string
		out = append(out, keys...)
		for _, spec := range parts[2:] {
			out = append(out, aggSpecAlias(spec))
		}
		st.focus.cols = out
		st.focus.rows = rowsUnknown
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
	case "collect", "reset", "show", "schema", "describe", "frames",
		"info", "timing", "clear", "help", "explain", "explain_tree",
		"tree", "graph", "show_graph", "mermaid", "save", "exit",
		"quit", "source":
		// No shape change: these either print or persist the current
		// frame, or are session-level metadata commands.
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
	}
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
