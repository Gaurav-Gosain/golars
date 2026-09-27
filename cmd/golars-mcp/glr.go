package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/analysis"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// glrTools expose the glr scripting language: run a script and get
// its result table, check or format a script without running it, and
// look up the language reference.
func glrTools() []Tool {
	scriptProp := map[string]any{"type": "string", "description": "glr script text: one statement per line, as in `load data.csv` / `filter x > 1` / `groupby k amount:sum`."}
	dirProp := map[string]any{"type": "string", "description": "Directory that relative paths in the script resolve against (default: the server's working directory)."}
	return []Tool{
		{
			Name: "run_glr",
			Description: "Run a glr script with golars and return its printed output and the focused frame as a table " +
				"(first max_rows rows, every column, with dtypes). The script is checked first; problems are returned " +
				"with the line, column and a fix hint instead of running it. Call glr_reference for the language.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"script":   scriptProp,
					"dir":      dirProp,
					"max_rows": map[string]any{"type": "integer", "minimum": 1, "default": 50, "description": "Rows of the result table to return."},
				},
				"required": []string{"script"},
			},
			Run: runGlr,
		},
		{
			Name: "check_glr",
			Description: "Check a glr script without running it: syntax, unknown columns (tracked through the pipeline from " +
				"the data files' schemas), bad function calls and dtype errors. Returns each finding with line, column, " +
				"message, hint and suggested replacement, plus the schema after every statement.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"script": scriptProp, "dir": dirProp},
				"required":   []string{"script"},
			},
			Run: runCheckGlr,
		},
		{
			Name:        "format_glr",
			Description: "Return a glr script in canonical form (what `golars fmt` prints).",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"script": scriptProp},
				"required":   []string{"script"},
			},
			Run: runFormatGlr,
		},
		{
			Name: "glr_reference",
			Description: "Look up the glr language. With `name`, document a command (load, filter, groupby, ...) or an " +
				"expression function (round, str.to_date, dt.year, cut, ...). With `namespace` (str, dt, list, arr, " +
				"struct, name, bin, cat, or free), list its functions. With neither, list every command.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name":      map[string]any{"type": "string", "description": "Command or function name, as in groupby or str.to_date."},
					"namespace": map[string]any{"type": "string", "description": "Namespace whose functions to list."},
				},
			},
			Run: runGlrReference,
		},
	}
}

func optString(args json.RawMessage, key string) string {
	var m map[string]any
	if json.Unmarshal(args, &m) != nil {
		return ""
	}
	s, _ := m[key].(string)
	return s
}

func scriptDir(args json.RawMessage) (string, error) {
	dir := optString(args, "dir")
	if dir == "" {
		return os.Getwd()
	}
	st, err := os.Stat(dir)
	if err != nil || !st.IsDir() {
		return "", fmt.Errorf("dir %q is not a directory", dir)
	}
	return filepath.Abs(dir)
}

type finding struct {
	Line     int    `json:"line"`
	Column   int    `json:"column"`
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Message  string `json:"message"`
	Hint     string `json:"hint,omitempty"`
	Fix      string `json:"fix,omitempty"`
}

func findings(r *analysis.Result) []finding {
	out := []finding{}
	for _, d := range r.Diags {
		f := finding{Line: d.Line + 1, Column: d.Col + 1, Severity: d.Severity.String(), Code: d.Code, Message: d.Msg, Hint: d.Hint}
		if d.Fix != nil {
			f.Fix = d.Fix.Text
		}
		out = append(out, f)
	}
	return out
}

func runCheckGlr(args json.RawMessage) (any, error) {
	src, err := asString(args, "script")
	if err != nil {
		return nil, err
	}
	dir, err := scriptDir(args)
	if err != nil {
		return nil, err
	}
	r := analysis.Analyze(src, analysis.Options{Dir: dir})
	type stepInfo struct {
		Line      int      `json:"line"`
		Statement string   `json:"statement"`
		Shape     string   `json:"shape"`
		Columns   []string `json:"columns,omitempty"`
	}
	var steps []stepInfo
	for _, s := range r.Steps {
		if !s.Changes {
			continue
		}
		var cols []string
		if s.After.Known() {
			for _, f := range s.After.Schema.Fields() {
				cols = append(cols, f.Name+": "+f.DType.String())
			}
		}
		steps = append(steps, stepInfo{Line: s.Stmt.Line + 1, Statement: strings.TrimSpace(s.Stmt.Text), Shape: s.After.Shape(), Columns: cols})
	}
	fs := findings(r)
	text := "no problems found"
	if len(fs) > 0 {
		text = strings.TrimRight(r.Render("script.glr"), "\n")
	}
	return structuredResult(map[string]any{"ok": len(fs) == 0, "findings": fs, "steps": steps}, text), nil
}

func runFormatGlr(args json.RawMessage) (any, error) {
	src, err := asString(args, "script")
	if err != nil {
		return nil, err
	}
	out := syntax.Format(src)
	return structuredResult(map[string]any{"script": out, "changed": out != src}, verbatim(out)), nil
}

func runGlrReference(args json.RawMessage) (any, error) {
	name := strings.TrimSpace(optString(args, "name"))
	ns := strings.TrimSpace(optString(args, "namespace"))
	switch {
	case name != "":
		if spec := script.FindCommand(name); spec != nil {
			return textContentResult(spec.Markdown()), nil
		}
		fns, fn := "", name
		if a, b, found := strings.Cut(name, "."); found {
			fns, fn = a, b
		}
		if info, found := exprparse.Lookup(fns, fn); found {
			return textContentResult(info.Markdown()), nil
		}
		msg := fmt.Sprintf("no command or function named %q", name)
		all := append(script.Names(), exprparse.Functions()["free"]...)
		all = append(all, exprparse.Functions()[""]...)
		if best := script.Closest(name, all); best != "" {
			msg += fmt.Sprintf("; did you mean %q?", best)
		}
		return nil, errors.New(msg)
	case ns != "":
		names, found := exprparse.Functions()[ns]
		if !found {
			return nil, fmt.Errorf("unknown namespace %q: want free, str, dt, list, arr, struct, name, bin or cat", ns)
		}
		lookupNS := ns
		if ns == "free" {
			lookupNS = ""
		}
		var b strings.Builder
		for _, n := range names {
			if info, found := exprparse.Lookup(lookupNS, n); found {
				b.WriteString(info.Label())
				if line, _, _ := strings.Cut(info.Doc, "\n"); line != "" {
					b.WriteString("  # " + line)
				}
				b.WriteByte('\n')
			}
		}
		return textContentResult(b.String()), nil
	}
	var b strings.Builder
	for _, cat := range script.Categories {
		fmt.Fprintf(&b, "## %s\n", cat)
		for _, c := range script.Commands {
			if c.Category == cat {
				fmt.Fprintf(&b, "%s  # %s\n", c.Signature, c.Summary)
			}
		}
	}
	b.WriteString("\nExpressions (filter, with, select, groupby name=expr) call any golars expression function by its " +
		"snake_case name: round(x, 2) or x.round(2), dt.year(ts) or ts.dt.year(). Use namespace=... to list them.\n")
	return textContentResult(b.String()), nil
}

func textContentResult(s string) any {
	return map[string]any{"content": []any{textContent(verbatim(s))}}
}

// hostResponse is the part of a kernel-host reply run_glr reads.
type hostResponse struct {
	Text  string          `json:"text"`
	Error string          `json:"error"`
	Table json.RawMessage `json:"table"`
	Shape *[2]int         `json:"shape"`
}

var ansiRE = regexp.MustCompile("\x1b\\[[0-9;?]*[A-Za-z]")

func runGlr(args json.RawMessage) (any, error) {
	src, err := asString(args, "script")
	if err != nil {
		return nil, err
	}
	dir, err := scriptDir(args)
	if err != nil {
		return nil, err
	}
	maxRows, err := asInt(args, "max_rows", 50)
	if err != nil {
		return nil, err
	}
	// Check first: a static error is reported with its position and
	// nothing runs.
	r := analysis.Analyze(src, analysis.Options{Dir: dir})
	var errs []finding
	for _, f := range findings(r) {
		if f.Severity == "error" {
			errs = append(errs, f)
		}
	}
	if len(errs) > 0 {
		return map[string]any{
			"isError":           true,
			"content":           []any{textContent(strings.TrimRight(r.Render("script.glr"), "\n"))},
			"structuredContent": map[string]any{"ok": false, "findings": errs},
		}, nil
	}
	bin, err := golarsBin()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "kernel-host")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", bin, err)
	}
	defer func() {
		stdin.Close()
		_ = cmd.Wait()
	}()
	rd := bufio.NewReaderSize(stdout, 1<<20)
	call := func(req map[string]any) (hostResponse, error) {
		line, _ := json.Marshal(req)
		if _, err := stdin.Write(append(line, '\n')); err != nil {
			return hostResponse{}, err
		}
		out, err := rd.ReadBytes('\n')
		if err != nil {
			return hostResponse{}, fmt.Errorf("golars kernel-host: %w", err)
		}
		var resp hostResponse
		err = json.Unmarshal(out, &resp)
		return resp, err
	}
	run, err := call(map[string]any{"id": "script", "code": src})
	if err != nil {
		return nil, err
	}
	output := strings.TrimRight(ansiRE.ReplaceAllString(run.Text, ""), "\n")
	if run.Error != "" {
		msg := ansiRE.ReplaceAllString(run.Error, "")
		return map[string]any{
			"isError":           true,
			"content":           []any{textContent(strings.TrimSpace(output + "\n\nerror: " + msg))},
			"structuredContent": map[string]any{"ok": false, "output": output, "error": msg},
		}, nil
	}
	structured := map[string]any{"ok": true, "output": output}
	text := output
	if tbl, err := call(map[string]any{"id": "table", "op": "table", "max_rows": maxRows}); err == nil && tbl.Error == "" && len(tbl.Table) > 0 {
		var table struct {
			Columns []string    `json:"columns"`
			Dtypes  []string    `json:"dtypes"`
			Rows    [][]*string `json:"rows"`
		}
		if json.Unmarshal(tbl.Table, &table) == nil {
			structured["table"] = table
			if tbl.Shape != nil && tbl.Shape[0] >= maxRows {
				structured["truncated"] = true
			}
			text += "\n\nresult (" + fmt.Sprint(len(table.Rows)) + " rows shown):\n" + tableCSV(table.Columns, table.Dtypes, table.Rows)
		}
	}
	return structuredResult(structured, strings.TrimSpace(text)), nil
}

// tableCSV renders a result table as CSV with a dtype comment line.
func tableCSV(cols, dtypes []string, rows [][]*string) string {
	var b strings.Builder
	b.WriteString("# " + strings.Join(dtypes, ",") + "\n")
	b.WriteString(strings.Join(cols, ",") + "\n")
	for _, r := range rows {
		cells := make([]string, len(r))
		for i, c := range r {
			if c == nil {
				continue
			}
			v := *c
			if strings.ContainsAny(v, ",\"\n") {
				v = `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
			}
			cells[i] = v
		}
		b.WriteString(strings.Join(cells, ",") + "\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// golarsBin finds the golars binary: $GOLARS_BIN, next to this
// binary, then $PATH.
func golarsBin() (string, error) {
	if env := os.Getenv("GOLARS_BIN"); env != "" {
		if _, err := os.Stat(env); err == nil {
			return env, nil
		}
	}
	if exe, err := os.Executable(); err == nil {
		cand := filepath.Join(filepath.Dir(exe), "golars")
		if _, err := os.Stat(cand); err == nil {
			return cand, nil
		}
	}
	p, err := exec.LookPath("golars")
	if err != nil {
		return "", errors.New("run_glr needs the golars binary: install it or set GOLARS_BIN")
	}
	return p, nil
}
