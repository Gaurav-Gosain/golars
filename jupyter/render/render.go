// Package render produces multi-mimetype representations of golars
// values for Jupyter and other notebook frontends.
//
// HTML mirrors the polars-py table style: a wrapped <table> with a
// shape caption, dtype subheader, and zebra rows. Markdown is GFM
// pipe-tables. Plain text reuses dataframe.DataFrame.Format() so it
// stays bit-identical to the REPL.
//
// Use directly with gonb:
//
//	import "github.com/janpfeifer/gonb/gonbui"
//	import jrender "github.com/Gaurav-Gosain/golars/jupyter/render"
//
//	gonbui.DisplayHTML(jrender.HTML(df))
//
// or via the bundled custom kernel (cmd/golars-kernel) which renders
// every materialised frame as text/plain + text/html automatically.
package render

import (
	"fmt"
	"html"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// Limits caps the rendered window so a 10M-row frame doesn't blow up
// the notebook. Defaults match dataframe.DefaultFormatOptions: head 5
// + tail 5, max 8 columns. Set a field to -1 to disable that limit.
type Limits struct {
	MaxRows     int
	MaxCols     int
	MaxCellRune int
}

// DefaultLimits matches polars-py / dataframe.Format defaults.
func DefaultLimits() Limits {
	return Limits{MaxRows: 10, MaxCols: 8, MaxCellRune: 64}
}

// HTML renders df as a self-contained HTML fragment. No external CSS
// dependency: all styling is inline so the table renders identically
// in JupyterLab, classic Notebook, nbviewer, and Quarto exports.
func HTML(df *dataframe.DataFrame) string { return HTMLWith(df, DefaultLimits()) }

// HTMLWith is HTML with caller-supplied limits.
func HTMLWith(df *dataframe.DataFrame, lim Limits) string {
	if df == nil {
		return `<pre>&lt;nil dataframe&gt;</pre>`
	}
	h, w := df.Shape()
	if w == 0 || h == 0 {
		return fmt.Sprintf(`<div class="golars-df"><small>shape: (%d, %d) — empty</small></div>`, h, w)
	}
	colIdx, colEllipsis := pickCols(w, lim.MaxCols)
	rowIdx, rowEllipsisAt := pickRows(h, lim.MaxRows)

	var b strings.Builder
	b.WriteString(`<div class="golars-df" style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px">`)
	fmt.Fprintf(&b, `<small style="color:#888">shape: (%d, %d)</small>`, h, w)
	b.WriteString(`<table style="border-collapse:collapse;border:1px solid #ddd;margin-top:4px">`)

	// Header row: column names.
	b.WriteString(`<thead><tr>`)
	for _, ci := range colIdx {
		if ci < 0 {
			b.WriteString(thHTML("…"))
			continue
		}
		b.WriteString(thHTML(df.ColumnAt(ci).Name()))
	}
	b.WriteString(`</tr><tr>`)
	// Subheader row: dtypes (dimmer).
	for _, ci := range colIdx {
		if ci < 0 {
			b.WriteString(dtypeHTML("…"))
			continue
		}
		b.WriteString(dtypeHTML(df.ColumnAt(ci).DType().String()))
	}
	b.WriteString(`</tr></thead><tbody>`)

	for ri, rowPos := range rowIdx {
		b.WriteString(`<tr>`)
		for _, ci := range colIdx {
			if ci < 0 || ri == rowEllipsisAt {
				b.WriteString(tdHTML("…", false))
				continue
			}
			cell, isNull := cellString(df.ColumnAt(ci), rowPos, lim.MaxCellRune)
			b.WriteString(tdHTML(cell, isNull))
		}
		b.WriteString(`</tr>`)
	}

	b.WriteString(`</tbody></table>`)
	if colEllipsis {
		fmt.Fprintf(&b, `<small style="color:#888">(showing %d of %d columns)</small>`, len(colIdx), w)
	}
	b.WriteString(`</div>`)
	return b.String()
}

// Markdown renders df as a GFM pipe-table. Useful when the consumer
// only renders markdown (e.g. some chat UIs) or when copy-pasting into
// a README.
func Markdown(df *dataframe.DataFrame) string { return MarkdownWith(df, DefaultLimits()) }

// MarkdownWith is Markdown with caller-supplied limits.
func MarkdownWith(df *dataframe.DataFrame, lim Limits) string {
	if df == nil {
		return "_<nil dataframe>_"
	}
	h, w := df.Shape()
	if w == 0 || h == 0 {
		return fmt.Sprintf("_shape: (%d, %d) — empty_", h, w)
	}
	colIdx, _ := pickCols(w, lim.MaxCols)
	rowIdx, rowEllipsisAt := pickRows(h, lim.MaxRows)

	var b strings.Builder
	fmt.Fprintf(&b, "shape: (%d, %d)\n\n", h, w)
	// Header.
	b.WriteString("|")
	for _, ci := range colIdx {
		name := "…"
		if ci >= 0 {
			name = df.ColumnAt(ci).Name() + " (" + df.ColumnAt(ci).DType().String() + ")"
		}
		fmt.Fprintf(&b, " %s |", mdEscape(name))
	}
	b.WriteString("\n|")
	for range colIdx {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	for ri, rowPos := range rowIdx {
		b.WriteString("|")
		for _, ci := range colIdx {
			if ci < 0 || ri == rowEllipsisAt {
				b.WriteString(" … |")
				continue
			}
			cell, _ := cellString(df.ColumnAt(ci), rowPos, lim.MaxCellRune)
			fmt.Fprintf(&b, " %s |", mdEscape(cell))
		}
		b.WriteString("\n")
	}
	return b.String()
}

// Text returns the existing ASCII repr - identical to fmt.Println(df).
// Lifted into this package so callers reach for one rendering API.
func Text(df *dataframe.DataFrame) string {
	if df == nil {
		return "<nil dataframe>"
	}
	return df.String()
}

// MimeBundle returns the multi-mimetype dict Jupyter clients consume.
// Always includes text/plain; HTML and markdown go alongside so any
// frontend can pick its richest supported format.
func MimeBundle(df *dataframe.DataFrame) map[string]string {
	return map[string]string{
		"text/plain":    Text(df),
		"text/html":     HTML(df),
		"text/markdown": Markdown(df),
	}
}

// --- helpers ----------------------------------------------------

func thHTML(s string) string {
	return fmt.Sprintf(
		`<th style="border:1px solid #ddd;padding:2px 6px;background:#f5f5f5;text-align:left;font-weight:600">%s</th>`,
		html.EscapeString(s),
	)
}

func dtypeHTML(s string) string {
	return fmt.Sprintf(
		`<th style="border:1px solid #ddd;padding:2px 6px;background:#fafafa;color:#888;text-align:left;font-weight:400;font-size:11px">%s</th>`,
		html.EscapeString(s),
	)
}

func tdHTML(s string, isNull bool) string {
	style := "border:1px solid #ddd;padding:2px 6px"
	if isNull {
		style += ";color:#bbb;font-style:italic"
	}
	return fmt.Sprintf(`<td style="%s">%s</td>`, style, html.EscapeString(s))
}

// mdEscape escapes pipes + newlines so a cell value can't break the
// GFM table grid.
func mdEscape(s string) string {
	s = strings.ReplaceAll(s, "|", `\|`)
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	return s
}

// cellString reads a single value out of a series chunk, returning
// "null" + isNull=true for missing values. Mirrors dataframe.renderCell
// without exporting that internal helper.
func cellString(s *series.Series, i int, maxRune int) (string, bool) {
	if s == nil || s.Len() == 0 {
		return "", false
	}
	arr := s.Chunk(0)
	if arr == nil {
		return "", false
	}
	if n, ok := arr.(interface{ IsNull(int) bool }); ok && n.IsNull(i) {
		return "null", true
	}
	v := "<?>"
	if vs, ok := arr.(interface{ ValueStr(int) string }); ok {
		v = vs.ValueStr(i)
	}
	if maxRune > 0 && runeCount(v) > maxRune {
		v = truncate(v, maxRune)
	}
	return v, false
}

func runeCount(s string) int { return len([]rune(s)) }

func truncate(s string, n int) string {
	if n < 1 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

// pickCols mirrors dataframe.selectColumns: keep the first half + last
// half if w > maxCols, with a -1 sentinel for the ellipsis column.
func pickCols(w, maxCols int) (idx []int, hasEllipsis bool) {
	if maxCols < 0 || w <= maxCols {
		out := make([]int, w)
		for i := range out {
			out[i] = i
		}
		return out, false
	}
	half := maxCols / 2
	out := make([]int, 0, maxCols+1)
	for i := range half {
		out = append(out, i)
	}
	out = append(out, -1)
	for i := w - (maxCols - half); i < w; i++ {
		out = append(out, i)
	}
	return out, true
}

// pickRows: head 5 + tail 5 with an ellipsis row marker for taller frames.
func pickRows(h, maxRows int) (idx []int, ellipsisAt int) {
	if maxRows < 0 || h <= maxRows {
		out := make([]int, h)
		for i := range out {
			out[i] = i
		}
		return out, -1
	}
	half := maxRows / 2
	out := make([]int, 0, maxRows+1)
	for i := range half {
		out = append(out, i)
	}
	ellipsisAt = len(out)
	out = append(out, -1) // sentinel row pos
	for i := h - (maxRows - half); i < h; i++ {
		out = append(out, i)
	}
	return out, ellipsisAt
}
