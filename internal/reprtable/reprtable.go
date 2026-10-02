// Package reprtable is the display-ready form of a DataFrame or Series
// that notebook front ends consume: column names, dtypes and cell strings
// already formatted, plus the shape and where rows or columns were
// elided. It renders the HTML used by jupyter/render and the kernel, and
// encodes as JSON under MIME so terminal front ends (for example
// gopyter) can draw their own themed table from the same data.
//
// The package depends only on the standard library so both dataframe and
// series can build tables without an import cycle.
package reprtable

import (
	"encoding/json"
	"fmt"
	"html"
	"strings"
)

// MIME is the media type of a Table encoded as JSON in a mime bundle.
const MIME = "application/vnd.golars.table+json"

// Table is a formatted window of a frame.
type Table struct {
	// Kind is "series" for a Series, empty for a DataFrame.
	Kind    string   `json:"kind,omitempty"`
	Columns []string `json:"columns"`
	Dtypes  []string `json:"dtypes"`
	// Rows holds the shown rows; a nil cell is a null.
	Rows [][]*string `json:"rows"`
	// Shape is the (height, width) of the whole frame.
	Shape [2]int `json:"shape"`
	// RowGap, when set, is the index in Rows where rows were left out:
	// Rows[:RowGap] are the first rows and Rows[RowGap:] the last ones.
	RowGap *int `json:"row_gap,omitempty"`
	// ColGap is the same for columns.
	ColGap *int `json:"col_gap,omitempty"`
}

// Pick chooses which of n items to show when at most limit fit: the first
// half and the last half. gap is the index in idx where items were left
// out, or -1 when all are shown. A negative limit shows everything.
func Pick(n, limit int) (idx []int, gap int) {
	if limit < 0 || n <= limit {
		idx = make([]int, n)
		for i := range idx {
			idx[i] = i
		}
		return idx, -1
	}
	head := limit / 2
	idx = make([]int, 0, limit)
	for i := range head {
		idx = append(idx, i)
	}
	for i := n - (limit - head); i < n; i++ {
		idx = append(idx, i)
	}
	return idx, head
}

// SetGaps records the row and column gaps returned by Pick.
func (t *Table) SetGaps(rowGap, colGap int) {
	if rowGap >= 0 {
		t.RowGap = &rowGap
	}
	if colGap >= 0 {
		t.ColGap = &colGap
	}
}

// JSON encodes the table for a mime bundle.
func (t *Table) JSON() string {
	b, err := json.Marshal(t)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// cellBorder is the rgba border every <th>/<td> uses. currentColor would
// track the text colour but breaks on themes that paint dark text on dark
// backgrounds; a fixed translucent grey reads on light and dark pages.
const cellBorder = "border:1px solid rgba(128,128,128,0.35)"

// HTML renders the table as a self-contained fragment with inline styles,
// in the polars-py look: a shape caption, a dtype subheader and "…" where
// rows or columns were left out.
func (t *Table) HTML() string {
	h, w := t.Shape[0], t.Shape[1]
	shape := fmt.Sprintf("(%d, %d)", h, w)
	if t.Kind == "series" {
		shape = fmt.Sprintf("(%d,)", h)
	}
	if len(t.Columns) == 0 || h == 0 && t.Kind != "series" {
		return fmt.Sprintf(`<div class="golars-df"><small style="opacity:0.6">shape: %s - empty</small></div>`, shape)
	}
	colGap, rowGap := -1, -1
	if t.ColGap != nil {
		colGap = *t.ColGap
	}
	if t.RowGap != nil {
		rowGap = *t.RowGap
	}

	var b strings.Builder
	b.WriteString(`<div class="golars-df" style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px;color:inherit">`)
	fmt.Fprintf(&b, `<small style="opacity:0.6">shape: %s</small>`, shape)
	b.WriteString(`<table style="border-collapse:collapse;margin-top:4px;border:1px solid;border-color:currentColor;border-color:rgba(128,128,128,0.4)">`)
	b.WriteString(`<thead><tr>`)
	eachCol := func(cells []string, cell func(string) string) {
		for i, s := range cells {
			if i == colGap {
				b.WriteString(cell("…"))
			}
			b.WriteString(cell(s))
		}
	}
	eachCol(t.Columns, thHTML)
	b.WriteString(`</tr><tr>`)
	eachCol(t.Dtypes, dtypeHTML)
	b.WriteString(`</tr></thead><tbody>`)
	shown := len(t.Columns)
	if colGap >= 0 {
		shown++
	}
	gapRow := func() {
		b.WriteString(`<tr>`)
		for range shown {
			b.WriteString(tdHTML("…", false))
		}
		b.WriteString(`</tr>`)
	}
	for ri, row := range t.Rows {
		if ri == rowGap {
			gapRow()
		}
		b.WriteString(`<tr>`)
		for ci, cell := range row {
			if ci == colGap {
				b.WriteString(tdHTML("…", false))
			}
			if cell == nil {
				b.WriteString(tdHTML("null", true))
			} else {
				b.WriteString(tdHTML(*cell, false))
			}
		}
		b.WriteString(`</tr>`)
	}
	if rowGap == len(t.Rows) {
		gapRow()
	}
	b.WriteString(`</tbody></table>`)
	if colGap >= 0 {
		fmt.Fprintf(&b, `<small style="opacity:0.6">(showing %d of %d columns)</small>`, len(t.Columns), w)
	}
	b.WriteString(`</div>`)
	return b.String()
}

func thHTML(s string) string {
	return fmt.Sprintf(`<th style="%s;padding:2px 6px;text-align:left;font-weight:600">%s</th>`,
		cellBorder, html.EscapeString(s))
}

func dtypeHTML(s string) string {
	return fmt.Sprintf(`<th style="%s;padding:2px 6px;opacity:0.6;text-align:left;font-weight:400;font-size:11px">%s</th>`,
		cellBorder, html.EscapeString(s))
}

func tdHTML(s string, isNull bool) string {
	style := cellBorder + ";padding:2px 6px"
	if isNull {
		style += ";opacity:0.45;font-style:italic"
	}
	return fmt.Sprintf(`<td style="%s">%s</td>`, style, html.EscapeString(s))
}
