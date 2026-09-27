package dataframe

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Default display bounds. These match polars' defaults closely enough for
// casual inspection; advanced users can override via Format/FormatOptions.
const (
	defaultMaxRows     = 10 // head 5 / tail 5, else full
	defaultMaxCols     = 8
	defaultMaxCellRune = 32
)

// FormatOptions controls DataFrame pretty-printing.
type FormatOptions struct {
	MaxRows     int // -1 = no row limit
	MaxCols     int // -1 = no column limit
	MaxCellRune int // -1 = no per-cell truncation
}

// DefaultFormatOptions returns the default display settings used by
// DataFrame.String(). Mirrors polars' default of showing head+tail around
// an ellipsis for frames taller than 10 rows.
func DefaultFormatOptions() FormatOptions {
	return FormatOptions{
		MaxRows:     defaultMaxRows,
		MaxCols:     defaultMaxCols,
		MaxCellRune: defaultMaxCellRune,
	}
}

// Format renders the DataFrame as a box-drawn table, close to polars'
// repr. Useful in tests and REPL sessions.
func (df *DataFrame) Format(opts FormatOptions) string {
	if opts.MaxRows <= 0 && opts.MaxRows != -1 {
		opts.MaxRows = defaultMaxRows
	}
	if opts.MaxCols <= 0 && opts.MaxCols != -1 {
		opts.MaxCols = defaultMaxCols
	}
	if opts.MaxCellRune <= 0 && opts.MaxCellRune != -1 {
		opts.MaxCellRune = defaultMaxCellRune
	}

	h := df.Height()
	w := df.Width()
	if w == 0 || h == 0 {
		return fmt.Sprintf("shape: (%d, %d)\n<empty dataframe>", h, w)
	}

	// Decide which columns to show.
	colIdx := selectColumns(w, opts.MaxCols)
	colEllipsis := len(colIdx) < w

	// Decide which rows to show.
	rowIdx, rowEllipsisAt := selectRows(h, opts.MaxRows)

	// Build header + dtype rows + body cells.
	names := make([]string, 0, len(colIdx))
	dtypes := make([]string, 0, len(colIdx))
	for _, ci := range colIdx {
		if ci < 0 {
			names = append(names, "…")
			dtypes = append(dtypes, "…")
			continue
		}
		s := df.ColumnAt(ci)
		names = append(names, s.Name())
		dtypes = append(dtypes, s.DType().String())
	}

	body := make([][]string, 0, len(rowIdx))
	for ri := range rowIdx {
		row := make([]string, 0, len(colIdx))
		for _, ci := range colIdx {
			if ci < 0 {
				row = append(row, "…")
				continue
			}
			if ri == rowEllipsisAt {
				row = append(row, "…")
				continue
			}
			s := df.ColumnAt(ci)
			row = append(row, renderCell(s.Chunk(0), rowIdx[ri], opts.MaxCellRune))
		}
		body = append(body, row)
	}

	// Column widths: max over header / dtype / all cells, at least 3.
	widths := make([]int, len(colIdx))
	for c := range colIdx {
		widths[c] = max3(runeLen(names[c]), runeLen(dtypes[c]), 3)
		for r := range body {
			widths[c] = max(widths[c], runeLen(body[r][c]))
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "shape: (%d, %d)\n", h, w)
	// Rounded-corner box: ╭ ╮ ╰ ╯ on the outside, ┬ ┴ ┼ inside, ═ ╪ for
	// the header separator. Matches polars' --table_style="rounded".
	drawBorder(&b, widths, '╭', '┬', '╮')
	drawRow(&b, widths, names)
	drawRow(&b, widths, dtypes)
	drawBorder(&b, widths, '├', '┼', '┤')
	for r := range body {
		drawRow(&b, widths, body[r])
	}
	drawBorder(&b, widths, '╰', '┴', '╯')
	if colEllipsis {
		fmt.Fprintf(&b, "(showing %d of %d columns)\n", len(colIdx)-btoi(colEllipsisHasCol(colIdx)), w)
	}
	return b.String()
}

func colEllipsisHasCol(idx []int) bool {
	for _, i := range idx {
		if i < 0 {
			return true
		}
	}
	return false
}

// selectColumns chooses which column indices to render. If w > maxCols,
// returns head/tail around a -1 sentinel that the renderer treats as "…".
func selectColumns(w, maxCols int) []int {
	if maxCols < 0 || w <= maxCols {
		out := make([]int, w)
		for i := range out {
			out[i] = i
		}
		return out
	}
	head := maxCols / 2
	tail := maxCols - head
	out := make([]int, 0, maxCols+1)
	for i := range head {
		out = append(out, i)
	}
	out = append(out, -1)
	for i := w - tail; i < w; i++ {
		out = append(out, i)
	}
	return out
}

// selectRows returns the row indices to render and the position at which
// an ellipsis row should be inserted (-1 if none).
func selectRows(h, maxRows int) ([]int, int) {
	if maxRows < 0 || h <= maxRows {
		out := make([]int, h)
		for i := range out {
			out[i] = i
		}
		return out, -1
	}
	head := maxRows / 2
	tail := maxRows - head
	out := make([]int, 0, maxRows+1)
	for i := range head {
		out = append(out, i)
	}
	out = append(out, -1) // placeholder row
	for i := h - tail; i < h; i++ {
		out = append(out, i)
	}
	return out, head
}

// renderCell formats an arrow cell for display, truncating if wider than
// maxRune runes. null → "null". Floats use %g so trailing zeros drop.
func renderCell(arr any, i int, maxRune int) string {
	type nullable interface {
		IsNull(int) bool
	}
	if a, ok := arr.(nullable); ok && a.IsNull(i) {
		return "null"
	}
	var s string
	switch a := arr.(type) {
	case *array.Int8:
		s = strconv.FormatInt(int64(a.Value(i)), 10)
	case *array.Int16:
		s = strconv.FormatInt(int64(a.Value(i)), 10)
	case *array.Int32:
		s = strconv.FormatInt(int64(a.Value(i)), 10)
	case *array.Int64:
		s = strconv.FormatInt(a.Value(i), 10)
	case *array.Uint8:
		s = strconv.FormatUint(uint64(a.Value(i)), 10)
	case *array.Uint16:
		s = strconv.FormatUint(uint64(a.Value(i)), 10)
	case *array.Uint32:
		s = strconv.FormatUint(uint64(a.Value(i)), 10)
	case *array.Uint64:
		s = strconv.FormatUint(a.Value(i), 10)
	case *array.Float32:
		s = formatFloat(float64(a.Value(i)))
	case *array.Float64:
		s = formatFloat(a.Value(i))
	case *array.Boolean:
		if a.Value(i) {
			s = "true"
		} else {
			s = "false"
		}
	case *array.String:
		s = quoteMaybe(a.Value(i))
	case *array.Dictionary:
		// Categorical / Enum: show the decoded category like a string.
		s = quoteMaybe(a.ValueStr(i))
	default:
		if v, ok := arr.(interface{ ValueStr(int) string }); ok {
			s = v.ValueStr(i)
		} else {
			s = "<?>"
		}
	}
	if maxRune > 0 && runeLen(s) > maxRune {
		s = truncateRune(s, maxRune)
	}
	return s
}

func formatFloat(f float64) string {
	if math.IsNaN(f) {
		return "NaN"
	}
	if math.IsInf(f, 1) {
		return "inf"
	}
	if math.IsInf(f, -1) {
		return "-inf"
	}
	return strconv.FormatFloat(f, 'g', 6, 64)
}

// quoteMaybe leaves simple identifiers unquoted but wraps strings with
// embedded whitespace in quotes for clarity: matches polars default repr.
func quoteMaybe(s string) string {
	// Escape tab/newline.
	needQuote := strings.ContainsAny(s, "\t\n\r")
	if needQuote {
		return strconv.Quote(s)
	}
	return s
}

func runeLen(s string) int { return utf8.RuneCountInString(s) }

func truncateRune(s string, n int) string {
	if n <= 1 {
		return "…"
	}
	r := 0
	for i := range s {
		r++
		if r == n {
			return s[:i] + "…"
		}
	}
	return s
}

func drawBorder(b *strings.Builder, widths []int, left, mid, right rune) {
	b.WriteRune(left)
	for i, w := range widths {
		writeRune(b, '─', w+2)
		if i < len(widths)-1 {
			b.WriteRune(mid)
		}
	}
	b.WriteRune(right)
	b.WriteByte('\n')
}

func drawRow(b *strings.Builder, widths []int, cells []string) {
	for i, cell := range cells {
		if i == 0 {
			b.WriteRune('│')
		}
		b.WriteByte(' ')
		b.WriteString(cell)
		pad := widths[i] - runeLen(cell)
		for range pad {
			b.WriteByte(' ')
		}
		b.WriteByte(' ')
		b.WriteRune('│')
	}
	b.WriteByte('\n')
}

func writeRune(b *strings.Builder, r rune, n int) {
	for range n {
		b.WriteRune(r)
	}
}

func max3(a, b, c int) int {
	if a > b {
		if a > c {
			return a
		}
		return c
	}
	if b > c {
		return b
	}
	return c
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
