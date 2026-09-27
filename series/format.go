package series

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// SeriesFormatOptions controls Series pretty-printing.
type SeriesFormatOptions struct {
	MaxRows     int // -1 = no limit
	MaxCellRune int // -1 = no truncation
}

// DefaultSeriesFormatOptions matches polars' default: show up to 10 rows
// with head/tail ellipsis when longer.
func DefaultSeriesFormatOptions() SeriesFormatOptions {
	return SeriesFormatOptions{MaxRows: 10, MaxCellRune: 32}
}

// Format renders the Series as a polars-style single-column box table.
func (s *Series) Format(opts SeriesFormatOptions) string {
	if opts.MaxRows <= 0 && opts.MaxRows != -1 {
		opts.MaxRows = 10
	}
	if opts.MaxCellRune <= 0 && opts.MaxCellRune != -1 {
		opts.MaxCellRune = 32
	}

	h := s.Len()
	if h == 0 {
		return fmt.Sprintf("shape: (0,)\nSeries: %q %s\n<empty>", s.Name(), s.DType())
	}

	rows, ellipsisAt := pickRows(h, opts.MaxRows)
	chunk := s.Chunk(0)
	cells := make([]string, len(rows))
	for i, ri := range rows {
		if i == ellipsisAt {
			cells[i] = "…"
			continue
		}
		cells[i] = renderSeriesCell(chunk, ri, opts.MaxCellRune)
	}

	name := s.Name()
	header := name
	if header == "" {
		header = "''"
	}
	dtype := s.DType().String()

	width := max4(runeLen(header), runeLen(dtype), 3, maxRuneLen(cells))

	var b strings.Builder
	fmt.Fprintf(&b, "shape: (%d,)\n", h)
	fmt.Fprintf(&b, "Series: %q [%s]\n", name, dtype)
	drawSeriesBorder(&b, width, '╭', '╮')
	drawSeriesRow(&b, width, header)
	drawSeriesRow(&b, width, dtype)
	drawSeriesBorder(&b, width, '├', '┤')
	for _, cell := range cells {
		drawSeriesRow(&b, width, cell)
	}
	drawSeriesBorder(&b, width, '╰', '╯')
	return b.String()
}

func pickRows(h, maxRows int) ([]int, int) {
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
	out = append(out, -1)
	for i := h - tail; i < h; i++ {
		out = append(out, i)
	}
	return out, head
}

func renderSeriesCell(arr any, i int, maxRune int) string {
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
		s = formatFloatCell(float64(a.Value(i)))
	case *array.Float64:
		s = formatFloatCell(a.Value(i))
	case *array.Boolean:
		if a.Value(i) {
			s = "true"
		} else {
			s = "false"
		}
	case *array.String:
		s = strconv.Quote(a.Value(i))
	case *array.Dictionary:
		// Categorical / Enum: show the decoded category like a string.
		s = strconv.Quote(a.ValueStr(i))
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

func formatFloatCell(f float64) string {
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

func drawSeriesBorder(b *strings.Builder, width int, left, right rune) {
	b.WriteRune(left)
	for j := 0; j < width+2; j++ {
		b.WriteRune('─')
	}
	b.WriteRune(right)
	b.WriteByte('\n')
}

func drawSeriesRow(b *strings.Builder, width int, cell string) {
	b.WriteRune('│')
	b.WriteByte(' ')
	b.WriteString(cell)
	pad := width - runeLen(cell)
	for range pad {
		b.WriteByte(' ')
	}
	b.WriteByte(' ')
	b.WriteRune('│')
	b.WriteByte('\n')
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

func maxRuneLen(ss []string) int {
	m := 0
	for _, s := range ss {
		if w := runeLen(s); w > m {
			m = w
		}
	}
	return m
}

func max4(a, b, c, d int) int { return max(a, b, c, d) }
