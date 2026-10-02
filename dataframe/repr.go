package dataframe

import (
	"github.com/Gaurav-Gosain/golars/internal/reprtable"
	"github.com/Gaurav-Gosain/golars/series"
)

// TableMIME is the media type of the JSON table in MimeBundle: column
// names, dtypes, formatted cells (null as JSON null), the shape and where
// rows or columns were left out. Terminal notebooks such as gopyter draw
// their own table from it.
const TableMIME = reprtable.MIME

// tableMaxCols caps the columns of the JSON table. It is wider than the
// HTML default because terminal front ends fit columns to their width.
const tableMaxCols = 64

// MimeBundle returns the frame as Jupyter-style mime data: text/plain
// (the String repr), text/html and TableMIME. Notebook front ends that
// duck type on this method (gopyter's nb.Display, for one) show the
// richest form they support.
func (df *DataFrame) MimeBundle() map[string]string {
	return df.MimeBundleWith(DefaultFormatOptions())
}

// MimeBundleWith is MimeBundle with custom bounds. MaxCols applies to
// text/plain and text/html; the JSON table keeps up to 64 columns.
func (df *DataFrame) MimeBundleWith(opts FormatOptions) map[string]string {
	opts = normalizeFormat(opts)
	wide := opts
	if wide.MaxCols != -1 {
		wide.MaxCols = max(wide.MaxCols, tableMaxCols)
	}
	t := df.reprTable(wide)
	return map[string]string{
		"text/plain": df.Format(opts),
		"text/html":  df.HTMLWith(opts),
		TableMIME:    t.JSON(),
	}
}

// HTML renders the frame as a self-contained HTML table (inline styles,
// shape caption, dtype row), using DefaultFormatOptions.
func (df *DataFrame) HTML() string { return df.HTMLWith(DefaultFormatOptions()) }

// HTMLWith is HTML with custom bounds.
func (df *DataFrame) HTMLWith(opts FormatOptions) string {
	t := df.reprTable(normalizeFormat(opts))
	return t.HTML()
}

func normalizeFormat(opts FormatOptions) FormatOptions {
	if opts.MaxRows <= 0 && opts.MaxRows != -1 {
		opts.MaxRows = defaultMaxRows
	}
	if opts.MaxCols <= 0 && opts.MaxCols != -1 {
		opts.MaxCols = defaultMaxCols
	}
	if opts.MaxCellRune <= 0 && opts.MaxCellRune != -1 {
		opts.MaxCellRune = defaultMaxCellRune
	}
	return opts
}

func (df *DataFrame) reprTable(opts FormatOptions) reprtable.Table {
	h, w := df.Shape()
	t := reprtable.Table{Shape: [2]int{h, w}, Columns: []string{}, Dtypes: []string{}, Rows: [][]*string{}}
	colIdx, colGap := reprtable.Pick(w, opts.MaxCols)
	rowIdx, rowGap := reprtable.Pick(h, opts.MaxRows)
	t.SetGaps(rowGap, colGap)
	for _, ci := range colIdx {
		s := df.ColumnAt(ci)
		t.Columns = append(t.Columns, s.Name())
		t.Dtypes = append(t.Dtypes, s.DType().String())
	}
	for _, ri := range rowIdx {
		row := make([]*string, len(colIdx))
		for j, ci := range colIdx {
			row[j] = seriesCell(df.ColumnAt(ci), ri, opts.MaxCellRune)
		}
		t.Rows = append(t.Rows, row)
	}
	return t
}

// seriesCell formats row i of s, walking its chunks; nil means null.
func seriesCell(s *series.Series, i, maxRune int) *string {
	for c := range s.NumChunks() {
		arr := s.Chunk(c)
		if i < arr.Len() {
			if arr.IsNull(i) {
				return nil
			}
			v := renderCell(arr, i, maxRune)
			return &v
		}
		i -= arr.Len()
	}
	return nil
}
