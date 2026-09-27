package series

import "github.com/Gaurav-Gosain/golars/internal/reprtable"

// MimeBundle returns the series as Jupyter-style mime data: text/plain
// (the String repr), text/html and the JSON table
// "application/vnd.golars.table+json" (see dataframe.TableMIME), for
// notebook front ends that duck type on this method.
func (s *Series) MimeBundle() map[string]string {
	opts := DefaultSeriesFormatOptions()
	t := s.reprTable(opts)
	return map[string]string{
		"text/plain":   s.Format(opts),
		"text/html":    t.HTML(),
		reprtable.MIME: t.JSON(),
	}
}

// HTML renders the series as a one-column HTML table.
func (s *Series) HTML() string {
	t := s.reprTable(DefaultSeriesFormatOptions())
	return t.HTML()
}

func (s *Series) reprTable(opts SeriesFormatOptions) reprtable.Table {
	h := s.Len()
	name := s.Name()
	t := reprtable.Table{
		Kind: "series", Shape: [2]int{h, 1},
		Columns: []string{name}, Dtypes: []string{s.DType().String()},
		Rows: [][]*string{},
	}
	rowIdx, rowGap := reprtable.Pick(h, opts.MaxRows)
	t.SetGaps(rowGap, -1)
	for _, ri := range rowIdx {
		t.Rows = append(t.Rows, []*string{s.cellAt(ri, opts.MaxCellRune)})
	}
	return t
}

// cellAt formats row i, walking the chunks; nil means null.
func (s *Series) cellAt(i, maxRune int) *string {
	for c := range s.NumChunks() {
		arr := s.Chunk(c)
		if i < arr.Len() {
			if arr.IsNull(i) {
				return nil
			}
			v := renderSeriesCell(arr, i, maxRune)
			return &v
		}
		i -= arr.Len()
	}
	return nil
}
