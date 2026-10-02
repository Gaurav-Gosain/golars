// Package browse provides an interactive TUI DataFrame viewer.
//
// Layout and keybindings are modeled after maaslalani/sheets
// (https://github.com/maaslalani/sheets, MIT). The grid uses the same
// box-drawing border style, single-letter column labels (A..Z, AA..),
// and vim-like modal navigation; we skip the spreadsheet-editing bits
// since the frame is read-only.
//
// Data is pulled from the underlying arrow chunks on demand via
// per-column closures built once at open time, so multi-million-row
// frames don't need to be copied into memory.
package browse

import (
	"context"
	"sort"
	"strings"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// stableSort is an indirection for tests; uses slices.SortStableFunc via
// the stdlib sort package to avoid a tight dependency on a specific
// stable-sort impl.
func stableSort(keep []int, less func(a, b int) bool) {
	sort.SliceStable(keep, func(i, j int) bool { return less(keep[i], keep[j]) })
}

// Run opens the viewer for df. Blocks until the user quits.
func Run(df *dataframe.DataFrame, title string) error {
	return RunWithContext(context.Background(), df, title)
}

// RunWithContext is Run with a cancellable context; the viewer exits
// when ctx is cancelled.
func RunWithContext(ctx context.Context, df *dataframe.DataFrame, title string) error {
	m := newModel(df, title)
	p := tea.NewProgram(m, tea.WithContext(ctx))
	_, err := p.Run()
	return err
}

// mode enumerates the viewer's modal state. Like sheets, but without
// insert/edit (our frames are read-only).
type mode uint8

const (
	modeNormal  mode = iota
	modeVisual       // row/range selection
	modeCommand      // `:` prompt
	modeFilter       // `/` prompt
)

// sortKey describes the active sort. idx is the column's position in
// m.cols; idx == -1 means no sort applied.
type sortKey struct {
	idx  int
	desc bool
}

// colView tracks the per-column presentation state. We never reorder
// the underlying storage; instead we keep original index + visibility
// flags and reorder at render time.
type colView struct {
	orig   int    // index into df.Columns()
	name   string // header label
	dtype  string // pretty dtype
	hidden bool
	frozen bool
	width  int // 0 until first measured
}

// model is the bubbletea state.
type model struct {
	df    *dataframe.DataFrame
	title string

	width  int
	height int

	// Columns: visibility / freeze / width. Never reorders data.
	cols []colView
	// cellFn: concrete per-column string extractor closed over the arrow
	// array so reads dodge the interface dispatch cost.
	cellFn []cellFunc

	// Row ordering. Nil means natural 0..N-1.
	order []int
	// Filter (substring). Empty means no filter.
	filter string
	sort   sortKey

	// Cursor + viewport.
	cursorRow int
	cursorCol int // position within visibleCols()
	topRow    int
	leftCol   int // leftmost non-frozen visible column
	// Visual-mode anchor row; cursorRow plays selection end.
	anchorRow int

	// Modal state.
	mode           mode
	commandInput   textinput.Model
	filterInput    textinput.Model
	commandMessage string
	commandError   bool

	// Pending motions.
	gotoPending bool
	gotoBuffer  string
}

func newModel(df *dataframe.DataFrame, title string) *model {
	cols := df.Columns()
	views := make([]colView, len(cols))
	fns := make([]cellFunc, len(cols))
	for i, c := range cols {
		views[i] = colView{
			orig:  i,
			name:  c.Name(),
			dtype: c.DType().String(),
		}
		fns[i] = makeCellFn(c.Chunk(0))
	}

	ci := textinput.New()
	ci.Prompt = ":"
	ci.CharLimit = 256

	fi := textinput.New()
	fi.Prompt = "/"
	fi.CharLimit = 128
	fi.Placeholder = "substring match"

	return &model{
		df:           df,
		title:        title,
		cols:         views,
		cellFn:       fns,
		sort:         sortKey{idx: -1},
		commandInput: ci,
		filterInput:  fi,
	}
}

// ---- row / column helpers -----------------------------------------

func (m *model) rowCount() int {
	if m.order != nil {
		return len(m.order)
	}
	return m.df.Height()
}

func (m *model) dfRow(visRow int) int {
	if m.order != nil {
		return m.order[visRow]
	}
	return visRow
}

func (m *model) cellAt(visRow, origCol int) string {
	return m.cellFn[origCol](m.dfRow(visRow))
}

// visibleCols returns ordered indices into m.cols: frozen first (in
// their column order), then non-frozen.
func (m *model) visibleCols() []int {
	out := make([]int, 0, len(m.cols))
	for i := range m.cols {
		if !m.cols[i].hidden && m.cols[i].frozen {
			out = append(out, i)
		}
	}
	for i := range m.cols {
		if !m.cols[i].hidden && !m.cols[i].frozen {
			out = append(out, i)
		}
	}
	return out
}

// minCellWidth matches sheets' default cellWidth. Cells narrower than
// this feel cramped; wider is only used when content or a header name
// demands it.
const minCellWidth = 12

// maxCellWidth caps per-column width to keep very wide string columns
// from eating the viewport.
const maxCellWidth = 32

// measureCol returns the display width of the column at the given
// position in vis, computed from the header + visible row window.
// Caches the result on colView.
func (m *model) measureCol(vis []int, pos int) int {
	ci := vis[pos]
	if m.cols[ci].width > 0 {
		return m.cols[ci].width
	}
	w := len(m.cols[ci].name)
	end := min(m.topRow+m.viewportRows(), m.rowCount())
	for r := m.topRow; r < end; r++ {
		if s := m.cellAt(r, m.cols[ci].orig); len(s) > w {
			w = len(s)
		}
	}
	switch {
	case w < minCellWidth:
		w = minCellWidth
	case w > maxCellWidth:
		w = maxCellWidth
	}
	m.cols[ci].width = w
	return w
}

// viewportRows reports how many data rows fit between the column
// header row and the status bar.
func (m *model) viewportRows() int {
	// 1 top bar + 1 column header + 1 border + 1 status-command line.
	v := m.height - 4
	if v < 1 {
		return 1
	}
	return v
}

// ensureVisible nudges the vertical viewport so cursorRow is on-screen.
func (m *model) ensureVisible() {
	v := m.viewportRows()
	switch {
	case m.cursorRow < m.topRow:
		m.topRow = m.cursorRow
	case m.cursorRow >= m.topRow+v:
		m.topRow = m.cursorRow - v + 1
	}
	if m.topRow < 0 {
		m.topRow = 0
	}
}

// ---- filter / sort -------------------------------------------------

// applyView rebuilds m.order to reflect the active filter + sort.
// Both passes go through golars compute kernels: filter via a
// per-column Str().Contains mask OR'd together, sort via
// compute.SortIndices. That keeps us honest - the viewer reuses the
// same typed / SIMD code paths the library exposes to users.
func (m *model) applyView() {
	if m.filter == "" && m.sort.idx == -1 {
		m.order = nil
		if m.cursorRow >= m.rowCount() {
			m.cursorRow = max(m.rowCount()-1, 0)
		}
		m.ensureVisible()
		return
	}
	ctx := context.Background()

	keep, err := m.computeFilterIndices(ctx)
	if err != nil {
		m.commandError = true
		m.commandMessage = "filter: " + err.Error()
		return
	}

	if m.sort.idx >= 0 && m.sort.idx < len(m.cols) {
		if err := m.applySortOnto(ctx, keep); err != nil {
			m.commandError = true
			m.commandMessage = "sort: " + err.Error()
			return
		}
	}
	m.order = keep
	if m.cursorRow >= len(keep) {
		m.cursorRow = max(len(keep)-1, 0)
	}
	m.topRow = 0
	m.ensureVisible()
}

// computeFilterIndices returns row indices where at least one column's
// stringified value contains the (case-insensitive) filter needle.
// When m.filter is empty, returns every row.
func (m *model) computeFilterIndices(ctx context.Context) ([]int, error) {
	n := m.df.Height()
	if m.filter == "" {
		out := make([]int, n)
		for i := range out {
			out[i] = i
		}
		return out, nil
	}
	needle := strings.ToLower(m.filter)
	cols := m.df.Columns()

	// Per-column bool mask: cast -> lower -> contains. OR the masks
	// together row-wise via compute.Or.
	var combined *series.Series
	defer func() {
		if combined != nil {
			combined.Release()
		}
	}()
	for _, c := range cols {
		mask, err := perColumnContainsMask(ctx, c, needle)
		if err != nil {
			return nil, err
		}
		if combined == nil {
			combined = mask
			continue
		}
		next, err := compute.Or(ctx, combined, mask)
		mask.Release()
		combined.Release()
		if err != nil {
			return nil, err
		}
		combined = next
	}
	if combined == nil {
		return nil, nil
	}
	// Extract row indices where the mask is true.
	arr := combined.Chunk(0).(*array.Boolean)
	keep := make([]int, 0, n)
	for i := range n {
		if arr.IsValid(i) && arr.Value(i) {
			keep = append(keep, i)
		}
	}
	return keep, nil
}

// perColumnContainsMask returns a bool series that's true for rows
// whose stringified + lowercased value in col contains needle.
func perColumnContainsMask(ctx context.Context, col *series.Series, needle string) (*series.Series, error) {
	asStr := col
	var cleanup *series.Series
	if col.DType().ID() != dtype.String().ID() {
		s, err := compute.Cast(ctx, col, dtype.String())
		if err != nil {
			return nil, err
		}
		asStr = s
		cleanup = s
	}
	lo, err := asStr.Str().Lower()
	if cleanup != nil {
		cleanup.Release()
	}
	if err != nil {
		return nil, err
	}
	defer lo.Release()
	return lo.Str().Contains(needle)
}

// applySortOnto reorders keep in-place so that keep[i] lists the
// original-DF row indices in ascending (or descending) order of the
// active sort column. Uses compute.SortIndices, which under the hood
// picks the right typed / SIMD / parallel-radix path.
func (m *model) applySortOnto(ctx context.Context, keep []int) error {
	sortCol := m.df.Columns()[m.cols[m.sort.idx].orig]
	perm, err := compute.SortIndices(ctx, sortCol, compute.SortOptions{
		Descending: m.sort.desc,
	})
	if err != nil {
		return err
	}
	// perm is a full permutation of every DF row. Build a rank table
	// for O(1) "which slot does this row land in" lookups.
	rank := make([]int, m.df.Height())
	for slot, origRow := range perm {
		rank[origRow] = slot
	}
	// Reorder keep by that rank. Stable sort on rank.
	sortKeepByRank(keep, rank)
	return nil
}

// sortKeepByRank is an in-place stable sort of keep by rank[keep[i]].
func sortKeepByRank(keep, rank []int) {
	// Small n: insertion sort keeps it simple and avoids allocating a
	// closure for sort.SliceStable. For big n (filter didn't prune),
	// use the stdlib.
	if len(keep) < 64 {
		for i := 1; i < len(keep); i++ {
			k := keep[i]
			rk := rank[k]
			j := i - 1
			for j >= 0 && rank[keep[j]] > rk {
				keep[j+1] = keep[j]
				j--
			}
			keep[j+1] = k
		}
		return
	}
	// Go stdlib stable sort.
	stableSort(keep, func(a, b int) bool { return rank[a] < rank[b] })
}

// ---- bubbletea -----------------------------------------------------

func (m *model) Init() tea.Cmd { return nil }

// clamp returns x constrained to [lo, hi]; if lo > hi it returns lo.
func clamp(x, lo, hi int) int {
	if hi < lo {
		return lo
	}
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}
