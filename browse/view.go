package browse

// view.go is a close port of the rendering code in
// github.com/maaslalani/sheets (MIT). The grid layout, column-header
// row, border-junction strategy, row-label gutter, and mode-chip
// status bar mirror sheets' file-for-file:
//
//   sheets.renderColumnHeaders -> (*model).renderColumnHeaders
//   sheets.renderGrid          -> (*model).renderGrid
//   sheets.renderBorderLine    -> (*model).renderBorderLine
//   sheets.renderContentLine   -> (*model).renderContentLine
//   sheets.renderStatusBar     -> (*model).renderStatusBar
//
// The changes relative to sheets:
//   * Cells are read-only (no formulas / insert mode), so the insert
//     accent colour and formula styles are gone.
//   * Cell widths are measured per-column from the data + header
//     rather than a global default, since a DataFrame viewer has
//     meaningful column names + heterogeneous widths.
//   * Column headers show the DataFrame column name (not A/B/C);
//     A/B/C is a spreadsheet affordance that doesn't map onto named
//     columns.
//
// The colour palette is copied verbatim from sheets.newModel.

import (
	"fmt"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// styles is the viewer's colour palette. Values map 1-1 to sheets'
// newModel() constants: gridGray, headerGray, activeHeader (white
// bold), selectBackground, selectAccent, statusGray, statusText,
// insertAccent used for the FILTER mode chip, statusSelectAccent
// for VISUAL.
var styles = struct {
	grid              lipgloss.Style
	header            lipgloss.Style
	activeHeader      lipgloss.Style
	rowLabel          lipgloss.Style
	activeRow         lipgloss.Style
	activeCell        lipgloss.Style
	selectCell        lipgloss.Style
	selectActiveCell  lipgloss.Style
	selectHeader      lipgloss.Style
	selectActiveHead  lipgloss.Style
	selectRow         lipgloss.Style
	selectBorder      lipgloss.Style
	frozenHeader      lipgloss.Style
	sortedHeader      lipgloss.Style
	statusBar         lipgloss.Style
	statusText        lipgloss.Style
	statusAccent      lipgloss.Style
	statusNormalMode  lipgloss.Style
	statusInsertMode  lipgloss.Style
	statusVisualMode  lipgloss.Style
	statusCommandMode lipgloss.Style
	statusFilterMode  lipgloss.Style
	commandLineText   lipgloss.Style
	commandLineError  lipgloss.Style
	title             lipgloss.Style
}{
	grid:              lipgloss.NewStyle().Foreground(lipgloss.Color("8")),
	header:            lipgloss.NewStyle().Foreground(lipgloss.Color("8")),
	activeHeader:      lipgloss.NewStyle().Foreground(lipgloss.Color("15")).Bold(true),
	rowLabel:          lipgloss.NewStyle().Foreground(lipgloss.Color("8")),
	activeRow:         lipgloss.NewStyle().Foreground(lipgloss.Color("15")).Bold(true),
	activeCell:        lipgloss.NewStyle().Reverse(true),
	selectCell:        lipgloss.NewStyle().Background(lipgloss.Color("#264F78")).Foreground(lipgloss.Color("15")).Bold(true),
	selectActiveCell:  lipgloss.NewStyle().Background(lipgloss.Color("#2F66C7")).Foreground(lipgloss.Color("15")).Bold(true).Underline(true),
	selectHeader:      lipgloss.NewStyle().Background(lipgloss.Color("#264F78")).Foreground(lipgloss.Color("15")).Bold(true),
	selectActiveHead:  lipgloss.NewStyle().Background(lipgloss.Color("#2F66C7")).Foreground(lipgloss.Color("15")).Bold(true),
	selectRow:         lipgloss.NewStyle().Background(lipgloss.Color("#264F78")).Foreground(lipgloss.Color("15")).Bold(true),
	selectBorder:      lipgloss.NewStyle().Background(lipgloss.Color("#264F78")).Foreground(lipgloss.Color("#2F66C7")),
	frozenHeader:      lipgloss.NewStyle().Foreground(lipgloss.Color("#10B981")).Bold(true),
	sortedHeader:      lipgloss.NewStyle().Foreground(lipgloss.Color("#F472B6")).Bold(true).Underline(true),
	statusBar:         lipgloss.NewStyle().Background(lipgloss.Color("0")).Foreground(lipgloss.Color("7")),
	statusText:        lipgloss.NewStyle().Background(lipgloss.Color("0")).Foreground(lipgloss.Color("7")),
	statusAccent:      lipgloss.NewStyle().Background(lipgloss.Color("0")).Foreground(lipgloss.Color("#D79921")),
	statusNormalMode:  lipgloss.NewStyle().Background(lipgloss.Color("33")).Foreground(lipgloss.Color("15")).Padding(0, 1),
	statusInsertMode:  lipgloss.NewStyle().Background(lipgloss.Color("#D79921")).Foreground(lipgloss.Color("15")).Padding(0, 1),
	statusVisualMode:  lipgloss.NewStyle().Background(lipgloss.Color("13")).Foreground(lipgloss.Color("15")).Padding(0, 1),
	statusCommandMode: lipgloss.NewStyle().Background(lipgloss.Color("#D79921")).Foreground(lipgloss.Color("15")).Padding(0, 1),
	statusFilterMode:  lipgloss.NewStyle().Background(lipgloss.Color("2")).Foreground(lipgloss.Color("15")).Padding(0, 1),
	commandLineText:   lipgloss.NewStyle().Foreground(lipgloss.Color("7")),
	commandLineError:  lipgloss.NewStyle().Foreground(lipgloss.Color("9")),
	title:             lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#F472B6")),
}

// layout is the precomputed geometry of a render pass.
type layout struct {
	vis           []int // column indices (into m.cols) drawn this frame, in order
	vispos        []int // for each rendered column, its index in visibleCols()
	widths        []int // visible widths, same length as vis
	rowLabelWidth int
	visibleRows   int
	// phantomCols is the number of empty "spreadsheet-style" trailing
	// columns drawn to fill the terminal width. Sheets does this so
	// the grid fills the viewport; we do the same so the viewer looks
	// like an actual spreadsheet rather than a cramped table.
	phantomCols int
}

func (m *model) computeLayout() layout {
	all := m.visibleCols()
	allWidths := make([]int, len(all))
	for i := range all {
		allWidths[i] = m.measureCol(all, i)
	}

	rowLabelWidth := max(len(strconv.Itoa(max(m.rowCount(), 1))), 3)

	frozenEnd := 0
	for i := range all {
		if !m.cols[all[i]].frozen {
			break
		}
		frozenEnd++
	}
	// Columns always have a leading border char plus one trailing border;
	// between columns sheets shares the border, so per column it's:
	// cellWidth + 1 (right border). Plus the row label gutter of width
	// rowLabelWidth+2 (" NNN " style) before the first border.
	used := rowLabelWidth + 2 + 1 // gutter + leading border
	visIdx := make([]int, 0, len(all))
	for i := 0; i < frozenEnd; i++ {
		used += allWidths[i] + 1
		visIdx = append(visIdx, i)
	}
	start := frozenEnd + m.leftCol
	for i := start; i < len(all); i++ {
		if used+allWidths[i]+1 > m.width && len(visIdx) > frozenEnd {
			break
		}
		used += allWidths[i] + 1
		visIdx = append(visIdx, i)
	}
	vis := make([]int, len(visIdx))
	vispos := make([]int, len(visIdx))
	widths := make([]int, len(visIdx))
	for i, pos := range visIdx {
		vis[i] = all[pos]
		vispos[i] = pos
		widths[i] = allWidths[pos]
	}

	// Phantom columns: pad the grid rightward with empty minCellWidth
	// cells until we hit the terminal edge, sheets-style.
	phantomCols := 0
	for used+minCellWidth+1 <= m.width {
		phantomCols++
		used += minCellWidth + 1
	}

	overhead := 4
	rows := max((m.height-overhead)/2, 1)

	return layout{
		vis:           vis,
		vispos:        vispos,
		widths:        widths,
		rowLabelWidth: rowLabelWidth,
		visibleRows:   rows,
		phantomCols:   phantomCols,
	}
}

func (m *model) View() tea.View {
	v := tea.View{AltScreen: true, MouseMode: tea.MouseModeCellMotion}
	if m.width == 0 || m.height == 0 {
		v.SetContent("loading...")
		return v
	}
	if len(m.visibleCols()) == 0 {
		v.SetContent(styles.rowLabel.Render("no visible columns - press = to un-hide all"))
		return v
	}

	lay := m.computeLayout()

	// Keep cursor on-screen vertically.
	if m.cursorRow >= m.topRow+lay.visibleRows {
		m.topRow = m.cursorRow - lay.visibleRows + 1
	}
	if m.topRow < 0 {
		m.topRow = 0
	}
	if m.cursorCol < len(lay.vis) && m.cols[lay.vis[m.cursorCol]].hidden {
		// safety; should never happen
	}

	var b strings.Builder
	b.WriteString(m.renderTopBar())
	b.WriteByte('\n')
	b.WriteString(m.renderColumnHeaders(lay))
	b.WriteByte('\n')
	b.WriteString(m.renderGrid(lay))
	b.WriteByte('\n')
	b.WriteString(m.renderStatusBar())
	v.SetContent(b.String())
	return v
}

// ---- top bar ------------------------------------------------------

func (m *model) renderTopBar() string {
	total := m.df.Height()
	visible := m.rowCount()
	visCols := len(m.visibleCols())
	title := fmt.Sprintf(" %s  %d/%d rows × %d cols ", m.title, visible, total, visCols)
	left := styles.title.Render(title) + styles.rowLabel.Render("  ? help")

	vis := m.visibleCols()
	colName := ""
	if m.cursorCol < len(vis) {
		colName = m.cols[vis[m.cursorCol]].name
	}
	pos := fmt.Sprintf(" %s @ row %d ", colName, m.cursorRow+1)
	right := styles.rowLabel.Render(pos)

	pad := max(m.width-lipgloss.Width(left)-lipgloss.Width(right), 0)
	return left + strings.Repeat(" ", pad) + right
}

// ---- column header row -------------------------------------------

// Mirrors sheets.renderColumnHeaders: a strip of column labels above
// the grid, each centered over its cell column, separated by single
// spaces aligning with the vertical border below.
func (m *model) renderColumnHeaders(lay layout) string {
	var b strings.Builder
	// rowLabelWidth+2 to align with the gutter ( label + space + │ ).
	b.WriteString(strings.Repeat(" ", lay.rowLabelWidth+2))

	for i, ci := range lay.vis {
		cv := m.cols[ci]
		label := alignCenter(truncate(cv.name, lay.widths[i]), lay.widths[i])
		if m.sort.idx == ci && lay.widths[i] > 2 {
			arrow := "▲"
			if m.sort.desc {
				arrow = "▼"
			}
			label = alignCenter(truncate(cv.name, lay.widths[i]-2)+" "+arrow, lay.widths[i])
		}

		var style lipgloss.Style
		switch {
		case m.sort.idx == ci:
			style = styles.sortedHeader
		case cv.frozen:
			style = styles.frozenHeader
		case i == m.cursorCol:
			style = styles.activeHeader
		default:
			style = styles.header
		}
		b.WriteString(style.Render(label))
		b.WriteByte(' ')
	}
	// Phantom spreadsheet-style column letters for empty trailing
	// columns (A, B, C... continuing past the last data column).
	startLetter := len(lay.vis)
	for j := range lay.phantomCols {
		label := alignCenter(columnLetter(startLetter+j), minCellWidth)
		b.WriteString(styles.header.Render(label))
		if j < lay.phantomCols-1 {
			b.WriteByte(' ')
		}
	}
	return b.String()
}

// columnLetter encodes n as a spreadsheet column label (0 -> "A",
// 25 -> "Z", 26 -> "AA").
func columnLetter(n int) string {
	if n < 0 {
		return ""
	}
	out := make([]byte, 0, 3)
	for {
		out = append(out, byte('A'+n%26))
		n = n/26 - 1
		if n < 0 {
			break
		}
	}
	// reverse in place
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

// ---- grid --------------------------------------------------------

// renderGrid is a direct transliteration of sheets.renderGrid:
// alternating border / content lines, closing with └─┴─┘. Unlike the
// original, we render the grid all the way to the bottom: rows past
// end-of-data get empty cells so the grid fills the viewport.
func (m *model) renderGrid(lay layout) string {
	total := lay.visibleRows
	nReal := min(total, m.rowCount()-m.topRow)
	lines := make([]string, 0, 1+total*2)

	lines = append(lines, m.renderBorderLine(lay, "┌", "┬", "┐"))

	for i := range total {
		row := m.topRow + i
		if i < nReal {
			lines = append(lines, m.renderContentLine(lay, row))
		} else {
			lines = append(lines, m.renderEmptyContentLine(lay))
		}
		left, mid, right := "├", "┼", "┤"
		if i == total-1 {
			left, mid, right = "└", "┴", "┘"
		}
		lines = append(lines, m.renderBorderLine(lay, left, mid, right))
	}
	return strings.Join(lines, "\n")
}

// renderEmptyContentLine draws a content-height row with no row label
// and no cell contents, only the │ borders. Mirrors the look sheets
// uses for rows past the end of a dataset.
func (m *model) renderEmptyContentLine(lay layout) string {
	var b strings.Builder
	b.WriteString(strings.Repeat(" ", lay.rowLabelWidth+1))
	b.WriteString(styles.grid.Render("│"))
	for _, w := range lay.widths {
		b.WriteString(strings.Repeat(" ", w))
		b.WriteString(styles.grid.Render("│"))
	}
	for range lay.phantomCols {
		b.WriteString(strings.Repeat(" ", minCellWidth))
		b.WriteString(styles.grid.Render("│"))
	}
	return b.String()
}

func (m *model) renderBorderLine(lay layout, left, mid, right string) string {
	var b strings.Builder
	// Gutter: rowLabelWidth + 1 leading space, matching sheets'
	// `strings.Repeat(" ", m.rowLabelWidth); b.WriteString(" ")`.
	b.WriteString(strings.Repeat(" ", lay.rowLabelWidth+1))
	b.WriteString(styles.grid.Render(left))
	total := len(lay.widths) + lay.phantomCols
	for i := range total {
		var w int
		if i < len(lay.widths) {
			w = lay.widths[i]
		} else {
			w = minCellWidth
		}
		b.WriteString(styles.grid.Render(strings.Repeat("─", w)))
		j := mid
		if i == total-1 {
			j = right
		}
		b.WriteString(styles.grid.Render(j))
	}
	return b.String()
}

func (m *model) renderContentLine(lay layout, row int) string {
	var b strings.Builder

	// Row number gutter.
	label := alignRight(strconv.Itoa(m.dfRow(row)+1), lay.rowLabelWidth)
	if row == m.cursorRow || (m.mode == modeVisual && m.rowInVisualRange(row)) {
		b.WriteString(styles.activeRow.Render(label))
	} else {
		b.WriteString(styles.rowLabel.Render(label))
	}
	b.WriteByte(' ')
	b.WriteString(styles.grid.Render("│"))

	for i, ci := range lay.vis {
		w := lay.widths[i]
		// sheets renders cell content left-justified with trailing fill;
		// `fitCell` mirrors that (not `fitLeft` which right-justifies).
		cell := fitCell(m.cellAt(row, m.cols[ci].orig), w)

		var style lipgloss.Style
		switch {
		case m.mode == modeVisual && m.rowInVisualRange(row) && i == m.cursorCol && row == m.cursorRow:
			style = styles.selectActiveCell
		case m.mode == modeVisual && m.rowInVisualRange(row):
			style = styles.selectRow
		case row == m.cursorRow && i == m.cursorCol:
			style = styles.activeCell
		case m.cols[ci].frozen:
			style = styles.frozenHeader
		default:
			style = lipgloss.NewStyle()
		}
		b.WriteString(style.Render(cell))
		b.WriteString(styles.grid.Render("│"))
	}
	// Phantom empty cells.
	for range lay.phantomCols {
		b.WriteString(strings.Repeat(" ", minCellWidth))
		b.WriteString(styles.grid.Render("│"))
	}
	return b.String()
}

// rowInVisualRange reports whether row is between the visual-mode
// anchor and cursor, inclusive.
func (m *model) rowInVisualRange(row int) bool {
	if m.mode != modeVisual {
		return false
	}
	lo, hi := m.visualRange()
	return row >= lo && row <= hi
}

// ---- status bar ---------------------------------------------------

func (m *model) renderStatusBar() string {
	modeLabel := m.modeChip()
	var middle string
	switch m.mode {
	case modeCommand:
		middle = " " + m.commandInput.View()
	case modeFilter:
		middle = " " + m.filterInput.View()
	default:
		msg := m.commandMessage
		if msg == "" {
			msg = m.summaryString(m.cursorCol)
		}
		if m.commandError {
			middle = " " + styles.commandLineError.Render(msg)
		} else {
			middle = " " + styles.commandLineText.Render(msg)
		}
	}
	midW := max(m.width-lipgloss.Width(modeLabel), 0)
	middle = truncate(middle, midW)
	middle = padRight(middle, midW)
	return modeLabel + styles.statusBar.Render(middle)
}

func (m *model) modeChip() string {
	switch m.mode {
	case modeVisual:
		return styles.statusVisualMode.Render("VISUAL")
	case modeCommand:
		return styles.statusCommandMode.Render("COMMAND")
	case modeFilter:
		return styles.statusFilterMode.Render("FILTER")
	default:
		return styles.statusNormalMode.Render("NORMAL")
	}
}

// ---- text helpers -------------------------------------------------

// alignCenter centres s in a field of width w; extra slack goes on
// the trailing side when odd. Matches sheets' alignCenter.
func alignCenter(s string, w int) string {
	if len(s) >= w {
		return s
	}
	slack := w - len(s)
	left := slack / 2
	right := slack - left
	return strings.Repeat(" ", left) + s + strings.Repeat(" ", right)
}

// alignRight right-justifies s in a field of width w.
func alignRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return strings.Repeat(" ", w-len(s)) + s
}

// fitCell renders s left-justified, truncating with an ellipsis when
// needed, filling with trailing spaces. This matches sheets' fit().
func fitCell(s string, w int) string {
	return padRight(truncate(s, w), w)
}
