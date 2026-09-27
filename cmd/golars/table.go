package main

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// tableHook, when set, receives every table a command would print, in
// place of the ASCII rendering. kernel-host sets it for structured
// requests so notebook front ends get tables as data.
var tableHook func(df *dataframe.DataFrame)

// printTable renders df as a fixed-width, colored table. Columns are ordered
// per df.Schema(). Strings are quoted, nulls shown as dim "null". It
// reports whether the table went to tableHook instead.
func printTable(df *dataframe.DataFrame) (hooked bool) {
	if tableHook != nil {
		tableHook(df)
		return true
	}
	if df.Width() == 0 {
		fmt.Println(dimStyle.Render("  (no columns)"))
		return false
	}
	names := df.Schema().Names()
	dtypes := df.Schema().DTypes()

	rows := make([][]string, df.Height())
	for i := range rows {
		rows[i] = make([]string, df.Width())
	}
	for ci, col := range df.Columns() {
		if df.Height() == 0 {
			break // a zero-row series may have no chunks at all
		}
		arr := col.Chunk(0)
		for ri := range df.Height() {
			rows[ri][ci] = renderCell(arr, ri)
		}
	}

	// Column widths: max of header repr, dtype repr, and cell widths.
	widths := make([]int, df.Width())
	dtypesShort := make([]string, df.Width())
	for i, dt := range dtypes {
		dtypesShort[i] = dt.String()
		widths[i] = max(lipgloss.Width(names[i]), len(dtypesShort[i]))
	}
	for _, row := range rows {
		for ci, cell := range row {
			if w := lipgloss.Width(cell); w > widths[ci] {
				widths[ci] = w
			}
		}
	}

	// Header line.
	var sb strings.Builder
	sb.WriteString("  ")
	for i, name := range names {
		sb.WriteString(padRightANSI(headerStyle.Render(name), widths[i]))
		if i < len(names)-1 {
			sb.WriteString("  ")
		}
	}
	fmt.Println(sb.String())

	// Dtype line.
	sb.Reset()
	sb.WriteString("  ")
	for i, d := range dtypesShort {
		sb.WriteString(dimStyle.Render(padRight(d, widths[i])))
		if i < len(dtypesShort)-1 {
			sb.WriteString("  ")
		}
	}
	fmt.Println(sb.String())

	// Separator.
	sb.Reset()
	sb.WriteString("  ")
	for i := range widths {
		sb.WriteString(dimStyle.Render(strings.Repeat("─", widths[i])))
		if i < len(widths)-1 {
			sb.WriteString("  ")
		}
	}
	fmt.Println(sb.String())

	// Body.
	for _, row := range rows {
		sb.Reset()
		sb.WriteString("  ")
		for i, cell := range row {
			sb.WriteString(padRightANSI(cell, widths[i]))
			if i < len(row)-1 {
				sb.WriteString("  ")
			}
		}
		fmt.Println(sb.String())
	}
	return false
}

// renderCell returns a styled string for the value at index i in arr.
func renderCell(arr arrow.Array, i int) string {
	if arr.IsNull(i) {
		return dimStyle.Render("null")
	}
	switch a := arr.(type) {
	case *array.Int8:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Int16:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Int32:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Int64:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Uint8:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Uint16:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Uint32:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Uint64:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Float32:
		return fmt.Sprintf("%g", a.Value(i))
	case *array.Float64:
		return fmt.Sprintf("%g", a.Value(i))
	case *array.Boolean:
		if a.Value(i) {
			return successStyle.Render("true")
		}
		return errMsgStyle.Render("false")
	case *array.String:
		return infoStyle.Render(fmt.Sprintf("%q", a.Value(i)))
	case *array.Binary:
		return fmt.Sprintf("[%d bytes]", len(a.Value(i)))
	}
	// Temporal, list, struct and anything else use arrow's formatting.
	return arr.ValueStr(i)
}

// padRightANSI pads considering ANSI color codes used by lipgloss.
func padRightANSI(s string, n int) string {
	w := lipgloss.Width(s)
	if w >= n {
		return s
	}
	return s + strings.Repeat(" ", n-w)
}
