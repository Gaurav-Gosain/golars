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

// printTable prints df in the library's table style, the same output
// as fmt.Println(df), so `golars run` and Go programs render frames
// alike. It reports whether the table went to tableHook instead.
func printTable(df *dataframe.DataFrame) (hooked bool) {
	if tableHook != nil {
		tableHook(df)
		return true
	}
	if df.Width() == 0 {
		fmt.Println(dimStyle.Render("  (no columns)"))
		return false
	}
	fmt.Println(df.String())
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
