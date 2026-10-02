package dataframe_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

type reprTable struct {
	Kind    string      `json:"kind"`
	Columns []string    `json:"columns"`
	Dtypes  []string    `json:"dtypes"`
	Rows    [][]*string `json:"rows"`
	Shape   [2]int      `json:"shape"`
	RowGap  *int        `json:"row_gap"`
	ColGap  *int        `json:"col_gap"`
}

func decodeTable(t *testing.T, bundle map[string]string) reprTable {
	t.Helper()
	var tbl reprTable
	if err := json.Unmarshal([]byte(bundle[dataframe.TableMIME]), &tbl); err != nil {
		t.Fatalf("table json: %v\n%s", err, bundle[dataframe.TableMIME])
	}
	return tbl
}

func TestMimeBundleTable(t *testing.T) {
	a, err := series.FromInt64("a", []int64{1, 2, 3}, []bool{true, false, true})
	if err != nil {
		t.Fatal(err)
	}
	b, err := series.FromString("b", []string{"x", "y", "z"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(a, b)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	bundle := df.MimeBundle()
	for _, k := range []string{"text/plain", "text/html", dataframe.TableMIME} {
		if bundle[k] == "" {
			t.Fatalf("bundle lacks %s", k)
		}
	}
	if !strings.Contains(bundle["text/html"], "<table") || !strings.Contains(bundle["text/plain"], "shape: (3, 2)") {
		t.Fatalf("bundle %v", bundle)
	}
	tbl := decodeTable(t, bundle)
	if tbl.Shape != [2]int{3, 2} || strings.Join(tbl.Columns, ",") != "a,b" || tbl.Dtypes[0] != "i64" {
		t.Fatalf("table %+v", tbl)
	}
	if tbl.RowGap != nil || tbl.ColGap != nil || len(tbl.Rows) != 3 {
		t.Fatalf("unexpected gaps %+v", tbl)
	}
	if tbl.Rows[1][0] != nil || *tbl.Rows[0][0] != "1" || *tbl.Rows[2][1] != "z" {
		t.Fatalf("rows %+v", tbl.Rows)
	}
}

func TestMimeBundleGaps(t *testing.T) {
	vals := make([]int64, 25)
	for i := range vals {
		vals[i] = int64(i)
	}
	var cols []*series.Series
	for c := range 70 {
		s, err := series.FromInt64(fmt.Sprintf("c%d", c), vals, nil)
		if err != nil {
			t.Fatal(err)
		}
		cols = append(cols, s)
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	tbl := decodeTable(t, df.MimeBundle())
	if tbl.Shape != [2]int{25, 70} || len(tbl.Rows) != 10 || len(tbl.Columns) != 64 {
		t.Fatalf("shape %v rows %d cols %d", tbl.Shape, len(tbl.Rows), len(tbl.Columns))
	}
	if tbl.RowGap == nil || *tbl.RowGap != 5 || tbl.ColGap == nil || *tbl.ColGap != 32 {
		t.Fatalf("gaps %v %v", tbl.RowGap, tbl.ColGap)
	}
	if *tbl.Rows[5][0] != "20" || tbl.Columns[32] != "c38" {
		t.Fatalf("tail row %v col %v", *tbl.Rows[5][0], tbl.Columns[32])
	}
	if html := df.HTML(); !strings.Contains(html, "showing 8 of 70 columns") {
		t.Fatalf("html column note missing")
	}
}

func TestSeriesMimeBundle(t *testing.T) {
	s, err := series.FromFloat64("x", []float64{1.5, 2}, []bool{true, false})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	bundle := s.MimeBundle()
	tbl := decodeTable(t, bundle)
	if tbl.Kind != "series" || tbl.Shape != [2]int{2, 1} || *tbl.Rows[0][0] != "1.5" || tbl.Rows[1][0] != nil {
		t.Fatalf("table %+v", tbl)
	}
	if !strings.Contains(bundle["text/html"], "shape: (2,)") {
		t.Fatalf("html %s", bundle["text/html"])
	}
}
