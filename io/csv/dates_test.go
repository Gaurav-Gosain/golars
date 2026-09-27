package csv

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
)

const datesCSV = "a,b,c,d,e,f,g\n" +
	"2021-01-02,2021-01-02 03:04:05,2021-01-02T03:04:05.123,x,02/01/2021,2021-01-02T03:04:05+01:00,03:04:05\n" +
	"2021-01-03,2021-01-03 00:00:00,2021-01-02T03:04:05,y,31/12/2021,2021-01-02T04:04:05+01:00,23:59:59\n"

// Expected dtypes and values come from polars 1.39.3:
// pl.read_csv(..., try_parse_dates=True / False).
func TestReadTryParseDates(t *testing.T) {
	alloc := testutil.NewCheckedAllocator(t)
	df, err := Read(context.Background(), strings.NewReader(datesCSV), WithAllocator(alloc), WithTryParseDates(true))
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	want := []string{"date", "datetime[us]", "datetime[us]", "str", "date", "datetime[us, UTC]", "time64[ns]"}
	for i, dt := range df.DTypes() {
		if dt.String() != want[i] {
			t.Errorf("column %d dtype = %s, want %s", i, dt, want[i])
		}
	}
	e := df.ColumnAt(4).Chunk(0).(*array.Date32)
	if e.Value(0) != 18629 || e.Value(1) != 18992 {
		t.Errorf("dmy dates = %d %d, want 18629 18992", e.Value(0), e.Value(1))
	}
	f := df.ColumnAt(5).Chunk(0).(*array.Timestamp)
	if f.Value(0) != 1609553045000000 || f.Value(1) != 1609556645000000 {
		t.Errorf("offset datetimes = %d %d", f.Value(0), f.Value(1))
	}
	c := df.ColumnAt(2).Chunk(0).(*array.Timestamp)
	if c.Value(0) != 1609556645123000 {
		t.Errorf("fractional datetime = %d", c.Value(0))
	}
}

func TestReadTryParseDatesFalseKeepsStrings(t *testing.T) {
	alloc := testutil.NewCheckedAllocator(t)
	df, err := Read(context.Background(), strings.NewReader(datesCSV), WithAllocator(alloc), WithTryParseDates(false))
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	for i, dt := range df.DTypes() {
		if dt.String() != "str" {
			t.Errorf("column %d dtype = %s, want str", i, dt)
		}
	}
	if got := df.ColumnAt(1).Chunk(0).(*array.String).Value(0); got != "2021-01-02 03:04:05" {
		t.Errorf("value = %q", got)
	}
}
