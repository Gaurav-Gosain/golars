package series_test

import (
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func int64sOf(t *testing.T, s *series.Series) []int64 {
	t.Helper()
	arr := s.Chunk(0)
	out := make([]int64, arr.Len())
	for i := range out {
		switch a := arr.(type) {
		case *array.Timestamp:
			out[i] = int64(a.Value(i))
		case *array.Date32:
			out[i] = int64(a.Value(i))
		case *array.Time64:
			out[i] = int64(a.Value(i))
		case *array.Int64:
			out[i] = a.Value(i)
		default:
			t.Fatalf("unexpected array %T", arr)
		}
	}
	return out
}

func equalInts(t *testing.T, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// Expected values come from polars 1.39.3 (pl.date_range /
// pl.datetime_range / pl.time_range with eager=True).
func TestDateRangeMonthEndClamping(t *testing.T) {
	alloc := testutil.NewCheckedAllocator(t)
	start := series.DateToDays(2020, 1, 29)
	end := series.DateToDays(2020, 5, 1)
	s, err := series.DateRange("literal", start, end, "1mo", "both", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	equalInts(t, int64sOf(t, s), []int64{
		series.DateToDays(2020, 1, 29), series.DateToDays(2020, 2, 29),
		series.DateToDays(2020, 3, 29), series.DateToDays(2020, 4, 29),
	})
	if _, err := series.DateRange("x", start, end, "12h", "both"); err == nil {
		t.Error("sub-daily date_range interval should fail")
	}
}

func TestDatetimeRangeAcrossDST(t *testing.T) {
	alloc := testutil.NewCheckedAllocator(t)
	us := func(y, m, d, h int) int64 {
		return series.TimeToTicks(time.Date(y, time.Month(m), d, h, 0, 0, 0, time.UTC), dtype.Microsecond, true)
	}
	s, err := series.DatetimeRange("literal", us(2024, 3, 9, 12), us(2024, 3, 11, 12), "1d", "both",
		dtype.Microsecond, "America/New_York", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	if s.DType().String() != "datetime[us, America/New_York]" {
		t.Errorf("dtype %s", s.DType())
	}
	equalInts(t, int64sOf(t, s), []int64{1710003600000000, 1710086400000000, 1710172800000000})

	h24, err := series.DatetimeRange("literal", us(2024, 3, 9, 12), us(2024, 3, 11, 12), "24h", "both",
		dtype.Microsecond, "America/New_York", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer h24.Release()
	equalInts(t, int64sOf(t, h24), []int64{1710003600000000, 1710090000000000})
}

func TestTimeRange(t *testing.T) {
	alloc := testutil.NewCheckedAllocator(t)
	s, err := series.TimeRange("literal", 9*3600e9, 12*3600e9, "1h30m", "", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	equalInts(t, int64sOf(t, s), []int64{9 * 3600e9, 10.5 * 3600e9, 12 * 3600e9})
}

func TestFromTimesNaiveAndZoned(t *testing.T) {
	alloc := testutil.NewCheckedAllocator(t)
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	ts := []time.Time{time.Date(2024, 7, 1, 12, 0, 0, 0, ny)}
	naive, err := series.FromTimes("t", ts, nil, dtype.Microsecond, "", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer naive.Release()
	// Wall clock 12:00 is kept for naive columns.
	equalInts(t, int64sOf(t, naive), []int64{1719835200000000})
	zoned, err := series.FromTimes("t", ts, nil, dtype.Microsecond, "America/New_York", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer zoned.Release()
	// The instant (16:00 UTC) is stored for zoned columns.
	equalInts(t, int64sOf(t, zoned), []int64{1719849600000000})
	h, err := zoned.Dt().Hour(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	if v := h.Chunk(0).(*array.Int8).Value(0); v != 12 {
		t.Errorf("local hour = %d, want 12", v)
	}
}

func TestRollingByUnsortedRestoresOrder(t *testing.T) {
	alloc := testutil.NewCheckedAllocator(t)
	hours := []int{3, 0, 2, 1, 1}
	vals := make([]int64, len(hours))
	for i, h := range hours {
		vals[i] = int64(h) * 3600e6
	}
	by, err := series.FromDatetime("t", vals, nil, dtype.Microsecond, "", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer by.Release()
	v, err := series.FromFloat64("v", []float64{3, 0, 2, 1, 10}, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Release()
	out, err := v.RollingBy("sum", by, series.RollingByOptions{WindowSize: "1h", Closed: "both"}, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	// polars: [5.0, 0.0, 13.0, 11.0, 11.0]
	want := []float64{5, 0, 13, 11, 11}
	got := out.Chunk(0).(*array.Float64).Float64Values()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestTemporalScalarSeries(t *testing.T) {
	alloc := testutil.NewCheckedAllocator(t)
	ts, err := series.ScalarDate(2024, 2, 29)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ts.Series("d", 3, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	equalInts(t, int64sOf(t, s), []int64{19782, 19782, 19782})
	if _, err := series.ScalarDate(2023, 2, 29); err == nil {
		t.Error("invalid date should fail")
	}
	d := series.ScalarFromDuration(1500 * time.Nanosecond)
	if !d.DType.Equal(dtype.Duration(dtype.Nanosecond)) || d.Value != 1500 {
		t.Errorf("duration scalar %+v", d)
	}
}
