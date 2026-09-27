package series_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestTemporalParallelKernels drives the chunked parallel paths (above
// the 32K-row cutoff) with a zoned column and checks every row against
// the time package. Run under -race it also guards the per-goroutine
// zone caches.
func TestTemporalParallelKernels(t *testing.T) {
	alloc := testutil.NewCheckedAllocator(t)
	const n = 200_000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = 1_500_000_000_000_000 + int64(i)*7_919_000_003%315_360_000_000_000
	}
	s, err := series.FromDatetime("t", vals, nil, dtype.Microsecond, "Europe/Berlin", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	loc, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Fatal(err)
	}
	hours, err := s.Dt().Hour(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer hours.Release()
	strs, err := s.Dt().Strftime("%Y-%m-%d %H:%M:%S%z", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer strs.Release()
	naive, err := s.Dt().ReplaceTimeZone("", "raise", "raise", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer naive.Release()
	back, err := naive.Dt().ReplaceTimeZone("Europe/Berlin", "earliest", "null", series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer back.Release()
	parsed, err := strs.Str().ToDatetime("%Y-%m-%d %H:%M:%S%z", "us", "Europe/Berlin", series.DefaultStrptimeOptions(), series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer parsed.Release()
	h := hours.Chunk(0).(*array.Int8)
	str := strs.Chunk(0).(*array.String)
	nv := naive.Chunk(0).(*array.Timestamp)
	pv := parsed.Chunk(0).(*array.Timestamp)
	for i, v := range vals {
		tm := time.UnixMicro(v).In(loc)
		if int(h.Value(i)) != tm.Hour() {
			t.Fatalf("row %d hour = %d, want %d", i, h.Value(i), tm.Hour())
		}
		if want := tm.Format("2006-01-02 15:04:05-0700"); str.Value(i) != want {
			t.Fatalf("row %d strftime = %q, want %q", i, str.Value(i), want)
		}
		_, off := tm.Zone()
		if int64(nv.Value(i)) != v+int64(off)*1_000_000 {
			t.Fatalf("row %d naive = %d", i, nv.Value(i))
		}
		if want := v - v%1_000_000; int64(pv.Value(i)) != want {
			t.Fatalf("row %d parsed = %d, want %d", i, pv.Value(i), want)
		}
	}
}
