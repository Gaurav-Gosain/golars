package series_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestStructJSONEncodeTemporal(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	us := []int64{
		base.Add(123 * time.Millisecond).UnixMicro(),
		base.UnixMicro(),
		time.Date(1960, 1, 2, 3, 4, 5, 1000, time.UTC).UnixMicro(),
	}
	// Expected strings come from polars 1.39.3.
	cases := []struct {
		tz   string
		want []string
	}{
		{"", []string{"2024-01-02 03:04:05.123", "2024-01-02 03:04:05", "1960-01-02 03:04:05.000001"}},
		{"UTC", []string{"2024-01-02T03:04:05.123+00:00", "2024-01-02T03:04:05+00:00", "1960-01-02T03:04:05.000001+00:00"}},
	}
	for _, c := range cases {
		tb := array.NewTimestampBuilder(mem, dtype.Datetime(dtype.Microsecond, c.tz).Arrow().(*arrow.TimestampType))
		for _, v := range us {
			tb.Append(arrow.Timestamp(v))
		}
		ts, err := series.New("x", tb.NewArray())
		tb.Release()
		if err != nil {
			t.Fatal(err)
		}
		st := jsonStructOf(t, ts)
		ts.Release()
		out, err := st.Struct().JSONEncode(opt)
		st.Release()
		if err != nil {
			t.Fatal(err)
		}
		a := out.Chunk(0).(*array.String)
		for i, w := range c.want {
			if got := a.Value(i); got != `{"x":"`+w+`"}` {
				t.Errorf("tz %q row %d: got %s want %s", c.tz, i, got, w)
			}
		}
		out.Release()
	}

	db := array.NewDate32Builder(mem)
	db.Append(-1)
	d, err := series.New("x", db.NewArray())
	db.Release()
	if err != nil {
		t.Fatal(err)
	}
	st := jsonStructOf(t, d)
	d.Release()
	out, err := st.Struct().JSONEncode(opt)
	st.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if got := out.Chunk(0).(*array.String).Value(0); got != `{"x":"1969-12-31"}` {
		t.Errorf("date: got %s", got)
	}
}

func TestStructJSONEncodeWrongDType(t *testing.T) {
	s, _ := series.FromInt64("i", []int64{1}, nil)
	defer s.Release()
	if _, err := s.Struct().JSONEncode(); err == nil {
		t.Error("json_encode on int should error")
	}
}
