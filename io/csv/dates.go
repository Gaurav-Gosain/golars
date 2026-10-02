package csv

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
	"github.com/Gaurav-Gosain/golars/series"
)

// WithTryParseDates mirrors polars' read_csv(try_parse_dates=...).
//
// true parses string columns whose values all look like dates, times or
// datetimes (ISO "2021-12-31", "31/12/2021", "2021-12-31 10:00",
// offsets such as "+01:00" giving a UTC column) into Date, Time and
// Datetime[us] columns. false keeps every column that is not numeric or
// boolean as String, like polars' default. When the option is not set
// the reader keeps its historical behaviour of letting arrow infer ISO
// dates and timestamps.
func WithTryParseDates(on bool) Option {
	return func(c *config) { c.tryParseDates = &on }
}

func resetTryParseDates() Option {
	return func(c *config) { c.tryParseDates = nil }
}

func withColumnTypes(types map[string]arrow.DataType) Option {
	return func(c *config) { c.columnTypes = types }
}

// readTryParseDates buffers the input so it can be re-read with
// temporal columns forced to String (try_parse_dates=false) or runs the
// polars inference over string columns (try_parse_dates=true).
func readTryParseDates(ctx context.Context, r io.Reader, cfg config, opts []Option) (*dataframe.DataFrame, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("csv: read: %w", err)
	}
	base := append(append([]Option(nil), opts...), resetTryParseDates())
	df, err := Read(ctx, bytes.NewReader(data), base...)
	if err != nil {
		return nil, err
	}
	if !*cfg.tryParseDates {
		types := map[string]arrow.DataType{}
		for _, c := range df.Columns() {
			if c.DType().IsTemporal() {
				types[c.Name()] = arrow.BinaryTypes.String
			}
		}
		if len(types) == 0 {
			return df, nil
		}
		df.Release()
		return Read(ctx, bytes.NewReader(data), append(base, withColumnTypes(types))...)
	}
	defer df.Release()
	opt := series.WithAllocator(cfg.alloc)
	cols := make([]*series.Series, 0, df.Width())
	release := func() {
		for _, c := range cols {
			c.Release()
		}
	}
	for _, c := range df.Columns() {
		var out *series.Series
		switch {
		case c.DType().IsDatetime():
			if u, _ := c.DType().TimeUnit(); u != dtype.Microsecond {
				out, err = c.Dt().CastTimeUnit(dtype.Microsecond, opt)
			}
		case c.DType().IsTime():
			if u, _ := c.DType().TimeUnit(); u != dtype.Nanosecond {
				out, err = compute.Cast(ctx, c, dtype.Time(dtype.Nanosecond), compute.WithAllocator(cfg.alloc))
			}
		case c.DType().IsString():
			out, err = inferTemporalColumn(c, opt)
		}
		if err != nil {
			release()
			return nil, err
		}
		if out == nil {
			out = c.Clone()
		}
		cols = append(cols, out)
	}
	res, err := dataframe.New(cols...)
	if err != nil {
		release()
		return nil, err
	}
	return res, nil
}

// inferTemporalColumn converts a string column to Date, Time or
// Datetime when every non-null value parses; otherwise it returns nil.
func inferTemporalColumn(s *series.Series, opt series.Option) (*series.Series, error) {
	first := ""
	found := false
	for i := range s.Len() {
		if s.Chunk(0).IsValid(i) {
			first = s.Chunk(0).(*array.String).Value(i)
			found = true
			break
		}
	}
	if !found {
		return nil, nil
	}
	lenient := series.StrptimeOptions{Exact: true}
	try := func(out *series.Series, err error) (*series.Series, error) {
		if err != nil {
			return nil, nil
		}
		if out.NullCount() != s.NullCount() {
			out.Release()
			return nil, nil
		}
		return out, nil
	}
	if temporal.InferDatePattern(first) != temporal.PatternNone {
		if out, _ := try(s.Str().ToDate("", lenient, opt)); out != nil {
			return out, nil
		}
	}
	if temporal.InferTimeFormat(first) != nil {
		if out, _ := try(s.Str().ToTime("", lenient, opt)); out != nil {
			return out, nil
		}
	}
	switch temporal.InferDatetimePattern(first) {
	case temporal.PatternNone:
		return nil, nil
	case temporal.PatternDatetimeYMDZ:
		return try(s.Str().ToDatetime("", "us", "UTC", lenient, opt))
	}
	return try(s.Str().ToDatetime("", "us", "", lenient, opt))
}
