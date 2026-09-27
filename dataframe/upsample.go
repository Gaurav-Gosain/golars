package dataframe

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// ErrNotTemporal is returned when Upsample is called on a non-datetime column.
var ErrNotTemporal = errors.New("dataframe: column is not a timestamp")

// ErrBadInterval is returned when the interval string cannot be parsed.
var ErrBadInterval = errors.New("dataframe: invalid interval string")

// Upsample returns a frame with rows interpolated at a regular
// interval between the first and last values of the named timestamp
// column. Missing time slots get null values for every other column.
// Mirrors polars' df.upsample for scalar intervals.
//
// The timestamp column must be sorted ascending; Upsample does not
// sort for you. Supported interval units: "ns", "us"/"μs", "ms",
// "s", "m", "h", "d". Month/year intervals are rejected because
// they are calendar-dependent.
//
// Example: df.Upsample(ctx, "ts", "1d") produces a dense daily
// frame from min(ts) through max(ts), left-joining source rows onto
// the grid.
func (df *DataFrame) Upsample(ctx context.Context, col string, every string) (*DataFrame, error) {
	idx, ok := df.sch.Index(col)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, col)
	}
	s := df.cols[idx]
	dt := s.DType()
	if dt.ID() != arrow.TIMESTAMP {
		return nil, fmt.Errorf("%w: %q has dtype %s", ErrNotTemporal, col, dt)
	}
	tst, ok := dt.Arrow().(*arrow.TimestampType)
	if !ok {
		return nil, fmt.Errorf("dataframe.Upsample: %q has no timestamp metadata", col)
	}
	stepNs, err := parseIntervalToUnit(every, tst.Unit)
	if err != nil {
		return nil, err
	}
	if stepNs <= 0 {
		return nil, fmt.Errorf("%w: %q must be positive", ErrBadInterval, every)
	}

	// Grab min/max from the sorted ts col. We don't verify sorting
	// at runtime here. Polars does the same; documented as a
	// caller invariant.
	ts := s.Chunk(0).(*array.Timestamp)
	if ts.Len() == 0 {
		return df.Clone(), nil
	}
	first := int64(ts.Value(0))
	last := int64(ts.Value(ts.Len() - 1))
	if last < first {
		return nil, errors.New("dataframe.Upsample: timestamp column is not sorted ascending")
	}

	// Build grid [first, first+step, ..., <= last].
	gridN := int((last-first)/stepNs) + 1
	gridVals := make([]int64, gridN)
	for i := 0; i < gridN; i++ {
		gridVals[i] = first + int64(i)*stepNs
	}

	// Materialise the grid as a Timestamp array so the result carries
	// the same dtype as the source col.
	mem := memory.DefaultAllocator
	tb := array.NewTimestampBuilder(mem, tst)
	for _, v := range gridVals {
		tb.Append(arrow.Timestamp(v))
	}
	gridArr := tb.NewArray()
	tb.Release()
	gridSer, err := series.New(col, gridArr)
	if err != nil {
		return nil, err
	}

	// Left-join the source onto the grid. The join key is the
	// timestamp column, which the join kernel handles via the
	// int64 underlying storage. Timestamps are stored as int64
	// with the Timestamp dtype tag.
	gridDF, err := New(gridSer)
	if err != nil {
		return nil, err
	}
	defer gridDF.Release()

	// The join key has to be a dtype the hash-join kernel
	// understands. Timestamps are physically int64 already, so we
	// reinterpret the buffer rather than calling compute.Cast (which
	// doesn't yet have a timestamp-to-int kernel).
	gridInt, err := reinterpretTimestampAsInt64(gridSer, col+"_gridts")
	if err != nil {
		return nil, err
	}
	gridKeyDF, err := gridDF.WithColumn(gridInt)
	if err != nil {
		gridInt.Release()
		return nil, err
	}
	defer gridKeyDF.Release()

	srcTS, _ := df.Column(col)
	srcInt, err := reinterpretTimestampAsInt64(srcTS, col+"_gridts")
	if err != nil {
		return nil, err
	}
	srcDF, err := df.Drop(col).WithColumn(srcInt)
	if err != nil {
		srcInt.Release()
		return nil, err
	}
	defer srcDF.Release()

	joined, err := gridKeyDF.Join(ctx, srcDF, []string{col + "_gridts"}, LeftJoin)
	if err != nil {
		return nil, err
	}
	defer joined.Release()
	// joined has: [col(original ts), col+"_gridts" (int), ...rest].
	// Drop the helper int64 col.
	clean := joined.Drop(col + "_gridts")
	return clean.Clone(), nil
}

// parseIntervalToUnit converts a shorthand interval like "1d",
// "500ms" into a count expressed in the target timestamp unit.
func parseIntervalToUnit(s string, unit arrow.TimeUnit) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("%w: %q", ErrBadInterval, s)
	}
	// Split numeric prefix from unit suffix.
	end := 0
	for end < len(s) && (s[end] >= '0' && s[end] <= '9') {
		end++
	}
	if end == 0 {
		return 0, fmt.Errorf("%w: %q", ErrBadInterval, s)
	}
	n, err := strconv.ParseInt(s[:end], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrBadInterval, s)
	}
	suffix := strings.ToLower(s[end:])
	// Convert to nanoseconds, then scale to the target unit.
	var ns int64
	switch suffix {
	case "ns":
		ns = n
	case "us", "μs":
		ns = n * 1_000
	case "ms":
		ns = n * 1_000_000
	case "s":
		ns = n * 1_000_000_000
	case "m":
		ns = n * 60 * 1_000_000_000
	case "h":
		ns = n * 3600 * 1_000_000_000
	case "d":
		ns = n * 86400 * 1_000_000_000
	case "w":
		ns = n * 7 * 86400 * 1_000_000_000
	case "y", "mo", "q":
		return 0, fmt.Errorf("%w: calendar-dependent unit %q not supported",
			ErrBadInterval, suffix)
	default:
		return 0, fmt.Errorf("%w: unknown unit %q", ErrBadInterval, suffix)
	}
	// Scale ns to the timestamp unit.
	switch unit {
	case arrow.Nanosecond:
		return ns, nil
	case arrow.Microsecond:
		return ns / 1_000, nil
	case arrow.Millisecond:
		return ns / 1_000_000, nil
	case arrow.Second:
		return ns / 1_000_000_000, nil
	}
	return 0, fmt.Errorf("dataframe.Upsample: unknown timestamp unit %s", unit)
}

// reinterpretTimestampAsInt64 returns a new Series that shares its
// value buffer with the input timestamp Series but carries an int64
// dtype. Timestamps are already stored as int64 under the hood, so
// this is a zero-copy tag swap. The input Series' reference is not
// consumed; the caller retains ownership.
func reinterpretTimestampAsInt64(s *series.Series, newName string) (*series.Series, error) {
	ch := s.ToArrowChunked()
	defer ch.Release()
	newChunks := make([]arrow.Array, 0, len(ch.Chunks()))
	for _, chunk := range ch.Chunks() {
		ts, ok := chunk.(*array.Timestamp)
		if !ok {
			return nil, fmt.Errorf("reinterpretTimestamp: chunk is %T", chunk)
		}
		data := ts.Data()
		bufs := data.Buffers()
		for _, b := range bufs {
			if b != nil {
				b.Retain()
			}
		}
		intData := array.NewData(arrow.PrimitiveTypes.Int64, data.Len(),
			bufs, nil, data.NullN(), data.Offset())
		arr := array.MakeFromData(intData)
		intData.Release()
		newChunks = append(newChunks, arr)
	}
	return series.New(newName, newChunks...)
}
