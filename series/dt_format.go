package series

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
)

// buildStrings builds a String array of n rows. appendRow appends row
// i to dst and reports whether the row is valid. Rows are formatted in
// parallel chunks, each into its own byte slice, then stitched into a
// single allocator-backed buffer.
func buildStrings(name string, n int, mem memory.Allocator, src arrow.Array, newWorker func() func(i int, dst []byte) ([]byte, bool, error)) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	chunks := 1
	if n >= temporalParallelCutoff {
		chunks = min(pool.DefaultParallelism()*2, (n+8191)/8192)
	}
	per := ((n+chunks-1)/max(chunks, 1) + 63) &^ 63
	if per == 0 {
		per = 64
	}
	chunks = (n + per - 1) / per
	type part struct {
		data []byte
		ends []int32
	}
	parts := make([]part, chunks)
	valid := newValidity(n, src, mem)
	vb := valid.Bytes()
	err := pool.ParallelFor(context.Background(), chunks, 0, func(_ context.Context, s, e int) error {
		for c := s; c < e; c++ {
			lo, hi := c*per, min((c+1)*per, n)
			fn := newWorker()
			p := part{data: make([]byte, 0, (hi-lo)*16), ends: make([]int32, hi-lo)}
			for i := lo; i < hi; i++ {
				if isValidBit(vb, i) {
					var ok bool
					var err error
					p.data, ok, err = fn(i, p.data)
					if err != nil {
						return err
					}
					if !ok {
						clearValid(vb, i)
					}
				}
				p.ends[i-lo] = int32(len(p.data))
			}
			parts[c] = p
		}
		return nil
	})
	if err != nil {
		valid.Release()
		return nil, err
	}
	total := 0
	for _, p := range parts {
		total += len(p.data)
	}
	offBuf := memory.NewResizableBuffer(mem)
	offBuf.Resize((n + 1) * arrow.Int32SizeBytes)
	defer offBuf.Release()
	dataBuf := memory.NewResizableBuffer(mem)
	dataBuf.Resize(total)
	defer dataBuf.Release()
	offs := unsafe.Slice((*int32)(unsafe.Pointer(&offBuf.Bytes()[0])), n+1)
	data := dataBuf.Bytes()
	pos := 0
	row := 0
	offs[0] = 0
	for _, p := range parts {
		copy(data[pos:], p.data)
		for _, e := range p.ends {
			offs[row+1] = int32(pos) + e
			row++
		}
		pos += len(p.data)
	}
	vbuf, nulls := finishValidity(valid, n)
	if vbuf != nil {
		defer vbuf.Release()
	}
	ad := array.NewData(arrow.BinaryTypes.String, n, []*memory.Buffer{vbuf, offBuf, dataBuf}, nil, nulls, 0)
	defer ad.Release()
	return New(name, array.MakeFromData(ad))
}

// defaultTemporalFormat returns the format polars' to_string and
// cast(String) use for a dtype.
func defaultTemporalFormat(info tinfo) string {
	switch info.kind {
	case tkDate:
		return "%Y-%m-%d"
	case tkTime:
		return "%T%.f"
	case tkDatetime:
		f := "%Y-%m-%d %H:%M:%S"
		switch info.unit {
		case dtype.Millisecond:
			f += "%.3f"
		case dtype.Microsecond:
			f += "%.6f"
		case dtype.Nanosecond:
			f += "%.9f"
		}
		if info.tz != "" {
			f += "%:z"
		}
		return f
	}
	return ""
}

// Strftime formats Date, Datetime and Time values with a chrono format
// string (the dialect polars uses: %Y, %m, %d, %H, %M, %S, %.f, %z,
// ...). Durations accept "iso" or "polars" (see ToString).
func (o DtOps) Strftime(format string, opts ...Option) (*Series, error) {
	info := o.info()
	if info.kind == tkDuration {
		return o.durationToString(format, opts)
	}
	if info.kind == tkNone {
		return nil, errTemporalOp("strftime", o.s.DType())
	}
	if format == "iso" || format == "iso:strict" {
		format = defaultTemporalFormat(info)
		if info.kind == tkDatetime {
			format = "%Y-%m-%dT%H:%M:%S%.f"
			if info.tz != "" {
				format += "%:z"
			}
		}
	}
	if info.kind == tkDate && formatNeedsTime(format) {
		// chrono cannot render a time field from a date; polars raises.
		return nil, fmt.Errorf("cannot format Date with format '%s'", format)
	}
	fm, err := temporal.CompileFormat(format)
	if err != nil {
		return nil, err
	}
	cfg := resolve(opts)
	arr := o.s.Chunk(0)
	n := arr.Len()
	vals := physicalInt64(arr)
	base, err := zoneFor(info.tz)
	if err != nil {
		return nil, err
	}
	per := temporal.UnitsPerSecond(info.unit)
	tzName := info.tz
	return buildStrings(o.s.Name(), n, cfg.alloc, arr, func() func(i int, dst []byte) ([]byte, bool, error) {
		z := base.Clone()
		var f temporal.Fields
		return func(i int, dst []byte) ([]byte, bool, error) {
			v := vals[i]
			switch info.kind {
			case tkDate:
				f.SetDays(v)
			case tkTime:
				f.SetTimeOfDay(v * temporal.NsPerUnit(info.unit))
			default:
				if tzName != "" {
					f.HasTZ = true
					if z != nil {
						off, abbr := z.Offset(temporal.FloorDiv(v, per))
						f.Offset, f.Abbr = off, abbr
						f.SetLocal(v+int64(off)*per, v, per)
					} else {
						f.Offset, f.Abbr = 0, "UTC"
						f.SetLocal(v, v, per)
					}
				} else {
					f.SetLocal(v, v, per)
				}
			}
			out, err := fm.Append(dst, &f)
			if err != nil {
				return dst, false, err
			}
			return out, true, nil
		}
	})
}

// ToString formats with a default format per dtype when format is
// empty, matching polars' dt.to_string(). Durations accept "iso"
// (default) and "polars".
func (o DtOps) ToString(format string, opts ...Option) (*Series, error) {
	info := o.info()
	if info.kind == tkDuration {
		if format == "" {
			format = "iso"
		}
		return o.durationToString(format, opts)
	}
	if format == "" {
		if info.kind == tkNone {
			return nil, errTemporalOp("to_string", o.s.DType())
		}
		format = defaultTemporalFormat(info)
	}
	return o.Strftime(format, opts...)
}

func (o DtOps) durationToString(format string, opts []Option) (*Series, error) {
	info := o.info()
	if format != "iso" && format != "polars" && format != "iso:strict" {
		return nil, fmt.Errorf("invalid duration format %q: expected 'iso' or 'polars'", format)
	}
	cfg := resolve(opts)
	arr := o.s.Chunk(0)
	vals := physicalInt64(arr)
	unitNs := temporal.NsPerUnit(info.unit)
	return buildStrings(o.s.Name(), arr.Len(), cfg.alloc, arr, func() func(i int, dst []byte) ([]byte, bool, error) {
		return func(i int, dst []byte) ([]byte, bool, error) {
			if format == "polars" {
				return appendPolarsDuration(dst, vals[i], info.unit), true, nil
			}
			return appendISODuration(dst, vals[i]*unitNs), true, nil
		}
	})
}

// appendISODuration renders ns as an ISO 8601 duration (P1DT2H3M4.5S).
func appendISODuration(dst []byte, ns int64) []byte {
	if ns == 0 {
		return append(dst, "PT0S"...)
	}
	if ns < 0 {
		dst = append(dst, '-')
		ns = -ns
	}
	dst = append(dst, 'P')
	days := ns / temporal.NsDay
	rem := ns % temporal.NsDay
	if days > 0 {
		dst = strconv.AppendInt(dst, days, 10)
		dst = append(dst, 'D')
	}
	if rem == 0 {
		return dst
	}
	dst = append(dst, 'T')
	h := rem / temporal.NsHour
	m := rem / temporal.NsMinute % 60
	s := rem / temporal.NsSecond % 60
	frac := rem % temporal.NsSecond
	if h > 0 {
		dst = strconv.AppendInt(dst, h, 10)
		dst = append(dst, 'H')
	}
	if m > 0 {
		dst = strconv.AppendInt(dst, m, 10)
		dst = append(dst, 'M')
	}
	if s > 0 || frac > 0 {
		dst = strconv.AppendInt(dst, s, 10)
		if frac > 0 {
			dst = temporal.AppendFracAuto(dst, int(frac))
		}
		dst = append(dst, 'S')
	}
	return dst
}

// appendPolarsDuration renders v (ticks of u) the way polars prints
// durations: "1d 2h 3m 4s 5µs", with every part carrying the sign.
func appendPolarsDuration(dst []byte, v int64, u dtype.TimeUnit) []byte {
	smallest := "ns"
	switch u {
	case dtype.Millisecond:
		smallest = "ms"
	case dtype.Microsecond:
		smallest = "µs"
	}
	if v == 0 {
		dst = append(dst, '0')
		return append(dst, smallest...)
	}
	ns := v * temporal.NsPerUnit(u)
	neg := ns < 0
	if neg {
		ns = -ns
	}
	parts := []struct {
		size int64
		name string
	}{
		{temporal.NsDay, "d"}, {temporal.NsHour, "h"}, {temporal.NsMinute, "m"}, {temporal.NsSecond, "s"},
		{temporal.NsMillisecond, "ms"}, {temporal.NsMicrosecond, "µs"}, {1, "ns"},
	}
	first := true
	for _, p := range parts {
		if p.size < temporal.NsPerUnit(u) {
			break
		}
		q := ns / p.size
		ns %= p.size
		if q == 0 {
			continue
		}
		if !first {
			dst = append(dst, ' ')
		}
		first = false
		if neg {
			dst = append(dst, '-')
		}
		dst = strconv.AppendInt(dst, q, 10)
		dst = append(dst, p.name...)
	}
	return dst
}

// formatNeedsTime reports whether a chrono format string uses a time
// of day, fraction or zone directive, which a Date cannot supply.
func formatNeedsTime(format string) bool {
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 >= len(format) {
			continue
		}
		j := i + 1
		for j < len(format) && strings.ContainsRune("-_0^#.:3469", rune(format[j])) {
			j++
		}
		if j >= len(format) {
			break
		}
		if format[j] == '%' {
			i = j
			continue
		}
		if strings.ContainsRune("HkIlPpMSfRTXrscZz", rune(format[j])) {
			return true
		}
		i = j
	}
	return false
}
