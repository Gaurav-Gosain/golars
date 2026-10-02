package series

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
)

// StrptimeOptions configures string to temporal parsing. The zero value
// is lenient; DefaultStrptimeOptions returns polars' defaults.
type StrptimeOptions struct {
	// Strict raises when a non-null string fails to parse; otherwise the
	// row becomes null.
	Strict bool
	// Exact requires the format to match the whole string. When false,
	// the first match anywhere in the string is used.
	Exact bool
	// Ambiguous resolves wall-clock times that occur twice when a time
	// zone is attached ("raise", "earliest", "latest", "null").
	Ambiguous string
}

// DefaultStrptimeOptions returns Strict=true, Exact=true, the polars
// defaults.
func DefaultStrptimeOptions() StrptimeOptions {
	return StrptimeOptions{Strict: true, Exact: true, Ambiguous: "raise"}
}

// failureError builds polars' strict-mode conversion error.
func failureError(a *array.String, name string, target dtype.DType, parsedValid []byte, srcValid func(int) bool) error {
	n := a.Len()
	var bad []string
	count := 0
	for i := range n {
		if srcValid(i) && !isValidBit(parsedValid, i) {
			count++
			if len(bad) < 10 {
				bad = append(bad, fmt.Sprintf("%q", a.Value(i)))
			}
		}
	}
	return fmt.Errorf("conversion from `str` to `%s` failed in column '%s' for %d out of %d values: [%s]",
		reprTemporal(target), name, count, n, strings.Join(bad, ", "))
}

func reprTemporal(dt dtype.DType) string {
	switch {
	case dt.IsDate():
		return "date"
	case dt.IsTime():
		return "time"
	case dt.IsDatetime():
		u, _ := dt.TimeUnit()
		us := u.String()
		if us == "us" {
			us = "μs"
		}
		if tz := dt.TimeZone(); tz != "" {
			return fmt.Sprintf("datetime[%s, %s]", us, tz)
		}
		return fmt.Sprintf("datetime[%s]", us)
	}
	return dt.String()
}

func (o StrOps) firstNonNull(a *array.String) (string, bool) {
	for i := range a.Len() {
		if a.IsValid(i) {
			return a.Value(i), true
		}
	}
	return "", false
}

// ToDate parses strings into Date values. An empty format infers the
// pattern from the first value (ISO "2021-12-31", "31/12/2021", ...).
func (o StrOps) ToDate(format string, so StrptimeOptions, opts ...Option) (*Series, error) {
	a, err := o.stringArr("to_date")
	if err != nil {
		return nil, err
	}
	cfg := resolve(opts)
	n := a.Len()
	var fm *temporal.Format
	pattern := temporal.PatternNone
	if format == "" {
		for i := range n {
			if !a.IsValid(i) {
				continue
			}
			if p := temporal.InferDatePattern(a.Value(i)); p != temporal.PatternNone {
				pattern = p
				break
			}
		}
		if pattern == temporal.PatternNone {
			if _, found := o.firstNonNull(a); found {
				return nil, fmt.Errorf("could not find an appropriate format to parse dates, please define a format")
			}
			return nullLikeTyped(o.s, dtype.Date(), cfg)
		}
	} else {
		if fm, err = compileStrptime(format); err != nil {
			return nil, err
		}
	}
	out, err := buildFixed(o.s.Name(), n, cfg.alloc, arrow.FixedWidthTypes.Date32, a, func(dst []int32, valid []byte) error {
		return forRanges(n, func(lo, hi int) error {
			var inf *temporal.Inferrer
			if fm == nil {
				inf = temporal.NewInferrer(pattern)
			}
			for i := lo; i < hi; i++ {
				if !isValidBit(valid, i) {
					dst[i] = 0
					continue
				}
				s := a.Value(i)
				var r temporal.ParseResult
				var ok bool
				switch {
				case inf != nil:
					r, ok = inf.Parse(s)
				case so.Exact:
					r, ok = fm.Parse(s)
				default:
					r, ok = fm.ParseSearch(s)
				}
				if !ok {
					clearValid(valid, i)
					dst[i] = 0
					continue
				}
				dst[i] = int32(r.Days)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return o.checkStrict(a, out, dtype.Date(), so.Strict)
}

func (o StrOps) checkStrict(a *array.String, out *Series, target dtype.DType, strict bool) (*Series, error) {
	if !strict || out.NullCount() == a.NullN() {
		return out, nil
	}
	arr := out.Chunk(0)
	vb := make([]byte, (arr.Len()+7)/8)
	copyBitmapInto(vb, arr, arr.Len())
	if arr.NullN() == 0 {
		for i := range vb {
			vb[i] = 0xFF
		}
	}
	err := failureError(a, o.s.Name(), target, vb, a.IsValid)
	out.Release()
	return nil, err
}

func nullLikeTyped(s *Series, dt dtype.DType, cfg config) (*Series, error) {
	if dt.IsDate() {
		return buildFixed(s.Name(), s.Len(), cfg.alloc, dt.Arrow(), nil, func(_ []int32, valid []byte) error {
			for i := range valid {
				valid[i] = 0
			}
			return nil
		})
	}
	return buildFixed(s.Name(), s.Len(), cfg.alloc, dt.Arrow(), nil, func(_ []int64, valid []byte) error {
		for i := range valid {
			valid[i] = 0
		}
		return nil
	})
}

// compileStrptime compiles a user format and applies polars' sanity
// checks on hour, minute and 12-hour directives.
func compileStrptime(format string) (*temporal.Format, error) {
	fm, err := temporal.CompileFormat(format)
	if err != nil {
		return nil, err
	}
	if fm.HasHour != fm.HasMinute {
		return nil, fmt.Errorf("invalid format string: please either specify both hour and minute, or neither")
	}
	return fm, nil
}

// ToDatetime parses strings into Datetime values. unit is "", "ms",
// "us" or "ns"; an empty unit is inferred from the format (%.9f gives
// ns, %.3f gives ms, else us). tz attaches a time zone: naive strings
// are localized into it, strings with an offset are converted.
func (o StrOps) ToDatetime(format, unit, tz string, so StrptimeOptions, opts ...Option) (*Series, error) {
	a, err := o.stringArr("to_datetime")
	if err != nil {
		return nil, err
	}
	cfg := resolve(opts)
	n := a.Len()
	var fm *temporal.Format
	pattern := temporal.PatternNone
	if format == "" {
		for i := range n {
			if !a.IsValid(i) {
				continue
			}
			if p := temporal.InferDatetimePattern(a.Value(i)); p != temporal.PatternNone {
				pattern = p
				break
			}
		}
	} else if fm, err = compileStrptime(format); err != nil {
		return nil, err
	}
	tu := dtype.Microsecond
	if unit != "" {
		var ok bool
		if tu, ok = dtype.ParseTimeUnit(unit); !ok || tu == dtype.Second {
			return nil, fmt.Errorf("invalid time unit %q", unit)
		}
	} else if fm != nil {
		switch fm.FracDigits {
		case 9:
			tu = dtype.Nanosecond
		case 3:
			tu = dtype.Millisecond
		}
	}
	if format == "" && pattern == temporal.PatternNone {
		if _, found := o.firstNonNull(a); found {
			return nil, fmt.Errorf("could not find an appropriate format to parse dates, please define a format")
		}
		return nullLikeTyped(o.s, dtype.Datetime(tu, tz), cfg)
	}
	tzAware := (fm != nil && fm.HasOffset) || pattern == temporal.PatternDatetimeYMDZ
	if pattern == temporal.PatternDatetimeYMDZ && tz == "" {
		return nil, fmt.Errorf("`strptime` / `to_datetime` was called with no format and no time zone, but a time zone is part of the data. This was previously allowed but led to unpredictable and erroneous results. Give a format string, set a time zone or perform the operation eagerly on a Series instead of on an Expr")
	}
	if tz != "" {
		if _, err := temporal.LoadLocation(tz); err != nil && tz != "UTC" {
			return nil, err
		}
	}
	outTZ := ""
	if tzAware {
		outTZ = tz
		if outTZ == "" {
			outTZ = "UTC"
		}
	}
	out, err := buildFixed(o.s.Name(), n, cfg.alloc, dtype.Datetime(tu, outTZ).Arrow(), a, func(dst []int64, valid []byte) error {
		return forRanges(n, func(lo, hi int) error {
			var inf *temporal.Inferrer
			if fm == nil {
				inf = temporal.NewInferrer(pattern)
			}
			for i := lo; i < hi; i++ {
				if !isValidBit(valid, i) {
					dst[i] = 0
					continue
				}
				s := a.Value(i)
				var r temporal.ParseResult
				var ok bool
				switch {
				case inf != nil:
					r, ok = inf.Parse(s)
				case so.Exact:
					r, ok = fm.Parse(s)
				default:
					r, ok = fm.ParseSearch(s)
				}
				if !ok || (tzAware && !r.HasOffset) {
					clearValid(valid, i)
					dst[i] = 0
					continue
				}
				dst[i] = r.Ticks(tu, tzAware)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	res, err := o.checkStrict(a, out, dtype.Datetime(tu, outTZ), so.Strict)
	if err != nil || tzAware || tz == "" {
		return res, err
	}
	defer res.Release()
	return res.Dt().ReplaceTimeZone(tz, so.Ambiguous, "raise", opts...)
}

// ToTime parses strings into Time values (nanosecond resolution). An
// empty format infers one of %T%.f, %T or %R from the first value.
func (o StrOps) ToTime(format string, so StrptimeOptions, opts ...Option) (*Series, error) {
	a, err := o.stringArr("to_time")
	if err != nil {
		return nil, err
	}
	cfg := resolve(opts)
	n := a.Len()
	var fm *temporal.Format
	if format == "" {
		for i := range n {
			if a.IsValid(i) {
				fm = temporal.InferTimeFormat(a.Value(i))
				if fm != nil {
					break
				}
			}
		}
		if fm == nil {
			if _, found := o.firstNonNull(a); found {
				return nil, fmt.Errorf("could not find an appropriate format to parse times, please define a format")
			}
			return nullLikeTyped(o.s, dtype.Time(dtype.Nanosecond), cfg)
		}
	} else if fm, err = compileStrptime(format); err != nil {
		return nil, err
	}
	out, err := buildFixed(o.s.Name(), n, cfg.alloc, dtype.Time(dtype.Nanosecond).Arrow(), a, func(dst []int64, valid []byte) error {
		return forRanges(n, func(lo, hi int) error {
			for i := lo; i < hi; i++ {
				if !isValidBit(valid, i) {
					dst[i] = 0
					continue
				}
				v, ok := fm.ParseTime(a.Value(i))
				if !ok {
					clearValid(valid, i)
					v = 0
				}
				dst[i] = v
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return o.checkStrict(a, out, dtype.Time(dtype.Nanosecond), so.Strict)
}

// Strptime parses strings into the target temporal dtype (Date,
// Datetime or Time). For Datetime the unit of to is used and its time
// zone is attached.
func (o StrOps) Strptime(to dtype.DType, format string, so StrptimeOptions, opts ...Option) (*Series, error) {
	switch {
	case to.IsDate():
		return o.ToDate(format, so, opts...)
	case to.IsDatetime():
		u, _ := to.TimeUnit()
		return o.ToDatetime(format, u.String(), to.TimeZone(), so, opts...)
	case to.IsTime():
		return o.ToTime(format, so, opts...)
	}
	return nil, fmt.Errorf("strptime: target dtype must be Date, Datetime or Time, got %s", to)
}
