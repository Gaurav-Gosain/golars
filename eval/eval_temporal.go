package eval

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
	"github.com/Gaurav-Gosain/golars/series"
)

var temporalFuncs = map[string]bool{
	"str.to_date": true, "str.to_datetime": true, "str.to_time": true, "str.strptime": true,
	"date": true, "datetime": true, "duration": true,
	"date_range": true, "datetime_range": true, "time_range": true,
	"rolling_sum_by": true, "rolling_mean_by": true, "rolling_min_by": true, "rolling_max_by": true,
	"rolling_std_by": true, "rolling_var_by": true, "rolling_median_by": true, "rolling_quantile_by": true,
}

// isTemporalFunction reports whether the FunctionNode belongs to the
// temporal family handled by evalTemporalFunction.
func isTemporalFunction(name string) bool {
	return strings.HasPrefix(name, "dt.") || temporalFuncs[name]
}

// litToScalar converts a temporal literal to a typed scalar.
func litToScalar(l expr.LitNode) (series.TemporalScalar, bool, error) {
	if !l.DType.IsTemporal() {
		return series.TemporalScalar{}, false, nil
	}
	if l.Value == nil {
		return series.TemporalScalar{DType: l.DType, Null: true}, true, nil
	}
	switch v := l.Value.(type) {
	case time.Time:
		if !l.DType.IsDatetime() {
			if l.DType.IsDate() {
				y, m, d := v.Date()
				ts, err := series.ScalarDate(y, int(m), d)
				return ts, true, err
			}
			break
		}
		u, _ := l.DType.TimeUnit()
		tz := l.DType.TimeZone()
		ts := series.TemporalScalar{
			DType:      l.DType,
			Value:      series.TimeToTicks(v, u, tz == ""),
			FromGoTime: tz == "",
			Instant:    series.TimeToTicks(v, u, false),
		}
		return ts, true, nil
	case time.Duration:
		u, _ := l.DType.TimeUnit()
		return series.TemporalScalar{DType: l.DType, Value: int64(v) / temporal.NsPerUnit(u)}, true, nil
	case expr.DateValue:
		ts, err := series.ScalarDate(v.Year, v.Month, v.Day)
		return ts, true, err
	case expr.TimeOfDayValue:
		u, _ := l.DType.TimeUnit()
		return series.TemporalScalar{DType: l.DType, Value: v.Nanos / temporal.NsPerUnit(u)}, true, nil
	case int64:
		return series.TemporalScalar{DType: l.DType, Value: v}, true, nil
	}
	return series.TemporalScalar{}, true, fmt.Errorf("eval: unsupported %s literal %T", l.DType, l.Value)
}

// temporalLiteralSeries materializes a temporal literal as n rows.
func temporalLiteralSeries(l expr.LitNode, n int, ec EvalContext) (*series.Series, bool, error) {
	ts, ok, err := litToScalar(l)
	if !ok || err != nil {
		return nil, ok, err
	}
	s, err := ts.Series("literal", n, series.WithAllocator(ec.Alloc))
	return s, true, err
}

// evalTemporalBinary handles binary nodes where one side is a temporal
// literal: the literal stays a length-1 Series that compute broadcasts.
// A literal built from a Go time.Time names an instant, so against a
// zoned column it is re-expressed in that zone instead of raising a
// time zone mismatch.
func evalTemporalBinary(ctx context.Context, ec EvalContext, n expr.BinaryNode, df *dataframe.DataFrame) (*series.Series, bool, error) {
	ll, lok := n.Left.Node().(expr.LitNode)
	rl, rok := n.Right.Node().(expr.LitNode)
	lTemporal := lok && ll.DType.IsTemporal()
	rTemporal := rok && rl.DType.IsTemporal()
	if !lTemporal && !rTemporal {
		return nil, false, nil
	}
	var other *series.Series
	var err error
	switch {
	case lTemporal && !rok:
		other, err = evalNode(ctx, ec, n.Right, df)
	case rTemporal && !lok:
		other, err = evalNode(ctx, ec, n.Left, df)
	}
	if err != nil {
		return nil, true, err
	}
	if other != nil {
		defer other.Release()
	}
	litSeries := func(l expr.LitNode) (*series.Series, error) {
		if !l.DType.IsTemporal() {
			return literalSeries(l, 1, ec.Alloc)
		}
		ts, _, err := litToScalar(l)
		if err != nil {
			return nil, err
		}
		if ts.FromGoTime && other != nil {
			if tz := other.DType().TimeZone(); tz != "" && other.DType().IsDatetime() {
				u, _ := ts.DType.TimeUnit()
				ts = series.TemporalScalar{DType: dtype.Datetime(u, tz), Value: ts.Instant}
			}
		}
		return ts.Series("literal", 1, series.WithAllocator(ec.Alloc))
	}
	var left, right *series.Series
	if lok {
		if left, err = litSeries(ll); err != nil {
			return nil, true, err
		}
	} else {
		left = other.Clone()
	}
	defer left.Release()
	if rok {
		if right, err = litSeries(rl); err != nil {
			return nil, true, err
		}
	} else {
		right = other.Clone()
	}
	defer right.Release()
	name := left.Name()
	if lok && !rok {
		name = right.Name()
	}
	opts := append(kernelOpts(ec), compute.WithName(name))
	out, err := applyBinary(ctx, n.Op, left, right, opts)
	return out, true, err
}

func applyBinary(ctx context.Context, op expr.BinaryOp, l, r *series.Series, opts []compute.Option) (*series.Series, error) {
	switch op {
	case expr.OpAdd:
		return compute.Add(ctx, l, r, opts...)
	case expr.OpSub:
		return compute.Sub(ctx, l, r, opts...)
	case expr.OpMul:
		return compute.Mul(ctx, l, r, opts...)
	case expr.OpDiv:
		return compute.Div(ctx, l, r, opts...)
	case expr.OpEq:
		return compute.Eq(ctx, l, r, opts...)
	case expr.OpNe:
		return compute.Ne(ctx, l, r, opts...)
	case expr.OpLt:
		return compute.Lt(ctx, l, r, opts...)
	case expr.OpLe:
		return compute.Le(ctx, l, r, opts...)
	case expr.OpGt:
		return compute.Gt(ctx, l, r, opts...)
	case expr.OpGe:
		return compute.Ge(ctx, l, r, opts...)
	}
	return nil, fmt.Errorf("eval: operator %d not supported on temporal operands", op)
}

// promoteExtra handles operand pairs promoteBinary does not: temporal
// operands (compute resolves their supertype) and mixed integer widths.
func promoteExtra(ctx context.Context, ec EvalContext, left, right *series.Series) (*series.Series, *series.Series, bool, error) {
	ld, rd := left.DType(), right.DType()
	if ld.IsTemporal() || rd.IsTemporal() {
		return left, right, true, nil
	}
	if !ld.IsNumeric() || !rd.IsNumeric() {
		return nil, nil, false, nil
	}
	target := dtype.Int64()
	if ld.IsFloating() || rd.IsFloating() {
		target = dtype.Float64()
	}
	cast := func(s *series.Series) (*series.Series, error) {
		if s.DType().Equal(target) {
			return s, nil
		}
		return compute.Cast(ctx, s, target, kernelOpts(ec)...)
	}
	l, err := cast(left)
	if err != nil {
		return nil, nil, true, err
	}
	r, err := cast(right)
	if err != nil {
		if l != left {
			l.Release()
		}
		return nil, nil, true, err
	}
	return l, r, true, nil
}

// ---- function dispatch ----

func evalTemporalFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	switch n.Name {
	case "date", "datetime", "duration":
		return evalTemporalCtor(ctx, ec, n, df)
	case "date_range", "datetime_range", "time_range":
		return evalRange(ctx, ec, n, df)
	}
	if strings.HasPrefix(n.Name, "rolling_") {
		return evalRollingBy(ctx, ec, n, df)
	}
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("eval: function %q requires at least one argument", n.Name)
	}
	arg0, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer arg0.Release()
	opt := seriesAlloc(ec)
	if strings.HasPrefix(n.Name, "str.") {
		return evalStrTemporal(arg0, n, opt)
	}
	d := arg0.Dt()
	p := func(i int) any {
		if i < len(n.Params) {
			return n.Params[i]
		}
		return nil
	}
	ps := func(i int) string { s, _ := p(i).(string); return s }
	pb := func(i int) bool { b, _ := p(i).(bool); return b }
	switch n.Name {
	case "dt.year":
		return d.Year(opt)
	case "dt.iso_year":
		return d.IsoYear(opt)
	case "dt.quarter":
		return d.Quarter(opt)
	case "dt.month":
		return d.Month(opt)
	case "dt.week":
		return d.Week(opt)
	case "dt.weekday":
		return d.Weekday(opt)
	case "dt.day":
		return d.Day(opt)
	case "dt.ordinal_day":
		return d.OrdinalDay(opt)
	case "dt.is_leap_year":
		return d.IsLeapYear(opt)
	case "dt.days_in_month":
		return d.DaysInMonth(opt)
	case "dt.century":
		return d.Century(opt)
	case "dt.millennium":
		return d.Millennium(opt)
	case "dt.hour":
		return d.Hour(opt)
	case "dt.minute":
		return d.Minute(opt)
	case "dt.second":
		return d.Second(opt)
	case "dt.millisecond":
		return d.Millisecond(opt)
	case "dt.microsecond":
		return d.Microsecond(opt)
	case "dt.nanosecond":
		return d.Nanosecond(opt)
	case "dt.date":
		return d.Date(opt)
	case "dt.time":
		return d.Time(opt)
	case "dt.datetime":
		return d.Datetime(opt)
	case "dt.epoch":
		return d.Epoch(ps(0), opt)
	case "dt.timestamp":
		return d.Timestamp(ps(0), opt)
	case "dt.total_days":
		return d.TotalDays(pb(0), opt)
	case "dt.total_hours":
		return d.TotalHours(pb(0), opt)
	case "dt.total_minutes":
		return d.TotalMinutes(pb(0), opt)
	case "dt.total_seconds":
		return d.TotalSeconds(pb(0), opt)
	case "dt.total_milliseconds":
		return d.TotalMilliseconds(pb(0), opt)
	case "dt.total_microseconds":
		return d.TotalMicroseconds(pb(0), opt)
	case "dt.total_nanoseconds":
		return d.TotalNanoseconds(pb(0), opt)
	case "dt.cast_time_unit", "dt.with_time_unit":
		u, ok := p(0).(dtype.TimeUnit)
		if !ok {
			return nil, fmt.Errorf("eval: %s: expected a time unit", n.Name)
		}
		if n.Name == "dt.cast_time_unit" {
			return d.CastTimeUnit(u, opt)
		}
		return d.WithTimeUnit(u, opt)
	case "dt.strftime":
		return d.Strftime(ps(0), opt)
	case "dt.to_string":
		return d.ToString(ps(0), opt)
	case "dt.truncate":
		return d.Truncate(ps(0), opt)
	case "dt.round":
		return d.Round(ps(0), opt)
	case "dt.offset_by":
		if len(n.Args) > 1 {
			by, err := evalNode(ctx, ec, n.Args[1], df)
			if err != nil {
				return nil, err
			}
			defer by.Release()
			return d.OffsetBySeries(by, opt)
		}
		return d.OffsetBy(ps(0), opt)
	case "dt.month_start":
		return d.MonthStart(opt)
	case "dt.month_end":
		return d.MonthEnd(opt)
	case "dt.replace_time_zone":
		return d.ReplaceTimeZone(ps(0), ps(1), ps(2), opt)
	case "dt.convert_time_zone":
		return d.ConvertTimeZone(ps(0))
	case "dt.combine":
		tm, err := evalNode(ctx, ec, n.Args[1], df)
		if err != nil {
			return nil, err
		}
		defer tm.Release()
		u, _ := p(0).(dtype.TimeUnit)
		if u == dtype.Second {
			u = dtype.Microsecond
		}
		return d.Combine(tm, u, opt)
	case "dt.replace":
		return evalDtReplace(ctx, ec, n, df, d, opt)
	case "dt.is_business_day", "dt.add_business_days":
		b, _ := p(0).(expr.BusinessDayArgs)
		bd := series.BusinessDayOptions{WeekMask: b.WeekMask, Roll: b.Roll}
		for _, h := range b.Holidays {
			bd.Holidays = append(bd.Holidays, series.DateToDays(h.Year, h.Month, h.Day))
		}
		if n.Name == "dt.is_business_day" {
			return d.IsBusinessDay(bd, opt)
		}
		cnt, err := evalNode(ctx, ec, n.Args[1], df)
		if err != nil {
			return nil, err
		}
		defer cnt.Release()
		return d.AddBusinessDays(cnt, bd, opt)
	}
	return nil, fmt.Errorf("eval: unknown temporal function %q", n.Name)
}

func evalStrTemporal(s *series.Series, n expr.FunctionNode, opt series.Option) (*series.Series, error) {
	str := s.Str()
	convert := func(o expr.StrptimeOptions) series.StrptimeOptions {
		return series.StrptimeOptions{Strict: o.Strict, Exact: o.Exact, Ambiguous: o.Ambiguous}
	}
	switch n.Name {
	case "str.strptime":
		to, _ := n.Params[0].(dtype.DType)
		format, _ := n.Params[1].(string)
		o, _ := n.Params[2].(expr.StrptimeOptions)
		return str.Strptime(to, format, convert(o), opt)
	}
	format, _ := n.Params[0].(string)
	o, _ := n.Params[1].(expr.StrptimeOptions)
	switch n.Name {
	case "str.to_date":
		return str.ToDate(format, convert(o), opt)
	case "str.to_datetime":
		return str.ToDatetime(format, o.TimeUnit, o.TimeZone, convert(o), opt)
	}
	return str.ToTime(format, convert(o), opt)
}

func evalDtReplace(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame, d series.DtOps, opt series.Option) (*series.Series, error) {
	var parts [7]*series.Series
	next := 1
	defer func() {
		for _, s := range parts {
			if s != nil {
				s.Release()
			}
		}
	}()
	for k := range 7 {
		if b, _ := n.Params[k].(bool); !b {
			continue
		}
		s, err := evalNode(ctx, ec, n.Args[next], df)
		if err != nil {
			return nil, err
		}
		next++
		parts[k] = s
	}
	amb, _ := n.Params[7].(string)
	return d.Replace(series.ReplaceParts{
		Year: parts[0], Month: parts[1], Day: parts[2], Hour: parts[3],
		Minute: parts[4], Second: parts[5], Microsecond: parts[6], Ambiguous: amb,
	}, opt)
}

// evalArgs evaluates the masked optional args of a constructor. present
// lists which of the len(mask) slots have an Expr in args (in order).
func evalMaskedArgs(ctx context.Context, ec EvalContext, df *dataframe.DataFrame, args []expr.Expr, mask []any) ([]*series.Series, func(), error) {
	out := make([]*series.Series, len(mask))
	release := func() {
		for _, s := range out {
			if s != nil {
				s.Release()
			}
		}
	}
	next := 0
	for k, m := range mask {
		if b, _ := m.(bool); !b {
			continue
		}
		s, err := evalScalarOrColumn(ctx, ec, args[next], df)
		if err != nil {
			release()
			return nil, nil, err
		}
		next++
		out[k] = s
	}
	return out, release, nil
}

// numericAt reads row i (broadcasting length 1) as float64.
type numCol struct {
	arr  arrow.Array
	ints []int64
	flts []float64
	n    int
}

func newNumCol(s *series.Series) (numCol, error) {
	arr := s.Chunk(0)
	c := numCol{arr: arr, n: arr.Len()}
	switch a := arr.(type) {
	case *array.Float64:
		c.flts = a.Float64Values()
		return c, nil
	case *array.Int64:
		c.ints = a.Int64Values()
		return c, nil
	}
	wide, err := compute.Cast(context.Background(), s, dtype.Int64())
	if err != nil {
		return c, err
	}
	defer wide.Release()
	c.ints = append([]int64(nil), wide.Chunk(0).(*array.Int64).Int64Values()...)
	return c, nil
}

func (c numCol) valid(i int) bool {
	if c.n == 1 {
		i = 0
	}
	return c.arr.IsValid(i)
}

func (c numCol) int(i int) int64 {
	if c.n == 1 {
		i = 0
	}
	if c.flts != nil {
		return int64(c.flts[i])
	}
	return c.ints[i]
}

func (c numCol) float(i int) float64 {
	if c.n == 1 {
		i = 0
	}
	if c.flts != nil {
		return c.flts[i]
	}
	return float64(c.ints[i])
}

func evalTemporalCtor(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	var (
		cols    []*series.Series
		release func()
		err     error
	)
	switch n.Name {
	case "date":
		cols = make([]*series.Series, 3)
		release = func() {
			for _, c := range cols {
				if c != nil {
					c.Release()
				}
			}
		}
		for k := range 3 {
			if cols[k], err = evalScalarOrColumn(ctx, ec, n.Args[k], df); err != nil {
				release()
				return nil, err
			}
		}
	case "datetime":
		base := make([]*series.Series, 3)
		for k := range 3 {
			if base[k], err = evalScalarOrColumn(ctx, ec, n.Args[k], df); err != nil {
				for _, c := range base[:k] {
					c.Release()
				}
				return nil, err
			}
		}
		rest, rel, err := evalMaskedArgs(ctx, ec, df, n.Args[3:], n.Params[:4])
		if err != nil {
			for _, c := range base {
				c.Release()
			}
			return nil, err
		}
		cols = append(base, rest...)
		release = func() {
			for _, c := range base {
				c.Release()
			}
			rel()
		}
	case "duration":
		cols, release, err = evalMaskedArgs(ctx, ec, df, n.Args, n.Params[:8])
		if err != nil {
			return nil, err
		}
	}
	defer release()
	length := 1
	nums := make([]*numCol, len(cols))
	for k, c := range cols {
		if c == nil {
			continue
		}
		nc, err := newNumCol(c)
		if err != nil {
			return nil, err
		}
		nums[k] = &nc
		if c.Len() != 1 {
			if length != 1 && c.Len() != length {
				return nil, fmt.Errorf("eval: %s: length mismatch %d vs %d", n.Name, length, c.Len())
			}
			length = c.Len()
		}
	}
	rowValid := func(i int) bool {
		for _, c := range nums {
			if c != nil && !c.valid(i) {
				return false
			}
		}
		return true
	}
	get := func(k, i int) int64 {
		if nums[k] == nil {
			return 0
		}
		return nums[k].int(i)
	}
	valid := make([]bool, length)
	switch n.Name {
	case "date":
		out := make([]int32, length)
		for i := range length {
			if !rowValid(i) {
				continue
			}
			y, m, d := get(0, i), int(get(1, i)), int(get(2, i))
			if !temporal.ValidDate(y, m, d) {
				return nil, fmt.Errorf("invalid date components (%d, %d, %d) supplied", y, m, d)
			}
			out[i] = int32(temporal.DaysFromCivil(y, m, d))
			valid[i] = true
		}
		return series.FromDate("date", out, valid, seriesAlloc(ec))
	case "datetime":
		unit, _ := n.Params[4].(dtype.TimeUnit)
		tz, _ := n.Params[5].(string)
		amb, _ := n.Params[6].(string)
		out := make([]int64, length)
		for i := range length {
			if !rowValid(i) {
				continue
			}
			c := temporal.Civil{Year: get(0, i), Month: int(get(1, i)), Day: int(get(2, i)),
				Hour: int(get(3, i)), Minute: int(get(4, i)), Second: int(get(5, i)), Nanosecond: int(get(6, i) * 1000)}
			if !temporal.ValidDate(c.Year, c.Month, c.Day) {
				return nil, fmt.Errorf("invalid date components (%d, %d, %d) supplied", c.Year, c.Month, c.Day)
			}
			out[i] = temporal.FromCivil(c, unit)
			valid[i] = true
		}
		s, err := series.FromDatetime("datetime", out, valid, unit, "", seriesAlloc(ec))
		if err != nil || tz == "" {
			return s, err
		}
		defer s.Release()
		return s.Dt().ReplaceTimeZone(tz, amb, "raise", seriesAlloc(ec))
	}
	// duration
	unitStr, _ := n.Params[8].(string)
	unit := dtype.Microsecond
	if nums[7] != nil {
		unit = dtype.Nanosecond
	}
	if unitStr != "" {
		u, ok := dtype.ParseTimeUnit(unitStr)
		if !ok {
			return nil, fmt.Errorf("invalid time unit %q", unitStr)
		}
		unit = u
	}
	scales := []float64{float64(temporal.NsWeek), float64(temporal.NsDay), float64(temporal.NsHour), float64(temporal.NsMinute),
		float64(temporal.NsSecond), float64(temporal.NsMillisecond), float64(temporal.NsMicrosecond), 1}
	iscales := []int64{temporal.NsWeek, temporal.NsDay, temporal.NsHour, temporal.NsMinute,
		temporal.NsSecond, temporal.NsMillisecond, temporal.NsMicrosecond, 1}
	out := make([]int64, length)
	per := temporal.NsPerUnit(unit)
	for i := range length {
		if !rowValid(i) {
			continue
		}
		var total int64
		var ftotal float64
		useFloat := false
		for k, c := range nums {
			if c == nil {
				continue
			}
			if c.flts != nil {
				useFloat = true
				ftotal += c.float(i) * scales[k]
			} else {
				total += c.int(i) * iscales[k]
			}
		}
		if useFloat {
			total += int64(math.Trunc(ftotal))
		}
		out[i] = total / per
		valid[i] = true
	}
	return series.FromDuration("duration", out, valid, unit, seriesAlloc(ec))
}

// scalarTicks evaluates a range bound and returns its value as ticks of
// unit (dates become midnight), plus whether the bound was a Date.
func scalarBound(ctx context.Context, ec EvalContext, e expr.Expr, df *dataframe.DataFrame) (*series.Series, error) {
	s, err := evalScalarOrColumn(ctx, ec, e, df)
	if err != nil {
		return nil, err
	}
	if s.Len() == 0 {
		s.Release()
		return nil, fmt.Errorf("eval: range bound is empty")
	}
	if s.NullCount() > 0 && s.Chunk(0).IsNull(0) {
		s.Release()
		return nil, fmt.Errorf("eval: range bound is null")
	}
	return s, nil
}

func firstPhysical(s *series.Series) int64 {
	switch a := s.Chunk(0).(type) {
	case *array.Date32:
		return int64(a.Value(0))
	case *array.Timestamp:
		return int64(a.Value(0))
	case *array.Time64:
		return int64(a.Value(0))
	case *array.Time32:
		return int64(a.Value(0))
	case *array.Int64:
		return a.Value(0)
	}
	return 0
}

func evalRange(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	start, err := scalarBound(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer start.Release()
	end, err := scalarBound(ctx, ec, n.Args[1], df)
	if err != nil {
		return nil, err
	}
	defer end.Release()
	interval, _ := n.Params[0].(string)
	closed, _ := n.Params[1].(string)
	opt := seriesAlloc(ec)
	switch n.Name {
	case "date_range":
		if !start.DType().IsDate() || !end.DType().IsDate() {
			return nil, fmt.Errorf("date_range: start and end must be Date, got %s and %s", start.DType(), end.DType())
		}
		if interval == "" {
			interval = "1d"
		}
		return series.DateRange("literal", firstPhysical(start), firstPhysical(end), interval, closed, opt)
	case "time_range":
		if interval == "" {
			interval = "1h"
		}
		toNs := func(s *series.Series) int64 {
			u, _ := s.DType().TimeUnit()
			return firstPhysical(s) * temporal.NsPerUnit(u)
		}
		return series.TimeRange("literal", toNs(start), toNs(end), interval, closed, opt)
	}
	unitStr, _ := n.Params[2].(string)
	tz, _ := n.Params[3].(string)
	if interval == "" {
		interval = "1d"
	}
	unit := dtype.Microsecond
	if unitStr != "" {
		u, ok := dtype.ParseTimeUnit(unitStr)
		if !ok {
			return nil, fmt.Errorf("invalid time unit %q", unitStr)
		}
		unit = u
	} else if d, err := temporal.ParseDuration(interval); err == nil && d.Nsecs%1000 != 0 {
		unit = dtype.Nanosecond
	}
	toTicks := func(s *series.Series) int64 {
		switch {
		case s.DType().IsDate():
			return firstPhysical(s) * temporal.UnitsPerDay(unit)
		case s.DType().IsDatetime():
			u, _ := s.DType().TimeUnit()
			v := firstPhysical(s)
			if stz := s.DType().TimeZone(); stz != "" {
				if z, _ := temporal.NewZone(stz); z != nil {
					v = z.ToLocal(v, u)
				}
			}
			return temporal.ConvertUnit(v, u, unit, true)
		}
		return firstPhysical(s)
	}
	if tz == "" {
		tz = start.DType().TimeZone()
	}
	return series.DatetimeRange("literal", toTicks(start), toTicks(end), interval, closed, unit, tz, opt)
}

func evalRollingBy(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf("eval: %s requires a value and a `by` expression", n.Name)
	}
	spec, ok := n.Params[0].(expr.RollingBySpec)
	if !ok {
		return nil, fmt.Errorf("eval: %s: missing window specification", n.Name)
	}
	vals, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer vals.Release()
	by, err := evalNode(ctx, ec, n.Args[1], df)
	if err != nil {
		return nil, err
	}
	defer by.Release()
	op := strings.TrimSuffix(strings.TrimPrefix(n.Name, "rolling_"), "_by")
	return vals.RollingBy(op, by, series.RollingByOptions{
		WindowSize: spec.WindowSize, MinPeriods: spec.MinPeriods, Closed: spec.Closed,
		Ddof: spec.Ddof, Quantile: spec.Quantile, Interpolation: spec.Interpolation,
	}, seriesAlloc(ec))
}
