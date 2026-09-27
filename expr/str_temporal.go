package expr

import "github.com/Gaurav-Gosain/golars/dtype"

// StrptimeOptions configures string to temporal parsing. Start from
// DefaultStrptimeOptions (strict and exact, like polars).
type StrptimeOptions struct {
	// Strict raises on unparseable strings instead of producing null.
	Strict bool
	// Exact requires the format to match the whole string.
	Exact bool
	// TimeUnit is "", "ms", "us" or "ns" for datetimes; "" infers it
	// from the format.
	TimeUnit string
	// TimeZone attaches a zone to parsed datetimes.
	TimeZone string
	// Ambiguous is "raise", "earliest", "latest" or "null".
	Ambiguous string
}

// DefaultStrptimeOptions returns the polars defaults.
func DefaultStrptimeOptions() StrptimeOptions {
	return StrptimeOptions{Strict: true, Exact: true, Ambiguous: "raise"}
}

// ToDate parses strings into Dates. An empty format infers it.
func (o StrOps) ToDate(format string) Expr {
	return o.ToDateWith(format, DefaultStrptimeOptions())
}

// ToDateWith is ToDate with explicit options.
func (o StrOps) ToDateWith(format string, opt StrptimeOptions) Expr {
	return fn1p("str.to_date", o.e, format, opt)
}

// ToDatetime parses strings into Datetimes. An empty format infers it.
func (o StrOps) ToDatetime(format string) Expr {
	return o.ToDatetimeWith(format, DefaultStrptimeOptions())
}

// ToDatetimeWith is ToDatetime with explicit options.
func (o StrOps) ToDatetimeWith(format string, opt StrptimeOptions) Expr {
	return fn1p("str.to_datetime", o.e, format, opt)
}

// ToTime parses strings into Times. An empty format infers it.
func (o StrOps) ToTime(format string) Expr {
	return o.ToTimeWith(format, DefaultStrptimeOptions())
}

// ToTimeWith is ToTime with explicit options.
func (o StrOps) ToTimeWith(format string, opt StrptimeOptions) Expr {
	return fn1p("str.to_time", o.e, format, opt)
}

// Strptime parses strings into the target dtype (Date, Datetime or
// Time) with a chrono format string.
func (o StrOps) Strptime(to dtype.DType, format string) Expr {
	return o.StrptimeWith(to, format, DefaultStrptimeOptions())
}

// StrptimeWith is Strptime with explicit options.
func (o StrOps) StrptimeWith(to dtype.DType, format string, opt StrptimeOptions) Expr {
	return fn1p("str.strptime", o.e, to, format, opt)
}
