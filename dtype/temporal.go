package dtype

import "github.com/apache/arrow-go/v18/arrow"

// IsDate reports whether d is the Date dtype.
func (d DType) IsDate() bool { return d.ID() == arrow.DATE32 || d.ID() == arrow.DATE64 }

// IsDatetime reports whether d is a Datetime (arrow timestamp) dtype.
func (d DType) IsDatetime() bool { return d.ID() == arrow.TIMESTAMP }

// IsDuration reports whether d is a Duration dtype.
func (d DType) IsDuration() bool { return d.ID() == arrow.DURATION }

// IsTime reports whether d is a time-of-day dtype.
func (d DType) IsTime() bool { return d.ID() == arrow.TIME32 || d.ID() == arrow.TIME64 }

// TimeUnit returns the resolution of a Datetime, Duration or Time dtype.
// ok is false for every other dtype.
func (d DType) TimeUnit() (unit TimeUnit, ok bool) {
	switch t := d.inner.(type) {
	case *arrow.TimestampType:
		return timeUnitFromArrow(t.Unit), true
	case *arrow.DurationType:
		return timeUnitFromArrow(t.Unit), true
	case *arrow.Time32Type:
		return timeUnitFromArrow(t.Unit), true
	case *arrow.Time64Type:
		return timeUnitFromArrow(t.Unit), true
	}
	return 0, false
}

// TimeZone returns the time zone of a Datetime dtype, or "" for naive
// datetimes and non-datetime dtypes.
func (d DType) TimeZone() string {
	if t, ok := d.inner.(*arrow.TimestampType); ok {
		return t.TimeZone
	}
	return ""
}

// ParseTimeUnit maps "s", "ms", "us" or "ns" to a TimeUnit.
func ParseTimeUnit(s string) (TimeUnit, bool) {
	switch s {
	case "s":
		return Second, true
	case "ms":
		return Millisecond, true
	case "us", "μs":
		return Microsecond, true
	case "ns":
		return Nanosecond, true
	}
	return 0, false
}
