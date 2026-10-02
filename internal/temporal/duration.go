package temporal

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// Duration is a polars calendar duration such as "1mo2d3h". Months,
// weeks and days are calendar units: adding one day keeps the wall
// clock time in a time zone even across a daylight saving shift. The
// fields are magnitudes; Negative carries the sign.
type Duration struct {
	Months, Weeks, Days, Nsecs int64
	Negative                   bool
	// ParsedInt is set for index durations such as "3i".
	ParsedInt bool
}

// ParseDuration parses a polars duration string: a sequence of
// integer-unit pairs with an optional leading sign. Units are ns, us,
// ms, s, m, h, d, w, mo, q, y and i.
func ParseDuration(s string) (Duration, error) {
	var d Duration
	pos := 0
	if s == "" {
		return d, fmt.Errorf("expected leading integer in the duration string, found ''")
	}
	switch s[0] {
	case '-':
		d.Negative = true
		pos++
	case '+':
		pos++
	}
	for pos < len(s) {
		c := s[pos]
		if c < '0' || c > '9' {
			if c == '-' || c == '+' {
				return Duration{}, fmt.Errorf("only a single sign is allowed, at the front of the string")
			}
			return Duration{}, fmt.Errorf("expected leading integer in the duration string, found '%c'", c)
		}
		var n int64
		for pos < len(s) && s[pos] >= '0' && s[pos] <= '9' {
			n = n*10 + int64(s[pos]-'0')
			pos++
		}
		if pos >= len(s) {
			return Duration{}, fmt.Errorf("expected a valid unit to follow integer in the duration string '%s'", s)
		}
		us := pos
		for pos < len(s) && ((s[pos] >= 'a' && s[pos] <= 'z') || (s[pos] >= 'A' && s[pos] <= 'Z')) {
			pos++
		}
		if us == pos {
			return Duration{}, fmt.Errorf("expected a valid unit to follow integer in the duration string '%s'", s)
		}
		switch s[us:pos] {
		case "ns":
			d.Nsecs += n
		case "us":
			d.Nsecs += n * NsMicrosecond
		case "ms":
			d.Nsecs += n * NsMillisecond
		case "s":
			d.Nsecs += n * NsSecond
		case "m":
			d.Nsecs += n * NsMinute
		case "h":
			d.Nsecs += n * NsHour
		case "d":
			d.Days += n
		case "w":
			d.Weeks += n
		case "mo":
			d.Months += n
		case "q":
			d.Months += 3 * n
		case "y":
			d.Months += 12 * n
		case "i":
			d.Nsecs += n
			d.ParsedInt = true
		default:
			return Duration{}, fmt.Errorf("unit: '%s' not supported; available units are: 'y', 'mo', 'q', 'w', 'd', 'h', 'm', 's', 'ms', 'us', 'ns'", s[us:pos])
		}
	}
	return d, nil
}

// MustParseDuration is ParseDuration that panics on error. Intended for
// constants in tests.
func MustParseDuration(s string) Duration {
	d, err := ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}

// FixedDuration returns a duration of exactly ns nanoseconds.
func FixedDuration(ns int64) Duration {
	if ns < 0 {
		return Duration{Nsecs: -ns, Negative: true}
	}
	return Duration{Nsecs: ns}
}

// IsZero reports whether every component is zero.
func (d Duration) IsZero() bool {
	return d.Months == 0 && d.Weeks == 0 && d.Days == 0 && d.Nsecs == 0
}

// Neg flips the sign.
func (d Duration) Neg() Duration {
	d.Negative = !d.Negative
	return d
}

// Mul scales every component by n.
func (d Duration) Mul(n int64) Duration {
	if n < 0 {
		n = -n
		d.Negative = !d.Negative
	}
	d.Months *= n
	d.Weeks *= n
	d.Days *= n
	d.Nsecs *= n
	return d
}

// IsFullDays reports whether the duration has no sub-daily part.
func (d Duration) IsFullDays() bool { return d.Nsecs == 0 }

// IsConstant reports whether the duration has a fixed length. Months
// never do; days and weeks do only for naive or UTC data.
func (d Duration) IsConstant(tz string) bool {
	if tz == "" || tz == "UTC" {
		return d.Months == 0
	}
	return d.Months == 0 && d.Weeks == 0 && d.Days == 0
}

// EstimateNs is the approximate length in nanoseconds, counting a month
// as 28 days. It matches polars' duration_ns.
func (d Duration) EstimateNs() int64 {
	return d.Months*28*NsDay + d.Weeks*NsWeek + d.Days*NsDay + d.Nsecs
}

// EstimateIn is EstimateNs expressed in unit u (polars' duration_ms and
// duration_us).
func (d Duration) EstimateIn(u dtype.TimeUnit) int64 {
	switch u {
	case dtype.Nanosecond:
		return d.EstimateNs()
	case dtype.Microsecond:
		return d.Months*28*NsDay/1000 + d.Weeks*NsWeek/1000 + d.Nsecs/1000 + d.Days*NsDay/1000
	case dtype.Millisecond:
		return d.Months*28*NsDay/1_000_000 + d.Weeks*NsWeek/1_000_000 + d.Nsecs/1_000_000 + d.Days*NsDay/1_000_000
	}
	return d.EstimateNs() / NsSecond
}

// NteIn is a not-to-exceed estimate of the length in unit u.
func (d Duration) NteIn(u dtype.TimeUnit) int64 {
	per := NsPerUnit(u)
	return d.Months*(31*24+1)*3600*UnitsPerSecond(u) + d.Weeks*NteNsWeek/per + d.Days*NteNsDay/per + d.Nsecs/per
}

// String renders the duration the way polars displays it.
func (d Duration) String() string {
	if d.IsZero() {
		return "0s"
	}
	var b strings.Builder
	if d.Negative {
		b.WriteByte('-')
	}
	if d.Months > 0 {
		b.WriteString(strconv.FormatInt(d.Months, 10) + "mo")
	}
	if d.Weeks > 0 {
		b.WriteString(strconv.FormatInt(d.Weeks, 10) + "w")
	}
	if d.Days > 0 {
		b.WriteString(strconv.FormatInt(d.Days, 10) + "d")
	}
	if d.Nsecs > 0 {
		switch {
		case d.Nsecs%NsSecond == 0:
			b.WriteString(strconv.FormatInt(d.Nsecs/NsSecond, 10) + "s")
		case d.Nsecs%1000 == 0:
			b.WriteString(strconv.FormatInt(d.Nsecs/1000, 10) + "us")
		default:
			b.WriteString(strconv.FormatInt(d.Nsecs, 10) + "ns")
		}
	}
	return b.String()
}

// AddMonths shifts a naive tick value by n calendar months, clamping
// the day to the end of the target month.
func AddMonths(t int64, u dtype.TimeUnit, n int64) int64 {
	days, rem := SplitDays(t, u)
	y, m, d := CivilFromDays(days)
	total := y*12 + int64(m-1) + n
	ny := FloorDiv(total, 12)
	nm := int(FloorMod(total, 12)) + 1
	if dim := DaysInMonth(ny, nm); d > dim {
		d = dim
	}
	return DaysFromCivil(ny, nm, d)*UnitsPerDay(u) + rem
}

// Add adds the duration to t (ticks of u). z is nil for naive or UTC
// data; otherwise calendar components are applied to the local wall
// clock and localized back, raising on ambiguous or missing results.
func (d Duration) Add(t int64, u dtype.TimeUnit, z *Zone) (int64, error) {
	sign := int64(1)
	if d.Negative {
		sign = -1
	}
	var err error
	if d.Months > 0 {
		if z != nil {
			l := AddMonths(z.ToLocal(t, u), u, sign*d.Months)
			if t, err = z.FromLocalRaise(l, u); err != nil {
				return 0, err
			}
		} else {
			t = AddMonths(t, u, sign*d.Months)
		}
	}
	if d.Weeks > 0 {
		step := sign * d.Weeks * NsWeek / NsPerUnit(u)
		if z != nil {
			if t, err = z.FromLocalRaise(z.ToLocal(t, u)+step, u); err != nil {
				return 0, err
			}
		} else {
			t += step
		}
	}
	if d.Days > 0 {
		step := sign * d.Days * NsDay / NsPerUnit(u)
		if z != nil {
			if t, err = z.FromLocalRaise(z.ToLocal(t, u)+step, u); err != nil {
				return 0, err
			}
		} else {
			t += step
		}
	}
	return t + sign*(d.Nsecs/NsPerUnit(u)), nil
}

// Truncate rounds t down to a multiple of the duration, following
// polars' dt.truncate rules (weeks start on Monday, months count from
// 1970-01).
func (d Duration) Truncate(t int64, u dtype.TimeUnit, z *Zone) (int64, error) {
	perDay := UnitsPerDay(u)
	if z == nil && d.Months == 0 && d.Weeks == 0 && !d.IsZero() {
		every := d.EstimateIn(u)
		if every == 0 {
			return t, nil
		}
		return t - FloorMod(t, every), nil
	}
	switch {
	case d.IsZero():
		return 0, fmt.Errorf("duration cannot be zero")
	case d.Months == 0 && d.Weeks == 0 && d.Days == 0:
		every := d.Nsecs / NsPerUnit(u)
		if every == 0 {
			return t, nil
		}
		return d.truncateFixed(t, u, z, every, 0)
	case d.Months == 0 && d.Weeks == 0 && d.Nsecs == 0:
		return d.truncateFixed(t, u, z, d.Days*perDay, 0)
	case d.Months == 0 && d.Days == 0 && d.Nsecs == 0:
		return d.truncateFixed(t, u, z, 7*d.Weeks*perDay, 4*perDay)
	case d.Weeks == 0 && d.Days == 0 && d.Nsecs == 0:
		return d.truncateMonthly(t, u, z)
	}
	return 0, fmt.Errorf("cannot mix month, week, day, and sub-daily units for this operation")
}

func (d Duration) truncateFixed(t int64, u dtype.TimeUnit, z *Zone, every, shift int64) (int64, error) {
	if z == nil {
		return t - FloorMod(t-shift, every), nil
	}
	l := z.ToLocal(t, u)
	return localizeResult(z, u, t, l, l-FloorMod(l-shift, every))
}

func (d Duration) truncateMonthly(t int64, u dtype.TimeUnit, z *Zone) (int64, error) {
	l := t
	if z != nil {
		l = z.ToLocal(t, u)
	}
	days, _ := SplitDays(l, u)
	y, m, _ := CivilFromDays(days)
	total := (y-1970)*12 + int64(m-1)
	total -= FloorMod(total, d.Months)
	res := DaysFromCivil(1970+FloorDiv(total, 12), int(FloorMod(total, 12))+1, 1) * UnitsPerDay(u)
	if z == nil {
		return res, nil
	}
	return localizeResult(z, u, t, l, res)
}

// localizeResult localizes a truncated wall-clock value, keeping the
// daylight saving fold of the original value when the result is
// ambiguous.
func localizeResult(z *Zone, u dtype.TimeUnit, origUTC, origLocal, resLocal int64) (int64, error) {
	per := UnitsPerSecond(u)
	sec := FloorDiv(resLocal, per)
	sub := resLocal - sec*per
	e, lt, n := z.Candidates(sec)
	if n == 1 {
		return e*per + sub, nil
	}
	amb := AmbiguousEarliest
	if oe, _, on := z.Candidates(FloorDiv(origLocal, per)); on == 2 {
		osub := origLocal - FloorDiv(origLocal, per)*per
		if oe*per+osub != origUTC {
			amb = AmbiguousLatest
		}
	}
	if n == 2 {
		if amb == AmbiguousLatest {
			return lt*per + sub, nil
		}
		return e*per + sub, nil
	}
	return z.FromLocalRaise(resLocal, u)
}

// Round rounds t to the nearest multiple of the duration using polars'
// rule: add half the estimated duration and truncate.
func (d Duration) Round(t int64, u dtype.TimeUnit, z *Zone) (int64, error) {
	if z == nil && d.Months == 0 && d.Weeks == 0 {
		every := d.EstimateIn(u)
		if every == 0 {
			return t, nil
		}
		v := t + every/2
		return v - FloorMod(v, every), nil
	}
	half := d.EstimateNs() / (2 * NsPerUnit(u))
	return d.Truncate(t+half, u, z)
}
