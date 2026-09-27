package temporal

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Gaurav-Gosain/golars/dtype"
)

var locCache sync.Map // name -> *time.Location

// LoadLocation resolves an IANA time zone name, caching the result.
// Fixed offsets such as "+01:00" are accepted like polars does.
func LoadLocation(name string) (*time.Location, error) {
	if v, ok := locCache.Load(name); ok {
		return v.(*time.Location), nil
	}
	var loc *time.Location
	if off, ok := parseFixedOffset(name); ok {
		loc = time.FixedZone(name, off)
	} else {
		l, err := time.LoadLocation(name)
		if err != nil {
			return nil, fmt.Errorf("unable to parse time zone: '%s'. Please check the Time Zone Database for a list of available time zones", name)
		}
		loc = l
	}
	locCache.Store(name, loc)
	return loc, nil
}

func parseFixedOffset(s string) (int, bool) {
	if len(s) != 6 || (s[0] != '+' && s[0] != '-') || s[3] != ':' {
		return 0, false
	}
	d := func(c byte) (int, bool) { return int(c - '0'), c >= '0' && c <= '9' }
	h1, ok1 := d(s[1])
	h2, ok2 := d(s[2])
	m1, ok3 := d(s[4])
	m2, ok4 := d(s[5])
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return 0, false
	}
	off := (h1*10+h2)*3600 + (m1*10+m2)*60
	if s[0] == '-' {
		off = -off
	}
	return off, true
}

// Zone wraps a location with a one-entry cache of the zone period that
// contains the last queried instant. Temporal columns are usually
// clustered in time, so most lookups hit the cache and skip the
// binary search inside the time package. A Zone is not safe for
// concurrent use; create one per goroutine.
type Zone struct {
	Name       string
	loc        *time.Location
	start, end int64 // seconds, [start, end)
	off        int
	abbr       string
	valid      bool
}

// NewZone returns a lookup cache for the named zone. An empty name or a
// UTC zone returns nil, which callers treat as "no conversion needed".
func NewZone(name string) (*Zone, error) {
	if name == "" || name == "UTC" || name == "Etc/UTC" || name == "Z" {
		return nil, nil
	}
	loc, err := LoadLocation(name)
	if err != nil {
		return nil, err
	}
	return &Zone{Name: name, loc: loc}, nil
}

// Clone returns an independent cache over the same location.
func (z *Zone) Clone() *Zone {
	if z == nil {
		return nil
	}
	return &Zone{Name: z.Name, loc: z.loc}
}

// Offset returns the UTC offset in seconds and the abbreviation in
// effect at the instant sec (seconds since the epoch).
func (z *Zone) Offset(sec int64) (int, string) {
	if z.valid && sec >= z.start && sec < z.end {
		return z.off, z.abbr
	}
	t := time.Unix(sec, 0).In(z.loc)
	abbr, off := t.Zone()
	s, e := t.ZoneBounds()
	z.start, z.end = math.MinInt64, math.MaxInt64
	if !s.IsZero() {
		z.start = s.Unix()
	}
	if !e.IsZero() {
		z.end = e.Unix()
	}
	z.off, z.abbr, z.valid = off, abbr, true
	return off, abbr
}

// ToLocal converts a UTC instant in unit u to the naive wall-clock
// value in the zone, expressed in the same unit.
func (z *Zone) ToLocal(t int64, u dtype.TimeUnit) int64 {
	per := UnitsPerSecond(u)
	off, _ := z.Offset(FloorDiv(t, per))
	return t + int64(off)*per
}

// Ambiguous selects how a wall-clock time that occurs twice (at the end
// of daylight saving time) is resolved.
type Ambiguous int8

const (
	AmbiguousRaise Ambiguous = iota
	AmbiguousEarliest
	AmbiguousLatest
	AmbiguousNull
)

// ParseAmbiguous maps the polars spelling to an Ambiguous value.
func ParseAmbiguous(s string) (Ambiguous, error) {
	switch s {
	case "", "raise":
		return AmbiguousRaise, nil
	case "earliest":
		return AmbiguousEarliest, nil
	case "latest":
		return AmbiguousLatest, nil
	case "null":
		return AmbiguousNull, nil
	}
	return 0, fmt.Errorf("invalid ambiguous value %q: expected one of 'raise', 'earliest', 'latest', 'null'", s)
}

// NonExistent selects how a wall-clock time skipped by a daylight
// saving jump is resolved.
type NonExistent int8

const (
	NonExistentRaise NonExistent = iota
	NonExistentNull
)

// ParseNonExistent maps the polars spelling to a NonExistent value.
func ParseNonExistent(s string) (NonExistent, error) {
	switch s {
	case "", "raise":
		return NonExistentRaise, nil
	case "null":
		return NonExistentNull, nil
	}
	return 0, fmt.Errorf("invalid non_existent value %q: expected one of 'raise', 'null'", s)
}

// Candidates returns the UTC instants (in seconds) whose wall clock in
// the zone equals localSec. The result has zero entries for a
// non-existent time, one normally and two for an ambiguous time
// (earliest first).
func (z *Zone) Candidates(localSec int64) (earliest, latest int64, n int) {
	var offs [3]int
	offs[0], _ = z.Offset(localSec - SecondsPerDay)
	offs[1], _ = z.Offset(localSec)
	offs[2], _ = z.Offset(localSec + SecondsPerDay)
	var found [3]int64
	cnt := 0
	for i, o := range offs {
		dup := false
		for _, p := range offs[:i] {
			if p == o {
				dup = true
			}
		}
		if dup {
			continue
		}
		inst := localSec - int64(o)
		if got, _ := z.Offset(inst); got == o {
			found[cnt] = inst
			cnt++
		}
	}
	switch cnt {
	case 0:
		return 0, 0, 0
	case 1:
		return found[0], found[0], 1
	}
	lo, hi := found[0], found[0]
	for _, f := range found[1:cnt] {
		lo = min(lo, f)
		hi = max(hi, f)
	}
	if lo == hi {
		return lo, lo, 1
	}
	return lo, hi, 2
}

// FromLocal converts a naive wall-clock value in unit u to the UTC
// instant in the zone. ok is false when the policy asks for null.
func (z *Zone) FromLocal(l int64, u dtype.TimeUnit, amb Ambiguous, ne NonExistent) (int64, bool, error) {
	per := UnitsPerSecond(u)
	sec := FloorDiv(l, per)
	sub := l - sec*per
	e, lt, n := z.Candidates(sec)
	switch n {
	case 0:
		if ne == NonExistentNull {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("datetime '%s' is non-existent in time zone '%s'. You may be able to use `non_existent='null'` to return `null` in this case", FormatNaive(l, u), z.Name)
	case 2:
		switch amb {
		case AmbiguousEarliest:
			return e*per + sub, true, nil
		case AmbiguousLatest:
			return lt*per + sub, true, nil
		case AmbiguousNull:
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("datetime '%s' is ambiguous in time zone '%s'. Please use `ambiguous` to tell how it should be localized", FormatNaive(l, u), z.Name)
	}
	return e*per + sub, true, nil
}

// FromLocalRaise is FromLocal with both policies set to raise.
func (z *Zone) FromLocalRaise(l int64, u dtype.TimeUnit) (int64, error) {
	v, _, err := z.FromLocal(l, u, AmbiguousRaise, NonExistentRaise)
	return v, err
}

// FormatNaive renders a naive tick value as "YYYY-MM-DD HH:MM:SS" (plus
// a fraction when non-zero). Used in error messages.
func FormatNaive(t int64, u dtype.TimeUnit) string {
	c := ToCivil(t, u)
	s := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", c.Year, c.Month, c.Day, c.Hour, c.Minute, c.Second)
	if c.Nanosecond != 0 {
		s += fmt.Sprintf(".%09d", c.Nanosecond)
	}
	return s
}
