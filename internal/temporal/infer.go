package temporal

import (
	"regexp"
	"strconv"
	"sync"
)

// Pattern names a family of formats polars tries when no format is
// given to str.to_date / str.to_datetime / str.to_time.
type Pattern int8

const (
	PatternNone Pattern = iota
	PatternDateDMY
	PatternDateYMD
	PatternDatetimeDMY
	PatternDatetimeYMD
	PatternDatetimeYMDZ
	PatternTime
)

var (
	dateDMY = []string{"%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y"}
	dateYMD = []string{"%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"}
	timeHMS = []string{"%T%.9f", "%T%.6f", "%T%.3f", "%T", "%R"}
)

func datetimePatterns(first string, withZ bool) []string {
	var out []string
	z := ""
	if withZ {
		z = "%#z"
	}
	for _, sep := range []string{"-", "/", "."} {
		var date string
		if first == "d" {
			date = "%d" + sep + "%m" + sep + "%Y"
		} else {
			date = "%Y" + sep + "%m" + sep + "%d"
		}
		for _, t := range []string{"T", " "} {
			for _, tm := range []string{
				"%H:%M:%S.%9f", "%H:%M:%S.%6f", "%H:%M:%S.%3f", "%H:%M:%S",
				"%H%M%S.%9f", "%H%M%S.%6f", "%H%M%S.%3f", "%H%M%S",
				"%H:%M", "%H%M",
			} {
				out = append(out, date+t+tm+z)
			}
		}
		if !withZ {
			out = append(out, date)
		}
	}
	if withZ {
		out = append(out, "%+")
	} else if first != "d" {
		out = append(out, "%Y-%m-%dT%H:%M:%S%.f")
	}
	return out
}

// compiledGroup compiles its formats on first use. Both the pattern
// list and the compiled formats are built lazily so importing the
// package costs nothing at process start.
type compiledGroup struct {
	once    sync.Once
	sources func() []string
	formats []*Format
}

func (g *compiledGroup) get() []*Format {
	g.once.Do(func() {
		for _, s := range g.sources() {
			f, err := CompileFormat(s)
			if err != nil {
				panic(err)
			}
			f.lenientFrac = true
			g.formats = append(g.formats, f)
		}
	})
	return g.formats
}

var (
	grpDateDMY     = &compiledGroup{sources: func() []string { return dateDMY }}
	grpDateYMD     = &compiledGroup{sources: func() []string { return dateYMD }}
	grpTime        = &compiledGroup{sources: func() []string { return timeHMS }}
	grpDatetimeDMY = &compiledGroup{sources: func() []string { return datetimePatterns("d", false) }}
	grpDatetimeYMD = &compiledGroup{sources: func() []string { return datetimePatterns("y", false) }}
	grpDatetimeZ   = &compiledGroup{sources: func() []string { return datetimePatterns("y", true) }}
)

// Formats returns the candidate formats of a pattern family.
func (p Pattern) Formats() []*Format {
	switch p {
	case PatternDateDMY:
		return grpDateDMY.get()
	case PatternDateYMD:
		return grpDateYMD.get()
	case PatternDatetimeDMY:
		return grpDatetimeDMY.get()
	case PatternDatetimeYMD:
		return grpDatetimeYMD.get()
	case PatternDatetimeYMDZ:
		return grpDatetimeZ.get()
	case PatternTime:
		return grpTime.get()
	}
	return nil
}

// The inference regexes are compiled on first use: they are only
// needed when parsing strings without an explicit format.
var (
	reDMY = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`^['"]?\d{1,2}[-/\.]([01]?\d)[-/\.]\d{4,}(?:[T ]\d{1,2}:?\d{1,2}(?::?\d{1,2}(?:\.\d{1,9})?)?)?['"]?$`)
	})
	reYMD = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`^['"]?\d{4,}[-/\.]([01]?\d)[-/\.]\d{1,2}(?:[T ]\d{1,2}:?\d{1,2}(?::?\d{1,2}(?:\.\d{1,9})?)?)?['"]?$`)
	})
	reYMZ = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`^['"]?\d{4,}[-/\.]([01]?\d)[-/\.]\d{1,2}[T ]\d{2}:?\d{2}(?::?\d{2}(?:\.\d{1,9})?)?(?:[+-]\d{2}(?::?\d{2})?|Z)['"]?$`)
	})
)

func monthOK(re *regexp.Regexp, v string) bool {
	m := re.FindStringSubmatch(v)
	if m == nil {
		return false
	}
	n, err := strconv.Atoi(m[1])
	return err == nil && n >= 1 && n <= 12
}

// IsInferable reports whether v looks like it belongs to the pattern
// family (polars only retries all formats for such values).
func (p Pattern) IsInferable(v string) bool {
	switch p {
	case PatternDatetimeDMY:
		return monthOK(reDMY(), v)
	case PatternDatetimeYMD:
		return monthOK(reYMD(), v)
	case PatternDatetimeYMDZ:
		return monthOK(reYMZ(), v)
	}
	return true
}

func anyParses(fs []*Format, v string, needOffset bool) bool {
	for _, f := range fs {
		if r, ok := f.Parse(v); ok && (!needOffset || r.HasOffset) {
			return true
		}
	}
	return false
}

// InferDatePattern picks the date family matching v.
func InferDatePattern(v string) Pattern {
	switch {
	case anyParses(PatternDateDMY.Formats(), v, false):
		return PatternDateDMY
	case anyParses(PatternDateYMD.Formats(), v, false):
		return PatternDateYMD
	}
	return PatternNone
}

// InferDatetimePattern picks the datetime family matching v.
func InferDatetimePattern(v string) Pattern {
	switch {
	case anyParses(PatternDatetimeDMY.Formats(), v, false):
		return PatternDatetimeDMY
	case anyParses(PatternDatetimeYMD.Formats(), v, false):
		return PatternDatetimeYMD
	case anyParses(PatternDatetimeYMDZ.Formats(), v, true):
		return PatternDatetimeYMDZ
	}
	return PatternNone
}

// InferTimeFormat returns the first time format that parses v.
func InferTimeFormat(v string) *Format {
	for _, f := range PatternTime.Formats() {
		if _, ok := f.ParseTime(v); ok {
			return f
		}
	}
	return nil
}

// Inferrer parses values with a pattern family, remembering the last
// format that worked (polars' DatetimeInfer).
type Inferrer struct {
	Pattern Pattern
	formats []*Format
	latest  *Format
}

// NewInferrer returns an inferrer for the family.
func NewInferrer(p Pattern) *Inferrer {
	fs := p.Formats()
	return &Inferrer{Pattern: p, formats: fs, latest: fs[0]}
}

// Parse parses v with the latest format, falling back to every format
// of the family when v looks inferable.
func (in *Inferrer) Parse(v string) (ParseResult, bool) {
	needOffset := in.Pattern == PatternDatetimeYMDZ
	if r, ok := in.latest.Parse(v); ok && (!needOffset || r.HasOffset) {
		return r, true
	}
	if !in.Pattern.IsInferable(v) {
		return ParseResult{}, false
	}
	for _, f := range in.formats {
		if r, ok := f.Parse(v); ok && (!needOffset || r.HasOffset) {
			in.latest = f
			return r, true
		}
	}
	return ParseResult{}, false
}
