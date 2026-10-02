package series

import "github.com/Gaurav-Gosain/golars/internal/temporal"

// fixedStep is the fast path of Truncate and Round for naive or UTC
// datetimes and a duration without months or weeks: plain integer
// arithmetic on the physical values, no per-row calendar work.
func (o DtOps) fixedStep(d temporal.Duration, round bool, opts []Option) (*Series, bool, error) {
	info := o.info()
	if info.kind != tkDatetime || (info.tz != "" && info.tz != "UTC") || d.Months != 0 || d.Weeks != 0 || d.IsZero() {
		return nil, false, nil
	}
	every := d.EstimateIn(info.unit)
	if every == 0 {
		return o.s.Clone(), true, nil
	}
	half := int64(0)
	if round {
		half = every / 2
	}
	cfg := resolve(opts)
	arr := o.s.Chunk(0)
	vals := physicalInt64(arr)
	n := arr.Len()
	out, err := buildFixed(o.s.Name(), n, cfg.alloc, o.s.DType().Arrow(), arr, func(dst []int64, _ []byte) error {
		return forRanges(n, func(lo, hi int) error {
			for i := lo; i < hi; i++ {
				v := vals[i] + half
				r := v % every
				if r < 0 {
					r += every
				}
				dst[i] = v - r
			}
			return nil
		})
	})
	return out, true, err
}
