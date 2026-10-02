package difftest

import "strings"

// glrForm maps a polars-spelled call onto the equivalent glr spelling
// where the two APIs differ only in how arguments are passed. Calls
// with no glr equivalent are returned unchanged so they fail to parse
// and are counted as unsupported.
//
// Known API differences handled here:
//   - glr str.contains, str.replace, str.replace_all, str.count_matches
//     and str.find are literal; polars defaults to regex. polars
//     literal=True maps to the plain glr call, polars regex
//     str.contains maps to str.contains_regex.
//   - rolling_* take (window, min_periods) positionally; polars
//     defaults min_samples to the window size.
//   - std/var with ddof map to std_ddof/var_ddof.
//   - polars unique(maintain_order=True) maps to unique(), which keeps
//     first-occurrence order in golars.
func glrForm(e *Expr) *Expr {
	kw := func(name string) *Expr {
		for _, k := range e.KW {
			if k.Name == name {
				return k.Val
			}
		}
		return nil
	}
	without := func(r *Expr, names ...string) *Expr {
		var out []KW
		for _, k := range r.KW {
			keep := true
			for _, n := range names {
				if k.Name == n {
					keep = false
				}
			}
			if keep {
				out = append(out, k)
			}
		}
		r.KW = out
		return r
	}
	isTrue := func(x *Expr) bool { return x != nil && x.Val == true }
	isFalse := func(x *Expr) bool { return x != nil && x.Val == false }
	r := e.Clone()
	switch {
	case strings.HasPrefix(e.Name, "rolling_") && !strings.HasSuffix(e.Name, "_by"):
		ms := kw("min_samples")
		if ms == nil {
			ms = e.Args[1]
		}
		r = without(r, "min_samples")
		if isFalse(kw("center")) {
			r = without(r, "center")
		}
		r.Args = append(r.Args[:2:2], ms.Clone())
	case e.Name == "std" || e.Name == "var":
		if len(e.Args) > 1 {
			r.Name += "_ddof"
		}
	case strings.HasPrefix(e.Name, "cum_"):
		if isFalse(kw("reverse")) {
			r = without(r, "reverse")
		}
	case e.Name == "cast":
		r = without(r, "strict")
	case e.Name == "str.contains":
		if isTrue(kw("literal")) {
			r = without(r, "literal")
		} else {
			r.Name = "str.contains_regex"
		}
	case e.Name == "str.replace" || e.Name == "str.replace_all" || e.Name == "str.count_matches" || e.Name == "str.find":
		if isTrue(kw("literal")) {
			r = without(r, "literal")
		} else {
			// glr has no regex form of these; the name does not parse.
			r.Name += "_regex"
		}
	case e.Name == "dt.replace_time_zone" && len(e.KW) > 0:
		amb, ne := kw("ambiguous"), kw("non_existent")
		if amb == nil {
			amb = Raw("raise")
		}
		if ne == nil {
			ne = Raw("raise")
		}
		r = without(r, "ambiguous", "non_existent")
		r.Name = "dt.replace_time_zone_with"
		r.Args = append(r.Args, amb.Clone(), ne.Clone())
	case e.Name == "list.get":
		if oob := kw("null_on_oob"); oob != nil {
			r = without(r, "null_on_oob")
			r.Name = "list.get_with_oob"
			r.Args = append(r.Args, oob.Clone())
		}
	case e.Name == "unique":
		if isTrue(kw("maintain_order")) {
			r = without(r, "maintain_order")
		}
	case e.Name == "ewm_mean" || e.Name == "ewm_std" || e.Name == "ewm_var":
		if a := kw("alpha"); a != nil {
			r = without(r, "alpha")
			r.Args = append(r.Args, a.Clone())
		}
	case e.Name == "quantile":
		if m := kw("interpolation"); m != nil {
			r = without(r, "interpolation")
			r.Name = "quantile_with"
			r.Args = append(r.Args, m.Clone())
		}
	case e.Name == "fill_null":
		if st := kw("strategy"); st != nil {
			switch st.Val {
			case "forward":
				r = without(r, "strategy")
				r.Name = "forward_fill"
				r.Args = append(r.Args, Raw(int64(0)))
			case "backward":
				r = without(r, "strategy")
				r.Name = "backward_fill"
				r.Args = append(r.Args, Raw(int64(0)))
			}
		}
	}
	return r
}
