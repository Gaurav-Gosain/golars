//go:build !amd64 || noasm

package compute

// The arm64 Go compiler auto-vectorizes max/min int64 reductions into
// NEON SMAX / SMIN quite well (intrinsics on builtin max/min); a
// hand-rolled kernel isn't a clear win. On arm64 we leave the hooks
// nil so dataframe falls back to its own scalar loop (which is the
// already-vectorized path).
//
// MaxInt64PairFold / MinInt64PairFold are still declared so the
// dataframe package can reference them unconditionally.

// MaxInt64PairFold is the AVX2-only hook; nil on non-amd64 means
// "use the scalar fallback".
var MaxInt64PairFold func(buf, col []int64) int

var MinInt64PairFold func(buf, col []int64) int

// MaxInt64PairFoldNT and MinInt64PairFoldNT are the NT-store
// variants - same story: nil on non-amd64 so the dataframe
// dispatcher falls back to the cached path.
var MaxInt64PairFoldNT func(buf, col []int64) int
var MinInt64PairFoldNT func(buf, col []int64) int
