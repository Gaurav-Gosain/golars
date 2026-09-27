package series

// Tests against the vectors generated from the Lean model in verify/.
// See docs/verification.md.

import (
	"encoding/json"
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
)

func loadVerified(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile("../testdata/verified/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func hexBits(t *testing.T, s string) uint64 {
	t.Helper()
	u, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// TestVerifiedFloatRank checks floatRank, the key of the float64 argsort,
// on edge patterns and random bit patterns. The model proves this rank is
// an exact order embedding of the polars float order.
func TestVerifiedFloatRank(t *testing.T) {
	var v struct {
		Float []struct {
			Bits  string `json:"bits"`
			IsNaN bool   `json:"is_nan"`
			Rank  string `json:"rank"`
		} `json:"float"`
	}
	loadVerified(t, "float_keys.json", &v)
	for _, c := range v.Float {
		f := math.Float64frombits(hexBits(t, c.Bits))
		if got, want := floatRank(f), hexBits(t, c.Rank); got != want {
			t.Fatalf("floatRank(%s) = %016x, want %016x", c.Bits, got, want)
		}
		if math.IsNaN(f) != c.IsNaN {
			t.Fatalf("%s: NaN classification differs", c.Bits)
		}
	}
}

func TestVerifiedArgSort(t *testing.T) {
	var cases []struct {
		DType     string            `json:"dtype"`
		Values    []json.RawMessage `json:"values"`
		Desc      bool              `json:"desc"`
		NullsLast bool              `json:"nulls_last"`
		Perm      []int             `json:"perm"`
	}
	loadVerified(t, "sort.json", &cases)
	for ci, c := range cases {
		alloc := testutil.NewCheckedAllocator(t)
		valid := make([]bool, len(c.Values))
		var s *Series
		var err error
		if c.DType == "f64" {
			vals := make([]float64, len(c.Values))
			for i, r := range c.Values {
				var h *string
				if err := json.Unmarshal(r, &h); err != nil {
					t.Fatal(err)
				}
				if h != nil {
					vals[i], valid[i] = math.Float64frombits(hexBits(t, *h)), true
				}
			}
			s, err = FromFloat64("x", vals, valid, WithAllocator(alloc))
		} else {
			vals := make([]int64, len(c.Values))
			for i, r := range c.Values {
				var p *int64
				if err := json.Unmarshal(r, &p); err != nil {
					t.Fatal(err)
				}
				if p != nil {
					vals[i], valid[i] = *p, true
				}
			}
			s, err = FromInt64("x", vals, valid, WithAllocator(alloc))
		}
		if err != nil {
			t.Fatal(err)
		}
		idx, err := s.ArgSortWith(c.Desc, c.NullsLast)
		if err != nil {
			t.Fatal(err)
		}
		testutil.AssertEqualAny(t, idx, c.Perm)
		if !c.Desc && c.NullsLast {
			// ArgSort is ascending with nulls last (the radix paths).
			idx, err := s.ArgSort()
			if err != nil {
				t.Fatal(err)
			}
			testutil.AssertEqualAny(t, idx, c.Perm)
		}
		if t.Failed() {
			t.Fatalf("case %d (%s n=%d desc=%v nulls_last=%v)", ci, c.DType, len(c.Values), c.Desc, c.NullsLast)
		}
		s.Release()
	}
}
