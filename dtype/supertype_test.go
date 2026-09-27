package dtype

import (
	"strings"
	"testing"
)

// polarsAddTable is the dtype of col(a) + col(b) in polars 1.39.3 for
// the row and column dtypes below (bool + bool is left out: polars sums
// it as u32, which is an arithmetic rule, not a supertype).
const polarsAddTable = `
i8   i16 i32 i64 i16 i32 i64 f64 f32 f64 i8
i16  i16 i32 i64 i16 i32 i64 f64 f32 f64 i16
i32  i32 i32 i64 i32 i32 i64 f64 f64 f64 i32
i64  i64 i64 i64 i64 i64 i64 f64 f64 f64 i64
i16  i16 i32 i64 u8  u16 u32 u64 f32 f64 u8
i32  i32 i32 i64 u16 u16 u32 u64 f32 f64 u16
i64  i64 i64 i64 u32 u32 u32 u64 f64 f64 u32
f64  f64 f64 f64 u64 u64 u64 u64 f64 f64 u64
f32  f32 f64 f64 f32 f32 f64 f64 f32 f64 f32
f64  f64 f64 f64 f64 f64 f64 f64 f64 f64 f64
i8   i16 i32 i64 u8  u16 u32 u64 f32 f64 -
`

func TestNumericSupertypeMatchesPolars(t *testing.T) {
	names := []string{"i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "bool"}
	byName := map[string]DType{
		"i8": Int8(), "i16": Int16(), "i32": Int32(), "i64": Int64(),
		"u8": Uint8(), "u16": Uint16(), "u32": Uint32(), "u64": Uint64(),
		"f32": Float32(), "f64": Float64(), "bool": Bool(),
	}
	rows := strings.Split(strings.TrimSpace(polarsAddTable), "\n")
	for i, row := range rows {
		for j, want := range strings.Fields(row) {
			if want == "-" {
				continue
			}
			got, ok := NumericSupertype(byName[names[i]], byName[names[j]])
			if !ok || !got.Equal(byName[want]) {
				t.Errorf("supertype(%s, %s) = %s, want %s", names[i], names[j], got, want)
			}
		}
	}
}

func TestDynIntTarget(t *testing.T) {
	cases := []struct {
		v     int64
		other DType
		want  DType
	}{
		{3, Int8(), Int8()},
		{1000, Int8(), Int16()},
		{200, Int8(), Int16()},
		{1 << 40, Int32(), Int64()},
		{300, Uint8(), Uint16()},
		{70000, Uint8(), Uint32()},
		{-200, Uint8(), Int16()},
		{-1, Uint64(), Int64()},
		{2, Uint8(), Uint8()},
		{1 << 40, Float32(), Float32()},
	}
	for _, c := range cases {
		got, ok := DynIntTarget(c.v, c.other)
		if !ok || !got.Equal(c.want) {
			t.Errorf("DynIntTarget(%d, %s) = %s, want %s", c.v, c.other, got, c.want)
		}
	}
	if !DefaultIntLiteral(3).Equal(Int32()) || !DefaultIntLiteral(1<<31).Equal(Int64()) {
		t.Error("DefaultIntLiteral")
	}
}
