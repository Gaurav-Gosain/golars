package dtype_test

import (
	"slices"
	"testing"

	"github.com/Gaurav-Gosain/golars/dtype"
)

func TestCategoricalDType(t *testing.T) {
	c := dtype.Categorical()
	if !c.IsCategorical() || c.IsEnum() || !c.IsDictionary() {
		t.Fatal("categorical predicates")
	}
	if c.String() != "cat" {
		t.Fatalf("repr = %q", c.String())
	}
	if c.IsString() || c.IsNested() {
		t.Fatal("categorical must not look like str or nested")
	}
	if !c.Equal(dtype.Categorical()) {
		t.Fatal("categorical must equal itself")
	}
}

func TestEnumDType(t *testing.T) {
	e := dtype.Enum([]string{"x", "y"})
	if !e.IsEnum() || e.IsCategorical() {
		t.Fatal("enum predicates")
	}
	if e.String() != "enum" {
		t.Fatalf("repr = %q", e.String())
	}
	cats, ok := e.EnumCategories()
	if !ok || !slices.Equal(cats, []string{"x", "y"}) {
		t.Fatalf("categories = %v %v", cats, ok)
	}
	if e.Equal(dtype.Categorical()) {
		t.Fatal("enum must not equal categorical")
	}
	if e.Equal(dtype.Enum([]string{"y", "x"})) {
		t.Fatal("enums with different categories must differ")
	}
	if !e.Equal(dtype.Enum([]string{"x", "y"})) {
		t.Fatal("enums with same categories must be equal")
	}
	// An Enum recovered from arrow has no category list and compares
	// equal on the arrow type alone.
	if !dtype.FromArrow(e.Arrow()).Equal(e) || !dtype.FromArrow(e.Arrow()).IsEnum() {
		t.Fatal("round trip through arrow")
	}
	if _, err := dtype.NewEnum([]string{"a", "a"}); err == nil {
		t.Fatal("duplicate categories must error")
	}
}
