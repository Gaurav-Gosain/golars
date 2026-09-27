package sql_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
	"github.com/Gaurav-Gosain/golars/sql"
)

// polars SQL: ORDER BY a, b DESC sorts by a first (nulls last for ASC,
// first for DESC):
// a=[1, 2, 2, None], b=[3, 4, 1, 2] and ORDER BY a DESC -> [None, 2, 2, 1].
func TestSQLOrderByNullsAndKeys(t *testing.T) {
	a, _ := series.FromInt64("a", []int64{2, 0, 1, 2}, []bool{true, false, true, true})
	b, _ := series.FromInt64("b", []int64{1, 2, 3, 4}, nil)
	df, _ := dataframe.New(a, b)
	defer df.Release()
	s := sql.NewSession()
	defer s.Close()
	if err := s.Register("df", df); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		q, col, want string
	}{
		{"SELECT * FROM df ORDER BY a, b DESC", "a", "[1, 2, 2, None]"},
		{"SELECT * FROM df ORDER BY a, b DESC", "b", "[3, 4, 1, 2]"},
		{"SELECT * FROM df ORDER BY a DESC", "a", "[None, 2, 2, 1]"},
	}
	for _, c := range cases {
		out, err := s.Query(context.Background(), c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		col, _ := out.Column(c.col)
		if got := testutil.PyRepr(col.Chunk(0)); got != c.want {
			t.Errorf("%s: %s = %s, want %s", c.q, c.col, got, c.want)
		}
		out.Release()
	}
}
