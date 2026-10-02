package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestJoinCommandOptions(t *testing.T) {
	cases := []struct {
		stmt       string
		rows, cols int
		names      string
	}{
		{"join eng on name,dept semi", 3, 3, "name,age,dept"},
		{"join eng on name anti", 2, 3, "name,age,dept"},
		{"join eng on name full suffix _e", 5, 6, "name,age,dept,name_e,age_e,dept_e"},
		{"join eng on name,dept left suffix _e", 5, 4, "name,age,dept,age_e"},
		{"join eng on name right", 3, 5, "age,dept,name,age_right,dept_right"},
		{"join eng on name outer", 5, 6, "name,age,dept,name_right,age_right,dept_right"},
	}
	for _, c := range cases {
		t.Run(c.stmt, func(t *testing.T) {
			s, dir := scriptState(t)
			path := filepath.Join(dir, "people.csv")
			mustRun(t, s, "load "+path+" as people", "filter age < 31", "stash eng", "use people", c.stmt)
			if h, w := focusShape(t, s); h != c.rows || w != c.cols {
				t.Fatalf("shape = %dx%d, want %dx%d", h, w, c.rows, c.cols)
			}
			df, err := s.materialize()
			if err != nil {
				t.Fatal(err)
			}
			defer df.Release()
			if got := strings.Join(df.ColumnNames(), ","); got != c.names {
				t.Fatalf("columns = %s, want %s", got, c.names)
			}
		})
	}
	s, _ := scriptState(t)
	mustRun(t, s, "stash eng")
	if err := s.handle("join eng on name sideways"); err == nil || !strings.Contains(err.Error(), `unknown join option "sideways"`) {
		t.Fatalf("err = %v", err)
	}
}
