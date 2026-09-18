package main

import (
	"strconv"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestFilterWithQuotedComment(t *testing.T) {
	s := newReviewState(t)
	err := s.handle(`filter name contains "a#b"`)
	if err != nil {
		t.Fatalf("quoted # must survive filter parse: %v", err)
	}
	out, err := s.materialize()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 1 || out.Width() != 1 {
		t.Fatalf("shape = %dx%d, want 1x1", out.Height(), out.Width())
	}
}

func TestParseNatBounds(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, tc := range []struct {
		input string
		want  int
		ok    bool
	}{
		{"0", 0, true},
		{"0010", 10, true},
		{strconv.Itoa(maxInt), maxInt, true},
		{strconv.FormatUint(uint64(maxInt)+1, 10), 0, false},
		{"18446744073709551616", 0, false},
		{"", 0, false},
		{"-1", 0, false},
		{"+1", 0, false},
	} {
		got, ok := parseNat(tc.input)
		if ok != tc.ok || ok && got != tc.want {
			t.Errorf("parseNat(%q) = (%d, %v), want (%d, %v)", tc.input, got, ok, tc.want, tc.ok)
		}
	}
}

func TestFormatPreservesContent(t *testing.T) {
	for _, input := range []string{
		"show\n", "show\n\n", "", "show",
		`with x = 'two  spaces'`,
		`with x = "escaped\"  spaces"`,
		"with x = 1 # keep   spacing\n",
	} {
		got := formatGlr(input)
		if got != input {
			t.Errorf("formatGlr(%q) = %q", input, got)
		}
		if again := formatGlr(got); again != got {
			t.Errorf("not idempotent: %q -> %q", got, again)
		}
	}
}

func newReviewState(t *testing.T) *state {
	t.Helper()
	mem := testutil.NewCheckedAllocator(t)
	col, err := series.FromString("name", []string{"alpha#beta", "other"}, nil, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(col)
	if err != nil {
		col.Release()
		t.Fatal(err)
	}
	s := newState(false)
	s.df = df
	t.Cleanup(func() {
		if s.df != nil {
			s.df.Release()
		}
		s.ReleaseAllFrames()
	})
	return s
}

func TestDispatcherQuotedComments(t *testing.T) {
	s := newReviewState(t)
	if err := s.handle(`filter name contains "alpha#beta" # note`); err != nil {
		t.Fatal(err)
	}
	out, err := s.materialize()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 1 {
		t.Fatalf("height = %d", out.Height())
	}
}

func TestKernelCellContinuation(t *testing.T) {
	s := newReviewState(t)
	resp := executeCell(s, kernelRequest{Code: "filter name contains \"alpha#beta\" \\\n  and name starts_with \"alpha\"\n"})
	if resp.Error != "" {
		t.Fatal(resp.Error)
	}
	if resp.Shape == nil || resp.Shape[0] != 1 {
		t.Fatalf("shape = %v", resp.Shape)
	}
}

func TestKernelPreservesExplicitDisplay(t *testing.T) {
	s := newReviewState(t)
	resp := executeCell(s, kernelRequest{Code: "select name\ntail 1"})
	if resp.Error != "" {
		t.Fatal(resp.Error)
	}
	if !strings.Contains(resp.Text, "other") {
		t.Fatalf("explicit tail result lost: %q", resp.Text)
	}
}

func TestKernelPreviewReportsCollectionError(t *testing.T) {
	s := newReviewState(t)
	resp := executeCell(s, kernelRequest{Code: "select missing"})
	if resp.Error == "" || resp.HTML != "" {
		t.Fatalf("error=%q html=%q", resp.Error, resp.HTML)
	}
}
