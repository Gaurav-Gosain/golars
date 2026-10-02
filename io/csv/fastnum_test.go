package csv_test

import (
	"strings"
	"testing"

	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
)

// TestFastNumberEdges covers the fields the fused number parser hands
// back to the tokenizer and the terminators it accepts.
func TestFastNumberEdges(t *testing.T) {
	input := "i,f\r\n" +
		"1,1.5\r\n" +
		"-2,-0\r\n" +
		"+3,.5\r\n" +
		",1.\r\n" +
		"\"4\",\"2.25\"\r\n" +
		"5,7\r\n" +
		"123456789012345678,1e3\r\n" +
		"-999,-999\r\n" +
		"6,12345678901234567890"
	df := readString(t, input)
	wantI := []string{"1", "-2", "3", "<null>", "4", "5", "123456789012345678", "-999", "6"}
	wantF := []string{"1.5", "-0", "0.5", "1", "2.25", "7", "1000", "-999", "1.2345678901234567e+19"}
	for r := range wantI {
		if got := cell(t, df, "i", r); got != wantI[r] {
			t.Errorf("i row %d = %s, want %s", r, got, wantI[r])
		}
		if got := cell(t, df, "f", r); got != wantF[r] {
			t.Errorf("f row %d = %s, want %s", r, got, wantF[r])
		}
	}

	// A numeric null marker disables the fused path.
	df = readString(t, "a,b\n1,-999\n-999,2\n", iocsv.WithNullValues("-999"))
	if got := cell(t, df, "b", 0) + "," + cell(t, df, "a", 1); got != "<null>,<null>" {
		t.Errorf("null markers = %s", got)
	}

	// A float past the inference sample widens an integer column.
	var b strings.Builder
	b.WriteString("x\n")
	for range 200 {
		b.WriteString("7\n")
	}
	b.WriteString("7.5\n")
	df = readString(t, b.String())
	if got := cell(t, df, "x", 200); got != "7.5" {
		t.Errorf("widened value = %s", got)
	}
}
