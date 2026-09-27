package csv

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/internal/mmapfile"
)

// TestTruncatedMappingIsError maps a CSV file, truncates it and parses
// the stale mapping. Touching pages past the new end of file faults;
// the reader must return an error instead of crashing. Truncating to
// zero faults in the header read on the calling goroutine; keeping the
// first 64 KiB faults inside the parallel parse workers.
func TestTruncatedMappingIsError(t *testing.T) {
	var b strings.Builder
	b.WriteString("a,b,s\n")
	for i := range 200_000 {
		b.WriteString(strconv.Itoa(i))
		b.WriteString(",1.5,word")
		b.WriteString(strconv.Itoa(i % 97))
		b.WriteByte('\n')
	}
	for _, keep := range []int64{0, 64 << 10} {
		t.Run(strconv.FormatInt(keep, 10), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "in.csv")
			if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
				t.Fatal(err)
			}
			m, err := mmapfile.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if !m.Mapped() {
				t.Skip("file not mapped on this platform")
			}
			if err := os.Truncate(path, keep); err != nil {
				t.Fatal(err)
			}
			df, err := readGuarded(context.Background(), m.Data, resolve(nil))
			if err == nil {
				df.Release()
				t.Fatal("read of a truncated mapping succeeded")
			}
			if !errors.Is(err, mmapfile.ErrFault) {
				t.Fatalf("err = %v, want a fault error", err)
			}
		})
	}
}
