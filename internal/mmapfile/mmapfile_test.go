package mmapfile_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Gaurav-Gosain/golars/internal/mmapfile"
)

// truncatedMapping maps a 1 MiB file and then truncates it to zero, so
// every page of the mapping faults on access. It skips where the
// platform does not map files.
func truncatedMapping(t *testing.T) *mmapfile.File {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("no mmap")
	}
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, make([]byte, 1<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := mmapfile.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if !m.Mapped() {
		t.Skip("file not mapped")
	}
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestReaderFaultIsError(t *testing.T) {
	m := truncatedMapping(t)
	r := mmapfile.NewReader(m.Data)
	buf := make([]byte, 4096)
	if _, err := r.ReadAt(buf, 1<<19); !errors.Is(err, mmapfile.ErrFault) {
		t.Fatalf("ReadAt err = %v, want ErrFault", err)
	}
	if _, err := io.ReadAll(r); !errors.Is(err, mmapfile.ErrFault) {
		t.Fatalf("Read err = %v, want ErrFault", err)
	}
}

func TestReaderReads(t *testing.T) {
	r := mmapfile.NewReader([]byte("hello world"))
	b := make([]byte, 5)
	if n, err := r.ReadAt(b, 6); n != 5 || err != nil || string(b) != "world" {
		t.Fatalf("ReadAt = %d %v %q", n, err, b)
	}
	all, err := io.ReadAll(r)
	if err != nil || string(all) != "hello world" {
		t.Fatalf("ReadAll = %q %v", all, err)
	}
	if _, err := r.Seek(-5, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if all, _ = io.ReadAll(r); string(all) != "world" {
		t.Fatalf("after Seek = %q", all)
	}
}
