// Package mmapfile maps a whole file read-only into memory, falling back
// to reading it when mapping is unavailable. Readers use it to parse
// files in place: no copy through the kernel and no anonymous buffer
// the size of the file.
package mmapfile

import (
	"fmt"
	"io"
	"os"
)

// File is a read-only view of a file's bytes.
type File struct {
	// Data holds the file contents. It is invalid after Close.
	Data   []byte
	mapped bool
}

// minMapSize is the smallest file that is mapped; smaller files are read.
const minMapSize = 64 << 10

// Open maps path. Files smaller than 64 KiB, non-regular files and
// platforms without mmap are read into memory instead.
func Open(path string) (*File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Mode().IsRegular() && st.Size() >= minMapSize && int64(int(st.Size())) == st.Size() {
		if b, err := mmap(f, int(st.Size())); err == nil {
			return &File{Data: b, mapped: true}, nil
		}
	}
	size := 0
	if st.Mode().IsRegular() {
		size = int(st.Size())
	}
	b, err := readAll(f, size)
	if err != nil {
		return nil, err
	}
	return &File{Data: b}, nil
}

// Mapped reports whether Data is a memory mapping.
func (m *File) Mapped() bool { return m.mapped }

// Close releases the mapping. Data must not be used afterwards.
func (m *File) Close() error {
	if m == nil || m.Data == nil {
		return nil
	}
	b := m.Data
	m.Data = nil
	if m.mapped {
		return munmap(b)
	}
	return nil
}

func readAll(r io.Reader, size int) ([]byte, error) {
	if size <= 0 {
		return io.ReadAll(r)
	}
	buf := make([]byte, size)
	n, err := io.ReadFull(r, buf)
	switch err {
	case nil:
	case io.ErrUnexpectedEOF, io.EOF:
		return buf[:n], nil
	default:
		return nil, fmt.Errorf("read: %w", err)
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return append(buf, rest...), nil
}
