package mmapfile

import (
	"errors"
	"fmt"
	"io"
	"runtime/debug"
)

// ErrFault reports a memory fault while reading a mapping, which happens
// when the file shrinks while it is mapped.
var ErrFault = errors.New("file changed while reading")

// IsFault reports whether a recovered panic value is a memory fault
// (raised as a panic under debug.SetPanicOnFault).
func IsFault(p any) bool {
	_, ok := p.(interface{ Addr() uintptr })
	return ok
}

// Recover turns a memory fault panic into an error stored in *err. Other
// panics propagate. Use it together with SetPanicOnFault in every
// goroutine that touches mapped memory:
//
//	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
//	defer mmapfile.Recover(&err)
func Recover(err *error) {
	if p := recover(); p != nil {
		if !IsFault(p) {
			panic(p)
		}
		if e, ok := p.(error); ok && errors.Is(e, ErrFault) {
			*err = e
			return
		}
		*err = fmt.Errorf("%w: %v", ErrFault, p)
	}
}

// Reader reads from an in-memory (possibly mapped) byte slice. Unlike
// bytes.Reader, a memory fault during a copy becomes an error instead of
// crashing the process, so it is safe to hand to code that reads from
// other goroutines.
type Reader struct {
	b   []byte
	off int64
}

// NewReader returns a Reader over b.
func NewReader(b []byte) *Reader { return &Reader{b: b} }

// Size returns the length of the underlying data.
func (r *Reader) Size() int64 { return int64(len(r.b)) }

// Len returns the number of unread bytes.
func (r *Reader) Len() int { return len(r.b) - int(min(r.off, int64(len(r.b)))) }

// ReadAt implements io.ReaderAt.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("mmapfile: negative offset")
	}
	if off >= int64(len(r.b)) {
		return 0, io.EOF
	}
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer Recover(&err)
	n = copy(p, r.b[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (int, error) {
	if r.off >= int64(len(r.b)) {
		return 0, io.EOF
	}
	n, err := r.ReadAt(p, r.off)
	r.off += int64(n)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, err
}

// Seek implements io.Seeker.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.off + offset
	case io.SeekEnd:
		abs = int64(len(r.b)) + offset
	default:
		return 0, errors.New("mmapfile: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("mmapfile: negative position")
	}
	r.off = abs
	return abs, nil
}
