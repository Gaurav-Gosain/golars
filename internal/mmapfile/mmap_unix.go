//go:build darwin || linux || freebsd || netbsd || openbsd || dragonfly

package mmapfile

import (
	"os"

	"golang.org/x/sys/unix"
)

func mmap(f *os.File, size int) ([]byte, error) {
	return unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
}

func munmap(b []byte) error { return unix.Munmap(b) }
