//go:build !(darwin || linux || freebsd || netbsd || openbsd || dragonfly)

package mmapfile

import (
	"errors"
	"os"
)

func mmap(*os.File, int) ([]byte, error) { return nil, errors.New("mmap unsupported") }

func munmap([]byte) error { return nil }
