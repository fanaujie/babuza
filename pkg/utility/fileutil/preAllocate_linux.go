package fileutil

import (
	"golang.org/x/sys/unix"
	"os"
)

func allocate(f *os.File, offset, size int64) error {
	err := unix.Fallocate(int(f.Fd()), 0, offset, size)
	if err != nil {
		errno, ok := err.(unix.Errno)
		if ok && errno == unix.ENOTSUP {
			return nil
		}
	}
	return err
}
