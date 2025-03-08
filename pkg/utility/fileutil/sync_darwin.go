package fileutil

import (
	"golang.org/x/sys/unix"
	"os"
)

func Sync(f *os.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)
	return err
}

func Datasync(f *os.File) error {
	return Sync(f)
}
