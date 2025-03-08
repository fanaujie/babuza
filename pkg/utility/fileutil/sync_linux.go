package fileutil

import (
	"os"
	"syscall"
)

func Sync(f *os.File) error {
	return f.Sync()
}

func Datasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
