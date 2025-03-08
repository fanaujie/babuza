//go:build !linux && !darwin
// +build !linux,!darwin

package fileutil

import "os"

func Sync(f *os.File) error {
	return f.Sync()
}

func Datasync(f *os.File) error {
	return f.Sync()
}
