// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package fileutil

import (
	"encoding/binary"
	"io"
	"os"
)

const (
	DirMode  = 0755
	FileMode = 0755
)

func Exist(filePath string) bool {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	}
	return true
}

func FileSize(filePath string) (int64, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func IsDirWriteable(dir string) error {
	fd, err := os.CreateTemp(dir, ".tmp")
	if err != nil {
		return err
	}
	clear := func(err0 error, f *os.File) error {
		if err1 := f.Close(); err1 != nil {
			return err1
		}
		if err1 := os.Remove(f.Name()); err1 != nil {
			return err1
		}
		return err0
	}

	_, err = fd.Write([]byte{1})
	if err != nil {
		return clear(err, fd)
	}
	return clear(nil, fd)
}

func CreateDirAndTouch(dir string) error {
	if Exist(dir) {
		return os.ErrExist
	}
	if err := os.MkdirAll(dir, DirMode); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "test")
	if err != nil {
		return err
	}
	return os.Remove(f.Name())
}

func AllocateFileSpace(f *os.File, offset, size int64) error {
	if size == 0 {
		return nil
	}
	return allocate(f, offset, size)
}

func SyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err = Sync(f); err != nil {
		return err
	}
	return f.Close()
}

func FileWriteUint64(w io.Writer, buf []byte, v uint64) error {
	binary.LittleEndian.PutUint64(buf, v)
	if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}

func FileReadUint64(r io.Reader, buf []byte) (uint64, error) {
	if n, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	} else if n != 8 {
		return 0, io.ErrUnexpectedEOF
	}
	return binary.LittleEndian.Uint64(buf), nil
}
