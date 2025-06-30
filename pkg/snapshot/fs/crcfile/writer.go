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


package crcfile

import (
	"hash"
	"hash/crc64"
	"io"
)

var (
	Crc64Table = crc64.MakeTable(crc64.ECMA)
)

type Writer struct {
	w        io.WriteCloser
	h        hash.Hash64
	fileSize int
}

func CreateWriter(w io.WriteCloser) *Writer {
	h := crc64.New(Crc64Table)
	return &Writer{
		w: w,
		h: h,
	}
}

func (cw *Writer) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	if err != nil {
		return n, err
	}
	cw.fileSize += n
	return cw.h.Write(p)
}

func (cw *Writer) FileSize() int {
	return cw.fileSize
}

func (cw *Writer) Crc() uint64 {
	return cw.h.Sum64()
}

func (cw *Writer) Close() error {
	return cw.w.Close()
}
