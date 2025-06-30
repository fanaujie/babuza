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

type Reader struct {
	r        io.ReadCloser
	h        hash.Hash64
	fileSize int
	tee      io.Reader
}

func CreateReader(r io.ReadCloser) *Reader {
	h := crc64.New(Crc64Table)
	return &Reader{
		r:   r,
		h:   h,
		tee: io.TeeReader(r, h),
	}
}

func (cr *Reader) Read(p []byte) (int, error) {
	n, err := cr.tee.Read(p)
	if err != nil && err != io.EOF {
		return n, err
	}
	cr.fileSize += n
	return n, err
}

func (cr *Reader) Crc() uint64 {
	return cr.h.Sum64()
}

func (cr *Reader) FileSize() int {
	return cr.fileSize
}

func (cr *Reader) Close() error {
	return cr.r.Close()
}
