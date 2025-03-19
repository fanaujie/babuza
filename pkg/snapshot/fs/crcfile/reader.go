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
	if err != nil {
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
