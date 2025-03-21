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
