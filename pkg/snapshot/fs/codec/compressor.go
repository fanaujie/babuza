package codec

import (
	"bufio"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/golang/snappy"
	"io"
)

const (
	bufferSize = 4096
)

func CreateCompressor(c babuzapb.SnapshotFileCompressionType, f io.WriteCloser) (io.WriteCloser, error) {
	switch c {
	case babuzapb.SnapshotFileCompression_None:
		return newBufferWrapper(f), nil
	case babuzapb.SnapshotFileCompression_Snappy:
		return newSnappyWrapper(f), nil
	}
	return nil, fmt.Errorf("snapshotor: not support compress type. type(%d)", c)
}

func CreateDeCompressor(c babuzapb.SnapshotFileCompressionType, f io.Reader) (io.Reader, error) {
	switch c {
	case babuzapb.SnapshotFileCompression_None:
		return bufio.NewReader(f), nil
	case babuzapb.SnapshotFileCompression_Snappy:
		return snappy.NewReader(f), nil
	}
	return nil, fmt.Errorf("snapshotor: not support compress type. type(%d)", c)
}

type bufferWrapper struct {
	f  io.WriteCloser
	bw *bufio.Writer
}

func newBufferWrapper(f io.WriteCloser) *bufferWrapper {
	return &bufferWrapper{
		f:  f,
		bw: bufio.NewWriterSize(f, 4096),
	}
}

func (bw *bufferWrapper) Write(p []byte) (int, error) {
	return bw.bw.Write(p)
}

func (bw *bufferWrapper) Close() error {
	if err := bw.bw.Flush(); err != nil {
		return err
	}
	if err := bw.f.Close(); err != nil {
		return err
	}
	return nil
}

type snappyWrapper struct {
	f  io.WriteCloser
	sw *snappy.Writer
}

func newSnappyWrapper(f io.WriteCloser) *snappyWrapper {
	return &snappyWrapper{
		f:  f,
		sw: snappy.NewBufferedWriter(f),
	}
}

func (sw *snappyWrapper) Write(p []byte) (int, error) {
	return sw.sw.Write(p)
}

func (sw *snappyWrapper) Close() error {
	if err := sw.sw.Close(); err != nil {
		return err
	}
	if err := sw.f.Close(); err != nil {
		return err
	}
	return nil
}
