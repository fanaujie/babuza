package codec

import (
	"encoding/binary"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"io"
)

type Handler func(logType pb.LogType, logBuf []byte, logSizeWithPadding int64, logCrc uint32) error

type Decoder struct {
	reader    io.Reader
	cascade   *allocator.TwoLevelPool
	handler   Handler
	headerBuf []byte
}

func NewDecoder(reader io.Reader, cascade *allocator.TwoLevelPool, handler Handler) *Decoder {

	if reader == nil {
		panic("decoder: reader can not be nil")
	}
	if cascade == nil {
		panic("decoder: cascade can not be nil")
	}
	if handler == nil {
		panic("decoder: handler can not be nil")
	}
	return &Decoder{
		reader:    reader,
		cascade:   cascade,
		handler:   handler,
		headerBuf: make([]byte, HeaderSize),
	}
}

func (d *Decoder) Decode() error {
	if _, err := io.ReadFull(d.reader, d.headerBuf); err != nil {
		return err
	}
	h := binary.LittleEndian.Uint32(d.headerBuf[0:crcOffset])
	logSize := int((h & logSizeMask) >> logSizeShift)
	padding := int(h & paddingMask)
	logType := pb.LogType((h & logTypeMask) >> logTypeShift)
	crc := binary.LittleEndian.Uint32(d.headerBuf[crcOffset:HeaderSize])
	nextReadSize := logSize + padding
	if logSize == 0 && padding == 0 && crc == 0 {
		//touch pre-allocate file space
		return io.EOF
	}
	// entry data may be nil, only header
	if nextReadSize == 0 {
		return d.handler(logType, nil, HeaderSize, crc)
	}

	allocBuf, secondPool := d.cascade.Acquire(nextReadSize)
	if allocBuf == nil {
		allocBuf = secondPool.Buffer
		defer d.cascade.Release(secondPool)
	}
	if n, err := io.ReadFull(d.reader, allocBuf[:nextReadSize]); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	} else if n != nextReadSize {
		return io.ErrUnexpectedEOF
	}

	return d.handler(logType, allocBuf[:logSize], int64(HeaderSize+nextReadSize), crc)
}
