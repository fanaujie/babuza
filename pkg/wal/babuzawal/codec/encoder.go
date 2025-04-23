package codec

import (
	"encoding/binary"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"hash/crc32"
	"io"
)

var (
	crc32Table = crc32.MakeTable(crc32.Castagnoli)
)

const (
	crcOffset    = 4
	crcSize      = 4
	HeaderSize   = 8
	logSizeShift = 8
	logSizeMask  = ^uint32(logSizeShift)
	paddingMask  = 7
	logTypeShift = 3
	logTypeMask  = uint32(0x1f << logTypeShift)
	maxLogSize   = 0xffffff
)

type Encoder struct {
	writer     io.Writer
	memPool    *allocator.TwoLevelPool
	currentCrc uint32
}

func NewEncoder(writer io.Writer, memPool *allocator.TwoLevelPool, currentCrc uint32) *Encoder {
	if writer == nil {
		panic("encoder: writer can not be nil")
	}
	if memPool == nil {
		panic("encoder: memPool can not be nil")
	}
	return &Encoder{
		writer:     writer,
		memPool:    memPool,
		currentCrc: currentCrc,
	}
}

func (e *Encoder) LastCrc() uint32 {
	return e.currentCrc
}

func (e *Encoder) calcPadding(logSize int) int {
	return (8 - ((HeaderSize + logSize) % 8)) % 8
}

// Encode header = { 3 bytes log size |  1 bytes ( 5 bits log type | 3 bits padding )}
// log layout = { header |  n bytes log body | padding }
func Encode[T EncodeLog](e *Encoder, logType pb.LogType, logSize int, marshaller T) error {
	if logSize > maxLogSize {
		return ErrLogSizeExceedLimit
	}
	padding := e.calcPadding(logSize)
	total := HeaderSize + logSize + padding
	allocBuf, p := e.memPool.Acquire(total)
	if allocBuf == nil {
		allocBuf = p.Buffer
		defer e.memPool.Release(p)
	}
	var err error
	binary.LittleEndian.PutUint32(allocBuf[:crcOffset], uint32((logSize<<logSizeShift)|int(logType<<logTypeShift)|padding&paddingMask))
	e.currentCrc, err = marshaller.Encode(allocBuf[HeaderSize:HeaderSize+logSize], logSize, e.currentCrc)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(allocBuf[crcOffset:crcOffset+crcSize], e.currentCrc)
	paddingOffset := HeaderSize + logSize
	for i := 0; i < padding; i++ {
		allocBuf[paddingOffset+i] = 0
	}
	if _, err = e.writer.Write(allocBuf[:total]); err != nil {
		return err
	}
	return nil
}
