package frame

import (
	"encoding/binary"
	"fmt"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"hash/crc32"
	"io"
)

type Reader struct {
	conn       io.Reader
	maxBufSize int
}

func NewReader(conn io.Reader, maxBufSize int) *Reader {
	return &Reader{
		conn:       conn,
		maxBufSize: maxBufSize,
	}
}

func (r *Reader) ReadFrame(msgHandler func(msgType MessageType, msgBuf []byte) error) error {
	sliceBuf := allocator.Acquire(r.maxBufSize)
	defer allocator.Release(sliceBuf)

	headerBuf := sliceBuf.Buffer[:headerSize]
	if _, err := io.ReadFull(r.conn, headerBuf); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	h := binary.LittleEndian.Uint32(headerBuf[0:crcOffset])
	msgSize := int((h & msgSizeMask) >> msgSizeShift)
	crc := binary.LittleEndian.Uint32(headerBuf[crcOffset:headerSize])

	messageBuf := sliceBuf.Buffer[headerSize : headerSize+msgSize]
	if _, err := io.ReadFull(r.conn, messageBuf); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	readCrc := crc32.Checksum(messageBuf, Crc32Table)
	if readCrc != crc {
		return fmt.Errorf("crc does not match.(expected=%d) (real=%d)", crc, readCrc)
	}
	return msgHandler(MessageType(h&msgTypeMask), messageBuf)
}
