package frame

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	crcOffset      = 4
	headerSize     = 8
	msgSizeShift   = 8
	msgSizeMask    = ^uint32(msgSizeShift)
	msgTypeMask    = 0xff
	maxMessageSize = 0xffffff
)

type Writer struct {
	conn io.Writer
}

func NewWriter(conn io.Writer) *Writer {
	return &Writer{conn: conn}
}

// Encode
// headerBuf = { 3 bytes message length | 1 bytes (message type) | 4 bytes crc32}
// frame layout = { headerBuf |  n bytes message body | padding }
func (w *Writer) Encode(buf []byte, msgType MessageType, msg Message) error {
	//TODO: need padding? check performance
	msgSize := msg.Size()
	if msgSize > maxMessageSize {
		return fmt.Errorf("message size %d exceeds max message size %d", msgSize, maxMessageSize)
	}
	frameSize := headerSize + msgSize
	if frameSize > len(buf) {
		return fmt.Errorf("buffer size %d is not enough for frame size %d", len(buf), frameSize)
	}
	msgBuf := buf[headerSize:frameSize]
	if _, err := msg.MarshalTo(msgBuf); err != nil {
		return err
	}
	msgCrc := crc32.Checksum(msgBuf, Crc32Table)
	binary.LittleEndian.PutUint32(buf[0:crcOffset], uint32((msgSize<<msgSizeShift)|(int(msgType&msgTypeMask))))
	binary.LittleEndian.PutUint32(buf[crcOffset:headerSize], msgCrc)
	_, err := w.conn.Write(buf[:frameSize])
	return err
}
