package frame

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
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
	if msgSize > MaxMessageSize {
		return fmt.Errorf("message size %d exceeds max message size %d", msgSize, MaxMessageSize)
	}
	frameSize := HeaderSize + msgSize
	if frameSize > len(buf) {
		return fmt.Errorf("buffer size %d is not enough for frame size %d", len(buf), frameSize)
	}
	msgBuf := buf[HeaderSize:frameSize]
	if _, err := msg.MarshalTo(msgBuf); err != nil {
		return err
	}
	msgCrc := crc32.Checksum(msgBuf, Crc32Table)
	binary.LittleEndian.PutUint32(buf[0:CrcOffset], uint32((msgSize<<MsgSizeShift)|(int(msgType&MsgTypeMask))))
	binary.LittleEndian.PutUint32(buf[CrcOffset:HeaderSize], msgCrc)
	_, err := w.conn.Write(buf[:frameSize])
	return err
}
