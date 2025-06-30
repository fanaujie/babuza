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


package frame

import (
	"encoding/binary"
	"fmt"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"hash/crc32"
	"io"
)

type Reader struct {
	conn io.Reader
}

func NewReader(conn io.Reader) *Reader {
	return &Reader{
		conn: conn,
	}
}

func (r *Reader) ReadFrame(msgHandler func(msgType MessageType, msgBuf []byte) error) error {
	headerSliceBuf := allocator.Acquire(HeaderSize)
	defer allocator.Release(headerSliceBuf)

	headerBuf := headerSliceBuf.Buffer[:HeaderSize]
	if _, err := io.ReadFull(r.conn, headerBuf); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	h := binary.LittleEndian.Uint32(headerBuf[0:CrcOffset])
	crc := binary.LittleEndian.Uint32(headerBuf[CrcOffset:HeaderSize])
	msgSize := int((h & MsgSizeMask) >> MsgSizeShift)
	var readCrc uint32
	if msgSize > 0 {
		msgSliceBuf := allocator.Acquire(msgSize)
		defer allocator.Release(msgSliceBuf)
		messageBuf := msgSliceBuf.Buffer[:msgSize]
		if _, err := io.ReadFull(r.conn, messageBuf); err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		readCrc = crc32.Checksum(messageBuf, Crc32Table)
		if readCrc != crc {
			return fmt.Errorf("crc does not match.(expected=%d) (real=%d)", crc, readCrc)
		}
		return msgHandler(MessageType(h&MsgTypeMask), messageBuf)
	}
	if readCrc != crc {
		return fmt.Errorf("crc does not match.(expected=%d) (real=%d)", crc, readCrc)
	}
	return msgHandler(MessageType(h&MsgTypeMask), nil)
}
