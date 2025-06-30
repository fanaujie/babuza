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
	memPool   *allocator.ByteSlicePool
	handler   Handler
	headerBuf []byte
}

func NewDecoder(reader io.Reader, memPool *allocator.ByteSlicePool, handler Handler) *Decoder {

	if reader == nil {
		panic("decoder: reader can not be nil")
	}
	if memPool == nil {
		panic("decoder: memPool can not be nil")
	}
	if handler == nil {
		panic("decoder: handler can not be nil")
	}
	return &Decoder{
		reader:    reader,
		memPool:   memPool,
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

	byteSlice := d.memPool.Acquire(nextReadSize)
	defer d.memPool.Release(byteSlice)
	if n, err := io.ReadFull(d.reader, byteSlice.Buffer[:nextReadSize]); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	} else if n != nextReadSize {
		return io.ErrUnexpectedEOF
	}

	return d.handler(logType, byteSlice.Buffer[:logSize], int64(HeaderSize+nextReadSize), crc)
}
