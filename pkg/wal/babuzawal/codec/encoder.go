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
	memPool    *allocator.ByteSlicePool
	currentCrc uint32
}

func NewEncoder(writer io.Writer, memPool *allocator.ByteSlicePool, currentCrc uint32) *Encoder {
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
	allocBuf := e.memPool.Acquire(total)
	defer e.memPool.Release(allocBuf)

	var err error
	binary.LittleEndian.PutUint32(allocBuf.Buffer[:crcOffset], uint32((logSize<<logSizeShift)|int(logType<<logTypeShift)|padding&paddingMask))
	e.currentCrc, err = marshaller.Encode(allocBuf.Buffer[HeaderSize:HeaderSize+logSize], logSize, e.currentCrc)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(allocBuf.Buffer[crcOffset:crcOffset+crcSize], e.currentCrc)
	paddingOffset := HeaderSize + logSize
	for i := 0; i < padding; i++ {
		allocBuf.Buffer[paddingOffset+i] = 0
	}
	if _, err = e.writer.Write(allocBuf.Buffer[:total]); err != nil {
		return err
	}
	return nil
}
