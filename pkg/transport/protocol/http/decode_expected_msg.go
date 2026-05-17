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

package http

import (
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/gogo/protobuf/proto"
	"io"
)

func decodeExpectedMessage(r io.Reader, expectedSize int64, expectedMsg proto.Message) error {
	if expectedSize == 0 {
		return proto.Unmarshal(nil, expectedMsg)
	}
	var byteSlice *allocator.ByteSlice
	byteSlice = allocator.Acquire(int(expectedSize))
	defer allocator.Release(byteSlice)
	buf := byteSlice.Buffer[:expectedSize]
	if _, err := io.ReadFull(r, buf); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
			return err
		}
		return err
	}
	return proto.Unmarshal(buf, expectedMsg)
}
