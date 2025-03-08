package http

import (
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/gogo/protobuf/proto"
	"io"
)

func decodeExpectedMessage(r io.Reader, expectedSize int64, expectedMsg proto.Message) error {
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
