package session

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"io"
)

const (
	stateMachineResponse    uint64 = 1
	stateMachineResponseErr uint64 = 2
	stateMachineResponseNil uint64 = 3
)

type applyResultSerializer struct {
	rs ibabuza.ResponseSerializer
}

func newApplyResultSerializer(rs ibabuza.ResponseSerializer) *applyResultSerializer {
	return &applyResultSerializer{
		rs: rs,
	}
}

func (m *applyResultSerializer) Marshal(w io.Writer, ar ibabuza.ApplyResult) error {
	sliceBuf := allocator.Acquire(8)
	defer allocator.Release(sliceBuf)
	buf8Bytes := sliceBuf.Buffer[:8]
	if err := fileutil.FileWriteUint64(w, buf8Bytes, ar.LogIndex); err != nil {
		return err
	}
	if ar.Response == nil {
		if err := fileutil.FileWriteUint64(w, buf8Bytes, stateMachineResponseNil); err != nil {
			return err
		}
	} else {
		if err, ok := ar.Response.(error); ok {
			if err := fileutil.FileWriteUint64(w, buf8Bytes, stateMachineResponseErr); err != nil {
				return err
			}
			data := []byte(err.Error())
			if err := fileutil.FileWriteUint64(w, buf8Bytes, uint64(len(data))); err != nil {
				return err
			}
			if _, err := w.Write(data); err != nil {
				return err
			}
		} else {
			if err := fileutil.FileWriteUint64(w, buf8Bytes, stateMachineResponse); err != nil {
				return err
			}
			if err := m.rs.Serialize(w, ar.Response); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *applyResultSerializer) Unmarshal(r io.Reader) (ibabuza.ApplyResult, error) {
	sliceBuf := allocator.Acquire(8)
	defer allocator.Release(sliceBuf)
	buf8Bytes := sliceBuf.Buffer[:8]
	logIndex, err := fileutil.FileReadUint64(r, buf8Bytes)
	if err != nil {
		return ibabuza.ApplyResult{}, err
	}
	responseType, err := fileutil.FileReadUint64(r, buf8Bytes)
	if err != nil {
		return ibabuza.ApplyResult{}, err
	}
	switch responseType {
	case stateMachineResponse:
		res, err := m.rs.Deserialize(r)
		if err != nil {
			return ibabuza.ApplyResult{}, err
		}
		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Response: res,
		}, nil
	case stateMachineResponseErr:
		dataSize, err := fileutil.FileReadUint64(r, buf8Bytes)
		if err != nil {
			return ibabuza.ApplyResult{}, err
		}
		return func(allocDataSize, index uint64) (ibabuza.ApplyResult, error) {
			resErrSliceBuf := allocator.Acquire(int(allocDataSize))
			defer allocator.Release(resErrSliceBuf)
			errBuf := resErrSliceBuf.Buffer[:allocDataSize]
			n, err := io.ReadFull(r, errBuf)
			if err != nil {

				return ibabuza.ApplyResult{}, err
			}
			if n != len(errBuf) {
				allocator.Release(resErrSliceBuf)
				return ibabuza.ApplyResult{}, io.ErrUnexpectedEOF
			}
			res := errors.New(string(errBuf))
			allocator.Release(resErrSliceBuf)
			return ibabuza.ApplyResult{
				LogIndex: index,
				Response: res,
			}, nil

		}(dataSize, logIndex)

	case stateMachineResponseNil:
		return ibabuza.ApplyResult{
			LogIndex: logIndex,
		}, nil
	}
	return ibabuza.ApplyResult{}, errors.New(fmt.Sprintf("applyResultSerializer: unknown response type (%d)", responseType))
}
