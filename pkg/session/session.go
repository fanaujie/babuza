package session

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"io"
)

const (
	NoOPSessionID      uint64 = 0
	fileVersion        uint32 = 0x0001
	expiredManagerType uint32 = 0x0001
	lruManagerType     uint32 = 0x0002
	noOpManagerType    uint32 = 0x0003
)

type Session struct {
	id                    uint64
	lastActiveNanoseconds int64
	result                map[uint64]ibabuza.ApplyResult
}

func NewSession(sid uint64, lastActiveNanoseconds int64) *Session {
	return &Session{
		id:                    sid,
		lastActiveNanoseconds: lastActiveNanoseconds,
		result:                make(map[uint64]ibabuza.ApplyResult),
	}
}

func (s *Session) Id() uint64 {
	return s.id
}

func (s *Session) LastActiveNanoseconds() int64 {
	return s.lastActiveNanoseconds
}

func (s *Session) ClearResult(lowestSeqNumNotYetReplied uint64) {
	for seq := range s.result {
		if seq < lowestSeqNumNotYetReplied {
			delete(s.result, seq)
		}
	}
}

func (s *Session) RepeatSequenceNum(seqNum uint64) bool {
	_, ok := s.result[seqNum]
	return ok
}

func (s *Session) AddResult(sequenceNum uint64, lastActiveNanoseconds int64, result ibabuza.ApplyResult) error {
	_, ok := s.result[sequenceNum]
	if ok {
		return fmt.Errorf("session: sequence number (%d) exists", sequenceNum)
	}
	s.lastActiveNanoseconds = lastActiveNanoseconds
	s.result[sequenceNum] = result
	return nil
}

func (s *Session) GetResult(seqNum uint64) (ibabuza.ApplyResult, bool) {
	result, ok := s.result[seqNum]
	return result, ok
}

func (s *Session) Snapshot(w io.Writer, ars ibabuza.ApplyResultSerializer) error {
	count := uint64(len(s.result))

	bs := allocator.Acquire(8)
	defer allocator.Release(bs)
	buf := bs.Buffer[:8]
	if err := fileutil.FileWriteUint64(w, buf, s.id); err != nil {
		return err
	}
	if err := fileutil.FileWriteUint64(w, buf, uint64(s.lastActiveNanoseconds)); err != nil {
		return err
	}
	if err := fileutil.FileWriteUint64(w, buf, count); err != nil {
		return err
	}
	for seqNum, result := range s.result {
		if err := fileutil.FileWriteUint64(w, buf, seqNum); err != nil {
			return err
		}
		if err := ars.Marshal(w, result); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) Restore(r io.Reader, serializer ibabuza.ApplyResultSerializer) error {
	result := make(map[uint64]ibabuza.ApplyResult)
	var err error
	bs := allocator.Acquire(8)
	defer allocator.Release(bs)
	buf := bs.Buffer[:8]
	if s.id, err = fileutil.FileReadUint64(r, buf); err != nil {
		return err
	}
	if n, err := fileutil.FileReadUint64(r, buf); err != nil {
		return err
	} else {
		s.lastActiveNanoseconds = int64(n)
	}
	count, err := fileutil.FileReadUint64(r, buf)
	if err != nil {
		return err
	}

	for i := uint64(0); i < count; i++ {
		seqNum, err := fileutil.FileReadUint64(r, buf)
		if err != nil {
			return err
		}
		ar, err := serializer.Unmarshal(r)
		if err != nil {
			return err
		}
		result[seqNum] = ar
	}
	s.result = result
	return nil
}
