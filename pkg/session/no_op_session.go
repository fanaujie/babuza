package session

import (
	"github.com/fanaujie/babuza/ibabuza"
	"io"
)

var (
	noOpSession NoOPSession
)

type NoOPSession struct {
}

func (s *NoOPSession) Id() uint64 {
	return 0
}

func (s *NoOPSession) LastActiveNanoseconds() int64 {
	return 0
}

func (s *NoOPSession) ClearResult(lowestSeqNumNotYetReplied uint64) {
}

func (s *NoOPSession) RepeatSequenceNum(seqNum uint64) bool {
	return false
}

func (s *NoOPSession) AddResult(sequenceNum uint64, lastActiveNanoseconds int64, result ibabuza.ApplyResult) error {
	return nil
}

func (s *NoOPSession) GetResult(seqNum uint64) (ibabuza.ApplyResult, bool) {
	return ibabuza.ApplyResult{}, false
}

func (s *NoOPSession) Snapshot(w io.Writer, ars ibabuza.ApplyResultSerializer) error {
	return nil
}

func (s *NoOPSession) Restore(r io.Reader, ars ibabuza.ApplyResultSerializer) error {
	return nil
}
