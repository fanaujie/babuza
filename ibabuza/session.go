package ibabuza

import (
	"io"
)

type ApplyResultSerializer interface {
	Marshal(io.Writer, ApplyResult) error
	Unmarshal(io.Reader) (ApplyResult, error)
}

type Session interface {
	Id() uint64
	LastActiveNanoseconds() int64
	ClearResult(uint64)
	RepeatSequenceNum(uint64) bool
	AddResult(uint64, int64, ApplyResult) error
	GetResult(uint64) (ApplyResult, bool)
	Snapshot(io.Writer, ApplyResultSerializer) error
	Restore(io.Reader, ApplyResultSerializer) error
}

type SessionManager interface {
	SetResponseSerializer(ResponseSerializer) error
	GetSession(uint64) (Session, error)
	Register(uint64, int64) error
	UnRegister(uint64) error
	ExpireSession(int64)
	Snapshot(io.Writer) error
	Restore(io.Reader) error
}
