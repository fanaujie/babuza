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
