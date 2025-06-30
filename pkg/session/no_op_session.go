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
