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


package datastruct

import (
	"github.com/puzpuzpuz/xsync/v4"
	"io"
)

type String struct {
	kv *xsync.Map[string, string]
}

func NewString() *String {
	return &String{
		kv: xsync.NewMap[string, string](),
	}
}

func (s *String) Set(key, value string) {
	s.kv.Store(key, value)
}

func (s *String) Get(key string) (string, bool) {
	value, exists := s.kv.Load(key)
	if !exists {
		return "", false
	}
	return value, true
}

func (s *String) Delete(key string) bool {
	_, exists := s.kv.LoadAndDelete(key)
	if !exists {
		return false
	}
	return true
}

func (s *String) Append(key, value string) string {
	currentValue, exists := s.kv.Load(key)
	if exists {
		newValue := currentValue + value
		s.kv.Store(key, newValue)
		return newValue
	}
	s.kv.Store(key, value)
	return value
}

func (s *String) SaveSnapshot(w io.WriteCloser) error {
	return nil
}

func (s *String) RestoreFromSnapshot(r io.Reader) error {
	return nil
}
