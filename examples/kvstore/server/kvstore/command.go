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


package kvstore

import (
	"encoding/json"
)

const (
	Set    uint64 = 1
	Append uint64 = 2
	Delete uint64 = 3
	Read   uint64 = 4
)

type KvCommand struct {
	Command uint64
	Key     string
	Value   string
}

func (r *KvCommand) Set(key, value string) ([]byte, error) {
	r.Key = key
	r.Value = value
	r.Command = Set
	return json.Marshal(r)

}

func (r *KvCommand) Append(key, value string) ([]byte, error) {
	r.Key = key
	r.Value = value
	r.Command = Append
	return json.Marshal(r)
}

func (r *KvCommand) Delete(key string) ([]byte, error) {
	r.Key = key
	r.Value = ""
	r.Command = Delete
	return json.Marshal(r)
}

func (r *KvCommand) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &r)
}
