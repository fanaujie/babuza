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
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

const (
	ResponseType      uint64 = 1 << 63
	ClearResponseType uint64 = ^ResponseType
)

type KvResult struct {
	Command uint64 `json:"command"`
	Key     string `json:"key"`
	Value   string `json:"value"`
}

type ResultSerializer struct {
	buf []byte
}

func NewResultSerializer() *ResultSerializer {
	return &ResultSerializer{buf: make([]byte, 8)}
}

func (s *ResultSerializer) Serialize(w io.Writer, res any) error {
	var resType uint64
	switch res.(type) {
	case *KvResult:
		resType = ResponseType
	default:
		return errors.New("can not cast res to a pointer to valid response")
	}
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(s.buf, resType|uint64(len(data)))
	if _, err = w.Write(s.buf); err != nil {
		return err
	}
	if _, err = w.Write(data); err != nil {
		return err
	}
	return nil
}

func (s *ResultSerializer) Deserialize(r io.Reader) (any, error) {
	if _, err := io.ReadFull(r, s.buf); err != nil {
		return nil, err
	}
	var res any
	dataLen := binary.LittleEndian.Uint64(s.buf)
	if dataLen&ResponseType == ResponseType {
		dataLen &= ClearResponseType
		res = &KvResult{}
	} else {
		return nil, errors.New("unknown response type")
	}
	buf := make([]byte, dataLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(buf, res); err != nil {
		return nil, err
	}
	return res, nil
}
