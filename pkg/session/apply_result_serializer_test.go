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
	"encoding/json"
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

type mockResponseA struct {
	Value int
}

type mockResponseB struct {
	Value string
}

type mockJsonResultSerializer struct {
}

func (m *mockJsonResultSerializer) Serialize(w io.Writer, res any) error {
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}
	buf := make([]byte, 8)
	switch res.(type) {
	case *mockResponseA:
		if err = fileutil.FileWriteUint64(w, buf, 100); err != nil {
			return nil
		}
	case *mockResponseB:
		if err = fileutil.FileWriteUint64(w, buf, 200); err != nil {
			return nil
		}
	}
	if err = fileutil.FileWriteUint64(w, buf, uint64(len(data))); err != nil {
		return nil
	}
	if _, err = w.Write(data); err != nil {
		return err
	}
	return nil
}

func (m *mockJsonResultSerializer) Deserialize(r io.Reader) (any, error) {
	buf := make([]byte, 8)
	resTypeCode, err := fileutil.FileReadUint64(r, buf)
	if err != nil {
		return nil, err
	}
	dataSize, err := fileutil.FileReadUint64(r, buf)
	if err != nil {
		return nil, err
	}
	data := make([]byte, dataSize)
	n, err := io.ReadFull(r, data)
	if err != nil {
		return nil, err
	}
	if n != len(data) {
		return nil, io.ErrUnexpectedEOF
	}
	switch resTypeCode {
	case 100:
		res := &mockResponseA{}
		if err = json.Unmarshal(data, res); err != nil {
			return nil, err
		}
		return res, nil
	case 200:
		res := &mockResponseB{}
		if err = json.Unmarshal(data, res); err != nil {
			return nil, err
		}
		return res, nil
	}
	return nil, errors.New("mockJsonResultSerializer: unknown type")
}

func TestApplyResultSerializer_MarshalUnmarshalStateMachineResponseNil(t *testing.T) {
	p := t.TempDir()
	defer func() {
		_ = os.RemoveAll(p)
	}()

	ars := newApplyResultSerializer(&mockJsonResultSerializer{})

	readFilePath := ""
	w, err := ioutil.TempFile(p, "")
	assert.Nil(t, err)
	readFilePath = w.Name()
	defer w.Close()

	assert.Nil(t, ars.Marshal(w, ibabuza.ApplyResult{
		LogIndex: 1,
		Response: nil,
	}))

	r, err := os.Open(readFilePath)
	assert.Nil(t, err)
	defer r.Close()
	ar, err := ars.Unmarshal(r)
	assert.Nil(t, err)

	assert.Equal(t, uint64(1), ar.LogIndex)
	assert.Nil(t, ar.Response)
}

func TestApplyResultSerializer_MarshalUnmarshalResponseErr(t *testing.T) {
	p := t.TempDir()
	defer func() {
		_ = os.RemoveAll(p)
	}()

	ars := newApplyResultSerializer(&mockJsonResultSerializer{})

	readFilePath := ""
	w, err := os.CreateTemp(p, "")
	assert.Nil(t, err)
	readFilePath = w.Name()
	defer w.Close()

	assert.Nil(t, ars.Marshal(w, ibabuza.ApplyResult{
		LogIndex: 1,
		Error:    errors.New("error1"),
	}))
	assert.Nil(t, ars.Marshal(w, ibabuza.ApplyResult{
		LogIndex: 2,
		Error:    errors.New("error2"),
	}))

	r, err := os.Open(readFilePath)
	assert.Nil(t, err)
	defer r.Close()

	smRes, err := ars.Unmarshal(r)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), smRes.LogIndex)
	assert.Equal(t, "error1", smRes.Error.Error())
	assert.Nil(t, smRes.Response)
	smRes, err = ars.Unmarshal(r)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), smRes.LogIndex)
	assert.Equal(t, "error2", smRes.Error.Error())
	assert.Nil(t, smRes.Response)
}

func TestApplyResultSerializer_MarshalUnmarshalStateMachineResponse(t *testing.T) {
	p := t.TempDir()
	defer func() {
		_ = os.RemoveAll(p)
	}()

	ars := newApplyResultSerializer(&mockJsonResultSerializer{})

	readFilePath := ""
	w, err := ioutil.TempFile(p, "")
	assert.Nil(t, err)
	readFilePath = w.Name()
	defer w.Close()

	assert.Nil(t, ars.Marshal(w, ibabuza.ApplyResult{
		LogIndex: 1,
		Response: &mockResponseA{Value: 10},
	}))
	assert.Nil(t, ars.Marshal(w, ibabuza.ApplyResult{
		LogIndex: 2,
		Response: &mockResponseB{Value: "hello"},
	}))

	r, err := os.Open(readFilePath)
	assert.Nil(t, err)
	defer r.Close()

	resA, err := ars.Unmarshal(r)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), resA.LogIndex)
	assert.Equal(t, 10, resA.Response.(*mockResponseA).Value)

	resB, err := ars.Unmarshal(r)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), resB.LogIndex)
	assert.Equal(t, "hello", resB.Response.(*mockResponseB).Value)

}
