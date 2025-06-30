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

import "container/list"

type KvStringPair struct {
	Key   string
	Value string
}

type Iterator struct {
	e *list.Element
}

type KvOperationOrderMap struct {
	oMap  map[string]*list.Element
	oList *list.List
}

func NewKvOperationOrderMap() *KvOperationOrderMap {
	return &KvOperationOrderMap{
		oMap:  make(map[string]*list.Element),
		oList: list.New(),
	}
}
func (m *KvOperationOrderMap) Length() int {
	return m.oList.Len()
}

func (m *KvOperationOrderMap) Set(key string, value string) {
	v, ok := m.oMap[key]
	if ok {
		v.Value.(*KvStringPair).Value = value
	} else {
		m.oMap[key] = m.oList.PushBack(&KvStringPair{Key: key, Value: value})
	}
}

func (m *KvOperationOrderMap) Append(key string, value string) string {
	v, ok := m.oMap[key]
	if ok {
		kvPair := v.Value.(*KvStringPair)
		kvPair.Value += value
		return kvPair.Value
	}
	m.oMap[key] = m.oList.PushBack(&KvStringPair{Key: key, Value: value})
	return value
}

func (m *KvOperationOrderMap) Delete(key string) bool {
	v, ok := m.oMap[key]
	if ok {
		m.oList.Remove(v)
		delete(m.oMap, key)
	}
	return ok
}

func (m *KvOperationOrderMap) Get(key string) (string, bool) {
	v, ok := m.oMap[key]
	if ok {
		return v.Value.(*KvStringPair).Value, true
	}
	return "", false
}
func (m *KvOperationOrderMap) Iterator() *Iterator {
	return &Iterator{e: m.oList.Front()}
}

func (i *Iterator) First() *KvStringPair {
	if i.e == nil {
		return nil
	}
	first := i.e
	i.e = i.e.Next()
	return first.Value.(*KvStringPair)
}

func (i *Iterator) Next() *KvStringPair {
	current := i.e
	if current == nil {
		return nil
	}
	i.e = current.Next()
	return current.Value.(*KvStringPair)
}
