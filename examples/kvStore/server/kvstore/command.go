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
