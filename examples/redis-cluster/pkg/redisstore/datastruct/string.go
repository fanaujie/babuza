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
