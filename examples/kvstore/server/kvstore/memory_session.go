package kvstore

import "github.com/fanaujie/babuza/ibabuza"

type MemoryStoreWithSession struct {
	*MemoryStore
}

func NewMemoryStoreWithSession() *MemoryStoreWithSession {
	return &MemoryStoreWithSession{
		MemoryStore: NewMemoryStore(),
	}
}

func (m *MemoryStoreWithSession) GetResponseSerializer() ibabuza.ResponseSerializer {
	return NewResultSerializer()
}
