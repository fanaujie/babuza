package kvstore

import (
	"github.com/fanaujie/babuza/ibabuza"
)

type DiskStoreWithSession struct {
	*Disk
}

func NewDiskStoreWithSession(dataDir string) *DiskStoreWithSession {
	return &DiskStoreWithSession{
		Disk: NewDisk(dataDir),
	}
}

func (m *DiskStoreWithSession) GetResponseSerializer() ibabuza.ResponseSerializer {
	return NewResultSerializer()
}
