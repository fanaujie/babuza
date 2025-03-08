package session

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"io"
)

type NoOpManager struct {
	logger ibabuza.Logger
}

func NewNoOpManager(logger ibabuza.Logger) *NoOpManager {
	logger.Info("no operation session manager: creating no operation session manager")
	return &NoOpManager{}
}

func (m *NoOpManager) SetResponseSerializer(arSerializer ibabuza.ResponseSerializer) error {
	return nil
}

func (m *NoOpManager) GetSession(sid uint64) (ibabuza.Session, error) {
	return &noOpSession, nil
}

func (m *NoOpManager) Register(sid uint64, currentNanoseconds int64) {
}

func (m *NoOpManager) ExpireSession(currentTime int64) {
}

func (m *NoOpManager) Snapshot(w io.Writer) error {
	buf := make([]byte, 8)
	return fileutil.FileWriteUint64(w, buf, uint64(fileVersion)<<32|uint64(noOpManagerType))
}

func (m *NoOpManager) Restore(r io.Reader) error {
	buf := make([]byte, 8)
	value, err := fileutil.FileReadUint64(r, buf)
	if err != nil {
		return err
	}
	version := uint32(value >> 32)
	if version != fileVersion {
		return errors.New(fmt.Sprintf("no operation session manager: mismatch file version (expected version=%d real version=%d)",
			fileVersion, version))
	}
	fileType := uint32(value & 0xffffffff)
	if fileType != noOpManagerType {
		return errors.New(fmt.Sprintf("no operation session manager: found invalid file fiype %d", fileType))
	}
	return nil
}
