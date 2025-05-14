package session

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"io"
	"sync"
	"time"
)

type ExpiredManager struct {
	opts         ExpiredMgrOptions
	sessions     map[uint64]ibabuza.Session
	arSerializer *applyResultSerializer
	logger       ibabuza.Logger
	mu           sync.RWMutex
}

//TODO: application need session expire error message to do next action if expire need to register again

func NewExpiredManager(logger ibabuza.Logger, setOpts ...SetExpiredMgrOptions) *ExpiredManager {
	opts := ExpiredMgrOptions{
		expiredTime:         time.Minute * 30,
		snapshotCompression: babuzapb.SnapshotFileCompression_None,
	}
	for _, s := range setOpts {
		s(&opts)
	}
	logger.Info("expired session manager: creating expired session manager")
	return &ExpiredManager{
		opts:     opts,
		sessions: make(map[uint64]ibabuza.Session),
		logger:   logger,
	}
}
func (m *ExpiredManager) SetResponseSerializer(rs ibabuza.ResponseSerializer) error {
	if rs == nil {
		return fmt.Errorf("expired session manager: need response serializer to serialize response of state machiche")
	}
	m.arSerializer = newApplyResultSerializer(rs)
	return nil
}

func (m *ExpiredManager) GetSession(sid uint64) (ibabuza.Session, error) {
	if sid == NoOPSessionID {
		return &noOpSession, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[sid]
	if !ok {
		return nil, ErrSessionExpired
	}
	return s, nil
}

func (m *ExpiredManager) Register(sId uint64, lastActivateTime int64) {
	m.mu.RLock()
	_, ok := m.sessions[sId]
	if ok {
		m.mu.RUnlock()
		return
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[sId] = NewSession(sId, lastActivateTime)
}

func (m *ExpiredManager) ExpireSession(currentNanoseconds int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for sid, s := range m.sessions {
		if s.LastActiveNanoseconds()+m.opts.expiredTime.Nanoseconds() <= currentNanoseconds {
			delete(m.sessions, sid)
		}
	}
}

func (m *ExpiredManager) Snapshot(w io.Writer) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := len(m.sessions)
	bs := allocator.Acquire(8)
	defer allocator.Release(bs)
	buf := bs.Buffer[:8]
	if err := fileutil.FileWriteUint64(w, buf, uint64(fileVersion)<<32|uint64(expiredManagerType)); err != nil {
		return err
	}
	if err := fileutil.FileWriteUint64(w, buf, uint64(count)); err != nil {
		return err
	}
	for _, s := range m.sessions {
		if err := s.Snapshot(w, m.arSerializer); err != nil {
			return err
		}
	}
	return nil
}

func (m *ExpiredManager) Restore(r io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	bs := allocator.Acquire(8)
	defer allocator.Release(bs)
	buf := bs.Buffer[:8]
	value, err := fileutil.FileReadUint64(r, buf)
	if err != nil {
		return err
	}
	version := uint32(value >> 32)
	if version != fileVersion {
		return errors.New(fmt.Sprintf("expire session manager: mismatch file version (expected version=%d real version=%d)",
			fileVersion, version))
	}
	fileType := uint32(value & 0xffffffff)
	if fileType != expiredManagerType {
		return errors.New(fmt.Sprintf("expire session manager: found invalid file fiype %d", fileType))
	}
	totalCount, err := fileutil.FileReadUint64(r, buf)
	if err != nil {
		return err
	}
	sessions := make(map[uint64]ibabuza.Session)
	for i := uint64(0); i < totalCount; i++ {
		ns := &Session{}
		if err = ns.Restore(r, m.arSerializer); err != nil {
			return err
		}
		sessions[ns.Id()] = ns
	}
	if uint64(len(sessions)) != totalCount {
		return errors.New(fmt.Sprintf("expired session manager: mismatched session count (expectd=%d) (real=%d)", totalCount, len(sessions)))
	}
	m.sessions = sessions
	return nil
}
