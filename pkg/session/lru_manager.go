package session

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"io"
	"sync"
)

type LruManager struct {
	opts         LruMgrOptions
	sessions     map[uint64]*list.Element
	lru          *list.List
	arSerializer *applyResultSerializer
	logger       ibabuza.Logger
	mu           sync.RWMutex
}

func NewLruManager(logger ibabuza.Logger, setOpts ...SetLruMgrOptions) *LruManager {
	opts := LruMgrOptions{
		maxSessions:         128,
		snapshotCompression: babuzapb.SnapshotFileCompression_None,
	}
	for _, s := range setOpts {
		s(&opts)
	}
	logger.Infof("lru session manager: creating lru session manager")
	return &LruManager{
		opts:     opts,
		sessions: make(map[uint64]*list.Element),
		lru:      list.New(),
		logger:   logger,
	}
}
func (m *LruManager) SetResponseSerializer(rs ibabuza.ResponseSerializer) error {
	if rs == nil {
		return fmt.Errorf("lru session manager: need response serializer to serialize response of state machiche")
	}
	m.arSerializer = newApplyResultSerializer(rs)
	return nil
}

func (m *LruManager) GetSession(sid uint64) (ibabuza.Session, error) {
	if sid == NoOPSessionID {
		return &noOpSession, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[sid]
	if !ok {
		return nil, ErrSessionExpired
	}
	m.lru.MoveToFront(s)
	return s.Value.(ibabuza.Session), nil
}

func (m *LruManager) Register(sId uint64, lastActivateTime int64) {
	m.mu.RLock()
	_, ok := m.sessions[sId]
	if ok {
		m.mu.RUnlock()
		return
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	ns := NewSession(sId, lastActivateTime)
	nl := m.lru.PushFront(ns)
	m.sessions[sId] = nl
	if int64(m.lru.Len()) > m.opts.maxSessions {
		tail := m.lru.Back()
		if tail != nil {
			m.lru.Remove(tail)
			delete(m.sessions, tail.Value.(ibabuza.Session).Id())
		}
	}
}

func (m *LruManager) ExpireSession(currentTime int64) {

}

func (m *LruManager) Snapshot(w io.Writer) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	bs := allocator.Acquire(8)
	defer allocator.Release(bs)
	buf := bs.Buffer[:8]
	if err := fileutil.FileWriteUint64(w, buf, uint64(fileVersion)<<32|uint64(lruManagerType)); err != nil {
		return err
	}
	if err := fileutil.FileWriteUint64(w, buf, uint64(m.lru.Len())); err != nil {
		return err
	}
	for e := m.lru.Front(); e != nil; e = e.Next() {
		if err := e.Value.(ibabuza.Session).Snapshot(w, m.arSerializer); err != nil {
			return err
		}
	}
	return nil
}

func (m *LruManager) Restore(r io.Reader) error {
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
		return errors.New(fmt.Sprintf("lru session manager: mismatch file version (expected version=%d real version=%d)",
			fileVersion, version))
	}
	fileType := uint32(value & 0xffffffff)
	if fileType != lruManagerType {
		return errors.New(fmt.Sprintf("lru session manager: found invalid file fiype %d", fileType))
	}
	totalCount, err := fileutil.FileReadUint64(r, buf)
	if err != nil {
		return err
	}
	sessions := make(map[uint64]*list.Element)
	lru := list.New()
	for i := uint64(0); i < totalCount; i++ {
		ns := &Session{}
		if err = ns.Restore(r, m.arSerializer); err != nil {
			return err
		}
		sessions[ns.Id()] = lru.PushBack(ns)
	}
	if uint64(lru.Len()) != totalCount {
		return errors.New(fmt.Sprintf("lru session manager: mismatched session count (expectd=%d) (real=%d)", totalCount, len(sessions)))
	}
	m.lru = lru
	m.sessions = sessions
	return nil
}
