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
	"container/list"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"io"
	"math"
	"sync"
)

type LruManager struct {
	opts         LruMgrOptions
	sessions     map[uint64]*list.Element
	lru          *list.List
	arSerializer *applyResultSerializer
	logger       ibabuza.Logger
	mu           sync.Mutex
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
func (l *LruManager) SetResponseSerializer(rs ibabuza.ResponseSerializer) error {
	if rs == nil {
		return fmt.Errorf("lru session manager: need response serializer to serialize response of state machiche")
	}
	l.arSerializer = newApplyResultSerializer(rs)
	return nil
}

func (l *LruManager) GetSession(sessionID uint64) (ibabuza.Session, error) {
	if sessionID == NoOPSessionID {
		return &noOpSession, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("lru session manager: session %d not found", sessionID)
	}
	l.lru.MoveToFront(s)
	return s.Value.(ibabuza.Session), nil
}

func (l *LruManager) Register(sessionID uint64, lastActivateTime int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.sessions[sessionID]
	if ok {
		return fmt.Errorf("lru session manager: session id %d already registered", sessionID)
	}
	ns := NewSession(sessionID, lastActivateTime)
	nl := l.lru.PushFront(ns)
	l.sessions[sessionID] = nl
	if int64(l.lru.Len()) > l.opts.maxSessions {
		tail := l.lru.Back()
		if tail != nil {
			l.lru.Remove(tail)
			delete(l.sessions, tail.Value.(ibabuza.Session).Id())
		}
	}
	return nil
}

func (l *LruManager) UnRegister(sessionID uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.sessions[sessionID]
	if !ok {
		return fmt.Errorf("lru session manager: session id %d not registered", sessionID)
	}
	s.Value.(ibabuza.Session).ClearResult(math.MaxInt64)
	l.lru.Remove(s)
	delete(l.sessions, sessionID)
	return nil
}

func (l *LruManager) ExpireSession(currentTime int64) {
	// not implemented
}

func (l *LruManager) Snapshot(w io.Writer) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	bs := allocator.Acquire(8)
	defer allocator.Release(bs)
	buf := bs.Buffer[:8]
	if err := fileutil.FileWriteUint64(w, buf, uint64(fileVersion)<<32|uint64(lruManagerType)); err != nil {
		return err
	}
	if err := fileutil.FileWriteUint64(w, buf, uint64(l.lru.Len())); err != nil {
		return err
	}
	for e := l.lru.Front(); e != nil; e = e.Next() {
		if err := e.Value.(ibabuza.Session).Snapshot(w, l.arSerializer); err != nil {
			return err
		}
	}
	return nil
}

func (l *LruManager) Restore(r io.Reader) error {
	l.mu.Lock()
	defer l.mu.Unlock()
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
		if err = ns.Restore(r, l.arSerializer); err != nil {
			return err
		}
		sessions[ns.Id()] = lru.PushBack(ns)
	}
	if uint64(lru.Len()) != totalCount {
		return errors.New(fmt.Sprintf("lru session manager: mismatched session count (expectd=%d) (real=%d)", totalCount, len(sessions)))
	}
	l.lru = lru
	l.sessions = sessions
	return nil
}
