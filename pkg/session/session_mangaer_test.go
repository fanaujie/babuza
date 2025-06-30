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
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"strconv"
	"testing"
	"time"
)

func genSessionManager(expiredNanoseconds time.Duration, lruMaxSize int64, sessionCompression babuzapb.SnapshotFileCompressionType) []ibabuza.SessionManager {
	var managers []ibabuza.SessionManager
	var mgr ibabuza.SessionManager
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	mgr = NewExpiredManager(l, SetExpiredMgrOptionsWithExpiredTime(expiredNanoseconds),
		SetExpiredMgrOptionsWithSnapshotCompressionType(sessionCompression))
	mgr.SetResponseSerializer(&mockJsonResultSerializer{})
	managers = append(managers, mgr)
	mgr = NewLruManager(l, SetLruMgrOptionsWithMaxSessions(lruMaxSize), SetLruMgrOptionsWithSnapshotCompressionType(sessionCompression))
	mgr.SetResponseSerializer(&mockJsonResultSerializer{})
	managers = append(managers, mgr)
	return managers
}

func TestNewSessionManager(t *testing.T) {
	manager := genSessionManager(time.Second, 128, babuzapb.SnapshotFileCompression_Snappy)
	assert.Equal(t, 2, len(manager))
	for _, mgr := range manager {
		if lru, ok := mgr.(*LruManager); ok {
			assert.Equal(t, int64(128), lru.opts.maxSessions)
			assert.Equal(t, babuzapb.SnapshotFileCompression_Snappy, lru.opts.snapshotCompression)
		} else if expired, ok := mgr.(*ExpiredManager); ok {
			assert.Equal(t, time.Second, expired.opts.expiredTime)
			assert.Equal(t, babuzapb.SnapshotFileCompression_Snappy, expired.opts.snapshotCompression)
		}
	}
}

func TestRegister(t *testing.T) {
	lruMaxSize := int64(128)
	manager := genSessionManager(time.Second, lruMaxSize, babuzapb.SnapshotFileCompression_None)
	assert.Equal(t, 2, len(manager))
	sessionCount := uint64(lruMaxSize + 1)
	for _, mgr := range manager {
		for i := uint64(1); i <= sessionCount; i++ {
			assert.Nil(t, mgr.Register(i, 0))
			assert.Error(t, mgr.Register(i, 0))
			assert.Nil(t, mgr.UnRegister(i))
			assert.Nil(t, mgr.Register(i, 0))
		}
		if lru, ok := mgr.(*LruManager); ok {
			_, ok = lru.sessions[1] // expire
			assert.Equal(t, false, ok)
			_, ok = lru.sessions[sessionCount]
			assert.Equal(t, true, ok)
			assert.Equal(t, lruMaxSize, int64(lru.lru.Len()))
		} else if expired, ok := mgr.(*ExpiredManager); ok {
			assert.Equal(t, sessionCount, uint64(len(expired.sessions)))
		}
	}
}

func TestGetSession(t *testing.T) {
	lruMaxSize := int64(128)
	manager := genSessionManager(time.Second, lruMaxSize, babuzapb.SnapshotFileCompression_None)
	assert.Equal(t, 2, len(manager))
	sessionCount := uint64(lruMaxSize + 1)
	for _, mgr := range manager {
		for i := uint64(1); i <= sessionCount; i++ {
			mgr.Register(i, 0)
		}
		for i := uint64(1); i <= sessionCount; i++ {
			_, err := mgr.GetSession(i)
			if _, ok := mgr.(*LruManager); ok {
				if err != nil {
					assert.Error(t, err)
					continue
				}
			}
			assert.Nil(t, err)
		}
	}
}

func TestExpireSession(t *testing.T) {
	lruMaxSize := int64(128)
	manager := genSessionManager(time.Second, lruMaxSize, babuzapb.SnapshotFileCompression_None)
	assert.Equal(t, 2, len(manager))
	sessionCount := uint64(lruMaxSize + 1)
	for _, mgr := range manager {
		for i := uint64(1); i <= sessionCount; i++ {
			mgr.Register(i, 0)
		}
		var expectedCount = 0
		for i := uint64(lruMaxSize / 2); i <= sessionCount; i++ {
			s, err := mgr.GetSession(i)
			assert.Nil(t, err)
			assert.Nil(t, s.AddResult(i, int64(time.Microsecond*500), ibabuza.ApplyResult{}))
			expectedCount++
		}
		if expired, ok := mgr.(*ExpiredManager); ok {
			expired.ExpireSession(int64(time.Second))
			assert.Equal(t, expectedCount, len(expired.sessions))
			for i := uint64(lruMaxSize / 2); i <= sessionCount; i++ {
				_, ok = expired.sessions[i]
				assert.Equal(t, true, ok)
			}
		}
	}
}

func TestSnapshotRestore(t *testing.T) {
	lruMaxSize := int64(64)
	manager := genSessionManager(time.Second, lruMaxSize, babuzapb.SnapshotFileCompression_None)
	restoreMgr := genSessionManager(time.Second, lruMaxSize, babuzapb.SnapshotFileCompression_None)

	assert.Equal(t, 2, len(manager))
	for index, mgr := range manager {
		for i := uint64(1); i <= uint64(lruMaxSize); i++ {
			mgr.Register(i, 0)
		}
		seq := uint64(1)
		for i := uint64(1); i <= uint64(lruMaxSize); i++ {
			s, err := mgr.GetSession(i)
			assert.Nil(t, err)
			assert.Nil(t, s.AddResult(seq, int64(time.Microsecond*500), ibabuza.ApplyResult{
				Response: &mockResponseA{Value: int(i)},
			}))
			seq++
			assert.Nil(t, s.AddResult(seq, int64(time.Microsecond*500), ibabuza.ApplyResult{
				Response: &mockResponseB{Value: strconv.Itoa(int(i))},
			}))
			seq++
		}
		func() {
			p, err := os.CreateTemp("", "session manger")
			assert.Nil(t, err)
			assert.Nil(t, mgr.Snapshot(p))
			assert.Nil(t, p.Close())
			p, err = os.Open(p.Name())
			assert.Nil(t, restoreMgr[index].Restore(p))
			assert.Nil(t, p.Close())
			if lru, ok := mgr.(*LruManager); ok {
				restore, ok := restoreMgr[index].(*LruManager)
				assert.Equal(t, true, ok)
				assert.Equal(t, len(lru.sessions), len(restore.sessions))
				assert.Equal(t, lru.lru.Len(), restore.lru.Len())
				assert.Equal(t, lru.opts.maxSessions, restore.opts.maxSessions)
				assert.Equal(t, lru.opts.snapshotCompression, restore.opts.snapshotCompression)
				for sid, element := range lru.sessions {
					resElement, ok := restore.sessions[sid]
					assert.Equal(t, true, ok)
					session := element.Value.(*Session)
					resSession := resElement.Value.(*Session)
					assert.Equal(t, session.Id(), resSession.Id())
					assert.Equal(t, session.LastActiveNanoseconds(), resSession.LastActiveNanoseconds())
					assert.Equal(t, len(session.result), len(resSession.result))
					for seqNum, ar := range session.result {
						resAr, ok := resSession.result[seqNum]
						assert.Equal(t, true, ok)
						compareApplyResult(t, ar, resAr)
					}
				}
			} else if expired, ok := mgr.(*ExpiredManager); ok {
				restore, ok := restoreMgr[index].(*ExpiredManager)
				assert.Equal(t, true, ok)
				assert.Equal(t, len(expired.sessions), len(restore.sessions))
				assert.Equal(t, expired.opts.expiredTime, restore.opts.expiredTime)
				assert.Equal(t, expired.opts.snapshotCompression, restore.opts.snapshotCompression)
				for sid, session := range expired.sessions {
					resSession, ok := restore.sessions[sid]
					assert.Equal(t, true, ok)
					session1 := session.(*Session)
					session2 := resSession.(*Session)
					assert.Equal(t, session1.Id(), session2.Id())
					assert.Equal(t, session1.LastActiveNanoseconds(), session2.LastActiveNanoseconds())
					assert.Equal(t, len(session1.result), len(session2.result))
					for seqNum, ar := range session1.result {
						resAr, ok := session2.result[seqNum]
						assert.Equal(t, true, ok)
						compareApplyResult(t, ar, resAr)
					}
				}
			}
		}()
	}
}

func TestNoOpManager_SnapshotRestore(t *testing.T) {
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	s := NewNoOpManager(l)
	r := NewNoOpManager(l)
	p, err := os.CreateTemp("", "session manger")
	assert.Nil(t, err)
	assert.Nil(t, s.Snapshot(p))
	assert.Nil(t, p.Close())
	p, err = os.Open(p.Name())
	assert.Nil(t, r.Restore(p))
	assert.Nil(t, p.Close())
}

func compareApplyResult(t *testing.T, ar, resAr ibabuza.ApplyResult) {
	assert.Equal(t, ar.LogIndex, resAr.LogIndex)
	res1 := ar.Response
	res2 := resAr.Response
	switch res1.(type) {
	case *mockResponseA:
		assert.Equal(t, res1.(*mockResponseA).Value, res2.(*mockResponseA).Value)
	case *mockResponseB:
		assert.Equal(t, res1.(*mockResponseB).Value, res2.(*mockResponseB).Value)
	default:
		assert.Fail(t, "unexpected response type")
	}
}
