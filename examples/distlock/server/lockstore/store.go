package lockstore

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"sync"
	"time"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
)

const (
	SnapshotTag = "lock-store"
)

type Lease struct {
	ID        uint64 `json:"id"`
	TTL       int64  `json:"ttl"`
	GrantedAt int64  `json:"granted_at"`
	ExpiresAt int64  `json:"expires_at"`
}

type WaitEntry struct {
	OwnerID   string `json:"owner_id"`
	RequestID string `json:"request_id"`
	LeaseID   uint64 `json:"lease_id"`
	Timestamp int64  `json:"timestamp"`
}

type Lock struct {
	Name         string      `json:"name"`
	OwnerID      string      `json:"owner_id"`
	LeaseID      uint64      `json:"lease_id"`
	FencingToken uint64      `json:"fencing_token"`
	AcquiredAt   int64       `json:"acquired_at"`
	WaitQueue    []WaitEntry `json:"wait_queue,omitempty"`
}

type AcquiredResult struct {
	Result    *LockResult `json:"result"`
	ExpiresAt int64       `json:"expires_at"`
}

type LockStore struct {
	leases           map[uint64]*Lease
	locks            map[string]*Lock
	locksByLease     map[uint64]map[string]bool
	acquiredResults  map[string]*AcquiredResult
	leaseSeq         uint64
	globalFencingSeq uint64
	mu               sync.RWMutex
}

func NewLockStore() *LockStore {
	return &LockStore{
		leases:           make(map[uint64]*Lease),
		locks:            make(map[string]*Lock),
		locksByLease:     make(map[uint64]map[string]bool),
		acquiredResults:  make(map[string]*AcquiredResult),
		leaseSeq:         0,
		globalFencingSeq: 0,
	}
}

func (s *LockStore) Apply(e ibabuza.Entry) ibabuza.ApplyResult {
	var cmd LockCommand
	if err := cmd.Unmarshal(e.Command); err != nil {
		panic(err)
	}

	switch cmd.Command {
	case CmdAcquire:
		return s.applyAcquire(e.Index, cmd)
	case CmdRelease:
		return s.applyRelease(e.Index, cmd)
	case CmdWait:
		return s.applyWait(e.Index, cmd)
	case CmdCancelWait:
		return s.applyCancelWait(e.Index, cmd)
	case CmdLeaseGrant:
		return s.applyLeaseGrant(e.Index, cmd)
	case CmdLeaseRevoke:
		return s.applyLeaseRevoke(e.Index, cmd)
	case CmdLeaseKeepAlive:
		return s.applyLeaseKeepAlive(e.Index, cmd)
	case CmdTick:
		return s.applyTick(e.Index, cmd)
	}

	return ibabuza.ApplyResult{
		LogIndex: e.Index,
		Error:    ErrUnknownCommand,
	}
}

func (s *LockStore) applyLeaseGrant(logIndex uint64, cmd LockCommand) ibabuza.ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.leaseSeq++
	lease := &Lease{
		ID:        s.leaseSeq,
		TTL:       cmd.TTLSeconds,
		GrantedAt: cmd.Timestamp,
		ExpiresAt: cmd.Timestamp + cmd.TTLSeconds*int64(time.Second),
	}
	s.leases[lease.ID] = lease
	s.locksByLease[lease.ID] = make(map[string]bool)

	return ibabuza.ApplyResult{
		LogIndex: logIndex,
		Response: &LeaseResult{
			Command:   CmdLeaseGrant,
			LeaseID:   lease.ID,
			TTL:       lease.TTL,
			ExpiresAt: lease.ExpiresAt,
		},
	}
}

func (s *LockStore) applyLeaseRevoke(logIndex uint64, cmd LockCommand) ibabuza.ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease := s.leases[cmd.LeaseID]
	if lease == nil {
		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Error:    ErrLeaseNotFound,
		}
	}

	lockNames, _ := s.revokeLeaseInternal(cmd.LeaseID)

	return ibabuza.ApplyResult{
		LogIndex: logIndex,
		Response: &LeaseResult{
			Command:       CmdLeaseRevoke,
			LeaseID:       cmd.LeaseID,
			ReleasedLocks: lockNames,
		},
	}
}

func (s *LockStore) applyLeaseKeepAlive(logIndex uint64, cmd LockCommand) ibabuza.ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease := s.leases[cmd.LeaseID]
	if lease == nil {
		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Error:    ErrLeaseNotFound,
		}
	}

	lease.ExpiresAt = cmd.Timestamp + lease.TTL*int64(time.Second)

	return ibabuza.ApplyResult{
		LogIndex: logIndex,
		Response: &LeaseResult{
			Command:   CmdLeaseKeepAlive,
			LeaseID:   lease.ID,
			TTL:       lease.TTL,
			ExpiresAt: lease.ExpiresAt,
		},
	}
}

func (s *LockStore) applyTick(logIndex uint64, cmd LockCommand) ibabuza.ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := cmd.Timestamp
	var expiredLeases []uint64
	var notifyResults []*LockResult

	for leaseID, lease := range s.leases {
		if now >= lease.ExpiresAt {
			expiredLeases = append(expiredLeases, leaseID)
		}
	}

	for _, leaseID := range expiredLeases {
		_, results := s.revokeLeaseInternal(leaseID)
		notifyResults = append(notifyResults, results...)
	}

	s.cleanupExpiredAcquiredResults(now)

	return ibabuza.ApplyResult{
		LogIndex: logIndex,
		Response: &TickResult{
			Command:        CmdTick,
			ExpiredLeases:  expiredLeases,
			NotifyResults:  notifyResults,
		},
	}
}

func (s *LockStore) revokeLeaseInternal(leaseID uint64) ([]string, []*LockResult) {
	var releasedLockNames []string
	var notifyResults []*LockResult

	lockNamesMap := s.locksByLease[leaseID]
	for lockName := range lockNamesMap {
		releasedLockNames = append(releasedLockNames, lockName)
		lock := s.locks[lockName]
		if lock == nil {
			continue
		}

		if len(lock.WaitQueue) > 0 {
			nextWaiter := lock.WaitQueue[0]
			lock.WaitQueue = lock.WaitQueue[1:]

			nextLease := s.leases[nextWaiter.LeaseID]
			if nextLease == nil {
				delete(s.locks, lockName)
				continue
			}

			s.globalFencingSeq++
			lock.OwnerID = nextWaiter.OwnerID
			lock.LeaseID = nextWaiter.LeaseID
			lock.FencingToken = s.globalFencingSeq
			lock.AcquiredAt = time.Now().UnixNano()

			delete(s.locksByLease[leaseID], lockName)
			s.locksByLease[nextWaiter.LeaseID][lockName] = true

			acquiredResult := &LockResult{
				Command:      CmdWait,
				LockName:     lockName,
				OwnerID:      nextWaiter.OwnerID,
				FencingToken: s.globalFencingSeq,
				Acquired:     true,
				LeaseID:      nextWaiter.LeaseID,
				WaitStatus:   WaitStatusAcquired,
			}
			s.acquiredResults[nextWaiter.RequestID] = &AcquiredResult{
				Result:    acquiredResult,
				ExpiresAt: nextLease.ExpiresAt,
			}

			notifyResults = append(notifyResults, &LockResult{
				Command:       CmdRelease,
				LockName:      lockName,
				NextOwnerID:   nextWaiter.OwnerID,
				NextRequestID: nextWaiter.RequestID,
				NextToken:     s.globalFencingSeq,
				NextLeaseID:   nextWaiter.LeaseID,
			})
		} else {
			delete(s.locks, lockName)
			delete(s.locksByLease[leaseID], lockName)
		}
	}

	delete(s.leases, leaseID)
	delete(s.locksByLease, leaseID)

	return releasedLockNames, notifyResults
}

func (s *LockStore) applyAcquire(logIndex uint64, cmd LockCommand) ibabuza.ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease := s.leases[cmd.LeaseID]
	if lease == nil {
		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Error:    ErrLeaseNotFound,
		}
	}

	existing := s.locks[cmd.LockName]

	if existing != nil {
		existingLease := s.leases[existing.LeaseID]
		if existingLease != nil && existing.OwnerID != cmd.OwnerID {
			return ibabuza.ApplyResult{
				LogIndex: logIndex,
				Response: &LockResult{
					Command:  CmdAcquire,
					LockName: cmd.LockName,
					Acquired: false,
				},
			}
		}
		if existingLease != nil && existing.OwnerID == cmd.OwnerID {
			delete(s.locksByLease[existing.LeaseID], cmd.LockName)
		}
	}

	s.globalFencingSeq++
	newLock := &Lock{
		Name:         cmd.LockName,
		OwnerID:      cmd.OwnerID,
		LeaseID:      cmd.LeaseID,
		FencingToken: s.globalFencingSeq,
		AcquiredAt:   cmd.Timestamp,
	}
	s.locks[cmd.LockName] = newLock
	s.locksByLease[cmd.LeaseID][cmd.LockName] = true

	return ibabuza.ApplyResult{
		LogIndex: logIndex,
		Response: &LockResult{
			Command:      CmdAcquire,
			LockName:     cmd.LockName,
			OwnerID:      cmd.OwnerID,
			FencingToken: newLock.FencingToken,
			Acquired:     true,
			LeaseID:      cmd.LeaseID,
		},
	}
}

func (s *LockStore) applyRelease(logIndex uint64, cmd LockCommand) ibabuza.ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	lock := s.locks[cmd.LockName]
	if lock == nil {
		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Error:    ErrLockNotFound,
		}
	}

	if lock.OwnerID != cmd.OwnerID || lock.FencingToken != cmd.FencingToken {
		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Error:    ErrNotLockOwner,
		}
	}

	oldLeaseID := lock.LeaseID

	if len(lock.WaitQueue) > 0 {
		nextWaiter := lock.WaitQueue[0]
		lock.WaitQueue = lock.WaitQueue[1:]

		nextLease := s.leases[nextWaiter.LeaseID]
		if nextLease == nil {
			delete(s.locks, cmd.LockName)
			delete(s.locksByLease[oldLeaseID], cmd.LockName)
			return ibabuza.ApplyResult{
				LogIndex: logIndex,
				Response: &LockResult{
					Command:  CmdRelease,
					LockName: cmd.LockName,
				},
			}
		}

		s.globalFencingSeq++
		lock.OwnerID = nextWaiter.OwnerID
		lock.LeaseID = nextWaiter.LeaseID
		lock.FencingToken = s.globalFencingSeq
		lock.AcquiredAt = time.Now().UnixNano()

		delete(s.locksByLease[oldLeaseID], cmd.LockName)
		s.locksByLease[nextWaiter.LeaseID][cmd.LockName] = true

		acquiredResult := &LockResult{
			Command:      CmdWait,
			LockName:     cmd.LockName,
			OwnerID:      nextWaiter.OwnerID,
			FencingToken: s.globalFencingSeq,
			Acquired:     true,
			LeaseID:      nextWaiter.LeaseID,
			WaitStatus:   WaitStatusAcquired,
		}
		s.acquiredResults[nextWaiter.RequestID] = &AcquiredResult{
			Result:    acquiredResult,
			ExpiresAt: nextLease.ExpiresAt,
		}

		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Response: &LockResult{
				Command:       CmdRelease,
				LockName:      cmd.LockName,
				NextOwnerID:   nextWaiter.OwnerID,
				NextRequestID: nextWaiter.RequestID,
				NextToken:     s.globalFencingSeq,
				NextLeaseID:   nextWaiter.LeaseID,
			},
		}
	}

	delete(s.locks, cmd.LockName)
	delete(s.locksByLease[oldLeaseID], cmd.LockName)

	return ibabuza.ApplyResult{
		LogIndex: logIndex,
		Response: &LockResult{
			Command:  CmdRelease,
			LockName: cmd.LockName,
		},
	}
}

func (s *LockStore) applyWait(logIndex uint64, cmd LockCommand) ibabuza.ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := cmd.Timestamp

	s.cleanupExpiredAcquiredResults(now)

	if cmd.RequestID != "" {
		if acquired, ok := s.acquiredResults[cmd.RequestID]; ok {
			return ibabuza.ApplyResult{
				LogIndex: logIndex,
				Response: acquired.Result,
			}
		}
	}

	lease := s.leases[cmd.LeaseID]
	if lease == nil {
		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Error:    ErrLeaseNotFound,
		}
	}

	lock := s.locks[cmd.LockName]

	if cmd.RequestID != "" && lock != nil {
		for i, entry := range lock.WaitQueue {
			if entry.RequestID == cmd.RequestID {
				return ibabuza.ApplyResult{
					LogIndex: logIndex,
					Response: &LockResult{
						Command:       CmdWait,
						LockName:      cmd.LockName,
						Acquired:      false,
						QueuePosition: i + 1,
						WaitStatus:    WaitStatusWaiting,
					},
				}
			}
		}
	}

	if lock == nil {
		s.globalFencingSeq++
		s.locks[cmd.LockName] = &Lock{
			Name:         cmd.LockName,
			OwnerID:      cmd.OwnerID,
			LeaseID:      cmd.LeaseID,
			FencingToken: s.globalFencingSeq,
			AcquiredAt:   now,
			WaitQueue:    nil,
		}
		s.locksByLease[cmd.LeaseID][cmd.LockName] = true

		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Response: &LockResult{
				Command:      CmdWait,
				LockName:     cmd.LockName,
				OwnerID:      cmd.OwnerID,
				FencingToken: s.globalFencingSeq,
				Acquired:     true,
				LeaseID:      cmd.LeaseID,
				WaitStatus:   WaitStatusAcquired,
			},
		}
	}

	existingLease := s.leases[lock.LeaseID]
	if existingLease == nil {
		s.globalFencingSeq++
		lock.OwnerID = cmd.OwnerID
		lock.LeaseID = cmd.LeaseID
		lock.FencingToken = s.globalFencingSeq
		lock.AcquiredAt = now
		s.locksByLease[cmd.LeaseID][cmd.LockName] = true

		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Response: &LockResult{
				Command:      CmdWait,
				LockName:     cmd.LockName,
				OwnerID:      cmd.OwnerID,
				FencingToken: s.globalFencingSeq,
				Acquired:     true,
				LeaseID:      cmd.LeaseID,
				WaitStatus:   WaitStatusAcquired,
			},
		}
	}

	if lock.OwnerID == cmd.OwnerID {
		delete(s.locksByLease[lock.LeaseID], cmd.LockName)
		s.globalFencingSeq++
		lock.LeaseID = cmd.LeaseID
		lock.FencingToken = s.globalFencingSeq
		lock.AcquiredAt = now
		s.locksByLease[cmd.LeaseID][cmd.LockName] = true

		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Response: &LockResult{
				Command:      CmdWait,
				LockName:     cmd.LockName,
				OwnerID:      cmd.OwnerID,
				FencingToken: s.globalFencingSeq,
				Acquired:     true,
				LeaseID:      cmd.LeaseID,
				WaitStatus:   WaitStatusAcquired,
			},
		}
	}

	lock.WaitQueue = append(lock.WaitQueue, WaitEntry{
		OwnerID:   cmd.OwnerID,
		RequestID: cmd.RequestID,
		LeaseID:   cmd.LeaseID,
		Timestamp: now,
	})

	return ibabuza.ApplyResult{
		LogIndex: logIndex,
		Response: &LockResult{
			Command:       CmdWait,
			LockName:      cmd.LockName,
			Acquired:      false,
			QueuePosition: len(lock.WaitQueue),
			WaitStatus:    WaitStatusWaiting,
		},
	}
}

func (s *LockStore) cleanupExpiredAcquiredResults(now int64) {
	for reqID, result := range s.acquiredResults {
		if now >= result.ExpiresAt {
			delete(s.acquiredResults, reqID)
		}
	}
}

func (s *LockStore) applyCancelWait(logIndex uint64, cmd LockCommand) ibabuza.ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	lock := s.locks[cmd.LockName]
	if lock == nil {
		return ibabuza.ApplyResult{
			LogIndex: logIndex,
			Response: &LockResult{
				Command:  CmdCancelWait,
				LockName: cmd.LockName,
			},
		}
	}

	newQueue := make([]WaitEntry, 0, len(lock.WaitQueue))
	for _, entry := range lock.WaitQueue {
		if entry.RequestID != cmd.RequestID {
			newQueue = append(newQueue, entry)
		}
	}
	lock.WaitQueue = newQueue

	return ibabuza.ApplyResult{
		LogIndex: logIndex,
		Response: &LockResult{
			Command:  CmdCancelWait,
			LockName: cmd.LockName,
		},
	}
}

func (s *LockStore) Query(key any) (any, error) {
	lockName, ok := key.(string)
	if !ok {
		return nil, ErrInvalidKeyType
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	lock := s.locks[lockName]
	if lock == nil {
		return &LockResult{
			LockName: lockName,
			Acquired: false,
		}, nil
	}

	lease := s.leases[lock.LeaseID]
	if lease == nil {
		return &LockResult{
			LockName: lockName,
			Acquired: false,
		}, nil
	}

	return &LockResult{
		LockName:      lockName,
		OwnerID:       lock.OwnerID,
		FencingToken:  lock.FencingToken,
		Acquired:      true,
		LeaseID:       lock.LeaseID,
		QueuePosition: len(lock.WaitQueue),
	}, nil
}

func (s *LockStore) QueryLease(leaseID uint64) (*LeaseResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lease := s.leases[leaseID]
	if lease == nil {
		return nil, ErrLeaseNotFound
	}

	var locks []string
	for lockName := range s.locksByLease[leaseID] {
		locks = append(locks, lockName)
	}

	return &LeaseResult{
		LeaseID:   lease.ID,
		TTL:       lease.TTL,
		ExpiresAt: lease.ExpiresAt,
		Locks:     locks,
	}, nil
}

func (s *LockStore) HasExpiredLeases(now int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, lease := range s.leases {
		if now >= lease.ExpiresAt {
			return true
		}
	}
	return false
}

type snapshotData struct {
	LeaseSeq         uint64                     `json:"lease_seq"`
	GlobalFencingSeq uint64                     `json:"global_fencing_seq"`
	Leases           map[uint64]*Lease          `json:"leases"`
	Locks            map[string]*Lock           `json:"locks"`
	LocksByLease     map[uint64]map[string]bool `json:"locks_by_lease"`
	AcquiredResults  map[string]*AcquiredResult `json:"acquired_results,omitempty"`
}

func (s *LockStore) SaveSnapshot(ctx ibabuza.StateMachineSnapshotContext, writer ibabuza.StateMachineSnapshotWriter) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	wc, err := writer.CreateStateMachineFile(SnapshotTag, babuzapb.SnapshotFileCompression_Snappy)
	if err != nil {
		return err
	}
	defer wc.Close()

	data := snapshotData{
		LeaseSeq:         s.leaseSeq,
		GlobalFencingSeq: s.globalFencingSeq,
		Leases:           s.leases,
		Locks:            s.locks,
		LocksByLease:     s.locksByLease,
		AcquiredResults:  s.acquiredResults,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(jsonData)))
	if _, err = wc.Write(buf); err != nil {
		return err
	}
	if _, err = wc.Write(jsonData); err != nil {
		return err
	}

	return nil
}

func (s *LockStore) RestoreFromSnapshot(reader ibabuza.StateMachineSnapshotReader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, _, err := reader.Open(SnapshotTag)
	if err != nil {
		return err
	}

	buf := make([]byte, 8)
	if _, err = io.ReadFull(r, buf); err != nil {
		return err
	}

	dataLen := binary.LittleEndian.Uint64(buf)
	jsonData := make([]byte, dataLen)
	if _, err = io.ReadFull(r, jsonData); err != nil {
		return err
	}

	var data snapshotData
	if err = json.Unmarshal(jsonData, &data); err != nil {
		return err
	}

	s.leaseSeq = data.LeaseSeq
	s.globalFencingSeq = data.GlobalFencingSeq
	s.leases = data.Leases
	if s.leases == nil {
		s.leases = make(map[uint64]*Lease)
	}
	s.locks = data.Locks
	if s.locks == nil {
		s.locks = make(map[string]*Lock)
	}
	s.locksByLease = data.LocksByLease
	if s.locksByLease == nil {
		s.locksByLease = make(map[uint64]map[string]bool)
	}
	s.acquiredResults = data.AcquiredResults
	if s.acquiredResults == nil {
		s.acquiredResults = make(map[string]*AcquiredResult)
	}

	return nil
}

func (s *LockStore) Hash() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	for name, lock := range s.locks {
		h.Write([]byte(name))
		h.Write([]byte(lock.OwnerID))
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, lock.FencingToken)
		h.Write(buf)
	}
	return h.Sum32()
}

func (s *LockStore) Close() error {
	return nil
}
