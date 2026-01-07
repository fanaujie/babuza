package lockstore

import (
	"encoding/json"
)

const (
	CmdAcquire    uint64 = 1
	CmdRelease    uint64 = 2
	CmdRenew      uint64 = 3
	CmdWait       uint64 = 4
	CmdCancelWait uint64 = 5

	CmdLeaseGrant     uint64 = 10
	CmdLeaseRevoke    uint64 = 11
	CmdLeaseKeepAlive uint64 = 12
	CmdTick           uint64 = 13
)

type LockCommand struct {
	Command      uint64 `json:"cmd"`
	LockName     string `json:"name"`
	OwnerID      string `json:"owner"`
	TTLSeconds   int64  `json:"ttl"`
	FencingToken uint64 `json:"token"`
	Timestamp    int64  `json:"ts"`
	RequestID    string `json:"req_id,omitempty"`
	LeaseID      uint64 `json:"lease_id,omitempty"`
}

func (c *LockCommand) Acquire(lockName, ownerID string, leaseID uint64, timestamp int64) ([]byte, error) {
	c.Command = CmdAcquire
	c.LockName = lockName
	c.OwnerID = ownerID
	c.LeaseID = leaseID
	c.Timestamp = timestamp
	return json.Marshal(c)
}

func (c *LockCommand) Release(lockName, ownerID string, fencingToken uint64) ([]byte, error) {
	c.Command = CmdRelease
	c.LockName = lockName
	c.OwnerID = ownerID
	c.FencingToken = fencingToken
	return json.Marshal(c)
}

func (c *LockCommand) Wait(lockName, ownerID string, leaseID uint64, timestamp int64, requestID string) ([]byte, error) {
	c.Command = CmdWait
	c.LockName = lockName
	c.OwnerID = ownerID
	c.LeaseID = leaseID
	c.Timestamp = timestamp
	c.RequestID = requestID
	return json.Marshal(c)
}

func (c *LockCommand) CancelWait(lockName, requestID string) ([]byte, error) {
	c.Command = CmdCancelWait
	c.LockName = lockName
	c.RequestID = requestID
	return json.Marshal(c)
}

func (c *LockCommand) LeaseGrant(ttlSeconds, timestamp int64) ([]byte, error) {
	c.Command = CmdLeaseGrant
	c.TTLSeconds = ttlSeconds
	c.Timestamp = timestamp
	return json.Marshal(c)
}

func (c *LockCommand) LeaseRevoke(leaseID uint64) ([]byte, error) {
	c.Command = CmdLeaseRevoke
	c.LeaseID = leaseID
	return json.Marshal(c)
}

func (c *LockCommand) LeaseKeepAlive(leaseID uint64, timestamp int64) ([]byte, error) {
	c.Command = CmdLeaseKeepAlive
	c.LeaseID = leaseID
	c.Timestamp = timestamp
	return json.Marshal(c)
}

func (c *LockCommand) Tick(timestamp int64) ([]byte, error) {
	c.Command = CmdTick
	c.Timestamp = timestamp
	return json.Marshal(c)
}

func (c *LockCommand) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}
