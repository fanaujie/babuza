package lockstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

const (
	LockResultType  uint64 = 1 << 63
	LeaseResultType uint64 = 1 << 62
	TickResultType  uint64 = 1 << 61
	ClearResultType uint64 = ^(LockResultType | LeaseResultType | TickResultType)
)

const (
	WaitStatusAcquired = "acquired"
	WaitStatusWaiting  = "waiting"
)

type LockResult struct {
	Command       uint64 `json:"command"`
	LockName      string `json:"lock_name"`
	OwnerID       string `json:"owner_id,omitempty"`
	FencingToken  uint64 `json:"fencing_token,omitempty"`
	Acquired      bool   `json:"acquired"`
	LeaseID       uint64 `json:"lease_id,omitempty"`
	QueuePosition int    `json:"queue_position,omitempty"`
	WaitStatus    string `json:"wait_status,omitempty"`
	NextOwnerID   string `json:"next_owner_id,omitempty"`
	NextRequestID string `json:"next_request_id,omitempty"`
	NextToken     uint64 `json:"next_token,omitempty"`
	NextLeaseID   uint64 `json:"next_lease_id,omitempty"`
}

type LeaseResult struct {
	Command       uint64   `json:"command"`
	LeaseID       uint64   `json:"lease_id"`
	TTL           int64    `json:"ttl,omitempty"`
	ExpiresAt     int64    `json:"expires_at,omitempty"`
	ReleasedLocks []string `json:"released_locks,omitempty"`
	Locks         []string `json:"locks,omitempty"`
}

type TickResult struct {
	Command       uint64        `json:"command"`
	ExpiredLeases []uint64      `json:"expired_leases,omitempty"`
	NotifyResults []*LockResult `json:"notify_results,omitempty"`
}

type ResultSerializer struct {
	buf []byte
}

func NewResultSerializer() *ResultSerializer {
	return &ResultSerializer{buf: make([]byte, 8)}
}

func (s *ResultSerializer) Serialize(w io.Writer, res any) error {
	var resType uint64
	switch res.(type) {
	case *LockResult:
		resType = LockResultType
	case *LeaseResult:
		resType = LeaseResultType
	case *TickResult:
		resType = TickResultType
	default:
		return errors.New("can not cast res to a pointer to valid response")
	}
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(s.buf, resType|uint64(len(data)))
	if _, err = w.Write(s.buf); err != nil {
		return err
	}
	if _, err = w.Write(data); err != nil {
		return err
	}
	return nil
}

func (s *ResultSerializer) Deserialize(r io.Reader) (any, error) {
	if _, err := io.ReadFull(r, s.buf); err != nil {
		return nil, err
	}
	var res any
	header := binary.LittleEndian.Uint64(s.buf)
	dataLen := header & ClearResultType
	switch {
	case header&LockResultType == LockResultType:
		res = &LockResult{}
	case header&LeaseResultType == LeaseResultType:
		res = &LeaseResult{}
	case header&TickResultType == TickResultType:
		res = &TickResult{}
	default:
		return nil, errors.New("unknown response type")
	}
	buf := make([]byte, dataLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(buf, res); err != nil {
		return nil, err
	}
	return res, nil
}
