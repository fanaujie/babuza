package idgenerator

import (
	"sync/atomic"
)

// id format
// | 1 bit  | 51 bits         | 12 bits   |
// | unused	| sequence number | member id |

const (
	sequenceNumBits  = 51
	memberBits       = 12
	sessionBitsMask  = 0x1
	sequenceBitsMask = uint64(^(int64(-1) << sequenceNumBits))
	memberBitsMask   = uint64(^(int64(-1) << memberBits))
)

type ReplyId struct {
	seqNum uint64 //must use atomic operations to access; keep 64-bit aligned
	peerID uint64
}

func New(peerID uint64, seed uint64) *ReplyId {
	return &ReplyId{
		seqNum: seed,
		peerID: peerID & memberBitsMask,
	}
}

func (a *ReplyId) Next() uint64 {
	return (atomic.AddUint64(&a.seqNum, 1)&sequenceBitsMask)<<memberBits | a.peerID
}
