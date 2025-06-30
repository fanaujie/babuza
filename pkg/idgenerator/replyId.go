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
