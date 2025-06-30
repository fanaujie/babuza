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
	"testing"
)

func genKey(sessionBit, seqNum, memberId uint64) uint64 {
	return (sessionBit&sessionBitsMask)<<(sequenceNumBits+memberBits) | (seqNum&sequenceBitsMask)<<memberBits | memberId
}

func TestReplyID(t *testing.T) {
	var seed uint64 = 100
	var peerID uint64 = 1
	a := New(peerID, seed)
	seed++
	var expected uint64 = 0x65001
	iter := 1000
	for i := 0; i < iter; i++ {
		if k := a.Next(); k != expected {
			t.Fatalf("expected key=%d, real key=%d", expected, k)
		}
		seed++
		expected = genKey(0, seed, peerID)
	}

}

func TestReplyID_Repeat(t *testing.T) {
	iter := 10000000
	ch := make(chan uint64, iter)
	var seed uint64 = 100
	a := New(1, seed)
	go func() {
		for i := 0; i < 10000; i++ {
			go func() {
				for j := 0; j < 1000; j++ {
					ch <- a.Next()
				}
			}()
		}
	}()
	value := make(map[uint64]struct{})
	check := 0
	for k := range ch {
		if _, ok := value[k]; ok {
			t.Fatalf("get repeat key=%d iter=%d", k, check)
		} else {
			value[k] = struct{}{}
		}
		check++
		if check%100000 == 0 {
			t.Log("check iter=", check)
		}
		if check == iter {
			close(ch)
		}
	}
}

func BenchmarkReplyIDNext(b *testing.B) {
	a := New(1, 0)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = a.Next()
		}
	})
}
