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


package raft

import "testing"

func TestDuplicateClusterPeerAdvertiseAddr(t *testing.T) {
	type testCase struct {
		cluster    map[uint64]string
		duplicated bool
	}

	c := []testCase{
		{
			cluster:    map[uint64]string{1: "localhost:1", 2: "localhost:2"},
			duplicated: false,
		},
		{
			cluster:    map[uint64]string{1: "10.10.10.10:1", 2: "10.10.10.10:1"},
			duplicated: true,
		},
	}

	for _, v := range c {
		if v.duplicated != duplicateClusterPeerEndpoint(v.cluster) {
			t.Fatalf("test case = %v", v)
		}
	}
}
