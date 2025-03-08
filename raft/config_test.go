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
