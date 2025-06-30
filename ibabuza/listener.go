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


package ibabuza

const (
	MemberJoined = iota + 1
	MemberUpdated
	MemberRemoved
	LeanerAdded
	LeanerPromoted
	LeaderChanged
	AcquiredLeader
	LostLeader
	RemoveSelf = iota + 1
)

type RaftEvent struct {
	Event   int
	GroupID RaftGroupID
	PeerID  uint64
}

type RaftListener interface {
	OnLeaderChange(term, leaderID uint64)
	OnAcquiredLeader()
	OnLostLeader()
	OnMemberChange(memberEvent int, term, peerID uint64)
	OnRaftShutdown()
}
