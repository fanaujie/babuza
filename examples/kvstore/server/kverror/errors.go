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


package kverror

import "errors"

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrUnknownCommand = errors.New("unknown command")
	ErrInvalidKeyType = errors.New("invalid key type, must be string")
	//ErrNotLeader                  = errors.New("raft: not leader")
	//ErrLearnerNotReady            = errors.New("raft: can only promote a learner which is in sync with leader")
	//ErrLearnerNotSwitchLeaderShip = errors.New("raft: learner can not switch to leadership")
	//ErrLearnerNotVotingMember     = errors.New("raft: learner can not become voting member")
	//ErrVotingMemberNotLeaner      = errors.New("raft: voting member can not become a leaner")
	//ErrRaftStopped                = errors.New("raft: raft has stopped")
	//ErrPeerIDRemoved              = errors.New("cluster: node id removed")
	//ErrPeerIDExists               = errors.New("cluster: node id exists")
	//ErrPeerIDNotFound             = errors.New("cluster: node id not found")
	//ErrPeerRaftListenAddrExists   = errors.New("cluster: peer raft-advertise-address exists")
	//ErrPeerNotLearner             = errors.New("cluster: peer is not a learner")
)
