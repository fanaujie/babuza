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

import "errors"

var (
	ErrNoLeader                      = errors.New("raft: no leader")
	ErrNotLeader                     = errors.New("raft: not leader")
	ErrLearnerNotReady               = errors.New("raft: can only promote a learner which is in sync with leader")
	ErrLearnerCanNotSwitchLeadership = errors.New("raft: learner can not switch to leadership")
	ErrLearnerCanNotVote             = errors.New("raft: learner can not become vote")
	ErrVotingMemberCanNotPromote     = errors.New("raft: voting member can not become a leaner")
	ErrNotLearner                    = errors.New("raft: not a learner")
	ErrStopped                       = errors.New("raft: raft has stopped")
	ErrNilEntryCache                 = errors.New("raft: entry cache is nil")
)
