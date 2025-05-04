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
