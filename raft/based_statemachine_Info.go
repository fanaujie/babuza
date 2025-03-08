package raft

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
)

type basedStateMachineInfo struct {
	appliedIndex              uint64
	supportConcurrentSnapshot bool
	supportSession            bool
	diskType                  bool
}

func newBasedStateMachineInfo(stateMachine ibabuza.BaseStateMachine) (basedStateMachineInfo, error) {
	b := basedStateMachineInfo{}
	_, b.diskType = stateMachine.(ibabuza.DiskStateMachine)
	_, b.supportConcurrentSnapshot = stateMachine.(ibabuza.ConcurrentSnapshotStateMachine)
	if b.diskType {
		if b.supportConcurrentSnapshot == false {
			return basedStateMachineInfo{}, fmt.Errorf("storage: StateMachine does not implement the interface ConcurrentSnapshotStateMachine")
		}
	}
	_, b.supportSession = stateMachine.(ibabuza.SessionEnabledStateMachine)
	return b, nil
}
