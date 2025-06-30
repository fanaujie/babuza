package raft

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
)

type BasedStateMachineInfo struct {
	openAppliedIndex          uint64
	supportConcurrentSnapshot bool
	supportSession            bool
	diskType                  bool
}

func NewBasedStateMachineInfo(stateMachine ibabuza.BaseStateMachine) (*BasedStateMachineInfo, error) {
	b := &BasedStateMachineInfo{}
	_, b.diskType = stateMachine.(ibabuza.DiskStateMachine)
	_, b.supportConcurrentSnapshot = stateMachine.(ibabuza.ConcurrentSnapshotStateMachine)
	if b.diskType {
		if b.supportConcurrentSnapshot == false {
			return nil, fmt.Errorf("storage: StateMachine does not implement the interface ConcurrentSnapshotStateMachine")
		}
	}
	_, b.supportSession = stateMachine.(ibabuza.SessionEnabledStateMachine)
	return b, nil
}

func (b *BasedStateMachineInfo) OpenAppliedIndex() uint64 {
	return b.openAppliedIndex
}

func (b *BasedStateMachineInfo) SetOpenAppliedIndex(index uint64) {
	b.openAppliedIndex = index
}

func (b *BasedStateMachineInfo) SupportConcurrentSnapshot() bool {
	return b.supportConcurrentSnapshot
}

func (b *BasedStateMachineInfo) SupportSession() bool {
	return b.supportSession
}

func (b *BasedStateMachineInfo) IsDiskType() bool {
	return b.diskType
}
