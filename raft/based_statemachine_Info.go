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
