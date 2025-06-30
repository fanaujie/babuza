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


package metrics

import "github.com/fanaujie/babuza/ibabuza"

type MockMetricsCollector struct {
}

func NewMockMetricsCollector() ibabuza.MetricsCollector {
	return &MockMetricsCollector{}
}
func (m *MockMetricsCollector) SetHasLeader(hasLeader int64) {

}
func (m *MockMetricsCollector) SetIsLeader(isLeader int64) {

}
func (m *MockMetricsCollector) IncrementLeaderChanges() {

}
func (m *MockMetricsCollector) IncrementLearnerPromoteSucceed()             {}
func (m *MockMetricsCollector) IncrementLearnerPromoteFailed()              {}
func (m *MockMetricsCollector) SetIsLearner(isFollower int64)               {}
func (m *MockMetricsCollector) RecordApplySec(duration float64)             {}
func (m *MockMetricsCollector) RecordDoSnapshotSec(duration float64)        {}
func (m *MockMetricsCollector) RecordApplySnapshotSec(duration float64)     {}
func (m *MockMetricsCollector) SetSnapshotApplyInProgress(applying int64)   {}
func (m *MockMetricsCollector) IncrementInflightSnapshots()                 {}
func (m *MockMetricsCollector) DecrementInflightSnapshots()                 {}
func (m *MockMetricsCollector) SetProposalCommited(commitedEntries uint64)  {}
func (m *MockMetricsCollector) SetProposalAppliedIndex(appliedIndex uint64) {}
func (m *MockMetricsCollector) IncrementProposalPending()                   {}
func (m *MockMetricsCollector) DecrementProposalPending()                   {}
func (m *MockMetricsCollector) IncrementProposalFailed()                    {}
func (m *MockMetricsCollector) IncrementSlowReadIndex()                     {}
func (m *MockMetricsCollector) IncrementReadIndexFailed()                   {}
