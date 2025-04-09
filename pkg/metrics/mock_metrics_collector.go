package metrics

type MockMetricsCollector struct {
}

func NewMockMetricsCollector() *MockMetricsCollector {
	return &MockMetricsCollector{}
}

func (m *MockMetricsCollector) SetHasLeader(hasLeader int64) {

}

func (m *MockMetricsCollector) SetIsLeader(isLeader int64) {

}

func (m *MockMetricsCollector) IncrementLeaderChanges() {

}

func (m *MockMetricsCollector) SetIsLearner(isFollower int64) {}

func (m *MockMetricsCollector) RecordApplySec(duration float64) {}

func (m *MockMetricsCollector) RecordDoSnapshotSec(duration float64) {}

func (m *MockMetricsCollector) RecordApplySnapshotSec(duration float64)     {}
func (m *MockMetricsCollector) SetSnapshotApplyInProgress(applying int64)   {}
func (m *MockMetricsCollector) IncrementInflightSnapshots()                 {}
func (m *MockMetricsCollector) DecrementInflightSnapshots()                 {}
func (m *MockMetricsCollector) SetProposalCommited(commitedEntries uint64)  {}
func (m *MockMetricsCollector) SetProposalAppliedIndex(appliedIndex uint64) {}
