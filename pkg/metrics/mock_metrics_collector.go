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
