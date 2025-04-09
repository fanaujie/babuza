package ibabuza

type MetricsCollector interface {
	SetHasLeader(hasLeader int64)
	SetIsLeader(isLeader int64)
	IncrementLeaderChanges()
	SetIsLearner(isFollower int64)
	RecordApplySec(duration float64)
}
