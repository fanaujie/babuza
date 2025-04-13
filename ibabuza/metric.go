package ibabuza

type MetricsCollector interface {
	SetHasLeader(hasLeader int64)
	SetIsLeader(isLeader int64)
	IncrementLeaderChanges()
	SetIsLearner(isFollower int64)
	IncrementLearnerPromoteSucceed()
	IncrementLearnerPromoteFailed()
	RecordApplySec(duration float64)
	RecordDoSnapshotSec(duration float64)
	RecordApplySnapshotSec(duration float64)
	SetSnapshotApplyInProgress(applying int64)
	IncrementInflightSnapshots()
	DecrementInflightSnapshots()
	SetProposalCommited(commitedEntries uint64)
	SetProposalAppliedIndex(appliedIndex uint64)
	IncrementProposalPending()
	DecrementProposalPending()
	IncrementProposalFailed()
	IncrementSlowReadIndex()
	IncrementReadIndexFailed()
}
