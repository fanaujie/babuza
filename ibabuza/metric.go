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
