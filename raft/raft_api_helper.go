package raft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
)

func (r *Raft) ProposeThenWaitResponse(ctx context.Context, session ClientSession, proposalLog []byte) ibabuza.ApplyResult {
	result := r.Propose(ctx, session, proposalLog)
	defer result.Release()
	return result.WaitForApplyResult()
}
