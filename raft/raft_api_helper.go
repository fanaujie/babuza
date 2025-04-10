package raft

import "context"

func (r *Raft) ProposeThenWaitResponse(ctx context.Context, session ClientSession, proposalLog []byte) (any, error) {
	result := r.Propose(ctx, session, proposalLog)
	defer result.Release()
	if err := result.Wait(); err != nil {
		return nil, err
	}
	return result.Response(), nil
}
