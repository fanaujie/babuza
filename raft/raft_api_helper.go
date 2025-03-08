package raft

import "context"

func (r *Raft) ProposeThenWait(ctx context.Context, session ClientSession, proposalLog []byte) (any, error) {
	result := r.Propose(ctx, session, proposalLog)
	defer result.Release()
	if err := result.Wait(); err != nil {
		return nil, err
	}
	proposalRes := result.Response()
	if err, ok := proposalRes.(error); ok {
		return nil, err
	}
	return proposalRes, nil
}
