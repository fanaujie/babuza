package multiraft

import (
	babuza "github.com/fanaujie/babuza/raft"
)

type leaderTransferResult struct {
	resultChan chan error
}

func newLeaderTransferResult(
	resultChan chan error) babuza.TransferLeaderResult {
	return &leaderTransferResult{
		resultChan: resultChan,
	}
}

func (r *leaderTransferResult) Wait() error {
	return <-r.resultChan
}
