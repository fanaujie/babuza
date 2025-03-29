package testcase

import (
	"testing"
)

func TestBasicCluster(t *testing.T) {
	RunTests(&BasicCluster{t: t})
}

func TestJoinPeer(t *testing.T) {
	RunTests(&BasicJoinPeer{t: t})
}

func TestJoinLearner(t *testing.T) {
	RunTests(&BasicJoinLearner{t: t})
}

func TestUpdatePeer(t *testing.T) {
	RunTests(&BasicUpdatePeer{t: t})
}

func TestRemoveFollower(t *testing.T) {
	RunTests(&BasicRemoveFollower{t: t})
}

func TestRemoveLeader(t *testing.T) {
	RunTests(&BasicRemoveLeader{t: t})
}

func TestPromoteLearner(t *testing.T) {
	RunTests(&BasicPromoteLearner{t: t})
}

func TestTransferLeader(t *testing.T) {
	RunTests(&BasicTransferLeader{t: t})
}

func TestFollowerForwardProposal(t *testing.T) {
	RunTests(&BasicFollowerForwardProposal{t: t})
}

func TestMultiClientProposal(t *testing.T) {
	RunTests(&BasicMultiClientProposal{t: t})
}

func TestMultiClientFollowerForwardProposal(t *testing.T) {
	RunTests(&BasicMultiClientFollowerForwardProposal{t: t})
}
