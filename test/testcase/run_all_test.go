package testcase

import (
	"testing"
)

func TestBasicCluster(t *testing.T) {
	RunTests(&BasicCluster{t: t})
}

func TestJoinPeer(t *testing.T) {
	RunTests(&JoinPeer{t: t})
}

func TestJoinLearner(t *testing.T) {
	RunTests(&JoinLearner{t: t})
}

func TestUpdatePeer(t *testing.T) {
	RunTests(&UpdatePeer{t: t})
}
