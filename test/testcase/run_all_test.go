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
