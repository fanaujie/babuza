package testcase

import (
	"github.com/fanaujie/babuza/ibabuza"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
)

type ICase interface {
	CreateTestComponents() []BabuzaComponent
	Run(*testcluster.BabuzaCluster)
	Log(string)
}

type BabuzaComponent struct {
	CaseName           string
	ClusterId          uint64
	CreateStateMachine func(storageDir string) ibabuza.BaseStateMachine
	CreateBuilder      func(babuzaConfig *babuza.BabuzaConfig, walDir, snapshotDir string,
		votingPeersCfg *babuza.VotingPeersConfiguration, proxyNetwork ibabuza.ProxyNetwork) *babuza.BootstrapBuilder
	ProxyNetwork   ibabuza.ProxyNetwork
	StorageRootDir string
}

type TestCase struct {
	Name    string
	Factory func() ICase
}
