package testcase

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
)

type ICase interface {
	CreateTestComponents() []BabuzaComponent
	Run(*testcluster.BabuzaCluster)
	Log(string)
}

type CustomComponentConfig struct {
	WalType, SnapshotType, TransportType, SessionType string
	babuzaConfig                                      *babuza.BabuzaConfig
}

type BabuzaComponent struct {
	CaseName              string
	ClusterId             uint64
	CreateStateMachine    func(storageDir string) ibabuza.BaseStateMachine
	CreateCustomComponent func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent)
	ProxyNetwork          ibabuza.ProxyNetwork
}

type TestCase struct {
	Name    string
	Factory func() ICase
}
