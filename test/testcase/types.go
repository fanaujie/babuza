package testcase

import (
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
)

type ICase interface {
	CreateTestComponents() []BabuzaComponent
	Run(*testcluster.BabuzaCluster, any)
	Log(string)
}

type CustomComponentConfig struct {
	WalType, SnapshotType, TransportType, SessionType string
	babuzaConfig                                      *babuza.BabuzaConfig
}

type BabuzaComponent struct {
	InitFunc              func() error
	DeferFunc             func() error
	CaseName              string
	ClusterId             uint64
	CreateStateMachine    func(storageDir string) ibabuza.BaseStateMachine
	CreateCustomComponent func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent)
	ProxyNetwork          ibabuza.ProxyNetwork
	TestParams            any
}

type TestCase struct {
	Name    string
	Factory func() ICase
}
