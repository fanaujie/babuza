package test

import (
	"os"
	"path/filepath"

	"github.com/fanaujie/babuza/examples/distlock/embedapp"
	"github.com/fanaujie/babuza/examples/distlock/server/lockstore"
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

type BabuzaComponent struct {
	CaseName              string
	ClusterId             uint64
	CreateStateMachine    func(storageDir string) *lockstore.LockStore
	CreateCustomComponent func(config *embedapp.DistLockAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.DistLockAppConfig, builder.BabuzaComponent)
	ProxyNetwork          ibabuza.ProxyNetwork
	TestParams            any
}

func RunTests(testCase ICase) {
	for _, component := range testCase.CreateTestComponents() {
		func() {
			storageDir, err := os.MkdirTemp("", "distlock-test-")
			if err != nil {
				panic(err)
			}
			defer func() {
				_ = os.RemoveAll(storageDir)
			}()
			tc := testcluster.CreateTestCluster(component.ClusterId, storageDir, component.ProxyNetwork,
				func(votingPeersCfg *babuza.PeersConfiguration, config babuza.BabuzaConfig,
					restart bool, recoverAsStandalone bool, proxyNet ibabuza.ProxyNetwork,
					appDir string, appServiceAddresses []string) (testcluster.EmbeddedApp, error) {
					appConfig := embedapp.DistLockAppConfig{
						BabuzaConfig:   config,
						VotingPeersCfg: votingPeersCfg,
						ServiceAddress: appServiceAddresses[0],
					}
					var customComponents builder.BabuzaComponent
					appConfig, customComponents = component.CreateCustomComponent(&appConfig,
						appDir, proxyNet)
					return embedapp.NewDistLockApp(appConfig,
						component.CreateStateMachine(filepath.Join(appDir, "store")), customComponents)
				})
			defer func() {
				_ = tc.Teardown()
			}()
			testCase.Log(component.CaseName)
			testCase.Run(tc, component.TestParams)
		}()
	}
}
