package testcase

import (
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"os"
	"path/filepath"
)

func RunTests(testCase ICase) {
	for _, component := range testCase.CreateTestComponents() {
		func() {
			storageDir, err := os.MkdirTemp("", "babuza-test-")
			if err != nil {
				panic(err)
			}
			defer func() {
				_ = os.RemoveAll(storageDir)
			}()
			tc := testcluster.CreateTestCluster(component.ClusterId, storageDir, component.ProxyNetwork,
				func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool,
					proxyNet ibabuza.ProxyNetwork, appDir string, appServiceAddresses []string) (testcluster.EmbeddedApp, error) {
					appConfig := embedapp.KvStoreAppConfig{
						BubuzaConfig:   config,
						VotingPeersCfg: votingPeersCfg,
						ServiceAddress: appServiceAddresses[0],
					}
					var customComponents builder.BabuzaComponent
					appConfig.BubuzaConfig, customComponents = component.CreateCustomComponent(&appConfig.BubuzaConfig,
						appDir, proxyNet)
					return embedapp.NewKvStoreApp(appConfig,
						component.CreateStateMachine(filepath.Join(appDir, "store")), customComponents)
				})
			defer func() {
				_ = tc.Teardown()
			}()
			testCase.Run(tc)
		}()
	}
}
