package testcase

import (
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/ibabuza"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"os"
)

func RunTests(testCase ICase) {
	for _, component := range testCase.CreateTestComponents() {
		func() {
			if err := os.MkdirAll(component.StorageRootDir, 0755); err != nil {
				panic(err)
			}
			defer func() {
				_ = os.RemoveAll(component.StorageRootDir)
			}()
			tc := testcluster.CreateTestCluster(component.ClusterId, component.StorageRootDir, component.ProxyNetwork,
				func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool,
					pn ibabuza.ProxyNetwork, appDir string, appServiceAddresses []string) (testcluster.EmbeddedApp, error) {
					appConfig := embedapp.KvStoreAppConfig{
						BubuzaConfig:   config,
						VotingPeersCfg: votingPeersCfg,
						ServiceAddress: appServiceAddresses[0],
					}
					storageDir, err := createStorageDirectories(appDir)
					if err != nil {
						panic(err)
					}
					return embedapp.NewKvStoreApp(appConfig, component.CreateStateMachine(storageDir.stateMachineDir),
						component.CreateBuilder(&appConfig.BubuzaConfig, storageDir.walDir, storageDir.snapshotDir,
							appConfig.VotingPeersCfg, pn))
				})
			defer func() {
				_ = tc.Teardown()
			}()
			testCase.Run(tc)
		}()
	}
}
