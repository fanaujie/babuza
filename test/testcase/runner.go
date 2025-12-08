// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
				func(votingPeersCfg *babuza.PeersConfiguration, config babuza.BabuzaConfig,
					restart bool, recoverAsStandalone bool, proxyNet ibabuza.ProxyNetwork,
					appDir string, appServiceAddresses []string) (testcluster.EmbeddedApp, error) {
					appConfig := embedapp.KvStoreAppConfig{
						BubuzaConfig:        config,
						VotingPeersCfg:      votingPeersCfg,
						ServiceAddress:      appServiceAddresses[0],
						RecoverAsStandalone: recoverAsStandalone,
					}
					var customComponents builder.BabuzaComponent
					appConfig, customComponents = component.CreateCustomComponent(&appConfig,
						appDir, proxyNet)
					return embedapp.NewKvStoreApp(appConfig,
						component.CreateStateMachine(filepath.Join(appDir, "store")), customComponents)
				})
			defer func() {
				_ = tc.Teardown()
			}()
			testCase.Log(component.CaseName)
			if component.InitFunc != nil {
				err = component.InitFunc()
				if err != nil {
					panic(err)
				}
				if component.DeferFunc != nil {
					defer func() {
						err = component.DeferFunc()
						if err != nil {
							panic(err)
						}
					}()
				}
			}
			testCase.Run(tc, component.TestParams)
		}()
	}
}
