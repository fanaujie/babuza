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
