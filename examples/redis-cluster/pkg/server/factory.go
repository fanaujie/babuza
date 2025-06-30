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


package server

import (
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/redisstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/session"
)

type redisComponentFactory struct {
	logger ibabuza.Logger
}

func (f *redisComponentFactory) CreateStateMachine(stateMachineRootDir string, groupID ibabuza.RaftGroupID) (ibabuza.BaseStateMachine, error) {
	return redisstore.NewRedisStateMachine(f.logger), nil
}

func (f *redisComponentFactory) CreateCluster() ibabuza.Cluster {
	return cluster.NewCluster()
}

func (f *redisComponentFactory) CreateSessionManager() ibabuza.SessionManager {
	return session.NewNoOpManager(f.logger)
}

func (f *redisComponentFactory) GetLogger() ibabuza.Logger {
	return f.logger
}
