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
