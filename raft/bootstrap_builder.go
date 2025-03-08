package raft

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/raftnode"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/pkg/snapshot"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal"
	"go.uber.org/zap/zapcore"
	"os"
	"path/filepath"
)

type BootstrapBuilder struct {
	defaultStorageDir string
	config            *BabuzaConfig
	peersConfig       *VotingPeersConfiguration
	stateMachine      ibabuza.BaseStateMachine
	cluster           ibabuza.Cluster
	raftNode          ibabuza.RaftNode
	sessionManager    ibabuza.SessionManager
	snapshotManager   ibabuza.SnapshotManager
	walManager        ibabuza.WalManager
	transport         ibabuza.Transport
	logger            ibabuza.Logger
}

func NewBootstrapBuilder() *BootstrapBuilder {
	return &BootstrapBuilder{}
}

func (b *BootstrapBuilder) SetDefaultStorageDir(defaultStorageDir string) *BootstrapBuilder {
	b.defaultStorageDir = defaultStorageDir
	return b
}

func (b *BootstrapBuilder) SetConfig(config *BabuzaConfig) *BootstrapBuilder {
	b.config = config
	return b
}

func (b *BootstrapBuilder) SetPeersConfig(peersConfig *VotingPeersConfiguration) *BootstrapBuilder {
	b.peersConfig = peersConfig
	return b
}

func (b *BootstrapBuilder) SetStateMachine(stateMachine ibabuza.BaseStateMachine) *BootstrapBuilder {
	b.stateMachine = stateMachine
	return b
}

func (b *BootstrapBuilder) SetLogger(logger ibabuza.Logger) *BootstrapBuilder {
	b.logger = logger
	return b
}

func (b *BootstrapBuilder) SetCluster(cluster ibabuza.Cluster) *BootstrapBuilder {
	b.cluster = cluster
	return b
}

func (b *BootstrapBuilder) SetRaftNode(raftNode ibabuza.RaftNode) *BootstrapBuilder {
	b.raftNode = raftNode
	return b
}

func (b *BootstrapBuilder) SetSessionManager(sessionManager ibabuza.SessionManager) *BootstrapBuilder {
	b.sessionManager = sessionManager
	return b
}

func (b *BootstrapBuilder) SetSnapshotManager(snapshotManager ibabuza.SnapshotManager) *BootstrapBuilder {
	b.snapshotManager = snapshotManager
	return b
}

func (b *BootstrapBuilder) SetWalManager(walManager ibabuza.WalManager) *BootstrapBuilder {
	b.walManager = walManager
	return b
}

func (b *BootstrapBuilder) SetTransport(transport ibabuza.Transport) *BootstrapBuilder {
	b.transport = transport
	return b
}

func (b *BootstrapBuilder) Build() (*BootstrapRaftCluster, error) {
	if 0 == len(b.defaultStorageDir) {
		defaultStorageDir, err := os.Executable()
		if err != nil {
			return nil, err
		}
		b.defaultStorageDir = defaultStorageDir
	} else {
		if false == fileutil.Exist(b.defaultStorageDir) {
			return nil, errors.New("defaultStorageDir does not exist")
		}
	}
	if b.config == nil {
		return nil, errors.New("config is required")
	}
	if b.peersConfig == nil {
		return nil, errors.New("peersConfig is required")
	}
	if b.logger == nil {
		var zapLogger = logger.NewZapLogger(
			zapcore.DebugLevel, []string{"stdout"}, "")
		b.logger = logger.NewRaftLogger(zapLogger.Sugar())
	}
	if b.cluster == nil {
		b.cluster = cluster.NewCluster(b.logger)
	}
	if b.raftNode == nil {
		b.raftNode = raftnode.NewEtcdRaftNode()
	}
	if b.sessionManager == nil {
		b.sessionManager = session.NewNoOpManager(b.logger)
	}
	if b.snapshotManager == nil {
		b.snapshotManager = snapshot.NewDurableSnapshotManager(filepath.Join(b.defaultStorageDir, "snapshot"), b.logger)
	}
	if b.walManager == nil {
		b.walManager = babuzawal.NewWalManager(filepath.Join(b.defaultStorageDir, "wal"), b.logger)
	}
	if b.transport == nil {
		b.transport = transport.New(transport.DefaultOptions(),
			transport.NewPeerManager(), limiter.NewNoResourceLimiter(),
			limiter.NewNoOpRateLimiter(), breaker.NewNoOpBreaker(),
			protocol.NewTcp(networkio.NewTcpPhysicalIO(), b.logger), b.logger)
	}

	return NewBootstrapRaftCluster(
		b.config,
		b.peersConfig,
		b.stateMachine,
		b.cluster,
		b.raftNode,
		b.sessionManager,
		b.snapshotManager,
		b.walManager,
		b.transport,
		b.logger,
	)
}
