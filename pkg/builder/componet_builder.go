package builder

import (
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"go.uber.org/zap"
	"path/filepath"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/raftnode"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/pkg/snapshot"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/cloudstorage"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal"
	"github.com/fanaujie/babuza/pkg/wal/etcdwal"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal"
	"go.uber.org/zap/zapcore"
)

type BabuzaComponent struct {
	Cluster         ibabuza.Cluster
	RaftNode        ibabuza.RaftNode
	SessionManager  ibabuza.SessionManager
	SnapshotManager ibabuza.SnapshotManager
	WalManager      ibabuza.WalManager
	Transport       ibabuza.Transport
	Logger          ibabuza.Logger
}

type BabuzaComponentConfig struct {
	ClusterId      uint64
	StorageRootDir string

	// available types from constants
	SessionType   string // builder.NoOpSession, builder.ExpireSession, builder.LRUSession
	SnapshotType  string // builder.DurableSnapshot, builder.VolatileSnapshot, builder.MinIOSnapshot
	TransportType string // builder.TcpTransport, builder.TcpMemoryTransport, builder.HttpTransport, builder.GRPCTranspost
	WalType       string // builder.BabuzaWal, builder.ETCDWal, builder.LsmtWalDisk, builder.LsmtWalMemory

	CustomLogger        ibabuza.Logger
	CustomEtcdZapLogger *zap.Logger // used for etcd wal
	// MinIO configuration
	MinIOConfig      *cloudstorage.Config
	transportOptions []transport.SetTransportOptions
	// Transport configurations
	transportMemoryLimiter   limiter.ResourceLimiter
	snapshotChuckRateLimiter limiter.RateLimiter
	peerCircuitBreaker       breaker.Breaker

	tcpNetwork tcp.NetworkIO
}

type BabuzaComponentBuilder struct {
	config *BabuzaComponentConfig
	built  bool
}

func NewBabuzaComponentBuilder(config *BabuzaComponentConfig) *BabuzaComponentBuilder {
	if config == nil {
		panic("config cannot be nil")
	}
	return &BabuzaComponentBuilder{
		config: config,
		built:  false,
	}
}

func (b *BabuzaComponentBuilder) SetClusterId(clusterId uint64) *BabuzaComponentBuilder {
	b.config.ClusterId = clusterId
	return b
}

func (b *BabuzaComponentBuilder) SetStorageRootDir(storageRootDir string) *BabuzaComponentBuilder {
	b.config.StorageRootDir = storageRootDir
	return b
}

type TransportAssets struct {
	TransportMemoryLimiter   limiter.ResourceLimiter
	SnapshotChuckRateLimiter limiter.RateLimiter
	PeerCircuitBreaker       breaker.Breaker
}

func (b *BabuzaComponentBuilder) SetTransportAssets(assets TransportAssets) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}

	if assets.TransportMemoryLimiter != nil {
		b.config.transportMemoryLimiter = assets.TransportMemoryLimiter
	}
	if assets.SnapshotChuckRateLimiter != nil {
		b.config.snapshotChuckRateLimiter = assets.SnapshotChuckRateLimiter
	}
	if assets.PeerCircuitBreaker != nil {
		b.config.peerCircuitBreaker = assets.PeerCircuitBreaker
	}
	return b
}

func (b *BabuzaComponentBuilder) AddTransportOptions(options ...transport.SetTransportOptions) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.transportOptions = append(b.config.transportOptions, options...)
	return b
}

func (b *BabuzaComponentBuilder) SetTransportTcpNetwork(network tcp.NetworkIO) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	if b.config.TransportType == TcpTransport || b.config.TransportType == TcpMemoryTransport {
		b.config.tcpNetwork = network
	}
	return b
}

func (b *BabuzaComponentBuilder) Build() *BabuzaComponent {
	if b.built {
		panic("Builder has already been used to build a component")
	}
	var component BabuzaComponent
	var zapLogger *zap.Logger
	if b.config.CustomLogger != nil {
		component.Logger = b.config.CustomLogger
	}
	if b.config.CustomEtcdZapLogger != nil {
		zapLogger = b.config.CustomEtcdZapLogger
	}
	if b.config.CustomLogger == nil {
		zapLogger = logger.NewZapLogger(zapcore.DebugLevel, []string{"stdout"}, "")
		component.Logger = logger.NewRaftLogger(zapLogger.Sugar())
	}
	component.SessionManager = b.createSessionManager(component.Logger)
	component.WalManager = b.createWalManager(component.Logger, zapLogger)
	component.SnapshotManager = b.createSnapshotManager(component.Logger)
	component.Transport = b.createTransport(component.Logger)

	component.RaftNode = raftnode.NewEtcdRaftNode()
	component.Cluster = cluster.NewCluster(component.Logger)

	b.built = true

	return &component
}

func (b *BabuzaComponentBuilder) createSessionManager(logger ibabuza.Logger) ibabuza.SessionManager {
	switch b.config.SessionType {
	case NoOpSession:
		return session.NewNoOpManager(logger)
	case ExpireSession:
		return session.NewExpiredManager(logger)
	case LRUSession:
		return session.NewLruManager(logger)
	default:
		// 默認使用 NoOp
		return session.NewNoOpManager(logger)
	}
}

func (b *BabuzaComponentBuilder) createWalManager(logger ibabuza.Logger, zapLogger *zap.Logger) ibabuza.WalManager {
	walDir := filepath.Join(b.config.StorageRootDir, "wal")
	switch b.config.WalType {
	case BabuzaWal:
		return babuzawal.NewWalManager(walDir, logger)
	case ETCDWal:
		return etcdwal.NewWalManager(walDir, zapLogger)
	case LsmtWalDisk:
		return lsmtwal.NewBadgerWalManager(lsmtwal.Config{
			WalDir: walDir,
		}, logger)
	case LsmtWalMemory:
		return lsmtwal.NewBadgerWalManager(lsmtwal.Config{
			InMemory: true,
		}, logger)
	default:
		return babuzawal.NewWalManager(walDir, logger)
	}
}

func (b *BabuzaComponentBuilder) createSnapshotManager(logger ibabuza.Logger) ibabuza.SnapshotManager {
	snapDir := filepath.Join(b.config.StorageRootDir, "snap")

	switch b.config.SnapshotType {
	case DurableSnapshot:
		return snapshot.NewDurableSnapshotManager(snapDir, logger)
	case VolatileSnapshot:
		return snapshot.NewVolatileSnapshotManager(snapDir, logger)
	case MinIOSnapshot:
		if b.config.MinIOConfig == nil {
			panic("MinIOConfig cannot be nil when using MinIOSnapshot")
		}
		return snapshot.NewMinIOSnapshotManager("/snap", *b.config.MinIOConfig, logger)
	default:
		return snapshot.NewDurableSnapshotManager(snapDir, logger)
	}
}

func (b *BabuzaComponentBuilder) createTransport(logger ibabuza.Logger) ibabuza.Transport {
	peerManager := transport.NewPeerManager()
	resourceLimiter := b.config.transportMemoryLimiter
	rateLimiter := b.config.snapshotChuckRateLimiter
	circuitBreaker := b.config.peerCircuitBreaker
	tcpNetwork := b.config.tcpNetwork
	if tcpNetwork == nil {
		if b.config.TransportType == TcpTransport {
			tcpNetwork = networkio.NewTcpPhysicalIO()
		} else if b.config.TransportType == TcpMemoryTransport {
			tcpNetwork = networkio.NewTcpMemoryIO()
		}
	}
	if resourceLimiter == nil {
		resourceLimiter = limiter.NewNoResourceLimiter()
	}
	if rateLimiter == nil {
		rateLimiter = limiter.NewNoOpRateLimiter()
	}
	if circuitBreaker == nil {
		circuitBreaker = breaker.NewNoOpBreaker()
	}

	var proto ibabuza.TransportProtocol
	switch b.config.TransportType {
	case TcpTransport:
		fallthrough
	case TcpMemoryTransport:
		proto = protocol.NewTcp(tcpNetwork, logger)
	case HttpTransport:
		proto = protocol.NewHttp(logger)
	case GRPCTranspost:
		proto = protocol.NewGrpc(logger)
	default:
		// 默認使用 TCP
		proto = protocol.NewTcp(networkio.NewTcpPhysicalIO(), logger)
	}

	return transport.New(
		b.config.ClusterId,
		peerManager,
		resourceLimiter,
		rateLimiter,
		circuitBreaker,
		proto,
		logger,
		b.config.transportOptions...,
	)
}
