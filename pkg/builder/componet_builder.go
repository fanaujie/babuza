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
	TransportType string // builder.TcpTransport, builder.TcpMemoryTransport, builder.HttpTransport, builder.GRPCTransport
	WalType       string // builder.BabuzaWal, builder.ETCDWal, builder.LsmtWalDisk, builder.LsmtWalMemory

	CustomLogger        ibabuza.Logger
	CustomEtcdZapLogger *zap.Logger // used for etcd wal
	// MinIO configuration
	MinIOConfig *cloudstorage.Config

	// Transport configurations
	transportOptions         []transport.SetTransportOptions
	transportMemoryLimiter   limiter.ResourceLimiter
	snapshotChuckRateLimiter limiter.RateLimiter
	peerCircuitBreaker       breaker.Breaker
	tcpNetwork               tcp.NetworkIO

	// tcp protocol configurations
	tcpOptions  []protocol.SetTcpOptions
	httpOptions []protocol.SetHttpOptions
	grpcOptions []protocol.SetGrpcOptions

	// Session configurations
	lruSessionOptions    []session.SetLruMgrOptions
	expireSessionOptions []session.SetExpiredMgrOptions
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

func (b *BabuzaComponentBuilder) SetSessionType(sessionType string) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.SessionType = sessionType
	return b
}

func (b *BabuzaComponentBuilder) SetSnapshotType(snapshotType string) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.SnapshotType = snapshotType
	return b
}

func (b *BabuzaComponentBuilder) SetTransportType(transportType string) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.TransportType = transportType
	return b
}

func (b *BabuzaComponentBuilder) SetWalType(walType string) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.WalType = walType
	return b
}

func (b *BabuzaComponentBuilder) SetCustomLogger(logger ibabuza.Logger) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.CustomLogger = logger
	return b
}

func (b *BabuzaComponentBuilder) SetCustomEtcdZapLogger(logger *zap.Logger) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.CustomEtcdZapLogger = logger
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

func (b *BabuzaComponentBuilder) AddLruSessionOptions(options ...session.SetLruMgrOptions) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.lruSessionOptions = append(b.config.lruSessionOptions, options...)
	return b
}

func (b *BabuzaComponentBuilder) AddExpireSessionOptions(options ...session.SetExpiredMgrOptions) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.expireSessionOptions = append(b.config.expireSessionOptions, options...)
	return b
}

func (b *BabuzaComponentBuilder) SetMinIOConfig(config *cloudstorage.Config) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.MinIOConfig = config
	return b
}

func (b *BabuzaComponentBuilder) AddTcpOptions(options ...protocol.SetTcpOptions) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.tcpOptions = append(b.config.tcpOptions, options...)
	return b
}

func (b *BabuzaComponentBuilder) AddHttpOptions(options ...protocol.SetHttpOptions) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.httpOptions = append(b.config.httpOptions, options...)
	return b
}

func (b *BabuzaComponentBuilder) AddGrpcOptions(options ...protocol.SetGrpcOptions) *BabuzaComponentBuilder {
	if b.built {
		panic("Builder has already been used to build a component, cannot modify configuration")
	}
	b.config.grpcOptions = append(b.config.grpcOptions, options...)
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
	} else {
		zapLogger = logger.NewZapLogger(zapcore.DebugLevel, []string{"stdout"}, "")
		component.Logger = logger.NewRaftLogger(zapLogger.Sugar())
	}

	if b.config.CustomEtcdZapLogger != nil {
		zapLogger = b.config.CustomEtcdZapLogger
	} else if zapLogger == nil {
		zapLogger = logger.NewZapLogger(zapcore.DebugLevel, []string{"stdout"}, "")
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
		return session.NewExpiredManager(logger, b.config.expireSessionOptions...)
	case LRUSession:
		return session.NewLruManager(logger, b.config.lruSessionOptions...)
	default:
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
		return snapshot.NewMinIOSnapshotManager(snapDir, *b.config.MinIOConfig, logger)
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
		proto = protocol.NewTcp(tcpNetwork, logger, b.config.tcpOptions...)
	case HttpTransport:
		proto = protocol.NewHttp(logger, b.config.httpOptions...)
	case GRPCTransport:
		proto = protocol.NewGrpc(logger, b.config.grpcOptions...)
	default:
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
