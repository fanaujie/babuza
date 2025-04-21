package multiraft

import (
	"fmt"
	"github.com/Workiva/go-datastructures/queue"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/idgenerator"
	"github.com/fanaujie/babuza/pkg/metrics"
	"github.com/fanaujie/babuza/pkg/replier"
	"github.com/fanaujie/babuza/pkg/status"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/raft/multiraft/shard"
	"go.etcd.io/etcd/raft/v3"
	"time"
)

type ComponentsFactory interface {
	CreateStateMachine(groupID ibabuza.RaftGroupID) ibabuza.BaseStateMachine
	CreateCluster() ibabuza.Cluster
	CreateSessionManager() ibabuza.SessionManager
	CreateWalManager(walDir string) ibabuza.WalManager
	CreateSnapshotManager(snapshotDir string) ibabuza.SnapshotManager
}

func NewBootstrapNode(cfg NodeConfig, factory ComponentsFactory, trans ibabuza.MultiRaftTransport,
	logger ibabuza.Logger) (*Node, error) {
	var storage StorageManager

	if err := trans.SetupTransportConfig(ibabuza.TransportConfig{
		PeerId:      cfg.NodeID,
		PeerAddress: cfg.RaftListenAddress,
		TLSConfig:   cfg.TLSConfig,
	}); err != nil {
		return nil, err
	}
	if groupIDs, err := storage.HasExistingWalFiles(); err != nil {
		return nil, err
	} else if len(groupIDs) > 0 {
		if err = storage.ScanInstalledSnapshot(groupIDs); err != nil {
			return nil, err
		}
		return restartNode(cfg, groupIDs, trans, storage, factory, logger)

	}
	return startNode(cfg, trans, storage, factory, logger)
}

func startNode(config NodeConfig, trans ibabuza.MultiRaftTransport, storage StorageManager, factory ComponentsFactory,
	logger ibabuza.Logger) (*Node, error) {
	return newNode(config, trans, storage, factory, logger), nil
}

func restartNode(config NodeConfig, restartGroupIDs []ibabuza.RaftGroupID, trans ibabuza.MultiRaftTransport,
	storage StorageManager, factory ComponentsFactory, logger ibabuza.Logger) (*Node, error) {

	n := newNode(config, trans, storage, factory, logger)

	for _, groupID := range restartGroupIDs {
		//create cluster
		replicaCluster := factory.CreateCluster()
		replicaStatus := status.New()
		//create session
		replicaSessionManager := factory.CreateSessionManager()

		walSnapshots, err := storage.FindSnapshotFromWal(groupID)
		if err != nil {
			return nil, err
		}
		snap, err := storage.LoadLastValidFromSnapshot(groupID, walSnapshots)
		if err != nil {
			return nil, err
		}
		walReplayResult, err := storage.OpenWalAndReplay(groupID, snap, false)
		if err != nil {
			return nil, err
		}
		var metadata babuzapb.WalMetadata
		if err = metadata.Unmarshal(walReplayResult.Metadata()); err != nil {
			return nil, err
		}
		replicaCluster.SetClusterID(metadata.ClusterID)     // clusterID is the same as group id
		replicaCluster.SetLocalPeerID(metadata.LocalPeerID) // localPeerID is the same as node id
		entryStorage, err := storage.GetEntryStorage(groupID)
		if err != nil {
			return nil, err
		}
		hs, _, err := entryStorage.InitialState()
		if err != nil {
			return nil, err
		}
		replicaStatus.SetHardStateTerm(hs.Term)
		replicaStatus.SetCommittedIndex(hs.Commit)

		if err = storage.OpenStateMachine(groupID, snap); err != nil {
			return nil, err
		}
		if snap != nil {
			if err = storage.RestoreFromSnapshot(groupID, snap.Metadata.Index, false, replicaCluster, replicaSessionManager); err != nil {
				return nil, err
			}
			if replicaCluster.ClusterID() != uint64(groupID) || replicaCluster.LocalPeerID() != config.NodeID {
				return nil, fmt.Errorf("bootstrap: mistach configuration (groupID=%d, restore groupID=%d, localNodeID=%d, restore nodeID=%d) ",
					groupID, replicaCluster.ClusterID(), config.NodeID, replicaCluster.LocalPeerID())
			}
			for _, p := range replicaCluster.Peers() {
				trans.AddPeer(p.RaftPeerAttr.Id, p.RaftPeerAttr.RaftListenAddr)
			}
			replicaStatus.SetAppliedIndex(snap.Metadata.Index)
			replicaStatus.SetAppliedTerm(snap.Metadata.Term)
			replicaStatus.SetSnapshotIndex(snap.Metadata.Index)
			replicaStatus.SetConfState(snap.Metadata.ConfState)
		}
		if config.EnableWalNoSync {
			_ = storage.SetWalNoFSync(groupID)
		}
		replicaStorage, err := storage.GetReplicaStorage(groupID)
		if err != nil {
			return nil, err
		}
		replicaRaftConfig := ReplicaRaftConfig{
			EnableWalNoSync:     config.EnableWalNoSync,
			SnapshotCount:       config.SnapshotCount,
			RaftConfig:          config.RaftConfig,
			LearnerReadyPercent: config.LearnerReadyPercent,
		}
		rawNode, err := raft.NewRawNode(replicaRaftConfig.convertToRaftConfig(replicaCluster.LocalPeerID(), logger, entryStorage))
		if err != nil {
			return nil, err
		}
		firstCommitInTermNotifier := syncutil.NewNotifier()
		resultReplier := replier.NewResult[ibabuza.ApplyResult]()
		appliedFacade := babuza.NewAppliedFacade(replicaStorage, replicaStatus, firstCommitInTermNotifier, replicaSessionManager,
			resultReplier, replicaCluster, n, trans, logger, metrics.NewMockMetricsCollector())

		r := &replica{
			raftGroup:                 ibabuza.RaftGroup{},
			config:                    replicaRaftConfig,
			applyJobQueue:             n.applyJobQueue,
			cluster:                   replicaCluster,
			transport:                 trans,
			status:                    replicaStatus,
			session:                   replicaSessionManager,
			storage:                   replicaStorage,
			appliedFacade:             appliedFacade,
			rawNode:                   rawNode,
			idGenerator:               idgenerator.New(replicaCluster.LocalPeerID(), uint64(time.Now().Nanosecond())),
			resultReplier:             resultReplier,
			completionReplier:         replier.NewCompletion(),
			firstCommitInTermNotifier: firstCommitInTermNotifier,
			leaderChangeNotifier:      syncutil.NewNotifier(),
			leaderCh:                  nil,
			proposalQueue:             &queue.Queue{},
			configChangeQueue:         &queue.Queue{},
			applyConfChangeQueue:      &queue.Queue{},
			logger:                    logger,
			closer:                    syncutil.NewCloser(),
		}
		n.replicaSet.replica[groupID] = r
	}
	return n, nil
}

func newNode(config NodeConfig, trans ibabuza.MultiRaftTransport, storage StorageManager, factory ComponentsFactory, logger ibabuza.Logger) *Node {
	n := &Node{
		config:      config,
		trans:       trans,
		storage:     storage,
		factory:     factory,
		multiStatus: status.NewMultiRaftStatus(),
		logger:      logger,
	}
	scheduler := shard.NewScheduler(shard.Config{
		WorkerNum: config.SchedulerWorkerNum,
		QueueSize: config.SchedulerQueueSize,
		MaxTicks:  config.SchedulerMaxTicks,
	}, n, logger)
	n.scheduler = scheduler
	n.applyJobQueue = shard.NewApplyJobQueue(config.ApplyJobQueueWorkerNum, logger)
	n.replicaSet.replica = make(map[ibabuza.RaftGroupID]*replica)
	return n
}
