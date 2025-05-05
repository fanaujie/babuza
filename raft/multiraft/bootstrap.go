package multiraft

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/idgenerator"
	"github.com/fanaujie/babuza/pkg/metrics"
	"github.com/fanaujie/babuza/pkg/replier"
	"github.com/fanaujie/babuza/pkg/status"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"go.etcd.io/etcd/raft/v3"
	"path/filepath"
	"time"
)

const (
	stateMachineDir = "state_machine"
)

type ComponentsFactory interface {
	CreateStateMachine(stateMachineRootDir string, groupID ibabuza.RaftGroupID) (ibabuza.BaseStateMachine, error)
	CreateCluster() ibabuza.Cluster
	CreateSessionManager() ibabuza.SessionManager
}

func BootstrapOrRecoverNode(cfg NodeConfig, factory ComponentsFactory, trans ibabuza.MultiRaftTransport, walManager ibabuza.MultiRaftWalManager,
	snapshotManager ibabuza.MultiRaftSnapshotManager, logger ibabuza.Logger) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	stateMachineRootDir := filepath.Join(cfg.NodeHostDir, stateMachineDir)
	storage := newBootstrapStorage(stateMachineRootDir, factory, walManager, snapshotManager, logger)
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

func startNode(config NodeConfig, trans ibabuza.MultiRaftTransport, storage BootstrapStorage, factory ComponentsFactory,
	logger ibabuza.Logger) (*Node, error) {
	return newNode(config, trans, storage, factory, logger), nil
}

func restartNode(config NodeConfig, restartGroupIDs []ibabuza.RaftGroupID, trans ibabuza.MultiRaftTransport,
	storage BootstrapStorage, factory ComponentsFactory, logger ibabuza.Logger) (*Node, error) {

	n := newNode(config, trans, storage, factory, logger)

	for _, groupID := range restartGroupIDs {
		replicaCluster := factory.CreateCluster()
		replicaStatus := status.New()
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

		applyResultSerializer, err := storage.OpenStateMachine(groupID, snap)
		if err != nil {
			return nil, err
		}
		//create session
		replicaSession := factory.CreateSessionManager()
		if err = replicaSession.SetResponseSerializer(applyResultSerializer); err != nil {
			return nil, err
		}
		if snap != nil {
			replicaStorage, err := storage.GetReplicaStorage(groupID)
			if err != nil {
				return nil, err
			}
			if err = replicaStorage.RestoreFromSnapshot(snap.Metadata.Index, false, replicaCluster,
				replicaSession); err != nil {
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
		appliedFacade := babuza.NewAppliedFacade(replicaStorage, firstCommitInTermNotifier, replicaSession,
			resultReplier, replicaCluster, n, trans, logger, metrics.NewMockMetricsCollector())

		r := &replica{
			raftGroup: RaftGroup{
				GroupID: groupID,
				PeerID:  config.NodeID,
			},
			config:                    replicaRaftConfig,
			cluster:                   replicaCluster,
			transport:                 trans,
			status:                    replicaStatus,
			session:                   replicaSession,
			storage:                   replicaStorage,
			appliedFacade:             appliedFacade,
			rawNode:                   rawNode,
			idGenerator:               idgenerator.New(replicaCluster.LocalPeerID(), uint64(time.Now().Nanosecond())),
			resultReplier:             resultReplier,
			completionReplier:         replier.NewCompletion(),
			firstCommitInTermNotifier: firstCommitInTermNotifier,
			leaderChangeNotifier:      syncutil.NewNotifier(),
			leaderCh:                  nil,
			replicaEventCh:            n.replicaEventCh,
			scheduler:                 n.scheduler,
			applyJobQueue:             newJobQueue(groupID, n.config.JobQueueSize, n.logger),
			requestQueue:              newReplicaRequestQueue(),
			logger:                    logger,
			closer:                    syncutil.NewCloser(),
		}
		n.replicaSet.replica[groupID] = r
	}
	return n, nil
}

func newNode(config NodeConfig, trans ibabuza.MultiRaftTransport, storage BootstrapStorage, factory ComponentsFactory, logger ibabuza.Logger) *Node {
	n := &Node{
		config:         config,
		trans:          trans,
		storage:        storage,
		factory:        factory,
		logger:         logger,
		closer:         syncutil.NewCloser(),
		replicaEventCh: make(chan replicaEvent, 8),
	}
	scheduler := newScheduler(config.NodeID, schedulerConfig{
		shardNum:       config.SchedulerShardNum,
		shardWorkerNum: config.SchedulerShardWorkerNum,
		queueSize:      config.SchedulerQueueSize,
		maxTicks:       config.SchedulerMaxTicks,
	}, n, logger)
	n.scheduler = scheduler
	n.replicaSet.replica = make(map[ibabuza.RaftGroupID]*replica)
	return n
}

func bootstrapReplicaWithConfiguration(node *Node, groupID ibabuza.RaftGroupID, configuration *babuza.PeersConfiguration,
	joinExistingRaftGroup bool) (*replica, error) {

	replicaCluster := node.factory.CreateCluster()
	replicaCluster.SetClusterID(uint64(groupID))      // clusterID is the same as group id
	replicaCluster.SetLocalPeerID(node.config.NodeID) // localPeerID is the same as node id
	replicaStatus := status.New()

	replicaSession := node.factory.CreateSessionManager()

	responseSerializer, err := node.storage.CreateStateMachine(groupID)
	if err != nil {
		return nil, err
	}
	if err = replicaSession.SetResponseSerializer(responseSerializer); err != nil {
		return nil, err
	}
	var peers []raft.Peer
	if err = configuration.Validate(); err != nil {
		return nil, err
	}
	for _, raftPeerAttr := range configuration.RaftPeersAttribute() {
		if raftPeerAttr.Id != node.config.NodeID {
			node.trans.AddPeer(raftPeerAttr.Id, raftPeerAttr.RaftListenAddr)
		}
	}
	if joinExistingRaftGroup {
		client, err := node.trans.CreateTransportClient()
		if err != nil {
			return nil, err
		}
		if err = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			return configuration.MatchRemoteCluster(ctx, uint64(groupID), node.config.NodeID, client)
		}(); err != nil {
			return nil, err
		}
	}
	if err = node.storage.CreateWal(groupID, babuzapb.WalMetadata{
		ClusterID:   uint64(groupID),
		LocalPeerID: node.config.NodeID,
	}); err != nil {
		return nil, err
	}
	if node.config.EnableWalNoSync {
		err = node.storage.SetWalNoFSync(groupID)
		if err != nil {
			return nil, err
		}
	}
	entryStorage, err := node.storage.GetEntryStorage(groupID)
	if err != nil {
		return nil, err
	}
	replicaRaftConfig := ReplicaRaftConfig{
		EnableWalNoSync:     node.config.EnableWalNoSync,
		SnapshotCount:       node.config.SnapshotCount,
		RaftConfig:          node.config.RaftConfig,
		LearnerReadyPercent: node.config.LearnerReadyPercent,
	}

	raftCfg := replicaRaftConfig.convertToRaftConfig(node.config.NodeID, node.logger, entryStorage)
	rawNode, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, err
	}
	// joinExistingRaftGroup logic:
	// When true: If the local peer ID is in the voting config, it joins as a voting peer.
	//            Otherwise, it joins as a learner.
	// When false: It indicates starting a new raft group from scratch.
	if !joinExistingRaftGroup {
		// as a learner will not bootstrap
		peers, err = configuration.ToRaftPeers()
		if err != nil {
			return nil, err
		}
		if err = rawNode.Bootstrap(peers); err != nil {
			return nil, err
		}
	}
	firstCommitInTermNotifier := syncutil.NewNotifier()
	resultReplier := replier.NewResult[ibabuza.ApplyResult]()
	replicaStorage, err := node.storage.GetReplicaStorage(groupID)
	if err != nil {
		return nil, err
	}
	appliedFacade := babuza.NewAppliedFacade(replicaStorage, firstCommitInTermNotifier, replicaSession,
		resultReplier, replicaCluster, node, node.trans, node.logger, metrics.NewMockMetricsCollector())

	r := &replica{
		raftGroup: RaftGroup{
			GroupID: groupID,
			PeerID:  node.config.NodeID,
		},
		config:                    replicaRaftConfig,
		cluster:                   replicaCluster,
		transport:                 node.trans,
		status:                    replicaStatus,
		session:                   replicaSession,
		storage:                   replicaStorage,
		appliedFacade:             appliedFacade,
		rawNode:                   rawNode,
		idGenerator:               idgenerator.New(replicaCluster.LocalPeerID(), uint64(time.Now().Nanosecond())),
		resultReplier:             resultReplier,
		completionReplier:         replier.NewCompletion(),
		firstCommitInTermNotifier: firstCommitInTermNotifier,
		leaderChangeNotifier:      syncutil.NewNotifier(),
		leaderCh:                  nil,
		replicaEventCh:            node.replicaEventCh,
		scheduler:                 node.scheduler,
		applyJobQueue:             newJobQueue(groupID, node.config.JobQueueSize, node.logger),
		requestQueue:              newReplicaRequestQueue(),
		logger:                    node.logger,
		closer:                    syncutil.NewCloser(),
	}
	return r, nil
}
