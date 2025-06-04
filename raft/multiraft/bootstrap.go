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
	"github.com/fanaujie/babuza/pkg/utility/queue"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/puzpuzpuz/xsync/v4"
	"go.etcd.io/etcd/raft/v3"
	"path/filepath"
	"sync"
	"time"
)

const (
	stateMachineDir = "state_machine"
)

type ComponentsFactory interface {
	CreateStateMachine(stateMachineRootDir string, groupID ibabuza.RaftGroupID) (ibabuza.BaseStateMachine, error)
	CreateCluster() ibabuza.Cluster
	CreateSessionManager() ibabuza.SessionManager
	GetLogger() ibabuza.Logger
}

func BootstrapOrRecoverNode(cfg NodeConfig, factory ComponentsFactory, trans ibabuza.MultiRaftTransport, walManager ibabuza.MultiRaftWalManager,
	snapshotManager ibabuza.MultiRaftSnapshotManager, raftListener ibabuza.MultiRaftListener) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	stateMachineRootDir := filepath.Join(cfg.NodeHostDir, stateMachineDir)
	logger := factory.GetLogger()
	storage := newBootstrapStorage(stateMachineRootDir, factory, walManager, snapshotManager, logger)
	if err := trans.SetupTransportConfig(ibabuza.TransportConfig{
		LocalNodeID: cfg.NodeID,
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
		return restartNode(cfg, groupIDs, trans, storage, factory, logger, raftListener)

	}
	return startNode(cfg, trans, storage, factory, logger, raftListener)
}

func startNode(config NodeConfig, trans ibabuza.MultiRaftTransport, storage BootstrapStorage, factory ComponentsFactory,
	logger ibabuza.Logger, raftListener ibabuza.MultiRaftListener) (*Node, error) {
	return newNode(config, trans, storage, factory, logger, raftListener), nil
}

func restartNode(config NodeConfig, restartGroupIDs []ibabuza.RaftGroupID, trans ibabuza.MultiRaftTransport,
	storage BootstrapStorage, factory ComponentsFactory, logger ibabuza.Logger, raftListener ibabuza.MultiRaftListener) (*Node, error) {

	n := newNode(config, trans, storage, factory, logger, raftListener)

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
		if metadata.ClusterID != config.ClusterID || metadata.GroupID != uint64(groupID) {
			return nil, fmt.Errorf("wal metadata clusterID %d, groupID %d not match with config clusterID %d, groupID %d",
				metadata.ClusterID, metadata.GroupID, config.ClusterID, groupID)
		}
		replicaCluster.SetClusterID(metadata.ClusterID)
		replicaCluster.SetLocalPeerID(metadata.LocalPeerID)
		replicaCluster.SetGroupID(groupID)
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
			if replicaCluster.ClusterID() != config.ClusterID || replicaCluster.GroupID() != groupID {
				return nil, fmt.Errorf("clusterID %d, groupID %d, not match with config clusterID %d, groupID %d",
					replicaCluster.ClusterID(), replicaCluster.GroupID(),
					config.ClusterID, groupID)
			}
			for _, p := range replicaCluster.Peers() {
				trans.AddPeer(groupID, p.RaftPeerAttr.PeerID, p.RaftPeerAttr.RaftListenAddr)
			}
			replicaStatus.SetAppliedIndex(snap.Metadata.Index)
			replicaStatus.SetAppliedTerm(snap.Metadata.Term)
			replicaStatus.SetSnapshotIndex(snap.Metadata.Index)
			replicaStatus.SetConfState(snap.Metadata.ConfState)
			// For disk-type state machines, if not rebuilt, set the status appliedIndex to openAppliedIndex
			if replicaStorage.GetBasedStateMachineInfo().OpenAppliedIndex() > snap.Metadata.Index {
				replicaStatus.SetAppliedIndex(snap.Metadata.Index)
			}
		}
		if config.EnableWalNoSync {
			_ = storage.SetWalNoFSync(groupID)
		}

		replicaStorage, err := storage.GetReplicaStorage(groupID)
		if err != nil {
			return nil, err
		}
		replicaRaftConfig := ReplicaRaftConfig{
			EnableWalNoSync:              config.EnableWalNoSync,
			SnapshotCount:                config.SnapshotCount,
			RaftConfig:                   config.RaftConfig,
			LearnerReadyPercent:          config.LearnerReadyPercent,
			CoalescedHeartbeatQueueSize:  config.CoalescedHeartbeatQueueSize,
			LinearizedReadRetryTimeout:   config.LinearizedReadRetryTimeout,
			LinearizedReadRequestTimeout: config.LinearizedReadRequestTimeout,
		}
		rawNode, err := raft.NewRawNode(replicaRaftConfig.convertToRaftConfig(replicaCluster.LocalPeerID(), logger, entryStorage))
		if err != nil {
			return nil, err
		}
		firstCommitInTermNotifier := syncutil.NewEventSignal()
		resultReplier := replier.NewResult[ibabuza.ApplyResult]()
		appliedFacade := babuza.NewAppliedFacade(firstCommitInTermNotifier, replicaSession,
			resultReplier, replicaCluster, n, trans, nil, logger, metrics.NewMockMetricsCollector())

		r := &replica{
			raftGroup: RaftGroup{
				GroupID: groupID,
				PeerID:  metadata.LocalPeerID,
			},
			config:                    replicaRaftConfig,
			cluster:                   replicaCluster,
			transport:                 trans,
			status:                    replicaStatus,
			sessionManager:            replicaSession,
			storage:                   replicaStorage,
			appliedFacade:             appliedFacade,
			idGenerator:               n.idGenerator,
			resultReplier:             resultReplier,
			completionReplier:         replier.NewCompletion(),
			firstCommitInTermNotifier: firstCommitInTermNotifier,
			leaderChangeNotifier:      syncutil.NewEventSignal(),
			linearizeReqNotifier:      syncutil.NewSignalManager(),
			raftEventPublisher:        n.raftEventPublisher,
			receivedSnapshotMsgCh:     make(chan babuzapb.SnapshotMessage, 8),
			readStateCh:               make(chan raft.ReadState, 1),
			readIndexCh:               make(chan struct{}, 1),
			scheduler:                 n.scheduler,
			applyJobQueue:             newJobQueue(groupID, n.config.JobQueueSize, n.logger),
			requestQueue:              newReplicaRequestQueue(),
			coalescedHeartbeat:        n.coalescedHeartbeatQueue,
			mu: struct {
				lock        sync.Mutex
				rawNode     *raft.RawNode
				unreachable map[uint64]struct{}
			}{
				rawNode:     rawNode,
				unreachable: make(map[uint64]struct{}),
			},
			logger: logger,
			closer: syncutil.NewCloser(),
		}
		n.replicaSet.Store(groupID, r)
	}
	return n, nil
}

func newNode(config NodeConfig, trans ibabuza.MultiRaftTransport, storage BootstrapStorage, factory ComponentsFactory,
	logger ibabuza.Logger, raftListener ibabuza.MultiRaftListener) *Node {
	n := &Node{
		config:       config,
		trans:        trans,
		storage:      storage,
		factory:      factory,
		logger:       logger,
		raftListener: raftListener,
		closer:       syncutil.NewCloser(),
		replicaSet:   xsync.NewMap[ibabuza.RaftGroupID, *replica](),
		coalescedHeartbeatQueue: &coalescedHeartbeatQueue{
			heartbeatMsg:               xsync.NewMap[string, *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]](),
			heartbeatRespMsg:           xsync.NewMap[string, *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]](),
			heartbeatLastActiveUnixSec: xsync.NewMap[string, int64](),
		},
		idGenerator: idgenerator.New(config.NodeID, uint64(time.Now().Nanosecond())),
	}
	scheduler := newScheduler(config.NodeID, schedulerConfig{
		shardNum:       config.SchedulerShardNum,
		shardWorkerNum: config.SchedulerShardWorkerNum,
		queueSize:      config.SchedulerQueueSize,
		maxTicks:       config.SchedulerMaxTicks,
	}, n, logger)
	n.scheduler = scheduler
	n.raftEventPublisher = newRaftEventPublisher()
	return n
}

func bootstrapReplicaWithConfiguration(node *Node, configuration *PeersConfiguration,
	joinExistingRaftGroup bool) (*replica, error) {

	if err := configuration.Validate(); err != nil {
		return nil, err
	}
	var localPeerID uint64
	groupID := configuration.GroupID()
	_ = configuration.Visit(func(attribute babuzapb.RaftPeerAttribute) error {
		if attribute.RaftListenAddr == node.config.RaftListenAddress {
			localPeerID = attribute.PeerID
		}
		return nil
	})
	if localPeerID == 0 {
		return nil, fmt.Errorf("local peer ID not found in configuration: %v", configuration)
	}

	replicaCluster := node.factory.CreateCluster()
	replicaCluster.SetClusterID(node.config.ClusterID)
	replicaCluster.SetGroupID(groupID)
	replicaCluster.SetLocalPeerID(localPeerID)
	replicaStatus := status.New()

	replicaSession := node.factory.CreateSessionManager()

	responseSerializer, err := node.storage.CreateStateMachine(groupID)
	if err != nil {
		return nil, err
	}
	if err = replicaSession.SetResponseSerializer(responseSerializer); err != nil {
		return nil, err
	}
	for _, raftPeerAttr := range configuration.RaftPeersAttribute() {
		if raftPeerAttr.PeerID != localPeerID {
			node.trans.AddPeer(groupID, raftPeerAttr.PeerID, raftPeerAttr.RaftListenAddr)
		}
	}
	if joinExistingRaftGroup {
		if err = func() error {
			client, err := node.trans.CreateTransportClient()
			if err != nil {
				return err
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			return configuration.MatchRemoteCluster(ctx, node.config.ClusterID, localPeerID,
				groupID, client)
		}(); err != nil {
			return nil, err
		}
	}
	if err = node.storage.CreateWal(groupID, babuzapb.WalMetadata{
		ClusterID:   node.config.ClusterID,
		LocalPeerID: localPeerID,
		GroupID:     uint64(groupID),
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
		EnableWalNoSync:              node.config.EnableWalNoSync,
		SnapshotCount:                node.config.SnapshotCount,
		RaftConfig:                   node.config.RaftConfig,
		LearnerReadyPercent:          node.config.LearnerReadyPercent,
		CoalescedHeartbeatQueueSize:  node.config.CoalescedHeartbeatQueueSize,
		LinearizedReadRetryTimeout:   node.config.LinearizedReadRetryTimeout,
		LinearizedReadRequestTimeout: node.config.LinearizedReadRequestTimeout,
	}

	raftCfg := replicaRaftConfig.convertToRaftConfig(localPeerID, node.logger, entryStorage)
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
		peers, err := configuration.ToRaftPeers()
		if err != nil {
			return nil, err
		}
		if err = rawNode.Bootstrap(peers); err != nil {
			return nil, err
		}
	}
	firstCommitInTermNotifier := syncutil.NewEventSignal()
	resultReplier := replier.NewResult[ibabuza.ApplyResult]()
	replicaStorage, err := node.storage.GetReplicaStorage(groupID)
	if err != nil {
		return nil, err
	}
	appliedFacade := babuza.NewAppliedFacade(firstCommitInTermNotifier, replicaSession,
		resultReplier, replicaCluster, node, node.trans, nil, node.logger, metrics.NewMockMetricsCollector())

	r := &replica{
		raftGroup: RaftGroup{
			GroupID: groupID,
			PeerID:  localPeerID,
		},
		config:                    replicaRaftConfig,
		cluster:                   replicaCluster,
		transport:                 node.trans,
		status:                    replicaStatus,
		sessionManager:            replicaSession,
		storage:                   replicaStorage,
		appliedFacade:             appliedFacade,
		idGenerator:               node.idGenerator,
		resultReplier:             resultReplier,
		completionReplier:         replier.NewCompletion(),
		firstCommitInTermNotifier: firstCommitInTermNotifier,
		leaderChangeNotifier:      syncutil.NewEventSignal(),
		linearizeReqNotifier:      syncutil.NewSignalManager(),
		raftEventPublisher:        node.raftEventPublisher,
		receivedSnapshotMsgCh:     make(chan babuzapb.SnapshotMessage, 8),
		readStateCh:               make(chan raft.ReadState, 1),
		readIndexCh:               make(chan struct{}, 1),
		scheduler:                 node.scheduler,
		applyJobQueue:             newJobQueue(groupID, node.config.JobQueueSize, node.logger),
		requestQueue:              newReplicaRequestQueue(),
		coalescedHeartbeat:        node.coalescedHeartbeatQueue,
		mu: struct {
			lock        sync.Mutex
			rawNode     *raft.RawNode
			unreachable map[uint64]struct{}
		}{
			rawNode:     rawNode,
			unreachable: make(map[uint64]struct{}),
		},
		logger: node.logger,
		closer: syncutil.NewCloser(),
	}
	return r, nil
}
