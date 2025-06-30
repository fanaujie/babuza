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


package experimental

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

func setupStateMachineAndSession(factory ComponentsFactory, stateMachineRootDir string, groupID ibabuza.RaftGroupID, replicaSession ibabuza.SessionManager) (ibabuza.BaseStateMachine, *babuza.BasedStateMachineInfo, error) {
	basedStateMachine, err := factory.CreateStateMachine(stateMachineRootDir, groupID)
	if err != nil {
		return nil, nil, err
	}
	bsmInfo, err := babuza.NewBasedStateMachineInfo(basedStateMachine)
	if err != nil {
		return nil, nil, err
	}
	var responseSerializer ibabuza.ResponseSerializer
	if bsmInfo.SupportSession() {
		responseSerializer = basedStateMachine.(ibabuza.SessionEnabledStateMachine).GetResponseSerializer()
	}
	if err = replicaSession.SetResponseSerializer(responseSerializer); err != nil {
		return nil, nil, err
	}
	return basedStateMachine, bsmInfo, nil
}

func BootstrapOrRecoverStore(cfg StoreConfig, factory ComponentsFactory, trans ibabuza.MultiRaftTransport, walManager ibabuza.MultiRaftWalManager,
	snapshotManager ibabuza.MultiRaftSnapshotManager, raftListener ibabuza.MultiRaftListener) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	logger := factory.GetLogger()
	if err := trans.SetupTransportConfig(ibabuza.TransportConfig{
		LocalNodeID: cfg.StoreID,
		PeerAddress: cfg.RaftListenAddress,
		TLSConfig:   cfg.TLSConfig,
	}); err != nil {
		return nil, err
	}
	if groupIDs, err := walManager.HasExistingWals(); err != nil {
		return nil, err
	} else if len(groupIDs) > 0 {
		installSnapshots, err := snapshotManager.ScanInstalledSnapshots(groupIDs, true)
		if err != nil {
			return nil, err
		}
		return restartStore(cfg, groupIDs, trans, walManager, snapshotManager, installSnapshots, factory, logger, raftListener)

	}
	return startStore(cfg, trans, walManager, snapshotManager, factory, logger, raftListener)
}

func startStore(config StoreConfig, trans ibabuza.MultiRaftTransport, walManager ibabuza.MultiRaftWalManager,
	snapManager ibabuza.MultiRaftSnapshotManager, factory ComponentsFactory, logger ibabuza.Logger,
	raftListener ibabuza.MultiRaftListener) (*Store, error) {
	return newStore(config, trans, walManager, snapManager, factory, logger, raftListener), nil
}

func restartStore(config StoreConfig, restartGroupIDs []ibabuza.RaftGroupID, trans ibabuza.MultiRaftTransport,
	walManager ibabuza.MultiRaftWalManager, snapManager ibabuza.MultiRaftSnapshotManager, installSnapshots map[ibabuza.RaftGroupID]ibabuza.SnapshotManager,
	factory ComponentsFactory, logger ibabuza.Logger, raftListener ibabuza.MultiRaftListener) (*Store, error) {

	n := newStore(config, trans, walManager, snapManager, factory, logger, raftListener)

	for _, groupID := range restartGroupIDs {
		replicaCluster := factory.CreateCluster()
		replicaStatus := status.New()
		walSnapshots, err := walManager.FindSnapshot(groupID)
		if err != nil {
			return nil, err
		}
		installSnap, ok := installSnapshots[groupID]
		if !ok {
			return nil, fmt.Errorf("no installed snapshot found for groupID %d", groupID)
		}
		snapshot, err := installSnap.LoadLastValidSnapshot(walSnapshots)
		if err != nil {
			return nil, err
		}
		walReplayResult, entryStorage, wal, err := walManager.ReplayWal(groupID, snapshot, false)
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
		hs, _, err := entryStorage.InitialState()
		if err != nil {
			return nil, err
		}
		replicaStatus.SetHardStateTerm(hs.Term)
		replicaStatus.SetCommittedIndex(hs.Commit)

		//create session
		replicaSession := factory.CreateSessionManager()
		stateMachineRootDir := filepath.Join(config.StoreHostDir, stateMachineDir)
		basedStateMachine, bsmInfo, err := setupStateMachineAndSession(factory, stateMachineRootDir, groupID, replicaSession)
		if err != nil {
			return nil, err
		}
		restoreStateMachine := func() error {
			reader, err := installSnap.CreateInstalledSnapshotReader(snapshot.Metadata.Index, false)
			if err != nil {
				return err
			}
			if err = basedStateMachine.RestoreFromSnapshot(reader); err != nil {
				return err
			}
			bsmInfo.SetOpenAppliedIndex(snapshot.Metadata.Index)
			return nil
		}

		if bsmInfo.IsDiskType() {
			diskAppliedIndex, rebuildStateMachine := basedStateMachine.(ibabuza.DiskStateMachine).Open()
			if rebuildStateMachine {
				if snapshot != nil {
					if err = restoreStateMachine(); err != nil {
						return nil, err
					}
				}
				bsmInfo.SetOpenAppliedIndex(diskAppliedIndex)
			} else {
				if snapshot != nil && diskAppliedIndex < snapshot.Metadata.Index {
					return nil, fmt.Errorf("storage: on disk applied index (%d) is less than snapshot index (%d)",
						diskAppliedIndex, snapshot.Metadata.Index)

				}
				// open state machine is successful
				bsmInfo.SetOpenAppliedIndex(diskAppliedIndex)
			}
		} else {
			if snapshot != nil {
				if err = restoreStateMachine(); err != nil {
					return nil, err
				}
			}
		}

		replicaStorage := babuza.NewRaftStorage(installSnap, wal, entryStorage, basedStateMachine, bsmInfo)
		if snapshot != nil {
			if err = replicaStorage.RestoreFromSnapshot(snapshot.Metadata.Index, false, replicaCluster,
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
			replicaStatus.SetAppliedIndex(snapshot.Metadata.Index)
			replicaStatus.SetAppliedTerm(snapshot.Metadata.Term)
			replicaStatus.SetSnapshotIndex(snapshot.Metadata.Index)
			replicaStatus.SetConfState(snapshot.Metadata.ConfState)
			// For disk-type state machines, if not rebuilt, set the status appliedIndex to openAppliedIndex
			if bsmInfo.OpenAppliedIndex() > snapshot.Metadata.Index {
				replicaStatus.SetAppliedIndex(snapshot.Metadata.Index)
			}
		}
		if config.EnableWalNoSync {
			wal.SetUnsafeNoFsync()
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
			resultReplier, replicaCluster, &callbackProcessor{n}, trans, nil, logger, metrics.NewMockMetricsCollector())

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
			applyJobQueue:             n.shardedJobQueue,
			enqueueStepFunc:           n.enqueueStep,
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
		n.requestQueues.Store(groupID, newReplicaRequestQueue())
		n.replicaSet.Store(groupID, r)
	}
	return n, nil
}

func newStore(config StoreConfig, trans ibabuza.MultiRaftTransport, walManager ibabuza.MultiRaftWalManager,
	snapManager ibabuza.MultiRaftSnapshotManager, factory ComponentsFactory,
	logger ibabuza.Logger, raftListener ibabuza.MultiRaftListener) *Store {
	closer := syncutil.NewCloser()
	s := &Store{
		config:          config,
		trans:           trans,
		walManager:      walManager,
		snapshotManager: snapManager,
		factory:         factory,
		logger:          logger,
		raftListener:    raftListener,
		closer:          closer,
		replicaSet:      xsync.NewMap[ibabuza.RaftGroupID, *replica](),
		requestQueues:   xsync.NewMap[ibabuza.RaftGroupID, *replicaRequestQueue](),
		coalescedHeartbeatQueue: &coalescedHeartbeatQueue{
			heartbeatMsg:               xsync.NewMap[string, *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]](),
			heartbeatRespMsg:           xsync.NewMap[string, *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]](),
			heartbeatLastActiveUnixSec: xsync.NewMap[string, int64](),
		},
		idGenerator:           idgenerator.New(config.StoreID, uint64(time.Now().Nanosecond())),
		leaderTransferChecker: newLeaderTransferChecker(config.LeaderTransferCheckerShardNum, time.Second, closer),
	}
	scheduler := newScheduler(config.StoreID, schedulerConfig{
		shardNum:       config.SchedulerShardNum,
		shardWorkerNum: config.SchedulerShardWorkerNum,
		queueSize:      config.SchedulerQueueSize,
		maxTicks:       config.SchedulerMaxTicks,
	}, &callbackProcessor{s}, logger)
	s.scheduler = scheduler
	s.shardedJobQueue = newShardedJobQueue(config.JobQueueShardNum, config.JobQueueSize, logger)
	s.raftEventPublisher = newRaftEventPublisher()
	return s
}

func bootstrapReplicaWithConfiguration(store *Store, configuration *PeersConfiguration,
	joinExistingRaftGroup bool) (*replica, error) {

	if err := configuration.Validate(); err != nil {
		return nil, err
	}
	var localPeerID uint64
	groupID := configuration.GroupID()
	_ = configuration.Visit(func(attribute babuzapb.RaftPeerAttribute) error {
		if attribute.RaftListenAddr == store.config.RaftListenAddress {
			localPeerID = attribute.PeerID
		}
		return nil
	})
	if localPeerID == 0 {
		return nil, fmt.Errorf("local peer ID not found in configuration: %v", configuration)
	}

	replicaCluster := store.factory.CreateCluster()
	replicaCluster.SetClusterID(store.config.ClusterID)
	replicaCluster.SetGroupID(groupID)
	replicaCluster.SetLocalPeerID(localPeerID)
	replicaStatus := status.New()

	replicaSession := store.factory.CreateSessionManager()
	stateMachineRootDir := filepath.Join(store.config.StoreHostDir, stateMachineDir)
	basedStateMachine, bsmInfo, err := setupStateMachineAndSession(store.factory, stateMachineRootDir, groupID, replicaSession)
	if err != nil {
		return nil, err
	}
	for _, raftPeerAttr := range configuration.RaftPeersAttribute() {
		if raftPeerAttr.PeerID != localPeerID {
			store.trans.AddPeer(groupID, raftPeerAttr.PeerID, raftPeerAttr.RaftListenAddr)
		}
	}
	if joinExistingRaftGroup {
		if err = func() error {
			client, err := store.trans.CreateTransportClient()
			if err != nil {
				return err
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			return configuration.MatchRemoteCluster(ctx, store.config.ClusterID, localPeerID,
				groupID, client)
		}(); err != nil {
			return nil, err
		}
	}
	entryStorage, wal, err := store.walManager.CreateWal(groupID, babuzapb.WalMetadata{
		ClusterID:   store.config.ClusterID,
		LocalPeerID: localPeerID,
		GroupID:     uint64(groupID),
	})
	if err != nil {
		return nil, err
	}
	if store.config.EnableWalNoSync {
		wal.SetUnsafeNoFsync()
	}
	replicaRaftConfig := ReplicaRaftConfig{
		EnableWalNoSync:              store.config.EnableWalNoSync,
		SnapshotCount:                store.config.SnapshotCount,
		RaftConfig:                   store.config.RaftConfig,
		LearnerReadyPercent:          store.config.LearnerReadyPercent,
		CoalescedHeartbeatQueueSize:  store.config.CoalescedHeartbeatQueueSize,
		LinearizedReadRetryTimeout:   store.config.LinearizedReadRetryTimeout,
		LinearizedReadRequestTimeout: store.config.LinearizedReadRequestTimeout,
	}

	raftCfg := replicaRaftConfig.convertToRaftConfig(localPeerID, store.logger, entryStorage)
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
	replicaStorage := babuza.NewRaftStorage(store.snapshotManager.CreateSnapshotManager(groupID), wal, entryStorage, basedStateMachine, bsmInfo)
	appliedFacade := babuza.NewAppliedFacade(firstCommitInTermNotifier, replicaSession,
		resultReplier, replicaCluster, &callbackProcessor{store}, store.trans, nil,
		store.logger, metrics.NewMockMetricsCollector())

	r := &replica{
		raftGroup: RaftGroup{
			GroupID: groupID,
			PeerID:  localPeerID,
		},
		config:                    replicaRaftConfig,
		cluster:                   replicaCluster,
		transport:                 store.trans,
		status:                    replicaStatus,
		sessionManager:            replicaSession,
		storage:                   replicaStorage,
		appliedFacade:             appliedFacade,
		idGenerator:               store.idGenerator,
		resultReplier:             resultReplier,
		completionReplier:         replier.NewCompletion(),
		firstCommitInTermNotifier: firstCommitInTermNotifier,
		leaderChangeNotifier:      syncutil.NewEventSignal(),
		linearizeReqNotifier:      syncutil.NewSignalManager(),
		raftEventPublisher:        store.raftEventPublisher,
		receivedSnapshotMsgCh:     make(chan babuzapb.SnapshotMessage, 8),
		readStateCh:               make(chan raft.ReadState, 1),
		readIndexCh:               make(chan struct{}, 1),
		scheduler:                 store.scheduler,
		applyJobQueue:             store.shardedJobQueue,
		enqueueStepFunc:           store.enqueueStep,
		coalescedHeartbeat:        store.coalescedHeartbeatQueue,
		mu: struct {
			lock        sync.Mutex
			rawNode     *raft.RawNode
			unreachable map[uint64]struct{}
		}{
			rawNode:     rawNode,
			unreachable: make(map[uint64]struct{}),
		},
		logger: store.logger,
		closer: syncutil.NewCloser(),
	}
	store.requestQueues.Store(groupID, newReplicaRequestQueue())
	return r, nil
}

func newReplicaWithoutConfiguration(store *Store, groupID ibabuza.RaftGroupID, localPeerID uint64) (*replica, error) {

	replicaCluster := store.factory.CreateCluster()
	replicaCluster.SetClusterID(store.config.ClusterID)
	replicaCluster.SetGroupID(groupID)
	replicaCluster.SetLocalPeerID(localPeerID)
	replicaStatus := status.New()

	replicaSession := store.factory.CreateSessionManager()
	stateMachineRootDir := filepath.Join(store.config.StoreHostDir, stateMachineDir)
	basedStateMachine, bsmInfo, err := setupStateMachineAndSession(store.factory, stateMachineRootDir, groupID, replicaSession)
	if err != nil {
		return nil, err
	}

	entryStorage, wal, err := store.walManager.CreateWal(groupID, babuzapb.WalMetadata{
		ClusterID:   store.config.ClusterID,
		LocalPeerID: localPeerID,
		GroupID:     uint64(groupID),
	})
	if err != nil {
		return nil, err
	}
	if store.config.EnableWalNoSync {
		wal.SetUnsafeNoFsync()
	}
	replicaRaftConfig := ReplicaRaftConfig{
		EnableWalNoSync:              store.config.EnableWalNoSync,
		SnapshotCount:                store.config.SnapshotCount,
		RaftConfig:                   store.config.RaftConfig,
		LearnerReadyPercent:          store.config.LearnerReadyPercent,
		CoalescedHeartbeatQueueSize:  store.config.CoalescedHeartbeatQueueSize,
		LinearizedReadRetryTimeout:   store.config.LinearizedReadRetryTimeout,
		LinearizedReadRequestTimeout: store.config.LinearizedReadRequestTimeout,
	}

	raftCfg := replicaRaftConfig.convertToRaftConfig(localPeerID, store.logger, entryStorage)
	rawNode, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, err
	}

	firstCommitInTermNotifier := syncutil.NewEventSignal()
	resultReplier := replier.NewResult[ibabuza.ApplyResult]()
	replicaStorage := babuza.NewRaftStorage(store.snapshotManager.CreateSnapshotManager(groupID), wal, entryStorage, basedStateMachine, bsmInfo)
	appliedFacade := babuza.NewAppliedFacade(firstCommitInTermNotifier, replicaSession,
		resultReplier, replicaCluster, &callbackProcessor{store}, store.trans, nil,
		store.logger, metrics.NewMockMetricsCollector())

	r := &replica{
		raftGroup: RaftGroup{
			GroupID: groupID,
			PeerID:  localPeerID,
		},
		config:                    replicaRaftConfig,
		cluster:                   replicaCluster,
		transport:                 store.trans,
		status:                    replicaStatus,
		sessionManager:            replicaSession,
		storage:                   replicaStorage,
		appliedFacade:             appliedFacade,
		idGenerator:               store.idGenerator,
		resultReplier:             resultReplier,
		completionReplier:         replier.NewCompletion(),
		firstCommitInTermNotifier: firstCommitInTermNotifier,
		leaderChangeNotifier:      syncutil.NewEventSignal(),
		linearizeReqNotifier:      syncutil.NewSignalManager(),
		raftEventPublisher:        store.raftEventPublisher,
		receivedSnapshotMsgCh:     make(chan babuzapb.SnapshotMessage, 8),
		readStateCh:               make(chan raft.ReadState, 1),
		readIndexCh:               make(chan struct{}, 1),
		scheduler:                 store.scheduler,
		applyJobQueue:             store.shardedJobQueue,
		enqueueStepFunc:           store.enqueueStep,
		coalescedHeartbeat:        store.coalescedHeartbeatQueue,
		mu: struct {
			lock        sync.Mutex
			rawNode     *raft.RawNode
			unreachable map[uint64]struct{}
		}{
			rawNode:     rawNode,
			unreachable: make(map[uint64]struct{}),
		},
		logger: store.logger,
		closer: syncutil.NewCloser(),
	}
	store.requestQueues.Store(groupID, newReplicaRequestQueue())
	return r, nil
}
