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

package raft

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/status"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type BootstrapRaftCluster struct {
	cluster          ibabuza.Cluster
	storage          RaftStorage
	node             raft.Node
	walManager       ibabuza.WalManager
	snapshotManager  ibabuza.SnapshotManager
	sessionMgr       ibabuza.SessionManager
	trans            ibabuza.Transport
	status           ibabuza.Status
	logger           ibabuza.Logger
	metricsCollector ibabuza.MetricsCollector
}

func NewBootstrapRaftCluster(cfg BabuzaConfig, votingPeersConfig PeersConfiguration, stateMachine ibabuza.BaseStateMachine,
	cluster ibabuza.Cluster, raftNode ibabuza.RaftNode, sessions ibabuza.SessionManager, snapshotManager ibabuza.SnapshotManager,
	walManager ibabuza.WalManager, trans ibabuza.Transport, logger ibabuza.Logger, metricsController ibabuza.MetricsCollector) (*BootstrapRaftCluster, error) {

	bsmInfo, err := NewBasedStateMachineInfo(stateMachine)
	if err != nil {
		return nil, err
	}
	if bsmInfo.supportSession {
		responseSerializer := stateMachine.(ibabuza.SessionEnabledStateMachine).GetResponseSerializer()
		if err = sessions.SetResponseSerializer(responseSerializer); err != nil {
			return nil, err
		}
	}

	if err = trans.SetupTransportConfig(ibabuza.TransportConfig{
		LocalNodeID: cfg.LocalPeerID,
		PeerAddress: cfg.RaftListenAddress,
		TLSConfig:   cfg.TLSConfig,
	}); err != nil {
		return nil, err
	}
	cluster.SetClusterID(cfg.ClusterID)
	cluster.SetLocalPeerID(cfg.LocalPeerID)
	raftStatus := status.New()
	var node raft.Node
	var storage RaftStorage
	if exist, err := walManager.HasExistingWals(); err != nil {
		return nil, err
	} else if exist {
		if err = snapshotManager.ScanInstalledSnapshots(true); err != nil {
			return nil, err
		}
		node, storage, err = restartNode(cfg, raftNode, cluster, sessions, stateMachine, bsmInfo, walManager, snapshotManager, trans, raftStatus, logger)
		if err != nil {
			return nil, err
		}
	} else {
		node, storage, err = startNode(cfg, votingPeersConfig, raftNode, stateMachine, bsmInfo, walManager, snapshotManager, trans, logger)
		if err != nil {
			return nil, err
		}
	}
	return &BootstrapRaftCluster{
		cluster:          cluster,
		storage:          storage,
		node:             node,
		walManager:       walManager,
		snapshotManager:  snapshotManager,
		sessionMgr:       sessions,
		trans:            trans,
		status:           raftStatus,
		logger:           logger,
		metricsCollector: metricsController,
	}, nil
}

func RecoverAsStandalone(
	cfg BabuzaConfig,
	stateMachine ibabuza.BaseStateMachine,
	cluster ibabuza.Cluster,
	raftNode ibabuza.RaftNode,
	sessions ibabuza.SessionManager,
	snapshotManager ibabuza.SnapshotManager,
	walManager ibabuza.WalManager,
	trans ibabuza.Transport,
	logger ibabuza.Logger,
	metricsController ibabuza.MetricsCollector,
) (*BootstrapRaftCluster, error) {

	bsmInfo, err := NewBasedStateMachineInfo(stateMachine)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: failed to create state machine info: %w", err)
	}

	if bsmInfo.supportSession {
		responseSerializer := stateMachine.(ibabuza.SessionEnabledStateMachine).GetResponseSerializer()
		if err = sessions.SetResponseSerializer(responseSerializer); err != nil {
			return nil, fmt.Errorf("bootstrap: failed to set response serializer: %w", err)
		}
	}

	if err = trans.SetupTransportConfig(ibabuza.TransportConfig{
		LocalNodeID: cfg.LocalPeerID,
		PeerAddress: cfg.RaftListenAddress,
		TLSConfig:   cfg.TLSConfig,
	}); err != nil {
		return nil, fmt.Errorf("bootstrap: failed to setup transport: %w", err)
	}

	cluster.SetClusterID(cfg.ClusterID)
	cluster.SetLocalPeerID(cfg.LocalPeerID)

	exist, err := walManager.HasExistingWals()
	if err != nil {
		return nil, fmt.Errorf("bootstrap: failed to check WAL existence: %w", err)
	}
	if !exist {
		return nil, fmt.Errorf("bootstrap: cannot recover as standalone - no existing WAL found")
	}

	if err = snapshotManager.ScanInstalledSnapshots(true); err != nil {
		return nil, fmt.Errorf("bootstrap: failed to scan snapshots: %w", err)
	}

	snap, err := loadWALSnapshot(walManager, snapshotManager)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: failed to load snapshot: %w", err)
	}

	walReplayResult, entryStorage, wal, err := replayWALAndSetup(cfg, walManager, snap, true)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: failed to replay WAL: %w", err)
	}

	hardState := walReplayResult.HardState()
	if raft.IsEmptyHardState(hardState) {
		return nil, fmt.Errorf("bootstrap: WAL replay returned empty hardState")
	}

	logger.Infof("RecoverAsStandalone: replayed WAL (term=%d, commit=%d), uncommitted entries discarded",
		hardState.Term, hardState.Commit)

	nodeIds, err := listRaftConfChangeAddNodeIds(snap, walReplayResult)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: failed to list node IDs: %w", err)
	}

	logger.Infof("RecoverAsStandalone: found existing node IDs: %v, forcing standalone mode for node %d",
		nodeIds, cfg.LocalPeerID)

	//// Build the ConfState from WAL replay result by examining config change entries
	//confState := raftpb.ConfState{}
	//if snap != nil {
	//	confState = snap.Metadata.ConfState
	//}
	//// Apply config changes from WAL replay to get the final ConfState
	//if err = walReplayResult.ForEachConfChangeEntries(func(e raftpb.Entry) error {
	//	var cc raftpb.ConfChange
	//	if unmarshalErr := cc.Unmarshal(e.Data); unmarshalErr != nil {
	//		return unmarshalErr
	//	}
	//	confState = applyConfChange(confState, cc)
	//	return nil
	//}); err != nil {
	//	return nil, fmt.Errorf("bootstrap: failed to compute ConfState: %w", err)
	//}
	//
	//// Create a snapshot at the current commit index to preserve all existing state
	//// This ensures that when Raft restarts, it won't try to reapply old entries
	//compactIndex := hardState.Commit
	//recoverySnapshot, err := entryStorage.CreateSnapshot(compactIndex, &confState, nil)
	//if err != nil {
	//	return nil, fmt.Errorf("bootstrap: failed to create recovery snapshot: %w", err)
	//}
	//logger.Infof("RecoverAsStandalone: created snapshot at index %d to preserve existing state", compactIndex)
	//
	//// Compact all entries up to the snapshot index
	//// This removes old config change entries that should not be replayed
	//if err = entryStorage.Compact(compactIndex); err != nil {
	//	return nil, fmt.Errorf("bootstrap: failed to compact entries: %w", err)
	//}
	//logger.Infof("RecoverAsStandalone: compacted entries up to index %d", compactIndex)

	// Now create standalone config entries starting from commit+1
	standaloneEntries, err := createRaftConfigChangeEntries(
		cfg.LocalPeerID,
		cfg.RaftListenAddress,
		nodeIds,
		&hardState,
	)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: failed to create standalone config entries: %w", err)
	}

	logger.Infof("RecoverAsStandalone: created %d config change entries for standalone mode",
		len(standaloneEntries))
	if err = wal.Save(raftpb.HardState{}, standaloneEntries); err != nil {
		return nil, fmt.Errorf("bootstrap: failed to save standalone config to WAL: %w", err)
	}
	if err = wal.Sync(); err != nil {
		return nil, fmt.Errorf("bootstrap: failed to sync WAL: %w", err)
	}
	logger.Infof("RecoverAsStandalone: persisted standalone configuration to WAL")
	if err = entryStorage.SetHardState(hardState); err != nil {
		return nil, fmt.Errorf("bootstrap: failed to set hardState: %w", err)
	}
	if len(standaloneEntries) > 0 {
		if err = entryStorage.Append(standaloneEntries); err != nil {
			return nil, fmt.Errorf("bootstrap: failed to append standalone entries: %w", err)
		}
		logger.Infof("RecoverAsStandalone: appended %d standalone config entries", len(standaloneEntries))
	}

	storage := createRaftStorage(snapshotManager, wal, entryStorage, stateMachine, bsmInfo)

	// Restore state machine from the original snapshot (if it exists)
	// This ensures the state machine has all data up to the commit point
	if snap != nil {
		if bsmInfo.diskType {
			if err = handleDiskStateMachine(stateMachine, bsmInfo, snap, snapshotManager); err != nil {
				return nil, err
			}
		} else {
			if err = handleMemoryStateMachine(stateMachine, bsmInfo, snap, snapshotManager); err != nil {
				return nil, err
			}
		}

		// Restore cluster and session state from the original snapshot
		if err = storage.RestoreFromSnapshot(snap.Metadata.Index, false, cluster, sessions); err != nil {
			return nil, fmt.Errorf("bootstrap: failed to restore cluster/session state: %w", err)
		}

		logger.Infof("RecoverAsStandalone: restored state machine and components from snapshot")
	}

	raftStatus := status.New()
	// Use the recovery snapshot for status initialization (applied index = commit index)
	initializeStatusFromSnapshot(raftStatus, snap, hardState, bsmInfo)

	node, err := restartRaftNode(cfg, raftNode, logger, entryStorage)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: failed to restart Raft node: %w", err)
	}

	logger.Infof("RecoverAsStandalone: successfully restarted as standalone node (cluster=%d, peer=%d, commit=%d)",
		cfg.ClusterID, cfg.LocalPeerID, hardState.Commit)

	return &BootstrapRaftCluster{
		cluster:          cluster,
		storage:          storage,
		node:             node,
		walManager:       walManager,
		snapshotManager:  snapshotManager,
		sessionMgr:       sessions,
		trans:            trans,
		status:           raftStatus,
		logger:           logger,
		metricsCollector: metricsController,
	}, nil
}

func startNode(cfg BabuzaConfig, configuration PeersConfiguration, raftNode ibabuza.RaftNode,
	stateMachine ibabuza.BaseStateMachine, bsmInfo *BasedStateMachineInfo, walManager ibabuza.WalManager, snapshotManager ibabuza.SnapshotManager, trans ibabuza.Transport, logger ibabuza.Logger) (raft.Node, RaftStorage, error) {
	var peers []raft.Peer
	var err error

	if err = configuration.Validate(); err != nil {
		return nil, nil, err
	}
	for _, raftPeerAttr := range configuration.RaftPeersAttribute() {
		if raftPeerAttr.PeerID != cfg.LocalPeerID {
			trans.AddPeer(raftPeerAttr.PeerID, raftPeerAttr.RaftListenAddr)
		}
	}
	if cfg.Join {
		if err = func() error {
			client, err := trans.CreateTransportClient()
			if err != nil {
				return err
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			return configuration.MatchRemoteCluster(ctx, cfg.ClusterID, cfg.LocalPeerID, 0, client)
		}(); err != nil {
			return nil, nil, err
		}
	}
	entryStorage, wal, err := walManager.CreateWal(babuzapb.WalMetadata{
		ClusterID:   cfg.ClusterID,
		LocalPeerID: cfg.LocalPeerID,
	})
	if err != nil {
		return nil, nil, err
	}
	if cfg.EnableWalNoSync {
		wal.SetUnsafeNoFsync()
	}
	raftCfg := cfg.convertToRaftConfig(logger, entryStorage)
	// Join logic:
	// When true: If the local peer ID is in the voting config, it joins as a voting peer.
	//            Otherwise, it joins as a learner.
	// When false: It indicates starting a new raft group from scratch.
	if cfg.Join {
		// as a learner node will not start raft node
		node, err := raftNode.Restart(raftCfg)
		if err != nil {
			return nil, nil, err
		}
		storage := &raftStorage{
			snapshotor:   snapshotManager,
			wal:          wal,
			entryStorage: entryStorage,
			stateMachine: stateMachine,
			bsmInfo:      bsmInfo,
		}
		return node, storage, nil
	}
	peers, err = configuration.ToRaftPeers()
	if err != nil {
		return nil, nil, err
	}

	node, err := raftNode.Start(raftCfg, peers)
	if err != nil {
		return nil, nil, err
	}
	storage := &raftStorage{
		snapshotor:   snapshotManager,
		wal:          wal,
		entryStorage: entryStorage,
		stateMachine: stateMachine,
		bsmInfo:      bsmInfo,
	}
	return node, storage, nil
}

func restartNode(cfg BabuzaConfig, raftNode ibabuza.RaftNode, cluster ibabuza.Cluster, sessions ibabuza.SessionManager,
	stateMachine ibabuza.BaseStateMachine, bsmInfo *BasedStateMachineInfo, walManager ibabuza.WalManager,
	snapshotManager ibabuza.SnapshotManager, trans ibabuza.Transport, status ibabuza.Status, logger ibabuza.Logger) (raft.Node, RaftStorage, error) {

	//TODO: verify snap and wal match
	snap, err := loadWALSnapshot(walManager, snapshotManager)
	if err != nil {
		return nil, nil, err
	}
	_, entryStorage, wal, err := replayWALAndSetup(cfg, walManager, snap, false)
	if err != nil {
		return nil, nil, err
	}
	cluster.SetClusterID(cfg.ClusterID)
	cluster.SetLocalPeerID(cfg.LocalPeerID)
	hs, _, err := entryStorage.InitialState()
	if err != nil {
		return nil, nil, err
	}
	if bsmInfo.diskType {
		if err = handleDiskStateMachine(stateMachine, bsmInfo, snap, snapshotManager); err != nil {
			return nil, nil, err
		}
	} else {
		if err = handleMemoryStateMachine(stateMachine, bsmInfo, snap, snapshotManager); err != nil {
			return nil, nil, err
		}
	}
	storage := createRaftStorage(snapshotManager, wal, entryStorage, stateMachine, bsmInfo)
	if snap != nil {
		if err = storage.RestoreFromSnapshot(snap.Metadata.Index, false, cluster, sessions); err != nil {
			return nil, nil, err
		}
		if cluster.ClusterID() != cfg.ClusterID || cluster.LocalPeerID() != cfg.LocalPeerID {
			return nil, nil, fmt.Errorf("bootstrap: mistach configuration (clusterID=%d, restore clusterID=%d, localPeerID=%d, restore localPeerID=%d) ",
				cfg.ClusterID, cluster.ClusterID(), cfg.LocalPeerID, cluster.LocalPeerID())
		}
		for _, p := range cluster.Peers() {
			trans.AddPeer(p.RaftPeerAttr.PeerID, p.RaftPeerAttr.RaftListenAddr)
		}
	}
	initializeStatusFromSnapshot(status, snap, hs, bsmInfo)
	node, err := restartRaftNode(cfg, raftNode, logger, entryStorage)
	if err != nil {
		return nil, nil, err
	}
	return node, storage, nil
}

func applyConfChange(cs raftpb.ConfState, cc raftpb.ConfChange) raftpb.ConfState {
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		cs.Voters = append(cs.Voters, cc.NodeID)
	case raftpb.ConfChangeAddLearnerNode:
		cs.Learners = append(cs.Learners, cc.NodeID)
	case raftpb.ConfChangeRemoveNode:
		// Remove from voters
		newVoters := make([]uint64, 0, len(cs.Voters))
		for _, id := range cs.Voters {
			if id != cc.NodeID {
				newVoters = append(newVoters, id)
			}
		}
		cs.Voters = newVoters
		// Remove from learners
		newLearners := make([]uint64, 0, len(cs.Learners))
		for _, id := range cs.Learners {
			if id != cc.NodeID {
				newLearners = append(newLearners, id)
			}
		}
		cs.Learners = newLearners
	}
	return cs
}

func listRaftConfChangeAddNodeIds(snap *raftpb.Snapshot, result ibabuza.ReplayWalResult) (UInt64Slice, error) {
	nodeIds := make(map[uint64]struct{})
	if snap != nil {
		for _, id := range snap.Metadata.ConfState.Voters {
			nodeIds[id] = struct{}{}
		}
	}
	if err := result.ForEachConfChangeEntries(func(e raftpb.Entry) error {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(e.Data); err != nil {
			return err
		}
		switch cc.Type {
		case raftpb.ConfChangeAddLearnerNode:
			nodeIds[cc.NodeID] = struct{}{}
		case raftpb.ConfChangeAddNode:
			nodeIds[cc.NodeID] = struct{}{}
		case raftpb.ConfChangeRemoveNode:
			delete(nodeIds, cc.NodeID)
		case raftpb.ConfChangeUpdateNode:
		default:
			return errors.New("unknown conf change type")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	ids := make(UInt64Slice, 0, len(nodeIds))
	for id := range nodeIds {
		ids = append(ids, id)
	}
	sort.Sort(ids)
	return ids, nil
}
func createRaftConfigChangeEntries(newLocalPeerID uint64, newRaftListenAddr string, confChangeIds UInt64Slice,
	state *raftpb.HardState) ([]raftpb.Entry, error) {

	if state == nil || raft.IsEmptyHardState(*state) {
		return nil, errors.New("bootstrap: raftpb.HardState is empty")
	}
	match := false
	for _, id := range confChangeIds {
		if id == newLocalPeerID {
			match = true
		}
	}
	nextIndex := state.Commit + 1
	var ents []raftpb.Entry
	cc := raftpb.ConfChange{}
	if !match {

		raftPeerAttr := babuzapb.RaftPeerAttribute{
			PeerID:         newLocalPeerID,
			RaftListenAddr: newRaftListenAddr,
		}
		req := babuzapb.ConfChangeRequest{
			RaftPeerAttr: raftPeerAttr,
		}
		data, err := req.Marshal()
		if err != nil {
			return nil, err
		}
		cc.Type = raftpb.ConfChangeAddNode
		cc.NodeID = newLocalPeerID
		cc.Context = data
		ccData, err := cc.Marshal()
		if err != nil {
			return nil, err
		}
		ents = append(ents, raftpb.Entry{
			Term:  state.Term,
			Index: nextIndex,
			Type:  raftpb.EntryConfChange,
			Data:  ccData,
		})
		nextIndex++
		cc.Context = nil
	}
	for _, id := range confChangeIds {
		if id == newLocalPeerID {
			continue
		}
		cc.Type = raftpb.ConfChangeRemoveNode
		cc.NodeID = id
		req := babuzapb.ConfChangeRequest{
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				PeerID: id,
			},
		}
		data, err := req.Marshal()
		if err != nil {
			return nil, err
		}
		cc.Context = data
		ccData, err := cc.Marshal()
		if err != nil {
			return nil, err
		}
		ents = append(ents, raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Term:  state.Term,
			Index: nextIndex,
			Data:  ccData,
		})
		nextIndex++
		cc.Context = nil
	}
	if len(ents) != 0 {
		state.Commit = ents[len(ents)-1].Index
	}
	return ents, nil
}

func loadWALSnapshot(walManager ibabuza.WalManager, snapshotManager ibabuza.SnapshotManager) (*raftpb.Snapshot, error) {
	walSnapshots, err := walManager.FindSnapshot()
	if err != nil {
		return nil, err
	}
	return snapshotManager.LoadLastValidSnapshot(walSnapshots)
}

func replayWALAndSetup(cfg BabuzaConfig, walManager ibabuza.WalManager, snap *raftpb.Snapshot, discardUncommitted bool) (ibabuza.ReplayWalResult, ibabuza.EntryStorage, ibabuza.Wal, error) {
	walReplayResult, entryStorage, wal, err := walManager.ReplayWal(snap, discardUncommitted)
	if err != nil {
		return nil, nil, nil, err
	}
	if cfg.EnableWalNoSync {
		wal.SetUnsafeNoFsync()
	}
	return walReplayResult, entryStorage, wal, nil
}

func createRaftStorage(snapshotManager ibabuza.SnapshotManager, wal ibabuza.Wal,
	entryStorage ibabuza.EntryStorage, stateMachine ibabuza.BaseStateMachine,
	bsmInfo *BasedStateMachineInfo) *raftStorage {
	return &raftStorage{
		snapshotor:   snapshotManager,
		wal:          wal,
		entryStorage: entryStorage,
		stateMachine: stateMachine,
		bsmInfo:      bsmInfo,
	}
}

func restoreStateMachineFromSnapshot(snapshotManager ibabuza.SnapshotManager,
	stateMachine ibabuza.BaseStateMachine, bsmInfo *BasedStateMachineInfo,
	snapIndex uint64) error {
	reader, err := snapshotManager.CreateInstalledSnapshotReader(snapIndex, false)
	if err != nil {
		return err
	}
	defer reader.Close()

	if err = stateMachine.RestoreFromSnapshot(reader); err != nil {
		return err
	}
	bsmInfo.SetOpenAppliedIndex(snapIndex)
	return nil
}

func handleDiskStateMachine(stateMachine ibabuza.BaseStateMachine, bsmInfo *BasedStateMachineInfo,
	snap *raftpb.Snapshot, snapshotManager ibabuza.SnapshotManager) error {
	diskAppliedIndex, rebuildStateMachine := stateMachine.(ibabuza.DiskStateMachine).Open()

	if rebuildStateMachine {
		if snap != nil {
			if err := restoreStateMachineFromSnapshot(snapshotManager, stateMachine, bsmInfo, snap.Metadata.Index); err != nil {
				return fmt.Errorf("failed to restore disk state machine: %w", err)
			}
		}
	} else {
		if snap != nil && diskAppliedIndex < snap.Metadata.Index {
			return fmt.Errorf("disk applied index (%d) < snapshot index (%d)",
				diskAppliedIndex, snap.Metadata.Index)
		}
		bsmInfo.SetOpenAppliedIndex(diskAppliedIndex)
	}
	return nil
}

func handleMemoryStateMachine(stateMachine ibabuza.BaseStateMachine, bsmInfo *BasedStateMachineInfo,
	snap *raftpb.Snapshot, snapshotManager ibabuza.SnapshotManager) error {
	if snap != nil {
		if err := restoreStateMachineFromSnapshot(snapshotManager, stateMachine, bsmInfo, snap.Metadata.Index); err != nil {
			return fmt.Errorf("failed to restore memory state machine: %w", err)
		}
	}
	return nil
}

func initializeStatusFromSnapshot(status ibabuza.Status, snap *raftpb.Snapshot,
	hardState raftpb.HardState, bsmInfo *BasedStateMachineInfo) {
	status.SetHardStateTerm(hardState.Term)
	status.SetCommittedIndex(hardState.Commit)
	if snap != nil {
		status.SetAppliedIndex(snap.Metadata.Index)
		status.SetAppliedTerm(snap.Metadata.Term)
		status.SetSnapshotIndex(snap.Metadata.Index)
		status.SetConfState(snap.Metadata.ConfState)

		// Handle disk state machine with advanced applied index
		if bsmInfo.OpenAppliedIndex() > snap.Metadata.Index {
			status.SetAppliedIndex(bsmInfo.OpenAppliedIndex())
		}
	}
}

func restartRaftNode(cfg BabuzaConfig, raftNode ibabuza.RaftNode,
	logger ibabuza.Logger, entryStorage ibabuza.EntryStorage) (raft.Node, error) {
	raftCfg := cfg.convertToRaftConfig(logger, entryStorage)
	return raftNode.Restart(raftCfg)
}

type UInt64Slice []uint64

func (p UInt64Slice) Len() int           { return len(p) }
func (p UInt64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p UInt64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
