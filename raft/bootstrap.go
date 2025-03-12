package raft

import (
	"context"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/status"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sort"
	"time"
)

type BootstrapRaftCluster struct {
	cluster    ibabuza.Cluster
	storage    InternalStorage
	node       raft.Node
	sessionMgr ibabuza.SessionManager
	trans      ibabuza.Transport
	status     InternalStatus
	logger     ibabuza.Logger
}

func NewBootstrapRaftCluster(cfg *BabuzaConfig, configuration *VotingPeersConfiguration, stateMachine ibabuza.BaseStateMachine,
	cluster ibabuza.Cluster, raftNode ibabuza.RaftNode, sessions ibabuza.SessionManager, snapshotManager ibabuza.SnapshotManager,
	walManager ibabuza.WalManager, trans ibabuza.Transport, logger ibabuza.Logger) (*BootstrapRaftCluster, error) {

	storage, err := newStorageManager(stateMachine, snapshotManager, walManager)
	if err != nil {
		return nil, err
	}
	if err = sessions.SetResponseSerializer(storage.GetApplyResultSerializer()); err != nil {
		return nil, err
	}
	if err = trans.SetupTransportConfig(ibabuza.TransportConfig{
		PeerId:      cfg.LocalPeerId,
		PeerAddress: cfg.RaftListenAddress,
		TLSConfig:   cfg.TLSConfig,
	}); err != nil {
		return nil, err
	}
	cluster.SetClusterId(cfg.ClusterId)
	cluster.SetLocalPeerId(cfg.LocalPeerId)
	raftStatus := status.New()
	var node raft.Node
	if exist, err := storage.HasExistingWalFiles(); err != nil {
		return nil, err
	} else if exist {
		if err = storage.ScanInstallSnapshot(); err != nil {
			return nil, err
		}
		node, err = restartNode(cfg, raftNode, cluster, sessions, storage, trans, raftStatus, logger)
		if err != nil {
			return nil, err
		}
	} else {
		node, err = startNode(cfg, configuration, raftNode, storage, trans, logger)
		if err != nil {
			return nil, err
		}
	}
	return &BootstrapRaftCluster{
		cluster:    cluster,
		storage:    storage,
		node:       node,
		sessionMgr: sessions,
		trans:      trans,
		status:     raftStatus,
		logger:     logger,
	}, nil
}

//
//func RecoverAsStandalone(cfg BabuzaConfig, stateMachine iBabuza.StateMachineAdaptor, etcdRaftNode iBabuza.RaftNode,
//	cluster *cluster, sessions iBabuza.SessionManager, durable iBabuza.Storage,
//	trans iBabuza.Transport, logger iBabuza.Logger) error {
//
//	if err := sessions.SetApplyResultSerializer(stateMachine.GetApplyResultSerializer()); err != nil {
//		return err
//	}
//	if err := durable.ScanInstallSnapshot(); err != nil {
//		return err
//	}
//	if exist, err := durable.HasExistingWalFiles(cfg.WalDir); err != nil {
//		return err
//	} else if !exist {
//		return errors.New("")
//	}
//	walSnapshots, err := durable.FindSnapshotFromWal(cfg.WalDir)
//	if err != nil {
//		return err
//	}
//	snap, err := durable.LoadLastValidFromSnapshot(walSnapshots)
//	if err != nil {
//		return err
//	}
//	result, err := durable.OpenWalAndReplay(cfg.WalDir, snap, false)
//	if err != nil {
//		return err
//	}
//	cs, err := durable.GetCacheStorage()
//	if err != nil {
//		return err
//	}
//
//	if snap != nil {
//		reader, err := durable.CreateSnapshotReader(snap.Metadata.Index)
//		if err != nil {
//			return err
//		}
//		defer reader.Close()
//		if err = stateMachine.Open(snap, reader); err != nil {
//			return err
//		}
//		if err = cluster.Restore(reader); err != nil {
//			return err
//		}
//		if err = sessions.Restore(reader); err != nil {
//			return err
//		}
//		if cluster.ClusterId() != cfg.ClusterId || cluster.LocalPeerID() != cfg.LocalPeerId {
//			return errors.New("")
//		}
//		for _, p := range cluster.Peers() {
//			trans.AddPeer(p.RaftPeerAttr.Id, p.RaftPeerAttr.RaftListenAddr)
//		}
//	} else {
//		if err = stateMachine.Open(nil, nil); err != nil {
//			return err
//		}
//	}
//	var metadata babuzapb.WalMetadata
//	if err = metadata.Unmarshal(result.Metadata()); err != nil {
//		return err
//	}
//	if metadata.ClusterId != cfg.ClusterId || metadata.LocalPeerId != cfg.LocalPeerId {
//		return errors.New("")
//	}
//	hardState := result.HardState()
//	confChangeIds, err := listRaftConfChangeAddNodeIds(snap, result)
//	if err != nil {
//		return err
//	}
//	appendEntries, err := createRaftConfigChangeEntries(cfg.LocalPeerId, cfg.LocalPeerEndpoint, confChangeIds, &hardState)
//	if err != nil {
//		return err
//	}
//	if err = durable.Save(raftpb.HardState{}, appendEntries, raftpb.Snapshot{}); err != nil {
//		return err
//	}
//	if err = cs.SetHardState(hardState); err != nil {
//		return err
//	}
//	if err = cs.Append(appendEntries); err != nil {
//		return err
//	}
//	cluster.SetClusterId(cfg.ClusterId)
//	cluster.SetLocalPeerId(cfg.LocalPeerId)
//	if err = cluster.Add(babuzapb.RaftPeerAttribute{
//		Id:             cfg.LocalPeerId,
//		RaftListenAddr: cfg.LocalPeerEndpoint,
//	}); err != nil {
//		return err
//	}
//	if err = etcdRaftNode.Restart(iBabuza.NodeConfig{
//		BabuzaConfig: cfg.convertToRaftConfig(logger, cs),
//	}); err != nil {
//		return err
//	}
//	return nil
//
//}

func startNode(cfg *BabuzaConfig, configuration *VotingPeersConfiguration, raftNode ibabuza.RaftNode,
	storage InternalStorage, trans ibabuza.Transport, logger ibabuza.Logger) (raft.Node, error) {
	var peers []raft.Peer
	var err error

	if err = configuration.Validate(); err != nil {
		return nil, err
	}
	for _, raftPeerAttr := range configuration.RaftPeersAttribute() {
		if raftPeerAttr.Id != cfg.LocalPeerId {
			trans.AddPeer(raftPeerAttr.Id, raftPeerAttr.RaftListenAddr)
		}
	}
	if cfg.Join {
		if err = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()
			return matchRemoteCluster(ctx, cfg, configuration, trans)
		}(); err != nil {
			return nil, err
		}
	}
	if err = storage.CreateWal(babuzapb.WalMetadata{
		ClusterId:   cfg.ClusterId,
		LocalPeerId: cfg.LocalPeerId,
	}); err != nil {
		return nil, err
	}
	if cfg.EnableWalNoSync {
		err = storage.SetWalNoFSync()
		if err != nil {
			return nil, err
		}
	}
	entryStorage, err := storage.GetEntryStorage()
	if err != nil {
		return nil, err
	}
	raftCfg := cfg.convertToRaftConfig(logger, entryStorage)
	if cfg.Join {
		return raftNode.Restart(raftCfg)
	}
	peers, err = configuration.ToRaftPeers()
	if err != nil {
		return nil, err
	}
	return raftNode.Start(raftCfg, peers)
}

func restartNode(cfg *BabuzaConfig, raftNode ibabuza.RaftNode, cluster ibabuza.Cluster, sessions ibabuza.SessionManager,
	storage InternalStorage, trans ibabuza.Transport, status InternalStatus, logger ibabuza.Logger) (raft.Node, error) {

	//TODO: verify snap and wal match
	walSnapshots, err := storage.FindSnapshotFromWal()
	if err != nil {
		return nil, err
	}
	snap, err := storage.LoadLastValidFromSnapshot(walSnapshots)
	if err != nil {
		return nil, err
	}
	if _, err = storage.OpenWalAndReplay(snap, false); err != nil {
		return nil, err
	}
	cache, err := storage.GetEntryStorage()
	if err != nil {
		return nil, err
	}
	hs, _, err := cache.InitialState()
	if err != nil {
		return nil, err
	}
	status.SetHardStateTerm(hs.Term)
	status.SetCommittedIndex(hs.Commit)

	if err = storage.OpenStateMachine(snap); err != nil {
		return nil, err
	}
	if snap != nil {
		if err := storage.RestoreFromSnapshot(snap.Metadata.Index, false, cluster, sessions); err != nil {
			return nil, err
		}
		if cluster.ClusterId() != cfg.ClusterId || cluster.LocalPeerID() != cfg.LocalPeerId {
			return nil, fmt.Errorf("bootstrap: mistach configuration (clusterId=%d, restore clusterId=%d, localPeerId=%d, restore localPeerId=%d) ",
				cfg.ClusterId, cluster.ClusterId(), cfg.LocalPeerId, cluster.LocalPeerID())
		}
		for _, p := range cluster.Peers() {
			trans.AddPeer(p.RaftPeerAttr.Id, p.RaftPeerAttr.RaftListenAddr)
		}
		status.SetAppliedIndex(snap.Metadata.Index)
		status.SetAppliedTerm(snap.Metadata.Term)
		status.SetSnapshotIndex(snap.Metadata.Index)
		status.SetConfState(snap.Metadata.ConfState)
	}
	if cfg.EnableWalNoSync {
		storage.SetWalNoFSync()
	}
	return raftNode.Restart(cfg.convertToRaftConfig(logger, cache))
}

func matchRemoteCluster(remoteCtx context.Context, config *BabuzaConfig, remoteConfiguration *VotingPeersConfiguration,
	trans ibabuza.Transport) error {

	req := babuzapb.GetClusterPeersRequest{
		ClusterId: config.ClusterId,
		FromId:    config.LocalPeerId,
	}
	client, err := trans.CreateTransportClient()
	if err != nil {
		return errors.New("bootstrap: create transport client error")
	}
	defer client.Close()

	for _, raftPeerAttr := range remoteConfiguration.RaftPeersAttribute() {
		if raftPeerAttr.Id == config.LocalPeerId {
			continue
		}
		select {
		case <-remoteCtx.Done():
			return remoteCtx.Err()
		default:
		}
		res := func(toId uint64) babuzapb.GetClusterPeersResponse {
			req.ToId = toId
			return client.GetClusterPeers(req)
		}(raftPeerAttr.Id)
		if res.Status == babuzapb.FAILED {
			continue
		}
		if err := remoteConfiguration.MatchRemoteCluster(res.Peers); err != nil {
			continue
		}
		return nil
	}
	return fmt.Errorf("bootstrap: could not get remote cluster from %v", remoteConfiguration)

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
			return errors.New("")
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
func createRaftConfigChangeEntries(newLocalId uint64, newRaftListenAddr string, confChangeIds UInt64Slice,
	state *raftpb.HardState) ([]raftpb.Entry, error) {

	if state == nil || raft.IsEmptyHardState(*state) {
		return nil, errors.New("bootstrap: raftpb.HardState is empty")
	}
	match := false
	for _, id := range confChangeIds {
		if id == newLocalId {
			match = true
		}
	}
	nextIndex := state.Commit + 1
	var ents []raftpb.Entry
	cc := raftpb.ConfChange{}
	if !match {

		raftPeerAttr := babuzapb.RaftPeerAttribute{
			Id:             newLocalId,
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
		cc.NodeID = newLocalId
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
		if id == newLocalId {
			continue
		}
		cc.Type = raftpb.ConfChangeRemoveNode
		cc.NodeID = id
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
	}
	if len(ents) != 0 {
		state.Commit = ents[len(ents)-1].Index
	}
	return ents, nil
}

type UInt64Slice []uint64

func (p UInt64Slice) Len() int           { return len(p) }
func (p UInt64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p UInt64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
