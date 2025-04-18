package raft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/metrics"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"io"
	"time"
)

type mockRaftNodeStarter struct{}

func (m *mockRaftNodeStarter) Start(config raft.Config, peers []raft.Peer) (raft.Node, error) {
	return nil, nil
}

func (m *mockRaftNodeStarter) Restart(config raft.Config) (raft.Node, error) {
	return nil, nil
}

type mockBasicStateMachine struct{}

func (m *mockBasicStateMachine) Apply(i ibabuza.Entry) {}

func (m *mockBasicStateMachine) SaveSnapshot(machineSnapshotContext ibabuza.StateMachineSnapshotContext, writer ibabuza.StateMachineSnapshotWriter) error {
	return nil
}

func (m *mockBasicStateMachine) RestoreFromSnapshot(reader ibabuza.StateMachineSnapshotReader) error {
	return nil
}

func (m *mockBasicStateMachine) Close() error {
	return nil
}

type mockWalManager struct{}

func (m *mockWalManager) FindSnapshot() ([]walpb.Snapshot, error) {
	return nil, nil
}

func (m *mockWalManager) CreateWal(metadata babuzapb.WalMetadata) (ibabuza.EntryStorage, ibabuza.Wal, error) {
	return nil, nil, nil
}

func (m *mockWalManager) ReplayWal(snapshot *raftpb.Snapshot, deleteUncommitted bool) (ibabuza.EntryStorage, ibabuza.Wal, ibabuza.ReplayWalResult, error) {
	return nil, nil, nil, nil
}

func (m *mockWalManager) HasExistingWals() (bool, error) {
	return false, nil
}

func (m *mockWalManager) PurgeWals(config ibabuza.WalPurgeConfig) {
}

type mockSnapshotManager struct{}

func (m *mockSnapshotManager) ScanInstalledSnapshots(removeUnfinishedSnapshotDir bool) error {
	return nil
}

func (m *mockSnapshotManager) LoadLastValidSnapshot(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return nil, nil
}

func (m *mockSnapshotManager) CreateAtomicSnapshotWriter(snapshotTerm, snapshotIndex uint64) (ibabuza.AtomicSnapshotWriter, error) {
	return nil, nil
}

func (m *mockSnapshotManager) CreateInstalledSnapshotReader(snapshotIndex uint64, validateFsmFiles bool) (ibabuza.SnapshotReader, error) {
	return nil, nil
}

func (m *mockSnapshotManager) CreateAtomicSnapshotReceiver(metadata babuzapb.SnapshotMetadata) (ibabuza.AtomicSnapshotReceiver, error) {
	return nil, nil
}

func (m *mockSnapshotManager) Purge(snapshot raftpb.Snapshot) error {
	return nil
}

func (m *mockSnapshotManager) Close() error {
	return nil
}

type mockSession struct {
}

func (s *mockSession) Id() uint64 {
	return 0
}
func (s *mockSession) LastActiveNanoseconds() int64 {
	return 0
}
func (s *mockSession) ClearResult(uint64) {

}
func (s *mockSession) RepeatSequenceNum(uint64) bool {
	return false
}
func (s *mockSession) AddResult(uint64, int64, ibabuza.ApplyResult) error {
	return nil
}

func (s *mockSession) GetResult(uint64) (ibabuza.ApplyResult, bool) {
	return ibabuza.ApplyResult{}, false
}
func (s *mockSession) Snapshot(io.Writer, ibabuza.ApplyResultSerializer) error {
	return nil
}

func (s *mockSession) Restore(io.Reader, ibabuza.ApplyResultSerializer) error {
	return nil
}

type mockSessionManager struct{}

func (m *mockSessionManager) SetResponseSerializer(serializer ibabuza.ResponseSerializer) error {
	return nil
}

func (m *mockSessionManager) GetSession(u uint64) (ibabuza.Session, error) {
	return &mockSession{}, nil
}

func (m *mockSessionManager) Register(u uint64, i int64) {}

func (m *mockSessionManager) ExpireSession(i int64) {}

func (m *mockSessionManager) Snapshot(writer io.Writer) error {
	return nil
}

func (m *mockSessionManager) Restore(reader io.Reader) error {
	return nil
}

type mockCluster struct{}

func (m *mockCluster) SetClusterID(clusterID uint64) {
}

func (m *mockCluster) SetLocalPeerID(localPeerID uint64) {
}

func (m *mockCluster) Peer(peerID uint64) (babuzapb.Peer, error) {
	return babuzapb.Peer{}, nil
}

func (m *mockCluster) Snapshot(writer io.Writer) error {
	return nil
}

func (m *mockCluster) Restore(reader io.Reader) error {
	return nil
}

func (m *mockCluster) Peers() []babuzapb.Peer {
	return nil
}

func (m *mockCluster) ClusterID() uint64 {
	return 0
}

func (m *mockCluster) LocalPeerID() uint64 {
	return 0
}

func (m *mockCluster) Add(attribute babuzapb.RaftPeerAttribute) error {
	return nil
}

func (m *mockCluster) Update(attribute babuzapb.RaftPeerAttribute) error {
	return nil
}

func (m *mockCluster) Remove(peerID uint64) error {
	return nil
}

func (m *mockCluster) Promote(peerID uint64) error {
	return nil
}

func (m *mockCluster) UpdateAppServiceAddresses(peerID uint64, addresses []string) error {
	return nil
}

type mockTransport struct {
	send       []raftpb.Message
	snap       []raftpb.Message
	mockClient ibabuza.TransportClient
}

func (m *mockTransport) Start() error                                       { return nil }
func (m *mockTransport) Stop() error                                        { return nil }
func (m *mockTransport) SetupTransportConfig(ibabuza.TransportConfig) error { return nil }
func (m *mockTransport) SetupTransportRaft(ibabuza.RaftNodeHandler) error {
	return nil
}
func (m *mockTransport) Send(msg raftpb.Message) { m.send = append(m.send, msg) }
func (m *mockTransport) SendSnapshot(snap raftpb.Message) {
	m.snap = append(m.snap, snap)
}
func (m *mockTransport) CreateTransportClient() (ibabuza.TransportClient, error) {
	return m.mockClient, nil
}
func (m *mockTransport) AddPeer(uint64, string)    {}
func (m *mockTransport) UpdatePeer(uint64, string) {}
func (m *mockTransport) RemovePeer(uint64)         {}
func (m *mockTransport) RemovePeers()              {}

type mockRaftNodeReadIndexFunc func(rctx []byte) error

type mockRaftNode struct {
	readyCh       chan raft.Ready
	readIndexFunc mockRaftNodeReadIndexFunc
	errorPropose  error
}

func newMockRaftNode() *mockRaftNode {
	return &mockRaftNode{
		readyCh: make(chan raft.Ready),
	}
}

func (m *mockRaftNode) Tick()                                          {}
func (m *mockRaftNode) Campaign(ctx context.Context) error             { return nil }
func (m *mockRaftNode) Propose(ctx context.Context, data []byte) error { return m.errorPropose }
func (m *mockRaftNode) ProposeConfChange(ctx context.Context, cc raftpb.ConfChangeI) error {
	return nil
}
func (m *mockRaftNode) Step(ctx context.Context, msg raftpb.Message) error              { return nil }
func (m *mockRaftNode) Ready() <-chan raft.Ready                                        { return m.readyCh }
func (m *mockRaftNode) Advance()                                                        {}
func (m *mockRaftNode) ApplyConfChange(cc raftpb.ConfChangeI) *raftpb.ConfState         { return nil }
func (m *mockRaftNode) TransferLeadership(ctx context.Context, lead, transferee uint64) {}
func (m *mockRaftNode) ReadIndex(ctx context.Context, rctx []byte) error {
	if m.readIndexFunc != nil {
		return m.readIndexFunc(rctx)
	}
	return nil
}
func (m *mockRaftNode) Status() raft.Status                                  { return raft.Status{} }
func (m *mockRaftNode) ReportUnreachable(id uint64)                          {}
func (m *mockRaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
func (m *mockRaftNode) Stop()                                                {}

type mockEntryStorage struct {
	appendEntries []raftpb.Entry
	applySnap     raftpb.Snapshot
}

func (m *mockEntryStorage) SetHardState(state raftpb.HardState) error { return nil }
func (m *mockEntryStorage) Append(entries []raftpb.Entry) error {
	m.appendEntries = entries
	return nil
}
func (m *mockEntryStorage) ApplySnapshot(snapshot raftpb.Snapshot) error {
	m.applySnap = snapshot
	return nil
}
func (m *mockEntryStorage) CreateSnapshot(snapshotIndex uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, nil
}
func (m *mockEntryStorage) Compact(compactIndex uint64) error { return nil }

func (m *mockEntryStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return raftpb.HardState{}, raftpb.ConfState{}, nil
}
func (m *mockEntryStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) { return nil, nil }
func (m *mockEntryStorage) Term(i uint64) (uint64, error)                          { return 0, nil }
func (m *mockEntryStorage) LastIndex() (uint64, error)                             { return 0, nil }
func (m *mockEntryStorage) FirstIndex() (uint64, error)                            { return 0, nil }
func (m *mockEntryStorage) Snapshot() (raftpb.Snapshot, error)                     { return raftpb.Snapshot{}, nil }

type mockSnapshotReader struct {
}

func (ms *mockSnapshotReader) Open(fileTag string) (io.Reader, ibabuza.StateMachineFileDesc, error) {
	return nil, ibabuza.StateMachineFileDesc{}, nil
}
func (ms *mockSnapshotReader) Close() error {
	return nil
}
func (ms *mockSnapshotReader) ForEachFile(visitor func(io.Reader, babuzapb.SnapshotFileDesc) error) error {
	return nil
}
func (ms *mockSnapshotReader) Metadata() babuzapb.SnapshotMetadata {
	return babuzapb.SnapshotMetadata{}
}
func (ms *mockSnapshotReader) Cluster() (io.Reader, error) {
	return nil, nil
}
func (ms *mockSnapshotReader) Session() (io.Reader, error) {
	return nil, nil
}
func (ms *mockSnapshotReader) CreateTarArchiveReader() (io.ReadCloser, error) {
	return nil, nil
}

type mockStorageMgr struct {
	hs           raftpb.HardState
	entries      []raftpb.Entry
	snap         raftpb.Snapshot
	releaseSnap  raftpb.Snapshot
	snapMetadata babuzapb.SnapshotMetadata
	entryStorage mockEntryStorage
}

func (m *mockStorageMgr) CompactAndReleaseSnapshot(index uint64, snapshot raftpb.Snapshot) error {
	m.releaseSnap = snapshot
	return nil
}

func (m *mockStorageMgr) ApplyAndReleaseSnapshot(snapshot raftpb.Snapshot) error {
	m.entryStorage.applySnap = snapshot
	m.releaseSnap = snapshot
	return nil
}

func (m *mockStorageMgr) OpenStateMachine(snapshot *raftpb.Snapshot) error {
	return nil
}

func (m *mockStorageMgr) GetApplyResultSerializer() ibabuza.ResponseSerializer {
	return nil
}

func (m *mockStorageMgr) CreateSnapshotContext(snapshotTerm, snapshotIndex uint64, confState raftpb.ConfState, cluster ibabuza.Cluster, sessionMgr ibabuza.SessionManager) (InternalStorageSnapshotContext, error) {
	return nil, nil
}

func (m *mockStorageMgr) SaveStateMachineSnapshot(ctx InternalStorageSnapshotContext) (babuzapb.SnapshotMetadata, error) {
	return babuzapb.SnapshotMetadata{}, nil
}

func (m *mockStorageMgr) RestoreFromSnapshot(snapShotIndex uint64, restoreStateMachine bool, cluster ibabuza.Cluster, session ibabuza.SessionManager) error {
	return nil
}

func (m *mockStorageMgr) GetStateMachineAppliedIndex() uint64 {
	return 0
}

func (m *mockStorageMgr) SetStateMachineAppliedIndex(index uint64) {
}

func (m *mockStorageMgr) Apply(e ibabuza.Entry) {
}

func (m *mockStorageMgr) SupportConcurrentSnapshot() bool {
	return false
}

func (m *mockStorageMgr) ReceiveSnapshotMessage(msg babuzapb.SnapshotMessage) (bool, error) {
	return false, nil
}

func (m *mockStorageMgr) GetEntryStorage() (ibabuza.EntryStorage, error) {
	return &m.entryStorage, nil
}

func (m *mockStorageMgr) EntryStorageApplySnapshot(snapshot raftpb.Snapshot) error {
	return m.entryStorage.ApplySnapshot(snapshot)
}

func (m *mockStorageMgr) EntryStorageAppend(entries []raftpb.Entry) error {
	return m.entryStorage.Append(entries)
}

func (m *mockStorageMgr) EntryStorageCompact(compactIndex uint64) error {
	return m.entryStorage.Compact(compactIndex)
}

func (m *mockStorageMgr) EntryStorageInfo() (lastIndex uint64, lastTerm uint64, snapshot raftpb.Snapshot, err error) {
	return 0, 0, raftpb.Snapshot{}, nil
}

func (m *mockStorageMgr) CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error) {
	return &mockSnapshotReader{}, nil
}
func (m *mockStorageMgr) CreateSnapshotWriter(snapshotTerm, snapshotIndex uint64) (w ibabuza.AtomicSnapshotWriter, err error) {
	return nil, err
}

func (m *mockStorageMgr) ScanInstalledSnapshot() error                   { return nil }
func (m *mockStorageMgr) FindSnapshotFromWal() ([]walpb.Snapshot, error) { return nil, nil }
func (m *mockStorageMgr) LoadLastValidFromSnapshot(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return nil, nil
}
func (m *mockStorageMgr) HasExistingWalFiles() (bool, error)            { return false, nil }
func (m *mockStorageMgr) CreateWal(metadata babuzapb.WalMetadata) error { return nil }
func (m *mockStorageMgr) OpenWalAndReplay(snapshot *raftpb.Snapshot,
	deleteUnCommittedEntries bool) (ibabuza.ReplayWalResult, error) {
	return nil, nil
}
func (m *mockStorageMgr) SetWalNoFSync() error                           { return nil }
func (m *mockStorageMgr) GetCacheStorage() (ibabuza.EntryStorage, error) { return nil, nil }
func (m *mockStorageMgr) Save(hs raftpb.HardState, entries []raftpb.Entry, snap raftpb.Snapshot) error {
	m.hs = hs
	m.entries = entries
	m.snap = snap
	return nil
}

func (m *mockStorageMgr) Close() error {
	return nil
}

func (m *mockStorageMgr) GetStateMachine() ibabuza.BaseStateMachine {
	return nil
}

type mockEventDelegate struct {
	acquiredLeaderStatus int
}

func (m *mockEventDelegate) NotifyLeaderAcquired() {
	m.acquiredLeaderStatus = 1
}

func (m *mockEventDelegate) NotifyLeaderLost() {
	m.acquiredLeaderStatus = 2
}

func (m *mockEventDelegate) NotifyMemberJoin(peerAttribute babuzapb.RaftPeerAttribute) {

}

func (m *mockEventDelegate) NotifyMemberUpdate(peerAttribute babuzapb.RaftPeerAttribute) {

}

func (m *mockEventDelegate) NotifyMemberLeave(peerID uint64) {

}

func (m *mockEventDelegate) NotifySystemReady() {

}

type mockIdGenerator struct {
	id uint64
}

func (m *mockIdGenerator) Next() uint64 {
	return m.id
}

type mockPubTransClient struct {
	status babuzapb.RpcStatus
	errMsg string
}

func (c *mockPubTransClient) SendBatchMessage(babuzapb.BatchMessage) error       { return nil }
func (c *mockPubTransClient) SendSnapshotMessage(babuzapb.SnapshotMessage) error { return nil }
func (c *mockPubTransClient) GetClusterPeers(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{}
}
func (c *mockPubTransClient) PublishApplicationService(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{
		Status:  c.status,
		Message: c.errMsg,
	}
}
func (c *mockPubTransClient) Close() error { return nil }

func newTestRaft(nodeId uint64) *Raft {
	return &Raft{
		config: BabuzaConfig{
			LocalPeerID: nodeId,
			RaftConfig: RaftConfig{
				LogicalTickMs: 100,
			},
			LinearizedReadRequestTimeout: time.Second * 5,
			LinearizedReadRetryTimeout:   time.Millisecond * 500,
		},
		metricsCollector:          metrics.NewMockMetricsCollector(),
		applyCh:                   make(chan applyEntryToStateMachine),
		manualSnapshotCh:          make(chan manualSnapshot),
		readStateCh:               make(chan raft.ReadState, 1),
		readIndexCh:               make(chan struct{}),
		leaderCh:                  make(chan bool, 1),
		linearizeReqNotifier:      syncutil.NewErrNotifier(),
		firstCommitInTermNotifier: syncutil.NewNotifier(),
		leaderChangeNotifier:      syncutil.NewNotifier(),
		closer:                    syncutil.NewCloser(),
	}
}

type KvStoreInput struct {
	Command uint64 // 0 => get, 1 => set, 2 => append, 3=>delete
	Key     string
	Value   string
}

type KvStoreOutput struct {
	Value string
}

//
//var KvStoreModel = porcupine.Model{
//	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
//		m := make(map[string][]porcupine.Operation)
//		for _, v := range history {
//			key := v.Input.(KvStoreInput).Key
//			m[key] = append(m[key], v)
//		}
//		keys := make([]string, 0, len(m))
//		for k := range m {
//			keys = append(keys, k)
//		}
//		sort.Strings(keys)
//		ret := make([][]porcupine.Operation, 0, len(keys))
//		for _, k := range keys {
//			ret = append(ret, m[k])
//		}
//		return ret
//	},
//	Init: func() interface{} {
//		return ""
//	},
//	Step: func(state, input, output interface{}) (bool, interface{}) {
//		kvInput := input.(KvStoreInput)
//		kvOutput := output.(KvStoreOutput)
//		st := state.(string)
//		switch kvInput.Command {
//		case 0:
//			return kvOutput.Value == st, state
//		case 1:
//			return true, kvInput.Value
//		case 2:
//			return true, st + kvInput.Value
//		case 3:
//			return true, ""
//		default:
//			panic("porcupine.Model: not support command of kvstore")
//		}
//	},
//	Equal: func(state1, state2 interface{}) bool {
//		return state1 == state2
//	},
//	DescribeOperation: func(input, output interface{}) string {
//		kvInput := input.(KvStoreInput)
//		kvOutput := output.(KvStoreOutput)
//		switch kvInput.Command {
//		case 0:
//			return fmt.Sprintf("get('%s') -> '%s'", kvInput.Key, kvOutput.Value)
//		case 1:
//			return fmt.Sprintf("put('%s', '%s')", kvInput.Key, kvInput.Value)
//		case 2:
//			return fmt.Sprintf("append('%s', '%s')", kvInput.Key, kvInput.Value)
//		case 3:
//			return fmt.Sprintf("delete('%s')", kvInput.Key)
//		default:
//			return "<invalid>"
//		}
//	},
//}
