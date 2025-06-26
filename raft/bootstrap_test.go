package raft

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io/ioutil"
	"os"
	"testing"
)

//type mockRemoteCluster struct {
//	clusterID     uint64
//	localNodeId   uint64
//	peerEndpoint  string
//	cluster       map[uint64]iBabuza.Peer
//	delayResponse time.Duration
//}
//
//func (m *mockRemoteCluster) GetRemoteClusterInfo(ctx context.Context, remotePeerEndpoint string) (iBabuza.RemoteCluster, error) {
//	if remotePeerEndpoint != m.peerEndpoint {
//		return iBabuza.RemoteCluster{}, context.DeadlineExceeded
//	}
//	time.Sleep(m.delayResponse)
//	var cl []iBabuza.Peer
//	for _, p := range m.cluster {
//		cl = append(cl, p)
//	}
//	return iBabuza.RemoteCluster{
//		RemoteClusterId: m.clusterID,
//		RemotePeers:     cl,
//	}, nil
//}
//
//func TestBootstrap_GetRemoteCluster(t *testing.T) {
//	type testCase struct {
//		mockRemoteCluster
//		genRemoteCtx func() context.Context
//		expectResult error
//	}
//	clId := uint64(100)
//	clMap := map[uint64]string{1: "localhost:14200", 2: "localhost:14201", 3: "localhost:14202"}
//	cl := make(map[uint64]iBabuza.Peer)
//	for id, endpoint := range clMap {
//		cl[id] = iBabuza.Peer{
//			Id:       id,
//			RaftListenAddr: endpoint,
//		}
//	}
//	for _, tc := range []testCase{
//		{
//			mockRemoteCluster: mockRemoteCluster{
//				clusterID:     100,
//				localNodeId:   2,
//				peerEndpoint:  "localhost:14201",
//				cluster:       cl,
//				delayResponse: 0,
//			},
//			genRemoteCtx: func() context.Context {
//				return context.Background()
//			},
//			expectResult: nil,
//		},
//		{
//			mockRemoteCluster: mockRemoteCluster{
//				clusterID:     100,
//				localNodeId:   2,
//				peerEndpoint:  "localhost:14201",
//				cluster:       cl,
//				delayResponse: time.Second * 2,
//			},
//			genRemoteCtx: func() context.Context {
//				ctx, _ := context.WithTimeout(context.Background(), time.Second)
//				return ctx
//			},
//			expectResult: errors.New("bootstrap: could not get remote cluster from map[1:localhost:14200 2:localhost:14201 3:localhost:14202]"),
//		},
//	} {
//		remote, err := getRemoteCluster("localhost:14200", clMap, tc.genRemoteCtx(), &tc)
//		if err == nil {
//			assert.Equal(t, clId, remote.RemoteClusterId)
//			assert.Equal(t, cl.Members(), remote.RemotePeers)
//		} else {
//			assert.Equal(t, tc.expectResult.Error(), err.Error())
//		}
//	}
//
//}

//func TestBootstrap_JoinRaftCluster(t *testing.T) {
//
//	cl, err := joinRaftCluster(100, 1, map[uint64]string{1: "localhost:14200", 2: "localhost:14201", 3: "localhost:14202"}, nil)
//	assert.Nil(t, err)
//	assert.NotNil(t, cl)
//
//	type testCase struct {
//		mockRemoteCluster
//		expectResult error
//	}
//	for _, tc := range []testCase{
//		{
//			mockRemoteCluster: mockRemoteCluster{
//				clusterID:     100,
//				localNodeId:   2,
//				peerAddr:      "localhost:14201",
//				cluster:       map[uint64]string{1: "localhost:14200", 2: "localhost:14201", 3: "localhost:14202"},
//				delayResponse: 0,
//			},
//			expectResult: nil,
//		},
//		{
//			mockRemoteCluster: mockRemoteCluster{
//				clusterID:     101,
//				localNodeId:   2,
//				peerAddr:      "localhost:14201",
//				cluster:       map[uint64]string{1: "localhost:14200", 2: "localhost:14201", 3: "localhost:14202"},
//				delayResponse: 0,
//			},
//			expectResult: errors.New("bootstrap: cluster id did not match (remote(101), local(100))"),
//		},
//		{
//			mockRemoteCluster: mockRemoteCluster{
//				clusterID:     100,
//				localNodeId:   2,
//				peerAddr:      "localhost:14201",
//				cluster:       map[uint64]string{1: "localhost:14200", 2: "localhost:14201"},
//				delayResponse: 0,
//			},
//			expectResult: cluster.ErrNodeCountNotEqual,
//		},
//		{
//			mockRemoteCluster: mockRemoteCluster{
//				clusterID:     100,
//				localNodeId:   2,
//				peerAddr:      "localhost:14201",
//				cluster:       map[uint64]string{1: "localhost:14200", 2: "localhost:14201", 3: "192.168.0.1:14202"},
//				delayResponse: 0,
//			},
//			expectResult: errors.New("cluster: mismatch two cluster. resolved cluster a([localhost:14200 localhost:14201 localhost:14202]) resolved cluster b([192.168.0.1:14202 localhost:14200 localhost:14201])"),
//		},
//	} {
//		cl, err = joinRaftCluster(100, 1, map[uint64]string{1: "localhost:14200", 2: "localhost:14201", 3: "localhost:14202"}, &tc.mockRemoteCluster)
//		if err != nil {
//			assert.Equal(t, tc.expectResult.Error(), err.Error())
//		}
//	}
//}
//
//type mockFsm struct {
//	data              []string
//	t                 *testing.T
//	enableRestoreFail bool
//}
//
//func (m *mockFsm) Apply([]byte) ApplyResult {
//	return ApplyResult{}
//}
//func (m *mockFsm) PrepareSnapshot() (interface{}, error) {
//	return nil, nil
//}
//func (m *mockFsm) SaveSnapshot(ctx SnapshotContext, w SnapshotFileWriter) error {
//	d, err := json.Marshal(m.data)
//	assert.Nil(m.t, err)
//	wc, err := w.Create("test", babuzapb.CompressionNone)
//	assert.Nil(m.t, err)
//	defer wc.Teardown()
//	_, err = wc.Write(d)
//	assert.Nil(m.t, err)
//	return nil
//}
//func (m *mockFsm) RestoreFormSnapshot(r SnapshotFileReader) error {
//	if m.enableRestoreFail {
//		return errors.New("fail")
//	}
//	reader, _, err := r.Open("test")
//	assert.Nil(m.t, err)
//	data, err := ioutil.ReadAll(reader)
//	assert.Nil(m.t, err)
//	assert.Nil(m.t, json.Unmarshal(data, &m.data))
//	return nil
//}
//
//func genSnapshot(t *testing.T, snapshotDir string, snapVersion uint64, snap *raftpb.Snapshot, maxSnapshotKeepFiles uint, cl *cluster.RaftCluster, mf *mockFsm) *snapshot.Snapshotor {
//
//	snapshotor := snapshot.NewSnapshotor(snapshotDir, snapVersion, maxSnapshotKeepFiles)
//	if snap != nil {
//		sw, err := snapshotor.CreateFileWriter(snap.Metadata.Term, snap.Metadata.Index)
//		assert.Nil(t, err)
//		assert.Nil(t, mf.SaveSnapshot(nil, sw))
//		cls, err := cl.Snapshot()
//		assert.Nil(t, err)
//		assert.Nil(t, snapshotor.FinalizeFileWriterAndInstall(cls, *snap, sw))
//	}
//	return snapshotor
//}

//
//func TestBootstrap_RestoreSnapshot(t *testing.T) {
//	p, err := ioutil.TempDir("", "bootstrap-restore-snapshot")
//	assert.Nil(t, err)
//	defer os.RemoveAll(p)
//	cl, err := cluster.NewFromPeers(100, 1, map[uint64]string{1: "localhsot:14200", 2: "localhsot:14201", 3: "localhsot:14202"})
//	assert.Nil(t, err)
//	mf := mockFsm{
//		data: []string{"hello", "world"},
//		t:    t,
//	}
//	snapshotor := genSnapshot(t, p, 1, 1, cl, &mf)
//
//	t.Run("success", func(t *testing.T) {
//		clr := cluster.New(100, 1)
//		mfr := mockFsm{}
//		assert.Nil(t, restoreSnapshot(snapshotor, 1, clr, &mfr))
//		assert.Equal(t, cl, clr)
//		assert.Equal(t, mf.data, mfr.data)
//	})
//
//	t.Run("mismatch cluster id and local node id", func(t *testing.T) {
//		clr := cluster.New(100, 2)
//		mfr := mockFsm{}
//		assert.Equal(t, "cluster: cluster identifier and local node id is different from restore data. original(ClusterId(100),LocalNodeId(2)) restore(ClusterId(100),LocalNodeId(1))",
//			restoreSnapshot(snapshotor, 1, clr, &mfr).Error())
//	})
//
//	t.Run("failed to restore fsm", func(t *testing.T) {
//		clr := cluster.New(100, 1)
//		mfr := mockFsm{enableRestoreFail: true}
//		assert.Equal(t, "fail", restoreSnapshot(snapshotor, 1, clr, &mfr).Error())
//	})
//	t.Run("not found snapshot files.", func(t *testing.T) {
//		clr := cluster.New(100, 1)
//		mfr := mockFsm{}
//		assert.Equal(t, "snapshotor: not found valid snapshot files. (snapshot index=2)", restoreSnapshot(snapshotor, 2, clr, &mfr).Error())
//	})
//}

//func TestBootstrap_RestoreSnapAndReplayWal(t *testing.T) {
//	snapshotDir, err := ioutil.TempDir("", "bootstrap-restore-snapshot")
//	assert.Nil(t, err)
//	defer os.RemoveAll(snapshotDir)
//	walDir, err := ioutil.TempDir("", "bootstrap-restore-wal-1")
//	assert.Nil(t, err)
//	defer os.RemoveAll(walDir)
//	cl, err := cluster.NewFromPeers(100, 1, map[uint64]string{1: "localhsot:14200", 2: "localhsot:14201", 3: "localhsot:14202"})
//	assert.Nil(t, err)
//
//	rs := raftStorage.NewBabuza(raftStorage.NewDefaultBabuzaConfig(), nil)
//	_, w, err := rs.Create(walDir, babuzapb.WalMetadata{
//		ClusterId:   100,
//		LocalNodeId: 1,
//	})
//	assert.Nil(t, err)
//
//	assert.Nil(t, w.Save(raftpb.HardState{}, []raftpb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}))
//	assert.Nil(t, w.Teardown())
//
//	t.Run("config match", func(t *testing.T) {
//		for _, tc := range []struct {
//			snap           *raftpb.Snapshot
//			expectEntsFunc func(t *testing.T, ms factory.RaftMemoryStorage)
//		}{
//			{
//				snap: &raftpb.Snapshot{
//					Metadata: raftpb.SnapshotMetadata{
//						Index: 1,
//						Term:  1,
//					}},
//				expectEntsFunc: func(t *testing.T, ms factory.RaftMemoryStorage) {
//					ents, cErr := ms.Entries(2, 3, math.MaxUint64)
//					assert.Nil(t, cErr)
//					assert.Equal(t, []raftpb.Entry{{Term: 1, Index: 2}}, ents)
//				},
//			},
//			{
//				snap: nil,
//				expectEntsFunc: func(t *testing.T, ms factory.RaftMemoryStorage) {
//					ents, cErr := ms.Entries(1, 3, math.MaxUint64)
//					assert.Nil(t, cErr)
//					assert.Equal(t, []raftpb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, ents)
//				},
//			},
//		} {
//			mf := mockFsm{
//				data: []string{"hello", "world"},
//				t:    t,
//			}
//			mfr := mockFsm{
//				t: t,
//			}
//			snapshotor := genSnapshot(t, snapshotDir, 1, tc.snap, 10, cl, &mf)
//			clr, ms, _, _, err := restoreSnapAndReplayWal(BabuzaConfig{
//				ClusterId:   100,
//				LocalPeerId: 1,
//				WalDir:      walDir,
//			}, snapshotor, tc.snap, &mfr, rs)
//			assert.Nil(t, err)
//			if tc.snap != nil {
//				assert.Equal(t, cl, clr)
//				assert.Equal(t, mf.data, mfr.data)
//
//				//fail
//				mfr.enableRestoreFail = true
//				_, _, _, _, err = restoreSnapAndReplayWal(BabuzaConfig{
//					ClusterId:   100,
//					LocalPeerId: 1,
//					WalDir:      walDir,
//				}, snapshotor, tc.snap, &mfr, rs)
//				assert.Equal(t, "fail", err.Error())
//			}
//			tc.expectEntsFunc(t, ms)
//		}
//	})
//	t.Run("config mismatch", func(t *testing.T) {
//		snapshotor := genSnapshot(t, snapshotDir, 1, nil, 10, nil, nil)
//		_, _, _, _, err = restoreSnapAndReplayWal(BabuzaConfig{
//			ClusterId:   100,
//			LocalPeerId: 2,
//			WalDir:      walDir,
//		}, snapshotor, nil, nil, rs)
//		assert.Equal(t, "bootstrap: cluster and local node id of the config is different from the wal. config(ClusterId(100),LocalNodeId(2)) wal(ClusterId(100),LocalNodeId(1))", err.Error())
//	})
//}
//

func genConfChangeEntry(index, nodeId uint64, confChangeType raftpb.ConfChangeType, result []raftpb.Entry) []raftpb.Entry {
	cc := raftpb.ConfChange{
		Type:    confChangeType,
		NodeID:  nodeId,
		Context: nil,
	}
	data, _ := cc.Marshal()
	result = append(result, raftpb.Entry{
		Term:  1,
		Index: index,
		Type:  raftpb.EntryConfChange,
		Data:  data,
	})
	return result
}

func TestBootstrap_ListRaftConfChangeAddNodeIds(t *testing.T) {

	walDir, err := ioutil.TempDir("", "bootstrap-wal")
	assert.Nil(t, err)
	defer os.RemoveAll(walDir)

	ws := babuzawal.NewWalManager(walDir, &logger.Mock{})
	expectIds := func() UInt64Slice {
		var result []raftpb.Entry
		result = genConfChangeEntry(1, 1, raftpb.ConfChangeAddNode, result)
		result = genConfChangeEntry(2, 1, raftpb.ConfChangeRemoveNode, result)
		result = genConfChangeEntry(3, 2, raftpb.ConfChangeAddNode, result)
		result = genConfChangeEntry(4, 3, raftpb.ConfChangeAddLearnerNode, result)
		result = genConfChangeEntry(5, 50, raftpb.ConfChangeUpdateNode, result)
		result = genConfChangeEntry(6, 51, raftpb.ConfChangeUpdateNode, result)
		_, w, err := ws.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 2,
		})
		assert.Nil(t, err)
		assert.Nil(t, w.Save(raftpb.HardState{
			Term:   1,
			Commit: 6,
		}, result))
		assert.Nil(t, w.Close())
		return UInt64Slice{2, 3}
	}()

	result, _, w, err := ws.ReplayWal(nil, false)
	assert.Nil(t, err)
	assert.Nil(t, w.Close())

	t.Run("snapshot is nil", func(t *testing.T) {
		idSlice, err := listRaftConfChangeAddNodeIds(nil, result)
		assert.Nil(t, err)
		assert.Equal(t, expectIds, idSlice)
	})

	t.Run("snapshot is not nil", func(t *testing.T) {
		voters := []uint64{51, 52}
		idSlice, err := listRaftConfChangeAddNodeIds(&raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{
					Voters: voters,
				},
			},
		}, result)
		assert.Nil(t, err)
		expectIds = append(expectIds, voters...)
		assert.Equal(t, expectIds, idSlice)
	})
}

func TestBootstrap_CreateRaftConfigChangeEntries(t *testing.T) {

	//TODO: live node is LearnerNode. learner node to voting node

	t.Run("live node in configuration", func(t *testing.T) {
		walDir, err := ioutil.TempDir("", "bootstrap-wal")
		assert.Nil(t, err)
		defer os.RemoveAll(walDir)

		ws := babuzawal.NewWalManager(walDir, &logger.Mock{})
		newLocalId := uint64(2)
		func() {
			var result []raftpb.Entry
			result = genConfChangeEntry(1, 1, raftpb.ConfChangeAddNode, result)
			result = genConfChangeEntry(2, 1, raftpb.ConfChangeRemoveNode, result)
			result = genConfChangeEntry(3, 2, raftpb.ConfChangeAddNode, result)
			result = genConfChangeEntry(4, 3, raftpb.ConfChangeAddLearnerNode, result)
			result = genConfChangeEntry(5, 4, raftpb.ConfChangeAddLearnerNode, result)
			result = genConfChangeEntry(6, 5, raftpb.ConfChangeAddNode, result)
			_, w, err := ws.CreateWal(babuzapb.WalMetadata{
				ClusterID:   100,
				LocalPeerID: newLocalId,
			})
			assert.Nil(t, err)
			assert.Nil(t, w.Save(raftpb.HardState{
				Term:   1,
				Commit: 5,
			}, result))
			assert.Nil(t, w.Close())
		}()

		result, _, w, err := ws.ReplayWal(nil, true)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())
		st := result.HardState()

		confChangeIds, err := listRaftConfChangeAddNodeIds(nil, result)
		assert.Nil(t, err)
		ents, err := createRaftConfigChangeEntries(newLocalId, "localhost:14200", confChangeIds, &st)
		assert.Nil(t, err)
		assert.Equal(t, ents[len(ents)-1].Index, st.Commit)
		assert.Equal(t, len(confChangeIds)-1, len(ents))
	})
	t.Run("removed node", func(t *testing.T) {
		walDir, err := ioutil.TempDir("", "bootstrap-wal")
		assert.Nil(t, err)
		defer os.RemoveAll(walDir)

		ws := babuzawal.NewWalManager(walDir, &logger.Mock{})
		removePeerId := uint64(1)
		func() {
			var result []raftpb.Entry
			result = genConfChangeEntry(1, 1, raftpb.ConfChangeAddNode, result)
			result = genConfChangeEntry(2, 1, raftpb.ConfChangeRemoveNode, result)
			result = genConfChangeEntry(3, 2, raftpb.ConfChangeAddNode, result)
			result = genConfChangeEntry(4, 3, raftpb.ConfChangeAddLearnerNode, result)
			result = genConfChangeEntry(5, 4, raftpb.ConfChangeAddLearnerNode, result)
			result = genConfChangeEntry(6, 5, raftpb.ConfChangeAddNode, result)
			_, w, err := ws.CreateWal(babuzapb.WalMetadata{
				ClusterID:   100,
				LocalPeerID: removePeerId,
			})
			assert.Nil(t, err)
			assert.Nil(t, w.Save(raftpb.HardState{
				Term:   1,
				Commit: 3,
			}, result))
			assert.Nil(t, w.Close())
		}()

		result, _, w, err := ws.ReplayWal(nil, true)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())
		st := result.HardState()

		confChangeIds, err := listRaftConfChangeAddNodeIds(nil, result)
		assert.Nil(t, err)
		ents, err := createRaftConfigChangeEntries(removePeerId, "localhost:14200", confChangeIds, &st)
		assert.Nil(t, err)
		assert.Equal(t, ents[len(ents)-1].Index, st.Commit)
		assert.Equal(t, 2, len(ents))
	})
}
