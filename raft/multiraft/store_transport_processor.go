package multiraft

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type transportProcessor struct {
	*Store
}

func (d *transportProcessor) ProcessMultiRaftMessage(batchMsg babuzapb.MultiRaftBatchMessage) {
	for _, msg := range batchMsg.Messages {
		switch msg.Message.Type {
		case raftpb.MsgHeartbeat:
			for _, m := range msg.HeartbeatMessages {
				groupID := ibabuza.RaftGroupID(m.GroupID)
				_, err := d.validateRequest(groupID, batchMsg.ClusterID, msg.Message.To)
				if err != nil {
					return
				}
				if err = d.enqueueStep(groupID, raftpb.Message{
					Type:    raftpb.MsgHeartbeat,
					To:      m.ToPeerID,
					From:    m.FromPeerID,
					Term:    m.Term,
					Commit:  m.Commit,
					Context: m.Context,
				}); err != nil {
					d.logger.Warningf("Store[%d] ProcessBatchMessage[heartbeat] groupID[%d] enqueue step error: %v", d.config.StoreID, groupID, err)
				}
			}
		case raftpb.MsgHeartbeatResp:
			for _, m := range msg.HeartbeatResponseMessages {
				groupID := ibabuza.RaftGroupID(m.GroupID)
				_, err := d.validateRequest(groupID, batchMsg.ClusterID, msg.Message.To)
				if err != nil {
					return
				}
				if err = d.enqueueStep(groupID, raftpb.Message{
					Type:    raftpb.MsgHeartbeatResp,
					To:      m.ToPeerID,
					From:    m.FromPeerID,
					Term:    m.Term,
					Commit:  m.Commit,
					Context: m.Context,
				}); err != nil {
					d.logger.Warningf("Store[%d] ProcessBatchMessage[heartbeat response] groupID[%d] enqueue step error: %v", d.config.StoreID, groupID, err)
				}
			}
		default:
			groupID := ibabuza.RaftGroupID(msg.GroupID)
			_, err := d.validateRequest(groupID, batchMsg.ClusterID, msg.Message.To)
			if err != nil {
				return
			}
			if err = d.enqueueStep(groupID, msg.Message); err != nil {
				d.logger.Warningf("Store[%d] ProcessBatchMessage groupID[%d] enqueue step error: %v", d.config.StoreID, groupID, err)
			}
		}
	}

}

func (d *transportProcessor) ProcessSnapshotMessage(msg babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
	groupID := ibabuza.RaftGroupID(msg.GroupID)
	r, err := d.validateRequest(groupID, msg.ClusterID, msg.To)
	if err != nil {
		return babuzapb.SnapshotMessageResponse{
			Status:  babuzapb.REJECTED,
			Message: err.Error(),
		}
	}
	switch msg.Type {
	case babuzapb.SnapshotMessageType_Metadata:
		if err = r.storage.ProcessMetadataSnapshotMessage(msg); err != nil {
			d.logger.Warningf("Store[%d] groupID[%d] failed to process snapshot metadata. err(%s)",
				r.cluster.LocalPeerID(), msg.GroupID, err.Error())
			return babuzapb.SnapshotMessageResponse{
				Status:  babuzapb.FAILED,
				Message: "Failed to receive snapshot metadata: " + err.Error(),
			}
		}
	case babuzapb.SnapshotMessageType_Chunk:
		if err = r.storage.ProcessChunkSnapshotMessage(msg); err != nil {
			r.logger.Warningf("Store[%d] groupID[%d] failed to process snapshot chunk. err(%s)",
				r.cluster.LocalPeerID(), msg.GroupID, err.Error())
			return babuzapb.SnapshotMessageResponse{
				Status:  babuzapb.FAILED,
				Message: "Failed to receive snapshot chunk: " + err.Error(),
			}
		}
	case babuzapb.SnapshotMessageType_Finish:
		if err = r.storage.ProcessFinishSnapshotMessage(msg); err != nil {
			r.logger.Warningf("Store[%d] groupID[%d] failed to process snapshot finish. err(%s)",
				r.cluster.LocalPeerID(), msg.GroupID, err.Error())
			return babuzapb.SnapshotMessageResponse{
				Status:  babuzapb.FAILED,
				Message: "Failed to finish snapshot: " + err.Error(),
			}
		} else {
			if err = d.enqueueStep(groupID, msg.FinishMessage); err != nil {
				r.logger.Warningf("Store[%d] groupID[%d] failed to enqueue finish message. err(%s)",
					r.cluster.LocalPeerID(), msg.GroupID, err.Error())
				return babuzapb.SnapshotMessageResponse{
					Status:  babuzapb.FAILED,
					Message: "Failed to enqueue finish message: " + err.Error(),
				}
			}
		}
	default:
		r.logger.Warningf("Store[%d] groupID[%d] unknown snapshot message type %d",
			r.cluster.LocalPeerID(), msg.GroupID, msg.Type)
		return babuzapb.SnapshotMessageResponse{
			Status:  babuzapb.REJECTED,
			Message: "Unknown snapshot message type",
		}
	}
	return babuzapb.SnapshotMessageResponse{
		Status:  babuzapb.SUCCESS,
		Message: "success",
	}
}

func (d *transportProcessor) GetClusterPeer(req babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	groupID := ibabuza.RaftGroupID(req.GroupID)
	r, err := d.validateRequest(groupID, req.ClusterID, req.To)
	if err != nil {
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.REJECTED,
			Message: err.Error(),
		}
	}
	return babuzapb.GetClusterPeersResponse{
		Status:  babuzapb.SUCCESS,
		Message: "success",
		Peers:   r.cluster.Peers()}
}

func (d *transportProcessor) PublishApplicationService(req babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{}
}

func (d *transportProcessor) ReportUnreachable(groupID ibabuza.RaftGroupID, nodeID uint64) {
	r, err := d.getReplica(groupID)
	if err != nil {
		d.logger.Warningf("Store[%d] ReportUnreachable groupID[%d] get replica error: %v", d.config.StoreID, groupID, err)
		return
	}
	if err = r.ReportUnreachable(nodeID); err != nil {
		d.logger.Warningf("Store[%d] ReportUnreachable groupID[%d] enqueue unreachable error: %v", d.config.StoreID, groupID, err)
	}

}
func (d *transportProcessor) ReportSnapshot(groupID ibabuza.RaftGroupID, nodeID uint64, status raft.SnapshotStatus) {
	r, err := d.getReplica(groupID)
	if err != nil {
		d.logger.Warningf("Store[%d] ReportSnapshot groupID[%d] get replica error: %v", d.config.StoreID, groupID, err)
		return
	}
	if err = r.ReportSnapshot(nodeID, status); err != nil {
		d.logger.Warningf("Store[%d] ReportSnapshot groupID[%d] enqueue snapshot error: %v", d.config.StoreID, groupID, err)
	}
}

func (d *transportProcessor) CreateSnapshotReader(groupID ibabuza.RaftGroupID, snapshotIndex uint64) (ibabuza.SnapshotReader, error) {
	r, err := d.getReplica(groupID)
	if err != nil {
		return nil, err
	}
	snapReader, err := r.storage.CreateSnapshotReader(snapshotIndex)
	if err != nil {
		return nil, err
	}
	return snapReader, nil
}

func (d *transportProcessor) validateRequest(groupID ibabuza.RaftGroupID, clusterID uint64, peerID uint64) (*replica, error) {
	r, err := d.getReplica(groupID)
	if err != nil {
		d.logger.Warningf("Store[%d] groupID[%d] get replica error: %v",
			d.config.StoreID, groupID, err)
		return nil, err
	}

	if clusterID != r.cluster.ClusterID() {
		d.logger.Warningf("Store[%d] groupID[%d] cluster id %d not match %d",
			d.config.StoreID, groupID, clusterID, r.cluster.ClusterID())
		return nil, errors.New("cluster id not match")
	}

	if peerID != r.cluster.LocalPeerID() {
		d.logger.Warningf("Store[%d] groupID[%d] received message with different peer id(%d)",
			d.config.StoreID, groupID, peerID)
		return nil, errors.New("peer id not match")
	}
	return r, nil
}
