package raft

import (
	"context"
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
)

type transportProcessor struct {
	*Raft
}

func (d *transportProcessor) ProcessBatchMessage(msg babuzapb.BatchMessage) {
	for i := 0; i < len(msg.Messages); i++ {
		if err := d.validateRequest(msg.ClusterID, msg.Messages[i].To); err != nil {
			continue
		}
		if msg.Messages[i].To != d.cluster.LocalPeerID() {
			d.logger.Warningf("raft[id=%d] received batch message with local peer id(%d)", d.cluster.LocalPeerID(), msg.Messages[i].To)
			continue
		}
		if err := d.raftNode.Step(context.TODO(), msg.Messages[i]); err != nil {
			d.logger.Warningf("raft[id=%d] failed to step. err(%s)", d.cluster.LocalPeerID(), err.Error())
		}
	}
}

func (d *transportProcessor) ProcessSnapshotMessage(msg babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
	if err := d.validateRequest(msg.ClusterID, msg.To); err != nil {
		return babuzapb.SnapshotMessageResponse{
			Status:  babuzapb.REJECTED,
			Message: err.Error(),
		}
	}
	switch msg.Type {
	case babuzapb.SnapshotMessageType_Metadata:
		if err := d.storage.ProcessMetadataSnapshotMessage(msg); err != nil {
			d.logger.Warningf("raft[id=%d] failed to process snapshot metadata. err(%s)",
				d.cluster.LocalPeerID(), err.Error())
			return babuzapb.SnapshotMessageResponse{
				Status:  babuzapb.FAILED,
				Message: "Failed to receive snapshot metadata: " + err.Error(),
			}
		}
	case babuzapb.SnapshotMessageType_Chunk:
		if err := d.storage.ProcessChunkSnapshotMessage(msg); err != nil {
			d.logger.Warningf("raft[%d] failed to process snapshot chunk. err(%s)",
				d.cluster.LocalPeerID(), err.Error())
			return babuzapb.SnapshotMessageResponse{
				Status:  babuzapb.FAILED,
				Message: "Failed to receive snapshot chunk: " + err.Error(),
			}
		}
	case babuzapb.SnapshotMessageType_Finish:
		if err := d.storage.ProcessFinishSnapshotMessage(msg); err != nil {
			d.logger.Warningf("raft[%d] failed to process snapshot finish. err(%s)",
				d.cluster.LocalPeerID(), err.Error())
			return babuzapb.SnapshotMessageResponse{
				Status:  babuzapb.FAILED,
				Message: "Failed to finish snapshot: " + err.Error(),
			}
		} else {
			if err = d.raftNode.Step(context.Background(), msg.FinishMessage); err != nil {
				d.logger.Warningf("raft[%d] failed to step finish message. err(%s)",
					d.cluster.LocalPeerID(), err.Error())
				return babuzapb.SnapshotMessageResponse{
					Status:  babuzapb.FAILED,
					Message: "Failed to step finish message: " + err.Error(),
				}
			}
		}
	default:
		d.logger.Warningf("raft[%d] unknown snapshot message type %d",
			d.cluster.LocalPeerID(), msg.Type)
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
	if err := d.validateRequest(req.ClusterID, req.To); err != nil {
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.REJECTED,
			Message: err.Error(),
		}
	}
	return babuzapb.GetClusterPeersResponse{
		Status:  babuzapb.SUCCESS,
		Message: "success",
		Peers:   d.cluster.Peers()}
}

func (d *transportProcessor) PublishApplicationService(req babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	if err := d.validateRequest(req.ClusterID, req.To); err != nil {
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.REJECTED,
			Message: err.Error(),
		}
	}
	normalRequest := babuzapb.NormalRequest{
		Context: babuzapb.RequestContext{
			ReplyID: req.ProposalReplyID,
		},
		PubAppService: &babuzapb.PubAppServiceRequest{
			PubServicePeerID:    req.From,
			AppServiceAddresses: req.AppServiceAddresses,
		},
	}

	data, err := normalRequest.Marshal()
	if err != nil {
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error()}
	}

	if err = d.raftNode.Propose(context.Background(), data); err != nil {
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error()}
	}
	return babuzapb.PublishApplicationServiceResponse{
		Status:  babuzapb.SUCCESS,
		Message: "success"}
}

func (d *transportProcessor) ReportUnreachable(peerID uint64) {
	d.raftNode.ReportUnreachable(peerID)
}
func (d *transportProcessor) ReportSnapshot(peerID uint64, status raft.SnapshotStatus) {
	d.status.AddInflightSnapshots(-1)
	d.metricsCollector.DecrementInflightSnapshots()
	if status == raft.SnapshotFinish {
		d.logger.Infof("raft[id=%d] finish to send snapshot to peer(id=%d)", d.cluster.LocalPeerID(), peerID)
	}
	d.raftNode.ReportSnapshot(peerID, status)
}

func (d *transportProcessor) CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error) {
	snapReader, err := d.storage.CreateSnapshotReader(snapshotIndex)
	if err != nil {
		return nil, err
	}
	d.status.AddInflightSnapshots(1)
	d.metricsCollector.IncrementInflightSnapshots()
	return snapReader, nil
}

func (d *transportProcessor) validateRequest(clusterID uint64, toID uint64) error {
	if clusterID != d.cluster.ClusterID() {
		d.logger.Warningf("raft[%d] cluster id %d not match %d",
			d.cluster.LocalPeerID(), clusterID, d.cluster.ClusterID())
		return errors.New("cluster id not match")
	}
	if toID != d.cluster.LocalPeerID() {
		d.logger.Warningf("raft[%d] received message with different peer id(%d)",
			d.cluster.LocalPeerID(), toID)
		return errors.New("peer id not match")
	}
	return nil
}
