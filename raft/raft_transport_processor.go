package raft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
)

type transportProcessor struct {
	*Raft
}

func (d *transportProcessor) ProcessBatchMessage(msg babuzapb.BatchMessage) {
	for i := 0; i < len(msg.Messages); i++ {
		if err := d.raftNode.Step(context.TODO(), msg.Messages[i]); err != nil {
			d.logger.Warningf("raft[id=%d] failed to step. err(%s)", d.cluster.LocalPeerID(), err.Error())
		}
	}
}

func (d *transportProcessor) ProcessSnapshotMessage(msg babuzapb.SnapshotMessage) {
	if bFinish, err := d.storage.ReceiveSnapshotMessage(msg); err != nil {
		d.logger.Warningf("raft[id=%d] failed to receiveSnapshotMessage. err(%s)", d.cluster.LocalPeerID(), err.Error())
	} else if bFinish {
		d.logger.Infof("raft[id=%d] received finish snapshot message (snapshot index=%d)", d.cluster.LocalPeerID(), msg.Index)
		if err = d.raftNode.Step(context.TODO(), *msg.FinishMessage); err != nil {
			d.logger.Warningf("raft[id=%d] step err(%s)", d.cluster.LocalPeerID(), err.Error())
		}
	}
}

func (d *transportProcessor) GetClusterPeersRequest(req babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{
		Status:  babuzapb.SUCCESS,
		Message: "success",
		Peers:   d.cluster.Peers()}
}

func (d *transportProcessor) PublishApplicationServiceRequest(req babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	res := babuzapb.PublishApplicationServiceResponse{}

	normalRequest := babuzapb.NormalRequest{
		Context: babuzapb.RequestContext{
			ReplyId: req.ProposalReplyId,
		},
		PubAppService: &babuzapb.PubAppServiceRequest{
			PubServicePeerId:    req.FromId,
			AppServiceAddresses: req.AppServiceAddresses,
		},
	}

	data, err := normalRequest.Marshal()
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}

	if err = d.raftNode.Propose(context.Background(), data); err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
	}
	res.Status = babuzapb.SUCCESS
	res.Message = "success"
	return res
}

func (d *transportProcessor) ReportUnreachable(id uint64) {
	d.raftNode.ReportUnreachable(id)
}
func (d *transportProcessor) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	d.status.AddInflightSnapshots(-1)
	if status == raft.SnapshotFinish {
		d.logger.Infof("raft[id=%d] finish to send snapshot to peer(id=%d)", d.cluster.LocalPeerID(), id)
	}
	d.raftNode.ReportSnapshot(id, status)
}

func (d *transportProcessor) CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error) {
	snapReader, err := d.storage.CreateSnapshotReader(snapshotIndex)
	if err != nil {
		return nil, err
	}
	d.status.AddInflightSnapshots(1)
	return snapReader, nil
}
