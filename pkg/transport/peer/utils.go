package peer

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func releaseMultiRaftMessageBuffers(freeBuffer []*babuzapb.MultiRaftMessage) []*babuzapb.MultiRaftMessage {
	for index, msg := range freeBuffer {
		switch msg.Message.Type {
		case raftpb.MsgHeartbeat:
			ReleaseMultiRaftHeartbeatMessage(msg)
		case raftpb.MsgHeartbeatResp:
			ReleaseMultiRaftHeartbeatResponseMessage(msg)
		default:
			ReleaseMultiRaftMessage(msg)
		}
		freeBuffer[index] = nil
	}
	freeBuffer = freeBuffer[:0]
	return freeBuffer
}

func releaseRaftMessageBuffers(freeBuffer []raftpb.Message) []raftpb.Message {
	for _, msg := range freeBuffer {
		msg.Entries = nil
		msg.Context = nil
	}
	freeBuffer = freeBuffer[:0]
	return freeBuffer

}
