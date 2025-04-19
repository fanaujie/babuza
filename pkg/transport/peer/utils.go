package peer

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func releaseBatchMessageBuffers(freeBuffer []babuzapb.BatchMessage) []babuzapb.BatchMessage {
	for _, msg := range freeBuffer {
		msg.Messages = nil
	}
	freeBuffer = freeBuffer[:0]
	return freeBuffer
}

func releaseMultiRaftMessageBuffers(freeBuffer []babuzapb.MultiRaftMessage) []babuzapb.MultiRaftMessage {
	for _, msg := range freeBuffer {
		msg.Message.Entries = nil
		msg.Message.Context = nil
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
