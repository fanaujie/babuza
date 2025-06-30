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
