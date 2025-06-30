package frame

import (
	"hash/crc32"
)

type MessageType int32

var (
	Crc32Table = crc32.MakeTable(crc32.Castagnoli)
)

const (
	BatchMsgType         MessageType = 1
	SnapshotMsgReqType   MessageType = 2
	SnapshotMsgResType   MessageType = 3
	ClusterPeersReqType  MessageType = 4
	ClusterPeersResType  MessageType = 5
	PubAppServiceReqType MessageType = 6
	PubAppServiceResType MessageType = 7
)

type Message interface {
	MarshalTo(dAtA []byte) (int, error)
	Size() int
}
