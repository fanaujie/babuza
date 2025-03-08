package frame

import (
	"hash/crc32"
)

var (
	Crc32Table = crc32.MakeTable(crc32.Castagnoli)
)

const (
	BatchMsgType         MessageType = 1
	SnapshotMsgType      MessageType = 2
	ClusterPeersReqType  MessageType = 3
	ClusterPeersResType  MessageType = 4
	PubAppServiceReqType MessageType = 5
	PubAppServiceResType MessageType = 6
)

type Message interface {
	MarshalTo(dAtA []byte) (int, error)
	Size() int
}
