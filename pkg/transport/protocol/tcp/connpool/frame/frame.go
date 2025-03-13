package frame

const (
	CrcOffset      = 4
	HeaderSize     = 8
	MsgSizeShift   = 8
	MsgSizeMask    = ^uint32(MsgSizeShift)
	MsgTypeMask    = 0xff
	MaxMessageSize = 0xffffff
)

func EncodeSize(msgSize int) int {
	if msgSize > MaxMessageSize {
		panic("message size exceeds max message size")
	}
	return HeaderSize + msgSize
}
