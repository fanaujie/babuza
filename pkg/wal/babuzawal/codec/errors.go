package codec

import "errors"

var (
	ErrNilPool            = errors.New("pool can not be nil")
	ErrNilReader          = errors.New("reader can not be nil")
	ErrNilWriter          = errors.New("writer can not be nil")
	ErrLogSizeExceedLimit = errors.New("message size exceeds limit")
	ErrNilHandler         = errors.New("handler can not be nil")
)
