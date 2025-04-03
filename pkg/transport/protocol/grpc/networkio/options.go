package networkio

type Options struct {
	RecvMsgSize int
}

func DefaultOptions() Options {
	return Options{
		// Default value for RecvMsgSize
	}
}

func SetOptionsRecvMsgSize(size int) func(*Options) {
	return func(o *Options) {
		o.RecvMsgSize = size
	}
}
