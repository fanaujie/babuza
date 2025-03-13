package protocol

import (
	"github.com/fanaujie/babuza/ibabuza"
	raftHttp "github.com/fanaujie/babuza/pkg/transport/protocol/http"
	"net/http"
	"time"
)

type Http struct {
	config  ibabuza.TransportConfig
	options raftHttp.Options
	logger  ibabuza.Logger
	client  *http.Client
}

func DefaultHttpOptions() raftHttp.Options {
	return raftHttp.Options{
		WriteDeadline:   time.Second * 5,
		ReadDeadline:    time.Second * 5,
		ShutdownTimeout: time.Second * 5,
	}
}

type SetHttpOptions func(opt *raftHttp.Options)

func SetHttpOptsWithWriteDeadline(d time.Duration) SetHttpOptions {
	return func(opt *raftHttp.Options) {
		opt.WriteDeadline = d
	}
}

func SetHttpOptsWithReadDeadline(d time.Duration) SetHttpOptions {
	return func(opt *raftHttp.Options) {
		opt.ReadDeadline = d
	}
}

func SetHttpOptsWithShutdownTimeout(d time.Duration) SetHttpOptions {
	return func(opt *raftHttp.Options) {
		opt.ShutdownTimeout = d
	}
}

func NewHttp(logger ibabuza.Logger, setOpts ...SetHttpOptions) *Http {
	opts := DefaultHttpOptions()
	for _, s := range setOpts {
		s(&opts)
	}
	logger.Infof("http protocol: creating http protocol")
	return &Http{
		options: opts,
		logger:  logger,
	}
}

func (h *Http) Setup(cfg ibabuza.TransportConfig) error {
	h.config = cfg
	client, err := raftHttp.NewClient(h.config.TLSConfig, h.options)
	if err != nil {
		return err
	}
	h.client = client
	return nil
}

func (h *Http) CreateServer(handler ibabuza.RaftMessageHandler) (ibabuza.TransportServer, error) {
	return raftHttp.NewRaftMsgServer(h.config, h.options, handler, h.logger), nil
}

func (h *Http) CreateClient(resolver ibabuza.TransportResolver) (ibabuza.TransportClient, error) {
	return raftHttp.NewRaftMsgClient(h.client, h.options, resolver, h.config.TLSConfig.EnableTLS), nil
}

func (h *Http) Close() error {
	return nil
}
