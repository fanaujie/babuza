package protocol

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"time"
)

type Tcp struct {
	network tcp.NetworkIO
	config  ibabuza.TransportConfig
	options tcp.Options
	logger  ibabuza.Logger
}

func defaultTcpOptions() tcp.Options {
	return tcp.Options{
		WriteDeadline: time.Second * 5,
		ReadDeadline:  time.Second * 5,
		MaxBufferSize: 4 * 1024 * 1024,
	}
}

type SetTcpOptions func(opt *tcp.Options)

func SetTcpOptsWithWriteDeadline(d time.Duration) SetTcpOptions {
	return func(opt *tcp.Options) {
		opt.WriteDeadline = d
	}
}

func SetTcpOptsWithReadDeadline(d time.Duration) SetTcpOptions {
	return func(opt *tcp.Options) {
		opt.ReadDeadline = d
	}
}

func SetTcpOptsWithMaxBufferSize(d int) SetTcpOptions {
	return func(opt *tcp.Options) {
		opt.MaxBufferSize = d
	}
}

func NewTcp(network tcp.NetworkIO, logger ibabuza.Logger, setOpts ...SetTcpOptions) *Tcp {
	opts := defaultTcpOptions()
	for _, s := range setOpts {
		s(&opts)
	}
	logger.Infof("tcp protocol: creating tcp protocol")
	return &Tcp{
		network: network,
		options: opts,
		logger:  logger,
	}
}

func (t *Tcp) Setup(cfg ibabuza.TransportConfig) error {
	t.config = cfg
	return nil
}

func (t *Tcp) CreateServer(handler ibabuza.RaftMessageHandler) (ibabuza.TransportServer, error) {
	return tcp.NewRaftMsgServer(t.config, t.options, t.network, handler, t.logger), nil
}

func (t *Tcp) Dial(ctx context.Context, endpoint string) (ibabuza.TransportClient, error) {
	//TODO: add dial timeout
	conn, err := t.network.Dial(t.config.TLSConfig, t.config.PeerId, endpoint)
	if err != nil {
		return nil, err
	}
	return tcp.NewRaftMsgClient(conn, t.options), nil
}
