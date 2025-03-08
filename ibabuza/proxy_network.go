package ibabuza

type ProxyConfig struct {
	Id      uint64
	InAddr  string
	OutAddr string
	TLSConfig
}

type ProxyNetwork interface {
	AddProxy(config ProxyConfig) error
	DeleteProxy(proxyId uint64) error
	ConnectProxy(proxyId uint64) error
	DisconnectProxy(proxyId uint64) error
	SetPartition(proxyIds []uint64) error
	IsProxyConnected(proxyId uint64) bool
	TeardownNetwork() error
	ConnectProxiesIds() []uint64
	DisconnectProxiesIds() []uint64
}
