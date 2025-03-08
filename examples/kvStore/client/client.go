package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/fanaujie/babuza/examples/kvStore/server/api"
	"github.com/fanaujie/babuza/examples/kvStore/server/request"
	"github.com/fanaujie/babuza/examples/kvStore/server/response"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"
)

const (
	Set    uint64 = 1
	Append uint64 = 2
	Delete uint64 = 3
	Read   uint64 = 4
)

var (
	ErrClientClosed = errors.New("client closed")
)

type ClusterPeer struct {
	Id               uint64
	KvServiceAddress string
}

type Config struct {
	EnableTLS             bool
	KvStoreClusterMembers []ClusterPeer
	AutoSyncInterval      time.Duration
}

type KvStoreClient struct {
	config         Config
	session        ISession
	proxy          *proxy
	closeCh        chan struct{}
	autoSyncDoneCh chan struct{}
	mu             sync.Mutex
}

func CreateKvStoreClient(cfg Config, session ISession) (*KvStoreClient, error) {
	var err error
	c := &KvStoreClient{
		config:         cfg,
		closeCh:        make(chan struct{}, 1),
		autoSyncDoneCh: make(chan struct{}, 1),
	}
	sort.Sort(clusterPeers(cfg.KvStoreClusterMembers))
	c.proxy = newProxy(cfg.EnableTLS, cfg.KvStoreClusterMembers)
	if err = session.Register(c.proxy, api.SessionsHttpPath); err != nil {
		return nil, err
	}
	c.session = session
	go func() {
		defer func() {
			c.autoSyncDoneCh <- struct{}{}
		}()
		if c.config.AutoSyncInterval == 0 {
			return
		}
		for {
			select {
			case <-c.closeCh:
				return
			case <-time.After(c.config.AutoSyncInterval):
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				_ = c.Sync(ctx)
				cancel()
			}
		}
	}()

	return c, nil
}

func (c *KvStoreClient) Sync(ctx context.Context) error {
	if c.isClosed() {
		return ErrClientClosed
	}
	res, err := c.GetClusterConfiguration(ctx)
	if err != nil {
		return err
	}
	var peer []ClusterPeer
	for _, p := range res.Peers {
		peer = append(c.config.KvStoreClusterMembers, ClusterPeer{
			Id:               p.Id,
			KvServiceAddress: p.AppServiceAddress,
		})
	}
	sort.Sort(clusterPeers(peer))
	c.proxy.SetClusterPeers(peer)
	return nil
}

func (c *KvStoreClient) Close() error {
	close(c.closeCh)
	<-c.autoSyncDoneCh
	return nil
}

func (c *KvStoreClient) Session() Session {
	return c.session.ClientSession()
}

func (c *KvStoreClient) Join(ctx context.Context, peerId uint64, raftListenAddr string, isLearner bool) error {
	if c.isClosed() {
		return ErrClientClosed
	}
	s := c.session.ClientSession()
	req := request.JoinPeerRequest{
		SessionID:                         s.SessionID,
		SequenceNumber:                    s.SequenceNumber,
		LowestSequenceNumberNotYetReplied: s.LowestSequenceNumberNotYetReplied,
		RaftPeerId:                        peerId,
		RaftListenAddr:                    raftListenAddr,
		IsLearner:                         isLearner,
	}
	var res response.ClusterConfigurationResponse
	if err := c.proxy.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = api.ClusterPeersHttpPath
		b, jErr := json.Marshal(&req)
		if jErr != nil {
			return nil, jErr
		}
		r, jErr := http.NewRequestWithContext(reqCtx, http.MethodPost, leaderUrl.String(), bytes.NewReader(b))
		if jErr != nil {
			return nil, jErr
		}
		r.Header.Set("Content-Type", "application/json")
		return r, nil
	}, &res); err != nil {
		return err
	}

	return nil
}

func (c *KvStoreClient) Update(ctx context.Context, peerId uint64, raftListenAddr string) error {
	if c.isClosed() {
		return ErrClientClosed
	}
	s := c.session.ClientSession()
	req := request.UpdatePeerRequest{
		SessionID:                         s.SessionID,
		SequenceNumber:                    s.SequenceNumber,
		LowestSequenceNumberNotYetReplied: s.LowestSequenceNumberNotYetReplied,
		RaftPeerId:                        peerId,
		RaftListenAddr:                    raftListenAddr,
	}
	var res response.ClusterConfigurationResponse
	if err := c.proxy.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = api.ClusterPeersHttpPath
		b, jErr := json.Marshal(&req)
		if jErr != nil {
			return nil, jErr
		}
		r, jErr := http.NewRequestWithContext(reqCtx, http.MethodPut, leaderUrl.String(), bytes.NewReader(b))
		if jErr != nil {
			return nil, jErr
		}
		r.Header.Set("Content-Type", "application/json")
		return r, nil
	}, &res); err != nil {
		return err
	}
	return nil
}

func (c *KvStoreClient) Remove(ctx context.Context, peerId uint64) error {
	if c.isClosed() {
		return ErrClientClosed
	}
	s := c.session.ClientSession()
	req := request.RemovePeerRequest{
		SessionID:                         s.SessionID,
		SequenceNumber:                    s.SequenceNumber,
		LowestSequenceNumberNotYetReplied: s.LowestSequenceNumberNotYetReplied,
		RaftPeerId:                        peerId,
	}
	var res response.ClusterConfigurationResponse
	if err := c.proxy.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = api.ClusterPeersHttpPath
		b, jErr := json.Marshal(&req)
		if jErr != nil {
			return nil, jErr
		}
		r, jErr := http.NewRequestWithContext(reqCtx, http.MethodDelete, leaderUrl.String(), bytes.NewReader(b))
		if jErr != nil {
			return nil, jErr
		}
		r.Header.Set("Content-Type", "application/json")
		return r, nil
	}, &res); err != nil {
		return err
	}
	return nil
}

func (c *KvStoreClient) PromoteLearner(ctx context.Context, peerId uint64) error {
	if c.isClosed() {
		return ErrClientClosed
	}
	s := c.session.ClientSession()
	req := request.PromoteLearnerRequest{
		SessionID:                         s.SessionID,
		SequenceNumber:                    s.SequenceNumber,
		LowestSequenceNumberNotYetReplied: s.LowestSequenceNumberNotYetReplied,
		RaftPeerId:                        peerId,
	}
	var res response.ClusterConfigurationResponse
	if err := c.proxy.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = api.PromoteLearnerHttpPath
		b, jErr := json.Marshal(&req)
		if jErr != nil {
			return nil, jErr
		}
		r, jErr := http.NewRequestWithContext(reqCtx, http.MethodPut, leaderUrl.String(), bytes.NewReader(b))
		if jErr != nil {
			return nil, jErr
		}
		r.Header.Set("Content-Type", "application/json")
		return r, nil
	}, &res); err != nil {
		return err
	}
	return nil
}

func (c *KvStoreClient) TransferLeader(ctx context.Context, transferee uint64) error {
	if c.isClosed() {
		return ErrClientClosed
	}
	req := request.TransferLeaderRequest{
		Transferee: transferee,
	}
	var res response.TransferLeaderResponse
	if err := c.proxy.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = api.TransferLeaderHttpPath
		b, jErr := json.Marshal(&req)
		if jErr != nil {
			return nil, jErr
		}
		r, jErr := http.NewRequestWithContext(reqCtx, http.MethodPut, leaderUrl.String(), bytes.NewReader(b))
		if jErr != nil {
			return nil, jErr
		}
		r.Header.Set("Content-Type", "application/json")
		return r, nil
	}, &res); err != nil {
		return err
	}
	return nil
}

func (c *KvStoreClient) GetClusterConfiguration(ctx context.Context) (*response.ClusterConfigurationResponse, error) {
	if c.isClosed() {
		return nil, ErrClientClosed
	}
	var res response.ClusterConfigurationResponse
	if err := c.proxy.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = api.ClusterPeersHttpPath
		return http.NewRequestWithContext(reqCtx, http.MethodGet, leaderUrl.String(), nil)
	}, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *KvStoreClient) Get(ctx context.Context, key string) (*response.KvStoreResponse, error) {
	if c.isClosed() {
		return nil, ErrClientClosed
	}
	var res response.KvStoreResponse
	if err := c.proxy.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = api.KvHttpPath
		return makeGetKvRequest(reqCtx, leaderUrl, key)
	}, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *KvStoreClient) Set(ctx context.Context, key, value string) (*response.KvStoreResponse, error) {
	if c.isClosed() {
		return nil, ErrClientClosed
	}
	s := c.session.ClientSession()
	req := request.KvStoreSetRequest{
		SessionID:                         s.SessionID,
		SequenceNumber:                    s.SequenceNumber,
		LowestSequenceNumberNotYetReplied: s.LowestSequenceNumberNotYetReplied,
		Key:                               key,
		Value:                             value,
	}
	var res response.KvStoreResponse
	if err := c.proxy.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = api.KvHttpPath
		return makeSetKvRequest(reqCtx, leaderUrl, &req)
	}, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

func (c *KvStoreClient) Append(ctx context.Context, key, value string) (*response.KvStoreResponse, error) {
	if c.isClosed() {
		return nil, ErrClientClosed
	}
	s := c.session.ClientSession()
	req := request.KvStoreAppendRequest{
		SessionID:                         s.SessionID,
		SequenceNumber:                    s.SequenceNumber,
		LowestSequenceNumberNotYetReplied: s.LowestSequenceNumberNotYetReplied,
		Key:                               key,
		Value:                             value,
	}
	var res response.KvStoreResponse
	if err := c.proxy.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = api.KvHttpPath
		return makeAppendKvRequest(reqCtx, leaderUrl, &req)
	}, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

func (c *KvStoreClient) Delete(ctx context.Context, key string) (*response.KvStoreResponse, error) {
	if c.isClosed() {
		return nil, ErrClientClosed
	}
	s := c.session.ClientSession()
	req := request.KvStoreDeleteRequest{
		SessionID:                         s.SessionID,
		SequenceNumber:                    s.SequenceNumber,
		LowestSequenceNumberNotYetReplied: s.LowestSequenceNumberNotYetReplied,
		Key:                               key,
	}
	var res response.KvStoreResponse
	if err := c.proxy.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = api.KvHttpPath
		return makeDeleteKvRequest(reqCtx, leaderUrl, &req)
	}, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

func (c *KvStoreClient) DirectKvStore(ctx context.Context, peerId uint64, command uint64, key, value string) (*response.KvStoreResponse, error) {
	s := c.session.ClientSession()
	var res response.KvStoreResponse
	var err error
	switch command {
	case Set:
		req := request.KvStoreSetRequest{
			SessionID:                         s.SessionID,
			SequenceNumber:                    s.SequenceNumber,
			LowestSequenceNumberNotYetReplied: s.LowestSequenceNumberNotYetReplied,
			Key:                               key,
			Value:                             value,
		}
		err = c.SendRequestWithPeerId(peerId, func(leaderUrl url.URL) (*http.Request, error) {
			leaderUrl.Path = api.KvHttpPath
			return makeSetKvRequest(ctx, leaderUrl, &req)
		}, &res)
	case Append:
		req := request.KvStoreAppendRequest{
			SessionID:                         s.SessionID,
			SequenceNumber:                    s.SequenceNumber,
			LowestSequenceNumberNotYetReplied: s.LowestSequenceNumberNotYetReplied,
			Key:                               key,
			Value:                             value,
		}
		err = c.SendRequestWithPeerId(peerId, func(leaderUrl url.URL) (*http.Request, error) {
			leaderUrl.Path = api.KvHttpPath
			return makeAppendKvRequest(ctx, leaderUrl, &req)
		}, &res)
	case Delete:
		req := request.KvStoreDeleteRequest{
			SessionID:                         s.SessionID,
			SequenceNumber:                    s.SequenceNumber,
			LowestSequenceNumberNotYetReplied: s.LowestSequenceNumberNotYetReplied,
			Key:                               key,
		}
		err = c.SendRequestWithPeerId(peerId, func(leaderUrl url.URL) (*http.Request, error) {
			leaderUrl.Path = api.KvHttpPath
			return makeDeleteKvRequest(ctx, leaderUrl, &req)
		}, &res)
	case Read:
		err = c.SendRequestWithPeerId(peerId, func(leaderUrl url.URL) (*http.Request, error) {
			leaderUrl.Path = api.KvHttpPath
			return makeGetKvRequest(ctx, leaderUrl, key)
		}, &res)
	}
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *KvStoreClient) SendRequestWithPeerId(peerId uint64, makeRequest func(leaderUrl url.URL) (*http.Request, error), result any) error {
	return c.proxy.SendRequestWithPeerId(peerId, makeRequest, result)
}

func (c *KvStoreClient) isClosed() bool {
	select {
	case <-c.closeCh:
		return true
	default:
		return false
	}
}

func makeGetKvRequest(ctx context.Context, targetUrl url.URL, key string) (*http.Request, error) {
	q := targetUrl.Query()
	q.Add("key", key)
	targetUrl.RawQuery = q.Encode()
	return http.NewRequestWithContext(ctx, http.MethodGet, targetUrl.String(), nil)
}

func makeSetKvRequest(ctx context.Context, targetUrl url.URL, req *request.KvStoreSetRequest) (*http.Request, error) {
	b, err := json.Marshal(&req)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, targetUrl.String(), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	return r, nil
}

func makeAppendKvRequest(ctx context.Context, targetUrl url.URL, req *request.KvStoreAppendRequest) (*http.Request, error) {
	b, err := json.Marshal(&req)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPut, targetUrl.String(), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	return r, nil
}

func makeDeleteKvRequest(ctx context.Context, targetUrl url.URL, req *request.KvStoreDeleteRequest) (*http.Request, error) {
	b, err := json.Marshal(&req)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodDelete, targetUrl.String(), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	return r, nil
}

type clusterPeers []ClusterPeer

func (p clusterPeers) Len() int           { return len(p) }
func (p clusterPeers) Less(i, j int) bool { return p[i].Id < p[j].Id }
func (p clusterPeers) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
