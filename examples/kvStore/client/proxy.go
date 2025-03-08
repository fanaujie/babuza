package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvStore/server/kverror"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/raft"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var (
	errNoPeers                  = errors.New("no peers")
	errInvalidLeaderIndex       = errors.New("invalid leader index")
	errLeaderIndexNotFoundMatch = errors.New("leader index does not found match ")
	errNewLeaderUrlNil          = errors.New("new leader url is nil")
)

type proxy struct {
	peers       []ClusterPeer
	httpScheme  string
	httpClient  *http.Client
	leaderIndex int
	mu          *sync.RWMutex
}

func newProxy(enableTLS bool, peers []ClusterPeer) *proxy {
	httpScheme := "http"
	if enableTLS {
		httpScheme = "https"
	}
	return &proxy{
		peers:      peers,
		httpScheme: httpScheme,
		httpClient: newHttpClient(enableTLS),
		mu:         &sync.RWMutex{},
	}
}

func (p *proxy) SetClusterPeers(peers []ClusterPeer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = peers
}

func (p *proxy) SendRequest(ctx context.Context, makeRequest func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error), result any) error {
	for {
		leaderUrl, err := p.currentLeaderUrl()
		if err != nil {
			return err
		}
		req, err := makeRequest(ctx, leaderUrl)
		if err != nil {
			return err
		}
		res, err := p.httpClient.Do(req)
		if err != nil {
			if err.(*url.Error).Timeout() {
				return err
			}
			if err = p.moveNextLeader(); err != nil {
				return err
			}
			time.Sleep(time.Millisecond * 500)
			continue
		}
		b, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}
		if err = res.Body.Close(); err != nil {
			return err
		}
		if res.StatusCode == http.StatusServiceUnavailable {
			if err = p.moveNextLeader(); err != nil {
				return err
			}
			time.Sleep(time.Millisecond * 500)
			continue
		} else if res.StatusCode == http.StatusMovedPermanently {
			newLeaderUrl, _ := url.Parse(res.Header["Location"][0])
			if err = p.updateLeaderIndex(newLeaderUrl); err != nil {
				return err
			}
			continue
		} else if res.StatusCode == http.StatusGatewayTimeout {
			if err = p.moveNextLeader(); err != nil {
				return err
			}
			time.Sleep(time.Millisecond * 500)
			continue
		}
		if res.StatusCode != http.StatusOK {
			return convertError(strings.TrimSuffix(string(b), "\n"))
		}
		return json.Unmarshal(b, result)
	}
}

func (p *proxy) SendRequestWithPeerId(peerId uint64, makeRequest func(leaderUrl url.URL) (*http.Request, error), result any) error {
	var leaderUrl *url.URL
	p.mu.RLock()
	for _, peer := range p.peers {
		if peer.Id == peerId {
			leaderUrl = &url.URL{
				Scheme: p.httpScheme,
				Host:   peer.KvServiceAddress,
			}
		}
	}
	p.mu.RUnlock()
	if leaderUrl == nil {
		return fmt.Errorf("not found service address. (peerId=%d)", peerId)
	}
	req, err := makeRequest(*leaderUrl)
	if err != nil {
		return err
	}
	res, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if err = res.Body.Close(); err != nil {
		return err
	}
	if res.StatusCode == http.StatusMovedPermanently {
		return raft.ErrNotLeader
	}
	if res.StatusCode != http.StatusOK {
		return convertError(strings.TrimSuffix(string(b), "\n"))
	}
	return json.Unmarshal(b, result)
}
func (p *proxy) currentLeaderUrl() (url.URL, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.peers) == 0 {
		return url.URL{}, errNoPeers
	}
	if p.leaderIndex >= len(p.peers) {
		return url.URL{}, errInvalidLeaderIndex
	}
	return url.URL{
		Scheme: p.httpScheme,
		Host:   p.peers[p.leaderIndex].KvServiceAddress,
	}, nil
}

func (p *proxy) moveNextLeader() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.peers) == 0 {
		return errNoPeers
	}
	p.leaderIndex++
	if p.leaderIndex >= len(p.peers) {
		p.leaderIndex = 0
	}
	return nil
}

func (p *proxy) updateLeaderIndex(leaderUrl *url.URL) error {
	if leaderUrl == nil {
		return errNewLeaderUrlNil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for index, u := range p.peers {
		if u.KvServiceAddress == leaderUrl.Host {
			p.leaderIndex = index
			return nil
		}
	}
	return errLeaderIndexNotFoundMatch
}

func convertError(errString string) error {
	switch errString {
	case kverror.ErrKeyNotFound.Error():
		return kverror.ErrKeyNotFound
	case raft.ErrNotLeader.Error():
		return raft.ErrNotLeader
	case raft.ErrStopped.Error():
		return raft.ErrStopped
	case raft.ErrLearnerNotReady.Error():
		return raft.ErrLearnerNotReady
	case raft.ErrLearnerCanNotSwitchLeaderShip.Error():
		return raft.ErrLearnerCanNotSwitchLeaderShip
	case raft.ErrLearnerCanNotVote.Error():
		return raft.ErrLearnerCanNotVote
	case raft.ErrVotingMemberCanNotPromote.Error():
		return raft.ErrVotingMemberCanNotPromote
	case cluster.ErrPeerIDExists.Error():
		return cluster.ErrPeerIDExists
	case cluster.ErrPeerRaftListenAddrExists.Error():
		return cluster.ErrPeerRaftListenAddrExists
	case cluster.ErrPeerIDNotFound.Error():
		return cluster.ErrPeerIDNotFound
	case cluster.ErrPeerIDRemoved.Error():
		return cluster.ErrPeerIDRemoved
	case cluster.ErrPeerNotLearner.Error():
		return cluster.ErrPeerNotLearner
	case context.DeadlineExceeded.Error():
		return context.DeadlineExceeded
	case session.ErrSessionExpired.Error():
		return session.ErrSessionExpired
	default:
		return errors.New(errString)
	}
}
