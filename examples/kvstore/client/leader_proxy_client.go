// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/server/kverror"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/raft"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

var (
	errNoPeers                  = errors.New("no peers")
	errInvalidLeaderIndex       = errors.New("invalid leader index")
	errLeaderIndexNotFoundMatch = errors.New("leader index does not found match ")
	errNewLeaderUrlNil          = errors.New("new leader url is nil")
)

type leaderProxyClient struct {
	peers       []ClusterPeer
	httpScheme  string
	httpClient  *http.Client
	leaderIndex int
	mu          *sync.RWMutex
}

func newLeaderProxyClient(enableTLS bool, peers []ClusterPeer) *leaderProxyClient {
	httpScheme := "http"
	if enableTLS {
		httpScheme = "https"
	}
	return &leaderProxyClient{
		peers:      peers,
		httpScheme: httpScheme,
		httpClient: newHttpClient(enableTLS),
		mu:         &sync.RWMutex{},
	}
}

func (p *leaderProxyClient) SetClusterPeers(peers []ClusterPeer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = peers
}

func (p *leaderProxyClient) SendRequest(ctx context.Context, makeRequest func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error), result any) error {
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
			var nErr net.Error
			if errors.As(err, &nErr) && nErr.Timeout() {
				return err
			}
			if err = p.moveNextLeader(); err != nil {
				return err
			}
			continue
		}
		b, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}
		if err = res.Body.Close(); err != nil {
			return err
		}
		if res.StatusCode >= 300 && res.StatusCode < 400 {
			locationURL, err := res.Location()
			if err != nil {
				return err
			}
			if err = p.updateLeaderIndex(locationURL); err != nil {
				return err
			}
			continue
		}
		if res.StatusCode == http.StatusServiceUnavailable || res.StatusCode == http.StatusGatewayTimeout {
			if err = p.moveNextLeader(); err != nil {
				return err
			}
			continue
		} else if res.StatusCode != http.StatusOK {
			return convertError(strings.TrimSuffix(string(b), "\n"))
		}
		return json.Unmarshal(b, result)
	}
}

func (p *leaderProxyClient) SendRequestWithPeerId(peerID uint64, makeRequest func(leaderUrl url.URL) (*http.Request, error), result any) error {
	var leaderUrl *url.URL
	p.mu.RLock()
	for _, peer := range p.peers {
		if peer.Id == peerID {
			leaderUrl = &url.URL{
				Scheme: p.httpScheme,
				Host:   peer.KvServiceAddress,
			}
		}
	}
	p.mu.RUnlock()
	if leaderUrl == nil {
		return fmt.Errorf("not found service address. (peerID=%d)", peerID)
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
	if res.StatusCode >= 300 && res.StatusCode < 400 {
		return raft.ErrNotLeader
	}
	if res.StatusCode == http.StatusServiceUnavailable {
		return raft.ErrNotLeader
	}
	if res.StatusCode != http.StatusOK {
		return convertError(strings.TrimSuffix(string(b), "\n"))
	}
	return json.Unmarshal(b, result)
}
func (p *leaderProxyClient) currentLeaderUrl() (url.URL, error) {
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

func (p *leaderProxyClient) moveNextLeader() error {
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

func (p *leaderProxyClient) updateLeaderIndex(leaderUrl *url.URL) error {
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
	case raft.ErrLearnerCanNotSwitchLeadership.Error():
		return raft.ErrLearnerCanNotSwitchLeadership
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
	default:
		return errors.New(errString)
	}
}
