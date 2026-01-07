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


package testcluster

import (
	"fmt"

	"github.com/fanaujie/babuza/ibabuza"
)

type Peer interface {
	ID() uint64
	IsPeerLearner() bool
	RaftListenAddress(useProxyNetwork bool) string
	ApplicationServiceAddresses() []string
	RaftTLSConfig() ibabuza.TLSConfig
	SetAppServiceAddresses([]string)
	SetRaftListenAddress(string)
}

type ProxyPeer interface {
	Peer
	ProxyListenAddress() string
	ProxyConfig() ibabuza.ProxyConfig
	SetProxyListenAddress(string)
}

type BabuzaPeer struct {
	Id                     uint64
	RaftListenAddr         string
	TLSConfig              ibabuza.TLSConfig
	ProxyListenAddr        string
	ProxyInListenTLSConfig ibabuza.TLSConfig
	ProxyOotDialTLSConfig  ibabuza.TLSConfig
	AppServiceAddresses    []string
	IsLearner              bool
}

func (b *BabuzaPeer) ID() uint64 {
	return b.Id
}

func (b *BabuzaPeer) IsPeerLearner() bool {
	return b.IsLearner
}

func (b *BabuzaPeer) RaftListenAddress(useProxyNetwork bool) string {
	if useProxyNetwork {
		return b.ProxyListenAddr
	}
	return b.RaftListenAddr
}

func (b *BabuzaPeer) SetRaftListenAddress(addr string) {
	b.RaftListenAddr = addr
}

func (b *BabuzaPeer) ProxyListenAddress() string {
	return b.ProxyListenAddr
}

func (b *BabuzaPeer) ApplicationServiceAddresses() []string {
	return b.AppServiceAddresses
}

func (b *BabuzaPeer) SetAppServiceAddresses(addrs []string) {
	b.AppServiceAddresses = addrs
}

func (b *BabuzaPeer) RaftTLSConfig() ibabuza.TLSConfig {
	return b.TLSConfig
}

func (b *BabuzaPeer) ProxyConfig() ibabuza.ProxyConfig {
	return ibabuza.ProxyConfig{
		Id:                b.Id,
		InAddr:            b.ProxyListenAddr,
		OutAddr:           b.RaftListenAddr,
		InListenTLSConfig: b.ProxyInListenTLSConfig,
		OutDialTLSConfig:  b.ProxyOotDialTLSConfig,
	}
}

func (b *BabuzaPeer) SetProxyListenAddress(addr string) {
	b.ProxyListenAddr = addr
}

type StandardPeer struct {
	Id                  uint64
	RaftListenAddr      string
	TLSConfig           ibabuza.TLSConfig
	AppServiceAddresses []string
	IsLearner           bool
}

func (s *StandardPeer) ID() uint64 {
	return s.Id
}

func (s *StandardPeer) IsPeerLearner() bool {
	return s.IsLearner
}

func (s *StandardPeer) RaftListenAddress(useProxyNetwork bool) string {
	return s.RaftListenAddr
}

func (s *StandardPeer) SetRaftListenAddress(addr string) {
	s.RaftListenAddr = addr
}

func (s *StandardPeer) ApplicationServiceAddresses() []string {
	return s.AppServiceAddresses
}

func (s *StandardPeer) SetAppServiceAddresses(addrs []string) {
	s.AppServiceAddresses = addrs
}

func (s *StandardPeer) RaftTLSConfig() ibabuza.TLSConfig {
	return s.TLSConfig
}

type PeerFactory struct {
	RaftPortBase    uint64
	AppPortBase     uint64
	ProxyPortBase   uint64
}

func NewPeerFactory(raftPortBase, appPortBase, proxyPortBase uint64) *PeerFactory {
	return &PeerFactory{
		RaftPortBase:  raftPortBase,
		AppPortBase:   appPortBase,
		ProxyPortBase: proxyPortBase,
	}
}

func (f *PeerFactory) MakeVotingStandardPeers(count int) ([]Peer, *ConnectedGroup) {
	var peers []Peer
	var peerIDs []uint64
	for i := 0; i < count; i++ {
		peerID := uint64(i + 1)
		peerIDs = append(peerIDs, peerID)
		peers = append(peers, f.MakeSingleStandardPeer(peerID, false))
	}
	return peers, NewConnectedGroup(peerIDs)
}

func (f *PeerFactory) MakeSingleStandardPeer(peerID uint64, isLearner bool) Peer {
	return &StandardPeer{
		Id:                  peerID,
		RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", f.RaftPortBase+peerID),
		AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", f.AppPortBase+peerID)},
		IsLearner:           isLearner,
	}
}

func (f *PeerFactory) MakeVotingProxyPeers(count int) ([]Peer, *ConnectedGroup) {
	var peers []Peer
	var peerIDs []uint64
	for i := 0; i < count; i++ {
		peerID := uint64(i + 1)
		peerIDs = append(peerIDs, peerID)
		peers = append(peers, f.MakeSingleProxyPeer(peerID, false))
	}
	return peers, NewConnectedGroup(peerIDs)
}

func (f *PeerFactory) MakeSingleProxyPeer(peerID uint64, isLearner bool) Peer {
	return &BabuzaPeer{
		Id:                  peerID,
		RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", f.RaftPortBase+peerID),
		ProxyListenAddr:     fmt.Sprintf("127.0.0.1:%d", f.ProxyPortBase+peerID),
		AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", f.AppPortBase+peerID)},
		IsLearner:           isLearner,
	}
}
