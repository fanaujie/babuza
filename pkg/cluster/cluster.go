package cluster

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/cluster/pb"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"io"
	"sort"
	"sync"
)

const (
	storeVersion uint64 = 1
)

type Cluster struct {
	clusterId   uint64
	localPeerId uint64
	store       pb.Store
	logger      ibabuza.Logger
	mu          *sync.RWMutex
}

func NewCluster(logger ibabuza.Logger) *Cluster {
	logger.Info("Cluster: creating cluster")
	return &Cluster{
		store: pb.Store{
			Peers:      make(map[uint64]babuzapb.Peer),
			RemovedIds: make(map[uint64]bool),
		},
		logger: logger,
		mu:     &sync.RWMutex{},
	}
}

func (c *Cluster) SetClusterId(clusterId uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clusterId = clusterId
}

func (c *Cluster) SetLocalPeerId(localPeerId uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.localPeerId = localPeerId
}

func (c *Cluster) Peer(peerId uint64) (babuzapb.Peer, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.store.Peers[peerId]
	if !ok {
		return babuzapb.Peer{}, ErrPeerIDNotFound
	}
	return p, nil
}

func (c *Cluster) Snapshot(w io.Writer) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	storeData, err := c.store.Marshal()
	if err != nil {
		return err
	}
	buf := make([]byte, 8)
	if err = fileutil.FileWriteUint64(w, buf, storeVersion); err != nil {
		return err
	}
	if err = fileutil.FileWriteUint64(w, buf, c.clusterId); err != nil {
		return err
	}
	// skip localPeerId
	if err = fileutil.FileWriteUint64(w, buf, uint64(len(storeData))); err != nil {
		return err
	}
	_, err = w.Write(storeData)
	return err
}

func (c *Cluster) Restore(r io.Reader) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	store := pb.Store{}
	buf := make([]byte, 8)
	ver, err := fileutil.FileReadUint64(r, buf)
	if err != nil {
		return err
	}
	if ver != storeVersion {
		return fmt.Errorf("Cluster: mismatch store version. expected (version=%d) real(version=%d)", storeVersion, ver)
	}
	c.clusterId, err = fileutil.FileReadUint64(r, buf)
	if err != nil {
		return err
	}
	// skip localPeerId
	dataSize, err := fileutil.FileReadUint64(r, buf)
	if err != nil {
		return err
	}
	buf = make([]byte, dataSize)
	if _, err = r.Read(buf); err != nil {
		return nil
	}
	if err = store.Unmarshal(buf); err != nil {
		return err
	}
	if len(store.RemovedIds) == 0 {
		store.RemovedIds = make(map[uint64]bool)
	}
	c.store = store
	return nil
}

func (c *Cluster) Peers() []babuzapb.Peer {
	c.mu.Lock()
	defer c.mu.Unlock()
	var peers clusterPeers
	for _, p := range c.store.Peers {
		peers = append(peers, p)
	}
	sort.Sort(peers)
	return peers
}

func (c *Cluster) ClusterId() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clusterId
}

func (c *Cluster) LocalPeerID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.localPeerId
}

func (c *Cluster) Add(peer babuzapb.RaftPeerAttribute) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.store.Peers[peer.Id]; ok {
		return ErrPeerIDExists
	}
	if _, ok := c.store.RemovedIds[peer.Id]; ok {
		return ErrPeerIDRemoved
	}
	for _, m := range c.store.Peers {
		if m.RaftPeerAttr.RaftListenAddr == peer.RaftListenAddr {
			return ErrPeerRaftListenAddrExists
		}
	}
	c.store.Peers[peer.Id] = babuzapb.Peer{
		RaftPeerAttr: peer,
	}
	return nil
}

func (c *Cluster) Update(peer babuzapb.RaftPeerAttribute) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.store.Peers[peer.Id]
	if !ok {
		return ErrPeerIDNotFound
	}
	for _, m := range c.store.Peers {
		if m.RaftPeerAttr.RaftListenAddr == peer.RaftListenAddr {
			return ErrPeerRaftListenAddrExists
		}
	}
	p.RaftPeerAttr.RaftListenAddr = peer.RaftListenAddr
	c.store.Peers[peer.Id] = p
	return nil
}

func (c *Cluster) UpdateAppServiceAddresses(peerId uint64, addresses []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.store.Peers[peerId]
	if !ok {
		return ErrPeerIDNotFound
	}
	p.AppServiceAddresses = addresses
	c.store.Peers[peerId] = p
	return nil
}

func (c *Cluster) Remove(peerId uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.store.Peers[peerId]; !ok {
		return ErrPeerIDNotFound
	}
	delete(c.store.Peers, peerId)
	c.store.RemovedIds[peerId] = true
	return nil
}

func (c *Cluster) Promote(peerId uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.store.Peers[peerId]
	if !ok {
		return ErrPeerIDNotFound
	}
	if !p.RaftPeerAttr.IsLearner {
		return ErrPeerNotLearner
	}
	p.RaftPeerAttr.IsLearner = false
	c.store.Peers[peerId] = p
	return nil
}

type clusterPeers []babuzapb.Peer

func (a clusterPeers) Len() int           { return len(a) }
func (a clusterPeers) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a clusterPeers) Less(i, j int) bool { return a[i].RaftPeerAttr.Id < a[j].RaftPeerAttr.Id }
