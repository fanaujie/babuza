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
	clusterID   uint64
	groupID     ibabuza.RaftGroupID
	localPeerID uint64
	store       pb.Store
	mu          sync.RWMutex
}

func NewCluster() *Cluster {
	return &Cluster{
		store: pb.Store{
			Peers:      make(map[uint64]babuzapb.Peer),
			RemovedIds: make(map[uint64]bool),
		},
	}
}

func (c *Cluster) SetClusterID(clusterID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clusterID = clusterID
}

func (c *Cluster) SetGroupID(groupID ibabuza.RaftGroupID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groupID = groupID
}

func (c *Cluster) SetLocalPeerID(localPeerID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.localPeerID = localPeerID
}

func (c *Cluster) Peer(peerID uint64) (babuzapb.Peer, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.store.Peers[peerID]
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
	if err = fileutil.FileWriteUint64(w, buf, c.clusterID); err != nil {
		return err
	}
	if err = fileutil.FileWriteUint64(w, buf, uint64(c.groupID)); err != nil {
		return err
	}
	// skip localPeerID
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
		return fmt.Errorf("cluster: mismatch store version. expected (version=%d) real(version=%d)", storeVersion, ver)
	}
	c.clusterID, err = fileutil.FileReadUint64(r, buf)
	if err != nil {
		return err
	}
	groupID, err := fileutil.FileReadUint64(r, buf)
	if err != nil {
		return err
	}
	c.groupID = ibabuza.RaftGroupID(groupID)

	// skip localPeerID
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

func (c *Cluster) ClusterID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clusterID
}

func (c *Cluster) GroupID() ibabuza.RaftGroupID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.groupID
}

func (c *Cluster) LocalPeerID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.localPeerID
}

func (c *Cluster) Add(peer babuzapb.RaftPeerAttribute) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.store.Peers[peer.PeerID]; ok {
		return ErrPeerIDExists
	}
	if _, ok := c.store.RemovedIds[peer.PeerID]; ok {
		return ErrPeerIDRemoved
	}
	c.store.Peers[peer.PeerID] = babuzapb.Peer{
		RaftPeerAttr: peer,
	}
	return nil
}

func (c *Cluster) Update(peerID uint64, peer babuzapb.RaftPeerAttribute) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.store.Peers[peerID]
	if !ok {
		return ErrPeerIDNotFound
	}
	p.RaftPeerAttr.RaftListenAddr = peer.RaftListenAddr
	p.RaftPeerAttr.IsLearner = peer.IsLearner
	p.RaftPeerAttr.StoreID = peer.StoreID
	c.store.Peers[peer.PeerID] = p
	return nil
}

func (c *Cluster) UpdateAppServiceAddresses(peerID uint64, addresses []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.store.Peers[peerID]
	if !ok {
		return ErrPeerIDNotFound
	}
	p.AppServiceAddresses = addresses
	c.store.Peers[peerID] = p
	return nil
}

func (c *Cluster) Remove(peerID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.store.Peers[peerID]; !ok {
		return ErrPeerIDNotFound
	}
	delete(c.store.Peers, peerID)
	c.store.RemovedIds[peerID] = true
	return nil
}

func (c *Cluster) Promote(peerID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.store.Peers[peerID]
	if !ok {
		return ErrPeerIDNotFound
	}
	if !p.RaftPeerAttr.IsLearner {
		return ErrPeerNotLearner
	}
	p.RaftPeerAttr.IsLearner = false
	c.store.Peers[peerID] = p
	return nil
}

type clusterPeers []babuzapb.Peer

func (a clusterPeers) Len() int           { return len(a) }
func (a clusterPeers) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a clusterPeers) Less(i, j int) bool { return a[i].RaftPeerAttr.PeerID < a[j].RaftPeerAttr.PeerID }
