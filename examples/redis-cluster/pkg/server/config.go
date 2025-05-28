package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/raft/multiraft"
)

type Config struct {
	NodeID        uint64
	ClusterID     uint64
	ListenAddr    string
	RaftAddr      string
	DataDir       string
	JoinExisting  bool
	InitialShards int
	PeerAddrs     []string
}

func (c *Config) ParsedPeers() (map[uint64]string, error) {
	peers := make(map[uint64]string)

	for _, peerStr := range c.PeerAddrs {
		parts := strings.Split(peerStr, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer format: %s (expected format: id=addr)", peerStr)
		}

		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid peer ID: %s", parts[0])
		}

		peers[id] = parts[1]
	}

	return peers, nil
}

func (c *Config) CreatePeersConfig(groupID ibabuza.RaftGroupID) (*multiraft.PeersConfiguration, error) {
	peersConfig := multiraft.NewPeersConfiguration()
	peersConfig.SetGroupID(groupID)

	peers, err := c.ParsedPeers()
	if err != nil {
		return nil, err
	}

	for id, addr := range peers {
		if err = peersConfig.AddPeer(id, addr, false); err != nil {
			return nil, fmt.Errorf("failed to add peer %d: %w", id, err)
		}
	}

	return peersConfig, nil
}
