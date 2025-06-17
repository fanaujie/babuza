package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/raft/multiraft"
)

type Config struct {
	StoreID                          uint64
	ClusterID                        uint64
	RedisListenAddr                  string
	RaftAddr                         string
	DataDir                          string
	InitialShards                    int
	StoreAddrs                       []string
	IntervalHeartbeatStore           int
	IntervalHeartbeatRaftGroupLeader int
	PdGRPCAddr                       string
}

func (c *Config) ParsedStores() (map[uint64]string, error) {
	stores := make(map[uint64]string)

	for _, storeStr := range c.StoreAddrs {
		parts := strings.Split(storeStr, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid store format: %s (expected format: id=addr)", storeStr)
		}

		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid peer ID: %s", parts[0])
		}

		stores[id] = parts[1]
	}

	return stores, nil
}

func (c *Config) CreatePeersConfig(groupID ibabuza.RaftGroupID) (*multiraft.PeersConfiguration, error) {
	peersConfig := multiraft.NewPeersConfiguration()
	peersConfig.SetGroupID(groupID)

	stores, err := c.ParsedStores()
	if err != nil {
		return nil, err
	}

	for storeID, addr := range stores {
		peerID := storeID + 100
		if err = peersConfig.AddPeer(peerID, storeID, addr, false); err != nil {
			return nil, fmt.Errorf("failed to add peer %d store %d addr %s: %w",
				peerID, storeID, addr, err)
		}
	}

	return peersConfig, nil
}
