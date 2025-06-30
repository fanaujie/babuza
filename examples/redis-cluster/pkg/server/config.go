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


package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/raft/experimental"
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

func (c *Config) CreatePeersConfig(groupID ibabuza.RaftGroupID) (*experimental.PeersConfiguration, error) {
	peersConfig := experimental.NewPeersConfiguration()
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
