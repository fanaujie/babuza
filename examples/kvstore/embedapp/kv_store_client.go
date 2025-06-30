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


package embedapp

import "github.com/fanaujie/babuza/examples/kvstore/client"

func NewKvStoreClient(allPeersServiceAddresses map[uint64][]string, session client.ISession) (*client.KvStoreClient, error) {
	var serviceAddresses []client.ClusterPeer
	for peerID, addresses := range allPeersServiceAddresses {
		serviceAddresses = append(serviceAddresses, client.ClusterPeer{
			Id:               peerID,
			KvServiceAddress: addresses[0],
		})
	}
	cfg := client.Config{
		//AutoSyncInterval:           time.Second * 2,
		KvStoreClusterMembers: serviceAddresses,
	}

	return client.CreateKvStoreClient(cfg, session)
}
