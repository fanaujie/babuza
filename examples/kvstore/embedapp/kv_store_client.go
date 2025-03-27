package embedapp

import "github.com/fanaujie/babuza/examples/kvstore/client"

func NewKvStoreClient(allPeersServiceAddresses map[uint64][]string, session client.ISession) (*client.KvStoreClient, error) {
	var serviceAddresses []client.ClusterPeer
	for peerId, addresses := range allPeersServiceAddresses {
		serviceAddresses = append(serviceAddresses, client.ClusterPeer{
			Id:               peerId,
			KvServiceAddress: addresses[0],
		})
	}
	cfg := client.Config{
		//AutoSyncInterval:           time.Second * 2,
		KvStoreClusterMembers: serviceAddresses,
	}

	return client.CreateKvStoreClient(cfg, session)
}
