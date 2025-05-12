package kvgrpc

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/test/kvbench/client"
	"github.com/fanaujie/babuza/test/kvbench/kvbenchpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sort"
	"sync"
)

// Factory implements the client.Factory interface for creating gRPC clients
type Factory struct {
	config         client.Config
	groupLeaderMap sync.Map //map[uint64]string
	groupMemberMap sync.Map //map[uint64][]string
	nodeClientMap  sync.Map //map[string][]*grpc.ClientConn

	connUsageStats sync.Map //map[*grpc.ClientConn]int64
}

// NewGRPCFactory creates a new gRPC client factory
func NewGRPCFactory(clusterID uint64, config client.Config) (*Factory, error) {
	if len(config.Endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints provided")
	}
	if config.ShardCount > config.Connections {
		return nil, fmt.Errorf("shard count cannot exceed total connections")
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	f := &Factory{
		config: config,
	}
	for _, addr := range config.Endpoints {
		// 1. connect to the first endpoint to get cluster configuration
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			fmt.Printf("failed to connect to %s: %v", addr, err)
			continue
		}
		clientpb := kvbenchpb.NewKVServiceClient(conn)

		// 2. acquire cluster configuration
		clusterResp, err := clientpb.ClusterConfiguration(context.Background(), &kvbenchpb.ClusterPeersRequest{
			ClusterID: clusterID,
		})
		if err != nil {
			conn.Close()
			fmt.Printf("failed to get cluster configuration: %s %v", addr, err)
			continue
		}

		// 3. build group/leader/member map
		for _, group := range clusterResp.GetGroupPeers() {
			var leaderAddr string
			var members []string
			for _, peer := range group.GetPeers() {
				grpcAddr := peer.GetGrpcListenAddr()
				members = append(members, grpcAddr)
				if peer.GetPeerID() == group.GetLeaderID() {
					leaderAddr = grpcAddr
				}
			}
			if leaderAddr != "" {
				f.groupLeaderMap.Store(group.GetGroupID(), leaderAddr)
			}
			f.groupMemberMap.Store(group.GetGroupID(), members)
		}
		conn.Close()
		if lenSyncMap(&f.groupLeaderMap) > 0 && lenSyncMap(&f.groupMemberMap) > 0 {
			break
		}
	}
	if lenSyncMap(&f.groupLeaderMap) == 0 || lenSyncMap(&f.groupMemberMap) == 0 {
		return nil, fmt.Errorf("failed to get cluster configuration")
	}

	// 4. create connections by policy
	if config.TargetLeader {
		// target leader policy
		leaderCountByAddr := make(map[string]int)
		leaderAddrList := []string{}

		// count the number of leaders for each address
		f.groupLeaderMap.Range(func(_, leaderAddr interface{}) bool {
			addr := leaderAddr.(string)
			if _, exists := leaderCountByAddr[addr]; !exists {
				leaderAddrList = append(leaderAddrList, addr)
			}
			leaderCountByAddr[addr]++
			return true
		})

		// sort addresses for deterministic allocation
		sort.Strings(leaderAddrList)

		// first round: allocate connections based on leader count (one connection per leader)
		totalConns := uint(0)
		for _, addr := range leaderAddrList {
			if totalConns >= config.Connections {
				break
			}

			leaderCount := leaderCountByAddr[addr]
			connsForThisNode := uint(leaderCount)
			if totalConns+connsForThisNode > config.Connections {
				connsForThisNode = config.Connections - totalConns
			}

			v, _ := f.nodeClientMap.LoadOrStore(addr, []*grpc.ClientConn{})
			nodeConns := v.([]*grpc.ClientConn)
			for i := uint(0); i < connsForThisNode; i++ {
				conn, err := grpc.NewClient(addr, opts...)
				if err != nil {
					fmt.Printf("failed to connect to %s: %v", addr, err)
					return nil, err
				}
				nodeConns = append(nodeConns, conn)
				totalConns++
			}
			f.nodeClientMap.Store(addr, nodeConns)
		}

		// second round: distribute remaining connections in round-robin fashion
		for totalConns < config.Connections {
			for _, addr := range leaderAddrList {
				if totalConns >= config.Connections {
					break
				}

				v, _ := f.nodeClientMap.LoadOrStore(addr, []*grpc.ClientConn{})
				nodeConns := v.([]*grpc.ClientConn)
				conn, err := grpc.NewClient(addr, opts...)
				if err != nil {
					fmt.Printf("failed to connect to %s: %v", addr, err)
					return nil, err
				}
				nodeConns = append(nodeConns, conn)
				totalConns++
				f.nodeClientMap.Store(addr, nodeConns)
			}
		}
	} else {
		// round-robin policy
		totalConns := uint(0)
		nextNodeIndex := uint64(0)
		for totalConns < config.Connections {
			addr := config.Endpoints[nextNodeIndex]
			conn, err := grpc.NewClient(addr, opts...)
			if err != nil {
				fmt.Printf("failed to connect to %s: %v", addr, err)
				return nil, err
			}
			conns, loaded := f.nodeClientMap.LoadOrStore(addr, []*grpc.ClientConn{conn})
			if loaded {
				clientSlice := conns.([]*grpc.ClientConn)
				f.nodeClientMap.Store(addr, append(clientSlice, conn))
			}
			totalConns++
			nextNodeIndex++
			if nextNodeIndex >= uint64(len(config.Endpoints)) {
				nextNodeIndex = 0
			}
		}
	}
	// 5. initialize connection usage stats
	f.nodeClientMap.Range(func(k, v interface{}) bool {
		conns := v.([]*grpc.ClientConn)
		for _, conn := range conns {
			f.connUsageStats.Store(conn, uint(0))
		}
		return true
	})
	return f, nil
}

// NewClient creates a new client with the given configuration
func (f *Factory) NewClient(config client.Config) client.Client {
	return NewGRPCClientWithRouting(config, f)
}

// Close closes the client connection
func (f *Factory) Close() error {
	f.nodeClientMap.Range(func(k, v interface{}) bool {
		conns := v.([]*grpc.ClientConn)
		for _, conn := range conns {
			if err := conn.Close(); err != nil {
				fmt.Printf("failed to close connection: %v", err)
			}
		}
		return true
	})
	return nil
}

func lenSyncMap(m *sync.Map) int {
	var i int
	m.Range(func(k, v interface{}) bool {
		i++
		return true
	})
	return i
}

func (f *Factory) recordConnUsage(conn *grpc.ClientConn) {
	currentUsage, _ := f.connUsageStats.LoadOrStore(conn, uint(0))
	f.connUsageStats.Store(conn, currentUsage.(uint)+1)
}

func (f *Factory) getLeastUsedConn(conns []*grpc.ClientConn) *grpc.ClientConn {
	if len(conns) == 0 {
		return nil
	}

	leastUsedConn := conns[0]
	minUsage := f.config.Connections
	for _, conn := range conns {
		usage, ok := f.connUsageStats.Load(conn)
		if !ok {
			panic("connection usage not found")
		}
		cUsage := usage.(uint)
		if cUsage < minUsage {
			minUsage = cUsage
			leastUsedConn = conn
		}
	}

	return leastUsedConn
}

func (f *Factory) getConnectionForClient(addr string) (*grpc.ClientConn, error) {

	v, ok := f.nodeClientMap.Load(addr)
	if !ok {
		return nil, fmt.Errorf("no connections for %s", addr)
	}

	conns := v.([]*grpc.ClientConn)
	if len(conns) == 0 {
		return nil, fmt.Errorf("empty connection list for %s", addr)
	}

	conn := f.getLeastUsedConn(conns)
	if conn == nil {
		return nil, fmt.Errorf("failed to get connection for %s", addr)
	}

	f.recordConnUsage(conn)

	return conn, nil
}

func (f *Factory) releaseConnection(conn *grpc.ClientConn) {
	usage, ok := f.connUsageStats.Load(conn)
	if ok {
		f.connUsageStats.Store(conn, usage.(uint)-1)
	}
}
