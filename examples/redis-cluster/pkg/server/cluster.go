package server

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/connpool"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/tidwall/redcon"
	"io"
	"net"
	"sync"
	"time"
)

type multiRaftStore interface {
	Propose(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession, log []byte) babuza.ProposedResult
	Query(groupID ibabuza.RaftGroupID, key any) (any, error)
}

type clusterMgr struct {
	localRedisListenAddr string
	store                multiRaftStore
	mu                   sync.RWMutex
	redisRoutingTable    map[uint64]string
	pool                 connpool.Pool[net.Conn]
}

type dialer struct {
}

func (t *dialer) Dial(address string) (net.Conn, error) {
	return net.Dial("tcp", address)
}

func newClusterMgr(localRedisListenAddr string, store multiRaftStore) *clusterMgr {
	return &clusterMgr{
		localRedisListenAddr: localRedisListenAddr,
		store:                store,
		redisRoutingTable:    make(map[uint64]string),
		pool: connpool.NewConnectionPool[net.Conn](&dialer{}, connpool.Config{
			MaxConnectionsPerHost: 10,
			IdleTimeout:           time.Minute * 5,
		}),
	}
}

func (m *clusterMgr) UpdateRoutingTable(table map[uint64]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.redisRoutingTable)
	for groupID, addr := range table {
		m.redisRoutingTable[groupID] = addr
	}
}

func (m *clusterMgr) Close() {
	_ = m.pool.Close()
}

func (m *clusterMgr) IsLocalLeaderForGroup(groupID ibabuza.RaftGroupID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	targetAddr, exist := m.redisRoutingTable[uint64(groupID)]
	if exist && targetAddr == m.localRedisListenAddr {
		return true
	}
	return false
}

func (m *clusterMgr) RedirectToLeader(conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID) {
	m.mu.RLock()
	targetAddr, exist := m.redisRoutingTable[uint64(groupID)]
	if !exist {
		m.mu.RUnlock()
		conn.WriteError(fmt.Sprintf("ERR group %d not found", groupID))
		return
	}
	m.mu.RUnlock()
	poolConn, err := m.pool.Get(targetAddr)
	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR failed to get connection to group %d leader at %s: %v", groupID, targetAddr, err))
		return
	}
	_, err = poolConn.Write(cmd.Raw)
	if err != nil {
		_ = m.pool.Remove(poolConn)
		conn.WriteError(fmt.Sprintf("ERR failed to send command to group %d leader at %s: %v", groupID, targetAddr, err))
		return
	}
	allocBuf := allocator.Acquire(4096)
	defer allocator.Release(allocBuf)
	buffer := allocBuf.Buffer
	for {
		n, err := poolConn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			_ = m.pool.Remove(poolConn)
			conn.WriteError(fmt.Sprintf("ERR failed to read response from group %d leader at %s: %v", groupID, targetAddr, err))
			return
		}
		_, err = conn.NetConn().Write(buffer[:n])
		if err != nil {
			_ = m.pool.Remove(poolConn)
			conn.WriteError(fmt.Sprintf("ERR failed to write response: %v", err))
			return
		}
		if n < len(buffer) {
			break
		}
	}
	_ = m.pool.Put(poolConn)
}

func (m *clusterMgr) LocalPropose(ctx context.Context, groupID ibabuza.RaftGroupID, log []byte) babuza.ProposedResult {
	return m.store.Propose(ctx, groupID, babuza.ClientSession{}, log)
}

func (m *clusterMgr) LocalQuery(groupID ibabuza.RaftGroupID, key *pb.RedisCommand) (any, error) {
	return m.store.Query(groupID, key)
}
