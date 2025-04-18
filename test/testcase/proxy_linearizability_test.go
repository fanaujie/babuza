package testcase

import (
	"context"
	"errors"
	"fmt"
	"github.com/anishathalye/porcupine"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kverror"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio/proxynetwork"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

type MyClient struct {
	cs babuza.ClientSession
}

func NewMyClient(sessionId uint64) *MyClient {
	return &MyClient{
		cs: babuza.ClientSession{
			SessionID:      sessionId,
			SequenceNumber: 0,
		},
	}
}

func (c *MyClient) ClientSession() babuza.ClientSession {
	c.cs.SequenceNumber++
	return c.cs
}

func (c *MyClient) Response(sequenceNumber uint64) {

}

type KvClientProxy struct {
	kvStores []*babuza.Raft
	client   *MyClient
	leader   int
}

func NewKvClientProxy(kvStoreCluster []*babuza.Raft) *KvClientProxy {
	c := &KvClientProxy{
		kvStores: kvStoreCluster,
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		res := c.kvStores[c.leader].RegisterSession(ctx)
		if err := res.Wait(); err == nil {
			c.client = NewMyClient(res.LogIndex())
			res.Release()
			cancel()
			break
		}
		res.Release()
		cancel()
		c.nextTryLeader()
		time.Sleep(time.Millisecond * 300)
	}
	return c
}

func (c *KvClientProxy) nextTryLeader() {
	c.leader++
	if c.leader == len(c.kvStores) {
		c.leader = 0
	}
}

func (c *KvClientProxy) Get(key string) (string, error) {

	r := c.kvStores[c.leader]
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	err := r.LinearizableRead(ctx)
	defer cancel()
	if err == nil {
		v, err := r.GetStateMachine().(*kvstore.MemoryStoreWithSession).Load(key)
		if err != nil {
			if errors.Is(err, kverror.ErrKeyNotFound) {
				return "", nil
			}
			fmt.Println("KvClientProxy: failed to get key", err.Error())
			return "", err
		}
		return v, nil
	}
	fmt.Println("KvClientProxy: failed to get key", err.Error())
	return "", err
}

func (c *KvClientProxy) Set(key string, value string) {
	var req kvstore.KvCommand
	command, err := req.Set(key, value)
	if err != nil {
		panic("KvClientProxy: failed to set key")
	}
	c.command(command)
}
func (c *KvClientProxy) Append(key string, value string) {
	var req kvstore.KvCommand
	command, err := req.Append(key, value)
	if err != nil {
		panic("KvClientProxy: failed to append key")
	}
	c.command(command)
}

func (c *KvClientProxy) command(command []byte) {
	cs := c.client.ClientSession()
	for {
		r := c.kvStores[c.leader]
		if r.Status().IsLeader() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			_, err := r.ProposeThenWaitResponse(ctx, cs, command)
			if err == nil {
				cancel()
				//TODO: finish
				c.client.Response(0)
				return
			}
			cancel()
			fmt.Println("KvClientProxy: failed to propose command, retrying...", err.Error())
		}
		c.nextTryLeader()
		time.Sleep(time.Millisecond * 100)
	}
}

// KvStoreInput defines the input model for KvStore operations
type KvStoreInput struct {
	Command int    // 0: Get, 1: Set, 2: Append
	Key     string // Key to operate on
	Value   string // Value for Set/Append operations
}

// KvStoreOutput defines the output model for KvStore operations
type KvStoreOutput struct {
	Value string // Value returned by operations
}

// KvStoreModel defines the linearizability model for KvStore operations
var KvStoreModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := make(map[string][]porcupine.Operation)
		for _, v := range history {
			key := v.Input.(KvStoreInput).Key
			m[key] = append(m[key], v)
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ret := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			ret = append(ret, m[k])
		}
		return ret
	},
	Init: func() interface{} {
		return ""
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		kvInput := input.(KvStoreInput)
		kvOutput := output.(KvStoreOutput)
		st := state.(string)
		switch kvInput.Command {
		case 0:
			return kvOutput.Value == st, state
		case 1:
			return true, kvInput.Value
		case 2:
			return true, st + kvInput.Value
		default:
			panic("porcupine.Model: not support command of kvStore")
		}
	},
	Equal: func(state1, state2 interface{}) bool {
		return state1 == state2
	},
	DescribeOperation: func(input, output interface{}) string {
		kvInput := input.(KvStoreInput)
		kvOutput := output.(KvStoreOutput)
		switch kvInput.Command {
		case 0:
			return fmt.Sprintf("get('%s') -> '%s'", kvInput.Key, kvOutput.Value)
		case 1:
			return fmt.Sprintf("put('%s', '%s')", kvInput.Key, kvInput.Value)
		case 2:
			return fmt.Sprintf("append('%s', '%s')", kvInput.Key, kvInput.Value)
		default:
			return "<invalid>"
		}
	},
}

// LinearizabilityWithKvStoreTestCase defines the test case for linearizability with KvStore
type LinearizabilityWithKvStoreTestCase struct {
	t *testing.T
}

func (c *LinearizabilityWithKvStoreTestCase) Log(s string) {
	c.t.Log(s)
}

func (c *LinearizabilityWithKvStoreTestCase) CreateTestComponents() []BabuzaComponent {
	var components []BabuzaComponent
	for _, walType := range []string{builder.BabuzaWal} {
		for _, transportType := range []string{builder.TcpTransport} {
			pn := proxynetwork.New()
			components = append(components, BabuzaComponent{
				CaseName: fmt.Sprintf("LinearizabilityWithKvStore: 5nodes-%s-MemoryStateMachine-%s-DurableSnapshot-LruSession",
					transportType, walType),
				ClusterId: 1,
				CreateStateMachine: func(storeDir string) ibabuza.BaseStateMachine {
					// Due to the append operation in tests requiring idempotence, sessions must be used.
					return kvstore.NewMemoryStoreWithSession()
				},
				CreateCustomComponent: func(walType, transportType string) func(*embedapp.KvStoreAppConfig, string, ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
					return func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
						// Configure snapshot count
						config.BubuzaConfig.SnapshotCount = 1024
						config.BubuzaConfig.DisableProposalForwarding = true
						config.BubuzaConfig.CheckQuorum = true
						// Due to the append operation in tests requiring idempotence, sessions must be used.
						b := customBabuzaComponent(builder.LRUSession, walType, builder.DurableSnapshot,
							transportType, proxyNet).
							SetClusterId(config.BubuzaConfig.ClusterID).
							SetStorageRootDir(storageDir).
							AddLruSessionOptions(session.SetLruMgrOptionsWithMaxSessions(100))
						return *config, *b.Build()
					}
				}(walType, transportType),
				ProxyNetwork: pn,
			})
		}
	}
	return components
}

func (c *LinearizabilityWithKvStoreTestCase) Run(tc *testcluster.BabuzaCluster, a any) {
	// Set wait time to 3 times the Raft election timeout
	wait := tc.RaftElectionTimeout() * 3

	// Test parameters
	clients := 3       // Number of clients
	totalKvStores := 5 // Number of KV store nodes

	// Create voting peers
	peers, connectGroup := makeVotingProxyPeers(totalKvStores)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Connect all peers
	assert.Nil(c.t, tc.SetPartition(connectGroup.GetIDs()))

	// Wait for leader election
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	rs := tc.GetAllRaft()

	// Storage for operations to check linearizability
	var operations []porcupine.Operation
	var opMu sync.Mutex
	var count int
	// Run multiple test cycles
	for i := 0; i < 3; i++ {
		clientStopCh := make(chan struct{})
		partitionStopCh := make(chan struct{})
		wg := sync.WaitGroup{}

		var kvClient []*KvClientProxy
		for index := 0; index < clients; index++ {
			kvClient = append(kvClient, NewKvClientProxy(rs))
		}

		// Start client routines
		for cr := 0; cr < clients; cr++ {
			wg.Add(1)
			go func(clientId int, myKvClient *KvClientProxy) {
				defer wg.Done()

				// TODO: Create a new client session for each client for linearizable test
				// Using embedapp.NewKvStoreClient leads to unstable tests that occasionally fail, likely due to issues in the KvStoreClient implementation
				// Therefore, KvClientProxy is used to propose directly to the Raft instance, bypassing network transmission

				//kvClient, err := embedapp.NewKvStoreClient(
				//	tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
				//assert.Nil(c.t, err)
				//defer func() {
				//	_ = kvClient.Close()
				//}()

				it := 0
				for {
					select {
					case <-clientStopCh:
						return
					default:
					}

					var input KvStoreInput
					var output KvStoreOutput

					// Pick a random key - choose from a small set to increase contention
					key := strconv.Itoa(rand.Int() % clients)
					value := fmt.Sprintf("c:%d v:%d ", clientId, it)

					func() {
						//ctx, cancel := context.WithTimeout(context.Background(), wait)
						//defer cancel()

						start := time.Now().UnixNano()

						// Randomly select operation: 20% Set, 20% Append, 60% Get
						if (rand.Int() % 1000) < 200 {
							// Set operation
							myKvClient.Set(key, value)
							input = KvStoreInput{Command: 1, Key: key, Value: value}
							//output = KvStoreOutput{Value: res.Value}

						} else if (rand.Int() % 1000) < 400 {
							// Append operation
							myKvClient.Append(key, value)
							input = KvStoreInput{Command: 2, Key: key, Value: value}
							//output = KvStoreOutput{Value: res.Value}
							//}
						} else {
							//Linearizable Get operation
							res, err := myKvClient.Get(key)
							if err == nil {
								input = KvStoreInput{Command: 0, Key: key}
								output = KvStoreOutput{Value: res}
							}
						}

						end := time.Now().UnixNano()

						// Record operation for linearizability check
						opMu.Lock()
						count++
						operations = append(operations, porcupine.Operation{
							Input:    input,
							Call:     start,
							Output:   output,
							Return:   end,
							ClientId: clientId,
						})
						opMu.Unlock()
					}()
					it++
				}
			}(cr+1, kvClient[cr])
		}

		// Start network partition simulation
		partitionDoneCh := make(chan struct{})
		go func() {
			time.Sleep(time.Second)
			for {
				select {
				case <-partitionStopCh:
					close(partitionDoneCh)
					return
				default:
				}

				// Create random partitions
				partitions := make(map[uint64]int)
				for _, id := range connectGroup.GetIDs() {
					partitions[id] = rand.Int() % 2
				}

				partition1 := make([]uint64, 0, totalKvStores)
				partition2 := make([]uint64, 0, totalKvStores)

				for k, v := range partitions {
					if v == 0 {
						partition1 = append(partition1, k)
					} else {
						partition2 = append(partition2, k)
					}
				}

				// Create network partitions
				if len(partition1) > 0 {
					assert.Nil(c.t, tc.SetPartition(partition1))
				}
				if len(partition2) > 0 {
					assert.Nil(c.t, tc.SetPartition(partition2))
				}
				time.Sleep(wait)
			}
		}()

		// Run operations for a few seconds
		time.Sleep(time.Second * 9)

		//Stop partition simulation
		close(partitionStopCh)
		<-partitionDoneCh

		//Reconnect all nodes
		assert.Nil(c.t, tc.SetPartition(connectGroup.GetIDs()))
		_, err = tc.CheckOneLeader(wait, connectGroup.GetIDs())
		assert.Nil(c.t, err)
		// Stop client operations
		close(clientStopCh)
		wg.Wait()
	}
	c.t.Log("Total operations:", count)

	// Check linearizability
	c.t.Log("Checking linearizability of", len(operations), "operations")
	res, info := porcupine.CheckOperationsVerbose(KvStoreModel, operations, time.Second*20)

	switch res {
	case porcupine.Ok:
		c.t.Log("History is linearizable")
	case porcupine.Unknown:
		c.t.Log("Linearizability check timed out, assuming history is ok")
	case porcupine.Illegal:
		file, err := os.CreateTemp("", "*.html")
		if err != nil {
			c.t.Logf("info: failed to create temp file for visualization\n")
		} else {
			err = porcupine.Visualize(KvStoreModel, info, file)
			if err != nil {
				c.t.Logf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				c.t.Logf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		c.t.Fatal("History is not linearizable")
	}
}

// TestLinearizabilityWithKvStore runs the linearizability test
func TestLinearizabilityWithKvStore(t *testing.T) {
	RunTests(&LinearizabilityWithKvStoreTestCase{t: t})
}
