package testcluster

import (
	"archive/tar"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/raftnode"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/pkg/snapshot"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal"
	babuza "github.com/fanaujie/babuza/raft"
	"go.uber.org/zap/zapcore"
	"io"
	"os"
	"path/filepath"
	"time"
)

type CreateSessionMgr func(logger ibabuza.Logger) ibabuza.SessionManager
type CreateStateMachine func(stateMachineDir string) ibabuza.BaseStateMachine

func makeVotingPeers(totalPeers int) ([]Peer, *ConnectedGroup) {
	var peers []Peer
	var peerIDs []uint64
	for i := 0; i < totalPeers; i++ {
		peerId := uint64(i + 1)
		peerIDs = append(peerIDs, peerId)
		peers = append(peers, &BabuzaPeer{
			Id:                  peerId,
			RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", 14200+peerId),
			ProxyListenAddr:     fmt.Sprintf("127.0.0.1:%d", 24200+peerId),
			AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", 10000+peerId)},
		})
	}
	return peers, NewConnectedGroup(peerIDs)
}
func makeSinglePeer(peerId uint64, isLearner bool) Peer {
	return &BabuzaPeer{
		Id:                  peerId,
		RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", 14200+peerId),
		ProxyListenAddr:     fmt.Sprintf("127.0.0.1:%d", 24200+peerId),
		AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", 10000+peerId)},
		IsLearner:           isLearner,
	}
}
func extractTarToDir(tr *tar.Reader, destDir string) ([]string, error) {
	// Slice to store all extracted file paths
	extractedPaths := make([]string, 0)

	extractTarFile := func(destPath string, reader io.Reader, header *tar.Header) error {
		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("creating parent directory for %s: %w", destPath, err)
		}

		// Create the file
		file, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
		if err != nil {
			return fmt.Errorf("creating file %s: %w", destPath, err)
		}
		defer file.Close()

		// Copy contents
		if _, err = io.Copy(file, reader); err != nil {
			return fmt.Errorf("writing to file %s: %w", destPath, err)
		}
		return nil
	}

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading tar header: %w", err)
		}

		// Create full destination path
		destPath := filepath.Join(destDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err = os.MkdirAll(destPath, 0755); err != nil {
				return nil, fmt.Errorf("creating directory %s: %w", destPath, err)
			}
			extractedPaths = append(extractedPaths, destPath)
		case tar.TypeReg:
			if err = extractTarFile(destPath, tr, header); err != nil {
				return nil, err
			}
			extractedPaths = append(extractedPaths, destPath)
		}
	}
	return extractedPaths, nil
}

type babuzaDirectory struct {
	stateMachineDir string
	walDir          string
	snapshotDir     string
}

func createDirectories(storageDir string) (*babuzaDirectory, error) {
	dirs := &babuzaDirectory{
		stateMachineDir: filepath.Join(storageDir, "stateMachine"),
		walDir:          filepath.Join(storageDir, "wal"),
		snapshotDir:     filepath.Join(storageDir, "snapshot"),
	}

	if !fileutil.Exist(dirs.stateMachineDir) {
		if err := fileutil.CreateDirAndTouch(dirs.stateMachineDir); err != nil {
			return nil, err
		}
	}
	if !fileutil.Exist(dirs.walDir) {
		if err := fileutil.CreateDirAndTouch(dirs.walDir); err != nil {
			return nil, err
		}
	}
	if !fileutil.Exist(dirs.snapshotDir) {
		if err := fileutil.CreateDirAndTouch(dirs.snapshotDir); err != nil {
			return nil, err
		}
	}

	return dirs, nil
}

func defaultBootstrapBuilder(babuzaConfig *babuza.BabuzaConfig, dirs *babuzaDirectory, votingPeersCfg *babuza.VotingPeersConfiguration,
	testNetwork ibabuza.ProxyNetwork, babuzaLogger ibabuza.Logger) (*babuza.BootstrapBuilder, error) {
	bootstrapBuilder := babuza.NewBootstrapBuilder()
	bootstrapBuilder.SetConfig(babuzaConfig)
	bootstrapBuilder.SetPeersConfig(votingPeersCfg)
	bootstrapBuilder.SetCluster(cluster.NewCluster(babuzaLogger))
	bootstrapBuilder.SetRaftNode(&raftnode.EtcdRaftNode{})
	bootstrapBuilder.SetSessionManager(session.NewNoOpManager(babuzaLogger))
	bootstrapBuilder.SetSnapshotManager(snapshot.NewDurableSnapshotManager(dirs.snapshotDir, babuzaLogger))
	bootstrapBuilder.SetWalManager(babuzawal.NewWalManager(dirs.walDir, babuzaLogger))
	bootstrapBuilder.SetTransport(transport.New(
		babuzaConfig.ClusterId,
		transport.NewPeerManager(), limiter.NewNoResourceLimiter(),
		limiter.NewNoOpRateLimiter(), breaker.NewNoOpBreaker(),
		protocol.NewTcp(networkio.NewTcpPhysicalIO(), babuzaLogger), babuzaLogger))
	bootstrapBuilder.SetLogger(babuzaLogger)
	return bootstrapBuilder, nil
}

//
//func peerConfigExists(ctx context.Context, c *client.KvStoreClient, peer Peer) error {
//	for {
//		select {
//		case <-ctx.Done():
//			return ctx.Err()
//		case <-time.After(time.Second):
//			ctx2, cancel := context.WithTimeout(context.Background(), time.Second)
//			clusterCfg, err := c.GetClusterConfiguration(ctx2)
//			cancel()
//			if err != nil {
//				return err
//			}
//			for _, p := range clusterCfg.Peers {
//				if p.Id == peer.Id && p.IsLearner == p.IsLearner && p.RaftListenAddr == peer.ProxyListenAddr {
//					return nil
//				}
//			}
//		}
//	}
//}

func runFuncWithContextTimeout(timeout time.Duration, run func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return run(ctx)
}

func createEmbeddedAppClient(allPeersServiceAddresses map[uint64][]string, session client.ISession) (*client.KvStoreClient, error) {
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

func createDefaultLogger() ibabuza.Logger {
	var zapLogger = logger.NewZapLogger(
		zapcore.DebugLevel, []string{"stdout"}, "")
	return logger.NewRaftLogger(zapLogger.Sugar())
}

func newKvOperationOrderMap(reader io.Reader) (*kvstore.KvOperationOrderMap, error) {
	buf := make([]byte, 8)
	store := kvstore.NewKvOperationOrderMap()

	var batchKv kvstore.BatchKVPair
	for {
		if _, err := io.ReadFull(reader, buf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		batchKvSize := binary.LittleEndian.Uint64(buf)
		data := make([]byte, batchKvSize)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &batchKv); err != nil {
			return nil, err
		}
		for _, pair := range batchKv {
			fmt.Println(pair.Key)
			fmt.Println(pair.Value)
			store.Set(string(pair.Key), string(pair.Value))
		}
		batchKv = batchKv[:0]
	}
	return store, nil
}
