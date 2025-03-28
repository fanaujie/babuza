package testcase

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/test/testcluster"
	"path/filepath"
	"time"
)

type babuzaDirectory struct {
	stateMachineDir string
	walDir          string
	snapshotDir     string
}

func makeVotingPeers(totalPeers int) ([]testcluster.BabuzaPeer, *testcluster.ConnectedGroup) {
	var peers []testcluster.BabuzaPeer
	for i := 0; i < totalPeers; i++ {
		peerId := uint64(i + 1)
		peers = append(peers, testcluster.BabuzaPeer{
			Id:                  peerId,
			RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", 14200+peerId),
			ProxyListenAddr:     fmt.Sprintf("127.0.0.1:%d", 24200+peerId),
			AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", 10000+peerId)},
		})
	}
	return peers, testcluster.NewConnectedGroup(peers)
}
func makeSinglePeer(peerId uint64, isLearner bool) testcluster.BabuzaPeer {
	return testcluster.BabuzaPeer{
		Id:                  peerId,
		RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", 14200+peerId),
		ProxyListenAddr:     fmt.Sprintf("127.0.0.1:%d", 24200+peerId),
		AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", 10000+peerId)},
		IsLearner:           isLearner,
	}
}

func createStorageDirectories(storageDir string) (*babuzaDirectory, error) {
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

func customBabuzaComponent(sessionType, walType, snapshotType, transport string,
	proxyNet ibabuza.ProxyNetwork) *builder.BabuzaComponentBuilder {
	b := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
		SessionType:   sessionType,
		TransportType: transport,
		WalType:       walType,
		SnapshotType:  snapshotType,
	})
	if proxyNet != nil {
		b.SetTransportTcpNetwork(proxyNet.(tcp.NetworkIO))
	}
	return b
}

func runWithCtxTimeout(timeout time.Duration, run func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return run(ctx)
}
